import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TaskManager {
    private static final Logger LOG = LoggerFactory.getLogger(TaskManager.class);
    private String yamlFile;
    private String executeFile;
    private static ConnectionFactory factory;
    protected static Connection connection;
    protected static Channel channel;
    static final String USER_NAME = "master";
    static final String PASSWORD = "1234";
    static final String VIRTUAL_HOST = "/";
    static final String HOST = "120.70.10.100";
    static final String QUEUE_NAME = "ligand-test";
    static final String EX_QUEUE_NAME = "executeTime-queue";
    static final int PORT = 5672;
    private int testContainerTime;
    private int executeContainerTime;
    private int numberOfExecutors;
    private int prevNumberOfExecutors;

    public TaskManager(String args[]) throws Exception {
        this.yamlFile = args[0];
        this.executeFile = args[1];
        this.numberOfExecutors = Integer.parseInt(args[2]);
        this.prevNumberOfExecutors = 0;
        createContainer(yamlFile, numberOfExecutors);
        connectQueueChannel();
    }

    public static void main(String args[]){
        TaskManager taskManager;
        try{
            taskManager = new TaskManager(args);
        } catch (Exception e){
            LOG.info("Can't create TaskManager\nCheck args\n[1]yamlFile\n[2]executeFile\n[3]numberOfExecutors");
            e.printStackTrace();
        }
    }

    public void createContainer(String containerName, int numberOfExecutors){
        Process process;
        try {
            process = Runtime.getRuntime().exec("kubectl apply -f " + containerName);
            process.waitFor();
            process.destroy();

            process = Runtime.getRuntime().exec("kubectl exec " + containerName + " /bin/bash");
            Thread.sleep(5000);
            process.destroy();
        } catch (Exception e){
            LOG.info("Fail to create Container.");
        }
    }

    public void changeScale(int executeNum){

    }

    public void getExecuteTime(){
        try {
            channel.basicConsume(EX_QUEUE_NAME, false, "consumerTag", new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    long deliveryTag = envelope.getDeliveryTag();
                    if(channel.messageCount(EX_QUEUE_NAME) <= 1) {
                        return;
                    }

                    String originMessage = body.toString();
                    String[] message = originMessage.split(", ");

                    if(message[0].equals("Executor")){
                        testContainerTime = Integer.parseInt(message[1]);
                    }else if(message[0].equals("Test")){
                        executeContainerTime = Integer.parseInt(message[1]);
                    } else {
                        LOG.info("Message Detection Error");
                        return;
                    }

                    overProvisioning(testContainerTime, executeContainerTime);

                    channel.basicAck(deliveryTag, false);
                }
            });
        } catch(IOException e){
            LOG.info("Execute time queue connection fail...");
        }
    }

    public void overProvisioning(int testContainerTime, int executeContainerTime){
        if((testContainerTime * numberOfExecutors) < executeContainerTime){
            prevNumberOfExecutors = numberOfExecutors;
            numberOfExecutors *= 2;
            changeScale((numberOfExecutors));
        } else{
            numberOfExecutors = ((numberOfExecutors - prevNumberOfExecutors)/2) + prevNumberOfExecutors;
            changeScale(numberOfExecutors);
        }
    }

    public void connectQueueChannel(){
        try {
            factory = new ConnectionFactory();
            factory.setUsername(USER_NAME);
            factory.setPassword(PASSWORD);
            factory.setVirtualHost(VIRTUAL_HOST);
            factory.setHost(HOST);
            factory.setPort(PORT);

            connection = factory.newConnection();
            channel = connection.createChannel();
        }catch (Exception e){
            LOG.info("Can't connect Queue");
        }
    }

    public void checkExecuteTime(){
        try {
            while (true){
                Thread.sleep(5000);
                getExecuteTime();
                if(channel.consumerCount(QUEUE_NAME) == 0){
                    connection.close();
                    return;
                }
            }
        }catch (Exception e){
            LOG.info("Thread sleep failed..");
        }
    }
}
