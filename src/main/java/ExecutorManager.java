import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeoutException;

public class ExecutorManager {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutorManager.class);
    private int numberOfExecutors;

    private static ConnectionFactory factory;
    protected static Connection connection;
    protected static Channel channel;
    static final String USER_NAME = "master";
    static final String PASSWORD = "1234";
    static final String VIRTUAL_HOST = "/";
    static final String HOST = "120.70.10.100";
    static final String QUEUE_NAME = "ligand-queue";
    static final String EX_QUEUE_NAME = "executeTime-queue";
    static final int PORT = 5672;
    private int testContainerTime = 0;
    private int executeContainerTime = 0;
    private int prevNumberOfExecutors;
    private ArrayList<Process> executors;
    private BufferedWriter bw;

    ExecutorManager(int numberOfExecutors){
        this.numberOfExecutors = numberOfExecutors;
        this.prevNumberOfExecutors = 1;
        connectQueueChannel();
    }

    public static void main(String args[]){
        try {
            ExecutorManager executorManager = new ExecutorManager(Integer.parseInt(args[0]));
            executorManager.init(executorManager.numberOfExecutors);
            executorManager.createTestContainer();
            executorManager.getExecuteTime();
        } catch (Exception e){
            LOG.warn("Can't create ExecutorManager Check args\n[1]Number of Executors");
        }
    }

    public void init(int numberOfExecutors){
        try {
            for (int offset = 0; offset < numberOfExecutors; offset++) {
                executors.add(createExecutors(offset));
            }
        }catch (Exception e){
            LOG.warn("Fail to create Executors");
        }
    }

    private Process createExecutors(int executors) {
        try {
            return Runtime.getRuntime().exec("java -jar TaskExecutors.jar " + executors);
        }catch (IOException e){
            LOG.warn("Create Executor Failed..");
            return null;
        }
    }

    public void getExecuteTime(){
        try {
            channel.basicConsume(EX_QUEUE_NAME, false, "ExecutorManager", new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    long deliveryTag = envelope.getDeliveryTag();

                    String originMessage = body.toString();
                    String[] message = originMessage.split(", ");

                    if(message[0].equals("Executor")){
                        testContainerTime = Integer.parseInt(message[3]);
                        LOG.info("ligand: " + message[1] + ", pocket: " + message[2]);
                    }else if(message[0].equals("Test")){
                        executeContainerTime = Integer.parseInt(message[3]);
                        LOG.info("ligand: " + message[1] + ", pocket: " + message[2]);
                    }
                    if(testContainerTime != 0 && executeContainerTime != 0) {
                        overProvisioning(testContainerTime, executeContainerTime);
                        testContainerTime = executeContainerTime = 0;
                        LOG.info("Current Arr Size: " + String.valueOf(executors.size()));
                    }

                    channel.basicAck(deliveryTag, false);

                    if(channel.consumerCount(QUEUE_NAME) == 0){
                        try {
                            channel.close();
                            connection.close();
                        } catch (TimeoutException e){
                            LOG.warn("Close Failed..");
                        }
                    }
                }
            });
        } catch(IOException e){
            LOG.warn("Execute time queue connection fail...");
        }
    }

    public void overProvisioning(int testContainerTime, int executeContainerTime) throws IOException {
        bw = new BufferedWriter(new FileWriter("numberOFExecutors"));
        if((testContainerTime * numberOfExecutors) > executeContainerTime){
            prevNumberOfExecutors = numberOfExecutors;
            numberOfExecutors *= 2;
            bw.write(numberOfExecutors);
            LOG.info("Current Executors: " + numberOfExecutors);
            for(int i = prevNumberOfExecutors ; i == numberOfExecutors ; i++){
                executors.add(createExecutors(i));
            }
        } else{
            numberOfExecutors = ((numberOfExecutors - prevNumberOfExecutors)/2) + prevNumberOfExecutors;
            bw.write(numberOfExecutors);
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
            LOG.warn("Can't connect Queue");
        }
    }

    public void createTestContainer(){
        try {
            Process ps = Runtime.getRuntime().exec("java -jar TestContainer.jar");
        } catch (IOException e){
            LOG.warn("Process Execute Error..");
        }
    }
}
