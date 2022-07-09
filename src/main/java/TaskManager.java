import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class TaskManager {
    private static final Logger LOG = LoggerFactory.getLogger(TaskManager.class);
    private String yamlFile;
    private String executeFile;
    private int cpuLimit;
    private int memLimit;
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

    public TaskManager(String args[]) throws Exception {
        this.yamlFile = args[0];
        this.executeFile = args[1];
        this.cpuLimit = Integer.parseInt(args[2]);
        this.memLimit = Integer.parseInt(args[3]);
        connectQueue();
    }

    public static void main(String args[]){
        TaskManager taskManager;
        boolean result = false;
        try{
            taskManager = new TaskManager(args);
            if(!result){
                LOG.info("Can't create TaskManager\n[1]yamlFile\n[2]executeFile\n[3]cpuLimit\n[4]memLimit");
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public void createContainer(String containerName){
        Process process;
        try {
            process = Runtime.getRuntime().exec("kubectl apply -f " + containerName);
            process.waitFor();
            process.destroy();
        } catch (Exception e){
            LOG.info("Fail to create Container.");
            e.printStackTrace();
        }
    }

    public void changeScale(String yamlFile, int cpuLim, int memLim){
        Process process;
        try{
            process = Runtime.getRuntime().exec("kubernetes scale code");
        } catch (Exception e){
            LOG.info("Can't change container resources");
        }
    }

    public void getExecuteTime(){
        try {
            channel.basicConsume(EX_QUEUE_NAME, false, "consumerTag", new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    if(channel.messageCount(EX_QUEUE_NAME) == 0) {
                        try {
                            channel.close();
                        } catch (TimeoutException e){
                            LOG.info("channel close failed...");
                        }
                    }


                }
            });
        } catch(IOException e){
            LOG.info("execute time queue connection fail...");
        }
    }

    public void overProvisioning(){

    }

    public void connectQueue() throws Exception{
        factory = new ConnectionFactory();
        factory.setUsername(USER_NAME);
        factory.setPassword(PASSWORD);
        factory.setVirtualHost(VIRTUAL_HOST);
        factory.setHost(HOST);
        factory.setPort(PORT);

        connection = factory.newConnection();
        channel = connection.createChannel();
        while(true){
            Thread.sleep(5000);
            getExecuteTime();
            if(channel.consumerCount(QUEUE_NAME) == 0){
                connection.close();
                return;
            }
        }
    }
}
