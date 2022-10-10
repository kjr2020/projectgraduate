import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class FirstExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(FirstExecutor.class);
    private String compareQueueName = "compare-queue";
    private String executeTimeQueue = "executeTime-queue";
    private String taskQueue = "ligand-queue";

    static final String USER_NAME = "master";
    static final String PASSWORD = "1234";
    static final String VIRTUAL_HOST = "/";
    static final int PORT = 5672;
    static Channel channel;
    static String HOST = "120.70.10.100";
    private static Connection connection;
    private static ConnectionFactory factory;
    private String consumerTag = "Consumer0";
    private Process ps;
    private long testCheckTime;
    private long startTime;
    private long endTime = 0;
    private boolean testFlag = false;

    public FirstExecutor(){
        connectRBMQServer();
        createCompareQueue();
        this.testCheckTime = System.currentTimeMillis();
    }

    public static void main(String args[]){
        try{
            FirstExecutor firstExecutor = new FirstExecutor();
            firstExecutor.consumeTask();
        } catch(Exception e){
            LOG.warn("Can't Create Class..");
        }
    }

    public void connectRBMQServer(){
        try {
            factory = new ConnectionFactory();
            factory.setUsername(USER_NAME);
            factory.setPassword(PASSWORD);
            factory.setVirtualHost(VIRTUAL_HOST);
            factory.setHost(HOST);
            factory.setPort(PORT);
            connection = factory.newConnection();
            channel = connection.createChannel();
        }catch (IOException | TimeoutException e){
            LOG.warn("RBMQServer connection failed..");
            e.printStackTrace();
        }
    }

    public void createCompareQueue(){
        try{
            channel.queueDeclare(compareQueueName, true, false, false, null);
        } catch(IOException e){
            LOG.warn("CompareQueue Create Failed..");
        }
    }

    public void publishCompareTask(String message){
        try {
            channel.basicPublish("", compareQueueName, null, message.getBytes(StandardCharsets.UTF_8));
        }catch (IOException e){
            LOG.warn("Compare Task Publish Failed..");
        }
    }

    public void publishExecuteTime(String message, long executeTime){
        String publishedMessage = "Executor, " + message + ", " + executeTime;
        try{
            channel.basicPublish("", executeTimeQueue, null, publishedMessage.getBytes(StandardCharsets.UTF_8));
        }catch (IOException e){
            LOG.warn("Execute Time Queue Publish Failed..");
        }
    }

    public void consumeTask(){
        try {
            channel.basicQos(1, true);
            channel.basicConsume(taskQueue, false, consumerTag, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    long deliveryTag = envelope.getDeliveryTag();

                    String message = new String(body, "UTF-8");
                    String messageArr[] = message.split(", ");
                    String ligand = messageArr[0];
                    String pockets = messageArr[1];

                    if(testFlag == true){
                        publishCompareTask(message);
                    }

                    startTime = System.currentTimeMillis();
                    try{
                        ps = Runtime.getRuntime().exec("./autodock_vina.sh ligand/" + ligand + " pockets/" + pockets + " scPDB_coordinates.tsv");
                        ps.waitFor();
                        ps.destroy();
                        LOG.info("Executor Num: 0, Ligand: " + ligand + ", Pockets: " + pockets + " End..");
                    }catch(InterruptedException e){
                        LOG.warn("Create Process Failed..");
                    }

                    endTime = System.currentTimeMillis();

                    if(testFlag == true){
                        publishExecuteTime(message, (endTime - startTime));
                    }

                    if((endTime - testCheckTime) >= 300000){
                        testCheckTime = System.currentTimeMillis();
                        testFlag = true;
                    } else{
                        testFlag = false;
                    }

                    channel.basicAck(deliveryTag, false);

                    if(channel.messageCount(taskQueue) == 0){
                        try {
                            channel.close();
                            connection.close();
                            LOG.info("Consumer0 End Process..");
                        }catch(TimeoutException e){
                            LOG.warn("Connection close Exception..");
                        }
                    }
                }
            });
        } catch(IOException e){
            LOG.warn("TaskQueue Consume Failed..");
        }
    }
}
