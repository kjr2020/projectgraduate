import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class TestContainer {
    private static final Logger LOG = LoggerFactory.getLogger(TestContainer.class);
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
    private String consumerTag = "TestContainer";
    private Process ps;
    private long startTime;

    public TestContainer(){
        connectRBMQServer();
        createExecuteTimeQueue(executeTimeQueue);
    }

    public static void main(String args[]){
        TestContainer testContainer = null;
        try{
            testContainer = new TestContainer();
        }catch (Exception e){
            LOG.info("Can't create TestContainer");
            e.printStackTrace();
        }

        try{
            testContainer.consumeCompareQueue();
        }catch (Exception e){
            LOG.info("Testing Failed..");
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
            LOG.info("RBMQServer connection failed..");
        }
    }

    public void consumeCompareQueue(){
        try {
            channel.basicQos(1, true);
            channel.basicConsume(compareQueueName, false, consumerTag, new DefaultConsumer(channel){

                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {

                    long deliveryTag = envelope.getDeliveryTag();

                    String message = new String(body, "UTF-8");
                    String messageArr[] = message.split(", ");
                    String ligand = messageArr[0];
                    String pockets = messageArr[1];
                    startTime = System.currentTimeMillis();
                    try {
                        ps = Runtime.getRuntime().exec("./autodock_vina.sh ligand/" + ligand + " pockets/" + pockets + " scPDB_coordinates.tsv");
                        ps.waitFor();
                        ps.destroy();
                    } catch (InterruptedException e){
                        LOG.info("Create process failed..");
                        e.printStackTrace();
                    }
                    channel.basicAck(deliveryTag, false);

                    connectExecuteTimeQueue(message, (System.currentTimeMillis() - startTime));

                    if(channel.consumerCount(taskQueue) == 0){
                        try {
                            channel.close();
                            connection.close();
                        }catch(TimeoutException e){
                            LOG.info("Channel close Failed..");
                        }
                    }
                }

            });
        } catch (IOException e){
            LOG.info("Consume Failed..");
            e.printStackTrace();
        }
    }

    public void connectExecuteTimeQueue(String message, long executeTime){

        String publishedMessage = "Test, " + message + ", " + executeTime;

        try {
            channel.basicPublish("", executeTimeQueue, null, publishedMessage.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e){
            LOG.info("executeTimeQueue Publish Failed..");
        }
    }

    public void createExecuteTimeQueue(String queueName){
        try {
            channel.queueDeclare(queueName, true, false, false, null);
        }catch (IOException e){
            LOG.info("Create Queue Failed..");
        }
    }
}
