import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class ComparableConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(ComparableConsumer.class);
    private String taskQueue = "test-queue";

    static final String USER_NAME = "master";
    static final String PASSWORD = "1234";
    static final String VIRTUAL_HOST = "/";
    static final int PORT = 5672;
    static Channel channel;
    static String HOST = "120.70.10.100";
    private static Connection connection;
    private static ConnectionFactory factory;
    private String consumerTag = "testconsumer";
    private Process ps;
    private long startTime;
    private String combinedMessage = null;

    public ComparableConsumer(){
        connectRBMQServer();
    }

    public static void main(String args[]){
        ComparableConsumer comparableConsumer = new ComparableConsumer();
        comparableConsumer.startTime = System.currentTimeMillis();
        comparableConsumer.consumeMassage();
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

    public void consumeMassage(){
        try {
            channel.basicQos(1);
        } catch (IOException e){
            LOG.warn("QOS Setting Failed..");
        }
        try {
            channel.basicConsume(taskQueue, false, consumerTag, new DefaultConsumer(channel) {
                int index = 0;
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    long deliveryTag = envelope.getDeliveryTag();

                    String message = new String(body, "UTF-8");
                    if(combinedMessage == null) combinedMessage = message;
                    else combinedMessage = combinedMessage + ", " + message;

                    channel.basicAck(deliveryTag, false);

                    if(++index == 10){
                        try {
                            ps = Runtime.getRuntime().exec("./parallelTest.sh \"" + combinedMessage + "\"");
                            ps.waitFor();
                            ps.destroy();
                        } catch(InterruptedException e){
                            LOG.warn("Process wait Failed..");
                        }
                        combinedMessage = null;
                        index = 0;
                    }

                    if(channel.messageCount(taskQueue) == 0){
                        try {
                            channel.close();
                            connection.close();
                            long makespan = System.currentTimeMillis() - startTime;
                            BufferedWriter bw = new BufferedWriter(new FileWriter("TestResult"));
                            bw.write("Makespan: " + String.valueOf(makespan));
                            bw.flush();
                            bw.close();
                        } catch (IOException | TimeoutException e){
                            LOG.warn("Channel close Failed...");
                        }
                    }
                }
            });
        } catch(IOException e){
            LOG.warn("Consume Failed..");
        }
    }
}
