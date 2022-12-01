import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class ComparableProducer {

    private static final Logger LOG = LoggerFactory.getLogger(ComparableProducer.class);
    private String taskQueue = "test-queue";

    static final String USER_NAME = "master";
    static final String PASSWORD = "1234";
    static final String VIRTUAL_HOST = "/";
    static final int PORT = 5672;
    static Channel channel;
    static String HOST = "120.70.10.100";
    private static Connection connection;
    private static ConnectionFactory factory;

    public ComparableProducer(){
        connectRBMQServer();
    }

    public static void main(String args[]){
        ComparableProducer comparableProducer = new ComparableProducer();
        comparableProducer.startProducing(args[0]);
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

    public void startProducing(String workloadFileName){
        try {
            channel.queueDeclare(taskQueue, true, false, false, null);
        } catch (IOException e){
            LOG.warn("Queue Declare Failed..");
        }
        try{
            BufferedReader br = new BufferedReader(new FileReader(workloadFileName));
            String line;
            while (br.ready()){
                line = br.readLine();
                channel.basicPublish("", taskQueue, null, line.getBytes(StandardCharsets.UTF_8));
            }
            try {
                channel.close();
                connection.close();
                br.close();
            }catch (IOException | TimeoutException e){
                LOG.warn("Close Failed..");
            }
        } catch (IOException e){
            LOG.warn("Message Publish Failed..");
        }
    }
}
