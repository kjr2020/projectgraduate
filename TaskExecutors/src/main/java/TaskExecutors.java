import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Time;
import java.util.concurrent.TimeoutException;

public class TaskExecutors {
    private static final Logger LOG = LoggerFactory.getLogger(TaskExecutors.class);
    private int offset;
    private static ConnectionFactory factory;
    protected static Connection connection;
    protected static Channel channel;
    static final String USER_NAME = "master";
    static final String PASSWORD = "1234";
    static final String VIRTUAL_HOST = "/";
    static final String HOST = "120.70.10.100";
    static final String QUEUE_NAME = "ligand-queue";
    static final int PORT = 5672;
    static final int QOS = 1;
    private String consumerTag;
    private String executorManageFile = "numberOfExecutors";

    BufferedWriter bw;
    private Process ps;
    private BufferedReader br;
    private int maxNumberOfExecutors;

    public TaskExecutors(int offset){
        this.offset = offset;
        this.consumerTag = "Consumer" + offset;
    }

    public static void main(String args[]){
        try{
            TaskExecutors taskExecutors = new TaskExecutors(Integer.parseInt(args[0]));
            taskExecutors.connectQueue();
            taskExecutors.consumeTasks(taskExecutors.offset);
        }catch (Exception e){
            LOG.warn("Create TaskExecutors Failed..");
        }
    }

    public void connectQueue(){
        try {
            factory = new ConnectionFactory();
            factory.setUsername(USER_NAME);
            factory.setPassword(PASSWORD);
            factory.setVirtualHost(VIRTUAL_HOST);
            factory.setPort(PORT);
            factory.setHost(HOST);
            connection = factory.newConnection();
            channel = connection.createChannel();
        } catch (IOException | TimeoutException e){
            LOG.warn("Queue Connection Failed..");
        }
    }

    public void consumeTasks(int offset){

        try {
            channel.basicQos(QOS, true);
        }catch (IOException e){
            LOG.warn("Qos Setting Failed..");
        }
        try {
            channel.basicConsume(QUEUE_NAME, false, consumerTag, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    long deliveryTag = envelope.getDeliveryTag();

                    String message = new String(body, "UTF-8");
                    String messageArr[] = message.split(", ");
                    String ligand = messageArr[0];
                    String pockets = messageArr[1];

                    try{
                        ps = Runtime.getRuntime().exec("./autodock_vina.sh ligand/" + ligand + " pockets/" + pockets + " scPDB_coordinates.tsv");
                        ps.waitFor();
                        ps.destroy();

                        LOG.info("Executor Num: " + offset + ", Ligand: " + ligand + ", Pockets: " + pockets + " End..");
                    }catch (InterruptedException e){
                        LOG.warn("Create Process Failed..");
                    }

                    channel.basicAck(deliveryTag, false);
                    try {
                        br = new BufferedReader(new FileReader(executorManageFile));
                        maxNumberOfExecutors = Integer.parseInt(br.readLine());
                        br.close();
                    }catch (IOException e){
                        LOG.warn("Read Number Failed..");
                    }

                    if(offset > maxNumberOfExecutors || channel.messageCount(QUEUE_NAME) == 0){
                        try {
                            channel.close();
                            connection.close();
                            LOG.info(consumerTag + " End Process..");
                        }catch (TimeoutException e){
                            LOG.warn("Channel Close Failed..");
                        }
                    }
                }
            });
        } catch(IOException e){
            LOG.warn("Consume Failed..");
        }
    }
}
