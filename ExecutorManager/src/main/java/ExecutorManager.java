import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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
    static final String CONTAINER_MANAGE_FILE_NAME = "numberOfExecutors";
    static final String RESULT_FILE_NAME= "graduateResult";
    static final int PORT = 5672;
    private long testContainerTime = 0;
    private long executeContainerTime = 0;
    private int prevNumberOfExecutors;
    private ArrayList<Process> executors;
    private BufferedWriter bw;
    private long startTime;
    private static final int MAX_CONTAINER_NUM = 30;
    private ArrayList<String> executorArr = new ArrayList<>();
    private ArrayList<String> testArr = new ArrayList<>();

    ExecutorManager(int numberOfExecutors){
        this.numberOfExecutors = numberOfExecutors;
        this.prevNumberOfExecutors = 1;
        executors = new ArrayList<Process>();
        connectQueueChannel();
    }

    public static void main(String args[]){
        try {
            ExecutorManager executorManager = new ExecutorManager(Integer.parseInt(args[0]));
            executorManager.init(executorManager.numberOfExecutors);
            Thread.sleep(60000);
            executorManager.getExecuteTime();
        } catch (Exception e){
            LOG.warn("Can't create ExecutorManager Check args\n[1]Number of Executors");
        }
    }

    public void init(int numberOfExecutors){
        try {
            bw = new BufferedWriter(new FileWriter(CONTAINER_MANAGE_FILE_NAME));
            bw.write(String.valueOf(numberOfExecutors));
            bw.flush();
            bw.close();
        } catch (IOException e){
            LOG.warn("BufferedWriter open failed..");
        }
        try {
            executors.add(Runtime.getRuntime().exec("java -jar FirstExecutor.jar"));
            System.out.println("FirstExecutor Created");
            for (int offset = 1; offset < numberOfExecutors; offset++) {
                LOG.info("Executor " + offset + " Created..");
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
        startTime = System.currentTimeMillis();
        LOG.info("Start Time: " + startTime);
        try {
//            while(true) {
//                if (channel.messageCount(QUEUE_NAME) == 0) {
//                    channel.close();
//                    connection.close();
//
//                    bw = new BufferedWriter(new FileWriter(RESULT_FILE_NAME));
//                    bw.write("Project Makespan: " + (System.currentTimeMillis() - startTime));
//                    bw.flush();
//                    bw.close();
//                    LOG.info("Done..");
//                    return;
//                } else if (channel.messageCount(EX_QUEUE_NAME) > 0){
                    LOG.info("Consume Start..");
                    channel.basicQos(1);
                    channel.basicConsume(EX_QUEUE_NAME, false, "consumerTag", new DefaultConsumer(channel) {
                        @Override
                        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                            long deliveryTag = envelope.getDeliveryTag();

                            String originMessage = new String(body, "UTF-8");
                            String[] message = originMessage.split(", ");

                            LOG.info("Original Message: " + originMessage);

                            if (message[0].equals("Executor")) {
                                executorArr.add(originMessage);
                                LOG.info("ligand: " + message[1] + ", pocket: " + message[2]);
                            } else if (message[0].equals("Test")) {
                                testArr.add(originMessage);
                                LOG.info("ligand: " + message[1] + ", pocket: " + message[2]);
                            }
                            if (executorArr.size() != 0 && testArr.size() != 0) {
                                overProvisioning();
                                //testContainerTime = executeContainerTime = 0;
                            }

                            LOG.info("Real Process: " + executors.size());
                            channel.basicAck(deliveryTag, false);
                            if(channel.messageCount(QUEUE_NAME) == 0){
                                try{
                                    channel.close();
                                    connection.close();
                                } catch (TimeoutException e){
                                    LOG.warn("Channel close Err");
                                }
                            }
                        }


                    });
//                } else {
//                    Thread.sleep(60000);
//                }
//            }
        } catch (IOException e){
            LOG.warn("consumer count err");
        }
    }

    public void checkEndOfTasks(){
        while(true) {
            try {
                if (channel.messageCount(QUEUE_NAME) == 0) {
                    try {
                        channel.close();
                        connection.close();
                    } catch (TimeoutException e){
                        LOG.warn("Channel Closed Err");
                    }
                    connection.close();
                    bw = new BufferedWriter(new FileWriter(RESULT_FILE_NAME));
                    bw.write("Project Makespan: " + (System.currentTimeMillis() - startTime));
                    bw.flush();
                    bw.close();
                    LOG.info("Done..");
                } else if (executorArr.size() != 0 || testArr.size() != 0) return;
            } catch (IOException e){
                LOG.warn("Message Count Err..");
            }
            try {
                Thread.sleep(30_000);
            }catch (InterruptedException e){
                LOG.warn("Sleep Err");
            }
        }
    }

    public void overProvisioning() throws IOException {
        String[] testContainerMessage, executeContainerMessage;
        long executeContainerTime, testContainerTime;

        executeContainerMessage = executorArr.get(0).split(", ");
        testContainerMessage = testArr.get(0).split(", ");

        if(!executeContainerMessage[1].equals(testContainerMessage[1]) || !executeContainerMessage[2].equals(testContainerMessage[2])) return;

        executeContainerTime = Long.parseLong(executeContainerMessage[3]);
        testContainerTime = Long.parseLong(testContainerMessage[3]);

        executorArr.remove(0);
        testArr.remove(0);

        bw = new BufferedWriter(new FileWriter(CONTAINER_MANAGE_FILE_NAME));
        if((testContainerTime * 1.2) > executeContainerTime){
            if(numberOfExecutors == MAX_CONTAINER_NUM) return;
            prevNumberOfExecutors = numberOfExecutors;
            numberOfExecutors *= 2;
            if(numberOfExecutors > MAX_CONTAINER_NUM) numberOfExecutors = MAX_CONTAINER_NUM;
            bw.write(String.valueOf(numberOfExecutors));
            bw.flush();
            for(int i = prevNumberOfExecutors ; i < numberOfExecutors ; i++){
                executors.add(createExecutors(i));
            }
        } else{
            numberOfExecutors = ((numberOfExecutors - prevNumberOfExecutors)/2) + prevNumberOfExecutors;
            bw.write(String.valueOf(numberOfExecutors));
            bw.flush();
        }
        if(prevNumberOfExecutors == numberOfExecutors) return;
        LOG.info("PrevNumberOfExecutors: " + prevNumberOfExecutors + ", NumberOfExecutors: " + numberOfExecutors);
        bw.close();
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
}
