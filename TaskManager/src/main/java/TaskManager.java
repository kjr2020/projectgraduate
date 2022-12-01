import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;;import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class TaskManager {
    private static final Logger LOG = LoggerFactory.getLogger(TaskManager.class);
    private String podName;
    private String executeFile;
    private String numberOfExecutors;
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


    public TaskManager(String args[]) throws Exception {
        this.podName = args[0];
        this.executeFile = args[1];
        this.numberOfExecutors = args[2];
        Process ps = Runtime.getRuntime().exec("./init.sh exWorkload");
        ps.waitFor();
        ps.destroy();
//        createCompareQueue();
//        createExecuteTimeQueue();
        init(podName, numberOfExecutors);
    }

    public static void main(String args[]){
        TaskManager taskManager = null;
        try{
            taskManager = new TaskManager(args);
        } catch (Exception e){
            LOG.info("Can't create TaskManager\nCheck args\n[1]yamlFile\n[2]executeFile\n[3]numberOfExecutors");
            e.printStackTrace();
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
            LOG.info("Connect Success");
        }catch (IOException | TimeoutException e){
            LOG.info("RBMQServer connection failed..");
        }
    }

    public void init(String podName, String numberOfExecutors){

        //connectRBMQServer();

        Process process;
        try {
            process = Runtime.getRuntime().exec("kubectl apply -f " + podName +".yaml");
            LOG.info("Create Task Container");
            Thread.sleep(5000);
            process.destroy();
        } catch (Exception e) {
            LOG.info("Fail to apply Yaml File.");
        }
        try{
            process = Runtime.getRuntime().exec("kubectl exec " + podName + " -- java -jar ExecutorManager.jar " + numberOfExecutors);
            LOG.info("Executor Start");
            Thread.sleep(5000);
            process.destroy();
        } catch (Exception e){
            LOG.info("Fail to Start Execute File.");
        }

        try{
            process = Runtime.getRuntime().exec("kubectl apply -f testContainer.yaml");
            LOG.info("Create Test Container");
            Thread.sleep(5000);
            process.destroy();
        } catch (IOException | InterruptedException e){
            LOG.info("Test Container Create Failed..");
        }

        try{
            process = Runtime.getRuntime().exec("kubectl exec rabbitmq-test -- java -jar TestContainer.jar");
            LOG.info("Test Container Start");
            Thread.sleep(5000);
            process.destroy();
        } catch( Exception e ){
            LOG.info("Test Container Create Failed..");
        }
    }

//    public void createCompareQueue(){
//        try{
//            channel.queueDeclare(compareQueueName, true, false, false, null);
//        }catch (IOException e){
//            LOG.warn("Compare Queue Declare Failed..");
//        }
//    }
//
//    public void createExecuteTimeQueue(){
//        try{
//            channel.queueDeclare(executeTimeQueue, true, false, false, null);
//        }catch (IOException e){
//            LOG.warn("Execute Time Queue Declare Failed..");
//        }
//    }

//    public void createTaskQueue(){
//        try{
//            channel.queueDeclare(taskQueue, true, false, false, null);
//        }catch (IOException e){
//            LOG.warn("Task Queue Declare Failed..");
//        }
//    }
}
