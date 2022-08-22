import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutorManager {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutorManager.class);
    private int numberOfExecutors;

    ExecutorManager(int numberOfExecutors){
        this.numberOfExecutors = numberOfExecutors;
    }

    static void main(String args[]){
        try {
            ExecutorManager executorManager = new ExecutorManager(Integer.parseInt(args[0]));
        } catch (Exception e){
            LOG.info("Can't create ExecutorManager\nCheck args\n[1]Number of Executors.");
        }
    }

    private void exitExecutors(){

    }

    private void createExecutors(){

    }
}
