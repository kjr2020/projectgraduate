import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskManager {
    private static final Logger LOG = LoggerFactory.getLogger(TaskManager.class);
    private String yamlFile;
    private String executeFile;
    private int cpuLimit;
    private int memLimit;

    public TaskManager(String args[]){
        this.yamlFile = args[0];
        this.executeFile = args[1];
        this.cpuLimit = 0;
        this.memLimit = 0;
    }

    public static void main(String args[]){
        TaskManager taskManager;
        boolean result = false;
        try{
            taskManager = new TaskManager(args);
            if(!result){
                LOG.info("Can't create TaskManager");
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public void craeteContainer(String containerName){
        Process process;
        try {
            process = Runtime.getRuntime().exec("kubectl apply -f " + containerName);
            process.waitFor();
            process.destroy();
        } catch (Exception e){
            LOG.info("Fail create Container.");
            e.printStackTrace();
        }
    }

    public void changeScale(){

    }

    public void getExecuteTime(){
        
    }

    public void overProvisioning(){

    }
}
