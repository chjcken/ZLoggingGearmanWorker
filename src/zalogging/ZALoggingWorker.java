/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package zalogging;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.gearman.Gearman;
import org.gearman.GearmanFunction;
import org.gearman.GearmanFunctionCallback;
import org.gearman.GearmanServer;
import org.gearman.GearmanWorker;

/**
 *
 * @author datbt
 */
public class ZALoggingWorker implements Runnable, GearmanFunction{
    private boolean isStopped = false;
    private final Logger LOGGER;
    private final String LOG_NAME_PATTERN = "zalog.%u.%g.txt";
    private final int LOG_FILE_SIZE = 1000000000;
    private final int LOG_FILE_COUNT = 1000;
    private Handler fileHandler;
    
    private final String GEARMAN_SERVER_HOST = "localhost";
    private final int GEARMAN_SERVER_PORT = 5555;
    private final String GEARMAN_FUNCTION_NAME = "writelog";
    private Gearman gearman;
    private GearmanServer server;
    private GearmanWorker worker;

    public ZALoggingWorker(String name, String path) throws IOException {
        LOGGER = Logger.getLogger(name);
        LOGGER.setUseParentHandlers(false);
        Handler _fileHandler = createFileHandler(path);
        LOGGER.addHandler(_fileHandler);
        this.fileHandler = _fileHandler;
    } 
    
    @Override
    public void run() {
//        int i = 0;
//        while(!isStopped){
//            LOGGER.log(Level.INFO, "test log {0}", i++);
//            if (i == 5000) try {
//                changePath("log2/");
//            } catch (IOException ex) {
//                
//            }
//            if (i >10000) break;
//        }
        
        gearman = Gearman.createGearman();
        server = gearman.createGearmanServer(
                GEARMAN_SERVER_HOST, GEARMAN_SERVER_PORT);
        worker = gearman.createGearmanWorker();
        worker.addFunction(GEARMAN_FUNCTION_NAME, this);
        worker.addServer(server);
        
        
    }
    
    private Handler createFileHandler(String path) throws IOException{
        Handler _fileHandler = new FileHandler(path + LOG_NAME_PATTERN, LOG_FILE_SIZE, LOG_FILE_COUNT, true);
        _fileHandler.setFormatter(new ZALogFormatter());
        return _fileHandler;
    }
    
    public void changePath(String path) throws IOException{
        Handler _fileHandler = createFileHandler(path);
        LOGGER.addHandler(_fileHandler);
        LOGGER.removeHandler(this.fileHandler);
        this.fileHandler = _fileHandler;
    }
    
    public void stop(){
        isStopped = true;
        worker.shutdown();
    }
    
//    /**
//     * @param args the command line arguments
//     * @throws java.io.IOException
//     */
//    public static void main(String[] args) throws IOException {
//        // TODO code application logic here
//        new Thread(new ZALoggingWorker("runlog", "log1/")).start();
//    }

    //TODO: code to write log here
    @Override
    public byte[] work(String string, byte[] bytes, 
            GearmanFunctionCallback gfc) throws Exception {
        System.out.println(string);
        return null;
    }
}
