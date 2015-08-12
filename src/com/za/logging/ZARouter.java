/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.za.logging;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import org.gearman.Gearman;

/**
 *
 * @author phamdung
 */
public class ZARouter {

    private final List<ZALoggingWorker> listWorker;
    private final Timer timer;
    private final Calendar tomorrow;
    private final String PARENT_FOLDER = "ZAlog/";
    private final String WORKER_NAME = "logworker";
    private String logPath;
    private final int dayInMilliSecs = 86400000;

    //ZAlog/year/month/day/

    public ZARouter() {
        listWorker = new ArrayList<>();
        
        timer = new Timer();
        
        tomorrow = Calendar.getInstance();        
        tomorrow.set(Calendar.DATE, tomorrow.get(Calendar.DAY_OF_MONTH) + 1);
        tomorrow.set(Calendar.HOUR, 00);
        tomorrow.set(Calendar.MINUTE, 00);
        tomorrow.set(Calendar.SECOND, 00);
        
        configuratePath();        
    }
    
    public void start(int workerNum){
        //start gearman server
//        try {
//            startGearmanServer();
//        } catch (IOException ex) {
//            System.err.println("Error when start gearman server: " + ex.getMessage());
//            return;//if gearman server fail to start -- exit
//        }
        
        //start workers
        for (int i = 0; i < workerNum; i++) {
            startNewWorker();
        }
        
        //start timer for routing
        startRoutingTask();
    }

    private void startNewWorker() {
        try {
            ZALoggingWorker worker = new ZALoggingWorker(
                    WORKER_NAME + listWorker.size(), logPath);
            listWorker.add(worker);
            new Thread(worker).start();
        } catch (IOException ex) {
            System.err.println("Error when startWorker: " + ex.getMessage());
        }
    }
    
    private void startGearmanServer() throws IOException{
        Gearman gearman = Gearman.createGearman();
        gearman.startGearmanServer(ZALoggingWorker.GEARMAN_SERVER_PORT);
    }
    
    private void configuratePath(){
        Calendar now = Calendar.getInstance();
        logPath = PARENT_FOLDER 
                + now.get(Calendar.YEAR) + "/" 
                + now.get(Calendar.MONTH) + "/" 
                + now.get(Calendar.DAY_OF_MONTH) + "/";
        new File(logPath).mkdirs();
    }

    private void startRoutingTask() {
        timer.schedule(new ChangePathTask(), tomorrow.getTime(), dayInMilliSecs);
    }

    public static void main(String[] args) {
        int workerNum = 2;
        if (args.length > 0){
            try {
                workerNum = Integer.parseInt(args[0]);
            } catch (Exception e) {
                workerNum = 2;
            }
            
            if (workerNum < 1 || workerNum > 16){
                workerNum = 2;
            }
        }   
        
        //start routing with default 2 worker for logging
        new ZARouter().start(workerNum);
    }

    /**
     * - this task will be executed everyday at 0h0m0s to change path of log
     * file
     *
     * @author phamdung
     */
    class ChangePathTask extends TimerTask {

        @Override
        public void run() {            
            configuratePath();
            for (ZALoggingWorker worker : listWorker) {
                try {
                    worker.changePath(logPath);
                } catch (IOException ex) {
                    System.err.println("Error when change worker log path: " + ex.getMessage());
                }
            }            
        }
    }
}

