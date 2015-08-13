/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.za.logging;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Date;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
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
public class ZALoggingWorker implements Runnable, GearmanFunction {

    private final String TAG = "[ZA Log Worker]\t";
    private final Logger LOGGER;
    private final String LOG_NAME_PATTERN = "zalog.%u.%g.txt";
    private final int LOG_FILE_SIZE = 100000000; //max size 100mb
    private final int LOG_FILE_COUNT = 1000;
    private final String DELIMITER = " ";
    private Handler fileHandler;

    private final String GEARMAN_SERVER_HOST = "localhost";
    public static final int GEARMAN_SERVER_PORT = 5555;
    private final String GEARMAN_FUNCTION_NAME = "writelog";
    private Gearman gearman;
    private GearmanServer server;
    private GearmanWorker worker;

    public ZALoggingWorker(String name, String path) throws IOException {
        LOGGER = Logger.getLogger(name);
        LOGGER.setLevel(Level.INFO);
        LOGGER.setUseParentHandlers(false);
        Handler _fileHandler = createFileHandler(path);
        LOGGER.addHandler(_fileHandler);
        this.fileHandler = _fileHandler;
    }

    @Override
    public void run() {
        gearman = Gearman.createGearman();
        server = gearman.createGearmanServer(
                GEARMAN_SERVER_HOST, GEARMAN_SERVER_PORT);
        worker = gearman.createGearmanWorker();
        worker.addFunction(GEARMAN_FUNCTION_NAME, this);
        worker.addServer(server);
        
        System.out.println(TAG + LOGGER.getName() + " is running...");
    }

    private Handler createFileHandler(String path) throws IOException {
        Handler _fileHandler = new FileHandler(path + LOG_NAME_PATTERN, LOG_FILE_SIZE, LOG_FILE_COUNT, true);
        _fileHandler.setFormatter(new ZALogFormatter());
        return _fileHandler;
    }

    public void changePath(String path) throws IOException {        
        Handler _fileHandler = createFileHandler(path);
        LOGGER.addHandler(_fileHandler);
        LOGGER.removeHandler(this.fileHandler);
        this.fileHandler = _fileHandler;
        
        System.out.println(TAG + LOGGER.getName() + " change path to: " + path + " at: " + new Date().toString());
    }

    public void stop() {
        if (worker != null)
            worker.shutdown();
        if (gearman != null)
            gearman.shutdown();
    }

    private String replaceSpaceWithHyphen(String value) {
        if (value == null || !value.contains(" ")) {
            return value;
        }
        return value.trim().replaceAll(" ", "-");
    }

    //TODO: code to write log here
    @Override
    public byte[] work(String string, byte[] bytes,
            GearmanFunctionCallback gfc) throws Exception {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bais);
        String[] logData = (String[]) ois.readObject();
        if (logData == null) {
            return null;
        }
        String log = "";
        for (String val : logData) {
            log += replaceSpaceWithHyphen(val) + DELIMITER;
        }
        LOGGER.info(log.trim());
        return null;
    }

    //log format class
    class ZALogFormatter extends Formatter {

        private final String NEWLINE = "\n";

        @Override
        public String format(LogRecord lr) {
            return formatMessage(lr) + NEWLINE;
        }
    }
}
