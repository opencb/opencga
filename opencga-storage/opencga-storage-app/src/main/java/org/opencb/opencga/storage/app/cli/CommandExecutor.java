package org.opencb.opencga.storage.app.cli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by imedina on 02/03/15.
 */
public abstract class CommandExecutor {

    protected String logLevel;
    protected boolean verbose;
    protected String configFile;

    protected Logger logger;
//    protected CellBaseConfiguration configuration;

    public CommandExecutor() {
    }

    public CommandExecutor(String logLevel, boolean verbose, String configFile) {
        this.logLevel = logLevel;
        this.verbose = verbose;
        this.configFile = configFile;

        if(logLevel != null && !logLevel.isEmpty()) {
            // We must call to this method
            setLogLevel(logLevel);
        }
    }

    public abstract void execute();

    public String getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(String logLevel) {
        // This small hack allow to configure the appropriate Logger level from the command line, this is done
        // by setting the DEFAULT_LOG_LEVEL_KEY before the logger object is created.
        System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, logLevel);
        logger = LoggerFactory.getLogger(this.getClass().toString());
        this.logLevel = logLevel;
    }


    public boolean isVerbose() {
        return verbose;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }


    public String getConfigFile() {
        return configFile;
    }

    public void setConfigFile(String configFile) {
        this.configFile = configFile;
    }

    public Logger getLogger() {
        return logger;
    }

//    public void readCellBaseConfiguration() throws URISyntaxException, IOException {
//        this.configuration = CellBaseConfiguration.load(CellBaseConfiguration.class.getClassLoader().getResourceAsStream("cellBaseConfiguration.json"));
//    }
}
