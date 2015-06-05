/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.app.cli;

import org.apache.commons.lang.StringUtils;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by imedina on 02/03/15.
 */
public abstract class CommandExecutor {

    protected String logLevel;
    protected boolean verbose;
    protected String configFile;

    protected String appHome;

    protected StorageConfiguration configuration;

    protected Logger logger;

    public CommandExecutor() {
        this("info", false, null);
    }

    public CommandExecutor(String logLevel, boolean verbose, String configFile) {
        this.logLevel = logLevel;
        this.verbose = verbose;
        this.configFile = configFile;

        /**
         * System property 'app.home' is automatically set up in opencga-storage.sh. If by any reason
         * this is 'null' then OPENCGA_HOME environment variable is used instead.
         */
        this.appHome = System.getProperty("app.home", System.getenv("OPENCGA_HOME"));

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

    /**
     * This method attempts to first data configuration from CLI parameter, if not present then uses
     * the configuration from installation directory, if not exists then loads JAR configuration.json
     * @throws URISyntaxException
     * @throws IOException
     */
    public void loadOpenCGAStorageConfiguration() throws URISyntaxException, IOException {
        if(this.configFile != null) {
            logger.debug("Loading configuration from '{}'", this.configFile);
            this.configuration = StorageConfiguration.load(new FileInputStream(new File(this.configFile)));
        }else {
            if(this.appHome != null && Files.exists(Paths.get(this.appHome + "/configuration.yml"))) {
                logger.debug("Loading configuration from '{}'", this.appHome+"/configuration.yml");
                this.configuration = StorageConfiguration
                        .load(new FileInputStream(new File(this.appHome + "/configuration.yml")));
            }else {
                logger.debug("Loading configuration from '{}'",
                        StorageConfiguration.class.getClassLoader()
                                .getResourceAsStream("configuration.yml")
                                .toString());
                this.configuration = StorageConfiguration
                        .load(StorageConfiguration.class.getClassLoader().getResourceAsStream("configuration.yml"));
            }
        }
    }

    @Deprecated
    protected void assertDirectoryExists(URI outdir){
        if (!java.nio.file.Files.exists(Paths.get(outdir.getPath()))) {
            logger.error("given output directory {} does not exist, please create it first.", outdir);
            System.exit(1);
        }
    }

    protected boolean runCommandLineProcess(File workingDirectory, String binPath, List<String> args, String logFilePath)
            throws IOException, InterruptedException {
        ProcessBuilder builder = getProcessBuilder(workingDirectory, binPath, args, logFilePath);

        logger.debug("Executing command: " + StringUtils.join(builder.command(), " "));
        Process process = builder.start();
        process.waitFor();

        // Check process output
        boolean executedWithoutErrors = true;
        int genomeInfoExitValue = process.exitValue();
        if (genomeInfoExitValue != 0) {
            logger.warn("Error executing {}, error code: {}. More info in log file: {}", binPath, genomeInfoExitValue, logFilePath);
            executedWithoutErrors = false;
        }
        return executedWithoutErrors;
    }

    private ProcessBuilder getProcessBuilder(File workingDirectory, String binPath, List<String> args, String logFilePath) {
        List<String> commandArgs = new ArrayList<>();
        commandArgs.add(binPath);
        commandArgs.addAll(args);
        ProcessBuilder builder = new ProcessBuilder(commandArgs);

        // working directoy and error and output log outputs
        if (workingDirectory != null) {
            builder.directory(workingDirectory);
        }
        builder.redirectErrorStream(true);
        if (logFilePath != null) {
            builder.redirectOutput(ProcessBuilder.Redirect.appendTo(new File(logFilePath)));
        }

        return builder;
    }

    public StorageConfiguration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(StorageConfiguration configuration) {
        this.configuration = configuration;
    }

}
