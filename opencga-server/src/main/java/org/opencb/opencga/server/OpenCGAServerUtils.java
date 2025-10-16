package org.opencb.opencga.server;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.opencb.opencga.core.config.Configuration;
import org.slf4j.Logger;

import java.net.URI;
import java.nio.file.Path;

public class OpenCGAServerUtils {


    public static void initLogger(Logger logger, Configuration configuration, Path configDirPath) {
        String logDir = configuration.getLogDir();
        boolean logFileEnabled;

        if (StringUtils.isNotBlank(configuration.getLogLevel())) {
            Level level = Level.toLevel(configuration.getLogLevel(), Level.INFO);
            System.setProperty("opencga.log.level", level.name());
        }

        if (StringUtils.isBlank(logDir) || logDir.equalsIgnoreCase("null")) {
            logFileEnabled = false;
        } else {
            logFileEnabled = true;
            System.setProperty("opencga.log.file.name", "opencga-rest");
            System.setProperty("opencga.log.dir", logDir);
        }
        System.setProperty("opencga.log.file.enabled", Boolean.toString(logFileEnabled));

        URI log4jconfFile = configDirPath.resolve("log4j2.service.xml").toUri();
        Configurator.reconfigure(log4jconfFile);

        logger.info("|  * Log configuration file: '{}'", log4jconfFile.getPath());
        if (logFileEnabled) {
            logger.info("|  * Log dir: '{}'", logDir);
        } else {
            logger.info("|  * Do not write logs to file");
        }
    }


}
