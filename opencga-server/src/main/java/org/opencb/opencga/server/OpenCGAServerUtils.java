package org.opencb.opencga.server;

import com.google.common.base.Splitter;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.Configuration;
import org.slf4j.Logger;

import javax.ws.rs.core.MultivaluedMap;
import java.net.URI;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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


    public static void parseParams(MultivaluedMap<String, String> inputParams, Query query, QueryOptions queryOptions) {
        for (Map.Entry<String, List<String>> entry : inputParams.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue().get(0);
            parseInputParam(query, queryOptions, key, value);
        }
    }

    public static void parseParams(Map<String, String> inputParams, Query query, QueryOptions queryOptions) {
        for (Map.Entry<String, String> entry : inputParams.entrySet()) {
            parseInputParam(query, queryOptions, entry.getKey(), entry.getValue());
        }
    }

    private static void parseInputParam(Query query, QueryOptions queryOptions, String key, String value) {
        switch (key) {
            case QueryOptions.INCLUDE:
            case QueryOptions.EXCLUDE:
                queryOptions.put(key, new LinkedList<>(Splitter.on(",").splitToList(value)));
                break;
            case QueryOptions.LIMIT:
            case QueryOptions.TIMEOUT:
                queryOptions.put(key, Integer.parseInt(value));
                break;
            case QueryOptions.SKIP:
                int skip = Integer.parseInt(value);
                queryOptions.put(key, (skip >= 0) ? skip : -1);
                break;
            case QueryOptions.SORT:
            case QueryOptions.ORDER:
            case QueryOptions.FACET:
                queryOptions.put(key, value);
                break;
            case QueryOptions.COUNT:
            case Constants.SILENT:
            case Constants.FORCE:
            case ParamConstants.FLATTEN_ANNOTATIONS:
            case ParamConstants.FAMILY_UPDATE_ROLES_PARAM:
            case ParamConstants.FAMILY_UPDATE_PEDIGREEE_GRAPH_PARAM:
            case ParamConstants.OTHER_STUDIES_FLAG:
            case ParamConstants.INCLUDE_RESULT_PARAM:
            case ParamConstants.SAMPLE_INCLUDE_INDIVIDUAL_PARAM:
            case "lazy":
                queryOptions.put(key, Boolean.parseBoolean(value));
                break;
            default:
                // Query
                query.put(key, value);
                break;
        }
    }

}
