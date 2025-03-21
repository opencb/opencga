/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.storage.hadoop.variant.executors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.hadoop.utils.AbstractHBaseDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions.*;

/**
 * Created on 18/01/16 .
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class MRExecutor {

    public static final String HADOOP_LIB_VERSION_PROPERTIES = "org/opencb/opencga/storage/hadoop/lib/version.properties";
    protected String dbName;
    protected Configuration conf;
    private ObjectMap options;
    private List<String> env;
    private static Logger logger = LoggerFactory.getLogger(MRExecutor.class);
    private static final Pattern STDOUT_KEY_VALUE_PATTERN = Pattern.compile("^[a-zA-Z0-9_\\-.]+=[^=]+$");
    private static final Pattern EXCEPTION = Pattern.compile("^Exception in thread \"main\" ([^:]+: .+)$");

    public static class Result {
        private final int exitValue;
        private final ObjectMap result;

        public Result(int exitValue, ObjectMap result) {
            this.exitValue = exitValue;
            this.result = result;
        }

        public int getExitValue() {
            return exitValue;
        }

        public ObjectMap getResult() {
            return result;
        }

        public String getErrorMessage() {
            return result.getString(AbstractHBaseDriver.ERROR_MESSAGE);
        }
    }

    public MRExecutor init(String dbName, Configuration conf, ObjectMap options) {
        this.dbName = dbName;
        this.options = options;
        this.conf = conf;
        env = options.getAsStringList(MR_HADOOP_ENV.key());
        return this;
    }

    public static String getJarWithDependencies(ObjectMap options) throws StorageEngineException {
        String jar = options.getString(MR_JAR_WITH_DEPENDENCIES.key(), null);
        if (jar == null) {
            Properties properties = new Properties();
            try {
                properties.load(MRExecutor.class.getClassLoader()
                        .getResourceAsStream(HADOOP_LIB_VERSION_PROPERTIES));
            } catch (IOException e) {
                throw new StorageEngineException("Error reading classpath file \"" + HADOOP_LIB_VERSION_PROPERTIES + "\" for building the "
                        + "\"" + MR_JAR_WITH_DEPENDENCIES + "\"" + " parameter", e);
            }
            jar = "opencga-storage-hadoop-lib-" + properties.getProperty("opencga-hadoop-shaded.id") + "-"
                    + GitRepositoryState.getInstance().getBuildVersion() + "-jar-with-dependencies.jar";
//            throw new StorageEngineException("Missing option " + MR_JAR_WITH_DEPENDENCIES);
        }
        if (!Paths.get(jar).isAbsolute()) {
            jar = getOpencgaHome() + "/" + jar;
        }
        return jar;
    }

    protected static String getOpencgaHome() {
        return System.getProperty("app.home", "");
    }

    public <T extends Tool> ObjectMap run(Class<T> execClass, String[] args, String taskDescription)
            throws StorageEngineException {

        StopWatch stopWatch = StopWatch.createStarted();
        logger.info("------------------------------------------------------");
        logger.info(taskDescription);
        logger.info("------------------------------------------------------");
        Result result = run(execClass, args);
        int exitValue = result.getExitValue();
        logger.info("------------------------------------------------------");
        logger.info("Exit value: {}", exitValue);
        logger.info("Total time: {}", TimeUtils.durationToString(stopWatch));

        if (exitValue != 0) {
            String message = "Error executing MapReduce for : \"" + taskDescription + "\"";
            if (StringUtils.isNotEmpty(result.getErrorMessage())) {
                message += " : " + result.getErrorMessage().replace("\\n", "\n");
            } else {
                message += " : Unidentified error executing MapReduce job. Check logs for more information.";
            }
            throw new StorageEngineException(message);
        }
        return result.getResult();
    }

    protected <T extends Tool> Result run(Class<T> execClass, String[] args) throws StorageEngineException {
        String hadoopRoute = options.getString(MR_HADOOP_BIN.key(), MR_HADOOP_BIN.defaultValue());
        String jar = getJarWithDependencies(options);
        String executable = hadoopRoute + " jar " + jar + ' ' + execClass.getName();
        Logger logger = LoggerFactory.getLogger(MRExecutor.class);
        if (logger.isDebugEnabled()) {
            logger.debug(executable + ' ' + Arrays.toString(args));
        }

        try {
            return run(executable, args);
        } catch (Exception e) {
            if (e.getMessage().contains("Argument list too long")) {
                logger.error("Error executing: " + executable + ' ' + Arrays.toString(args));
            }
            throw e;
        }
    }

    public abstract Result run(String executable, String[] args) throws StorageEngineException;

    protected ObjectMap getOptions() {
        return options;
    }

    protected List<String> getEnv() {
        return env;
    }

    protected static void redactSecureString(String[] args, String key) {
        int passwordIdx = Arrays.binarySearch(args, key);
        if (passwordIdx > 0 && args.length > passwordIdx) {
            args[passwordIdx + 1] = "_redacted_";
        }
    }

    protected static ObjectMap readResult(String output) {
        ObjectMap result = new ObjectMap();
        List<String> exceptions = new ArrayList<>();
        for (String line : output.split(System.lineSeparator())) {
            if (STDOUT_KEY_VALUE_PATTERN.matcher(line).find()) {
                String[] split = line.split("=");
                if (split.length == 2) {
                    Object old = result.put(split[0], split[1]);
                    if (old != null) {
                        result.put(split[0], old + "," + split[1]);
                    }
                }
            } else if (EXCEPTION.matcher(line).find()) {
                Matcher matcher = EXCEPTION.matcher(line);
                if (matcher.find()) {
                    exceptions.add(matcher.group(1));
                }
            }
        }
        if (!exceptions.isEmpty() && !result.containsKey(AbstractHBaseDriver.ERROR_MESSAGE)) {
            result.put(AbstractHBaseDriver.ERROR_MESSAGE, String.join(", ", exceptions));
        }
        return result;
    }
}
