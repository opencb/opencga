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

import org.apache.hadoop.util.StopWatch;
import org.apache.hadoop.util.Tool;
import org.apache.tools.ant.types.Commandline;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Created on 18/01/16 .
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface MRExecutor {

    default <T extends Tool> void run(Class<T> execClass, String args, ObjectMap options, String taskDescription)
            throws StorageEngineException {
        run(execClass, Commandline.translateCommandline(args), options, taskDescription);
    }

    default <T extends Tool> void run(Class<T> execClass, String[] args, ObjectMap options, String taskDescription)
            throws StorageEngineException {
        Logger logger = LoggerFactory.getLogger(MRExecutor.class);

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        logger.info("------------------------------------------------------");
        logger.info(taskDescription);
        logger.info("------------------------------------------------------");
        int exitValue = run(execClass, args, options);
        logger.info("------------------------------------------------------");
        logger.info("Exit value: {}", exitValue);
        logger.info("Total time: {}s", (stopWatch.now(TimeUnit.MILLISECONDS)) / 1000.0);

        if (exitValue != 0) {
            throw new StorageEngineException("Error executing MapReduce for : \"" + taskDescription + "\"");
        }
    }

    default <T extends Tool> int run(Class<T> execClass, String[] args, ObjectMap options) throws StorageEngineException {
        String hadoopRoute = options.getString(HadoopVariantStorageEngine.HADOOP_BIN, "hadoop");
        String jar = HadoopVariantStorageEngine.getJarWithDependencies(options);
        String executable = hadoopRoute + " jar " + jar + ' ' + execClass.getName();
        Logger logger = LoggerFactory.getLogger(MRExecutor.class);
        if (logger.isDebugEnabled()) {
            logger.debug(executable + ' ' + Arrays.toString(args));
        }

        return run(executable, Commandline.toString(args));
    }

    default int run(String executable, String[] args) {
        return run(executable, Commandline.toString(args));
    }

    int run(String executable, String args);

}
