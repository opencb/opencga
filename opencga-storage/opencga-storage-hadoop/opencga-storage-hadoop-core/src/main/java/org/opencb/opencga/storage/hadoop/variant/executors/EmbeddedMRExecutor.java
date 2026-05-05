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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * MRExecutor that runs MapReduce drivers in-process via reflection.
 *
 * <p>Instead of spawning a separate {@code hadoop jar} process (like {@link SystemMRExecutor}),
 * this executor calls the driver's {@code privateMain(String[], Configuration)} method directly
 * in the current JVM. This is useful in environments where no {@code hadoop} binary is available
 * (e.g., Docker containers running with {@code mapreduce.framework.name=local}).</p>
 *
 * <p>Configure via {@code storage.hadoop.mr.executor=embedded}.</p>
 *
 * @see MRExecutorFactory
 */
public class EmbeddedMRExecutor extends MRExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedMRExecutor.class);

    @Override
    protected <T extends Tool> Result run(Class<T> clazz, String[] args) throws StorageEngineException {
        try {
            Configuration jobConf = new Configuration(false);
            HBaseConfiguration.merge(jobConf, conf);

            LOGGER.info("Executing {} in-process: {}", clazz.getSimpleName(), Arrays.toString(args));
            Method method = clazz.getMethod("privateMain", String[].class, Configuration.class);

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            PrintStream originalErr = System.err;
            int exitCode;
            try {
                System.setErr(new PrintStream(outputStream));
                exitCode = ((Number) method.invoke(clazz.newInstance(), args, jobConf)).intValue();
            } finally {
                System.setErr(originalErr);
            }

            LOGGER.info("Finished {} with exit code {}", clazz.getSimpleName(), exitCode);
            return new Result(exitCode, readResult(outputStream.toString()));
        } catch (InvocationTargetException e) {
            throw new StorageEngineException("Error executing " + clazz.getSimpleName(), e.getCause());
        } catch (Exception e) {
            throw new StorageEngineException("Error executing " + clazz.getSimpleName(), e);
        }
    }

    @Override
    public Result run(String executable, String[] args) throws StorageEngineException {
        String className = executable.substring(executable.lastIndexOf(" ") + 1).trim();
        try {
            Class<?> clazz = Class.forName(className);
            Configuration jobConf = new Configuration(false);
            HBaseConfiguration.merge(jobConf, conf);

            LOGGER.info("Executing {} in-process: {}", clazz.getSimpleName(), Arrays.toString(args));
            Method method = clazz.getMethod("privateMain", String[].class, Configuration.class);

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            PrintStream originalErr = System.err;
            int exitCode;
            try {
                System.setErr(new PrintStream(outputStream));
                exitCode = ((Number) method.invoke(clazz.newInstance(), args, jobConf)).intValue();
            } finally {
                System.setErr(originalErr);
            }

            LOGGER.info("Finished {} with exit code {}", clazz.getSimpleName(), exitCode);
            if (exitCode != 0) {
                throw new StorageEngineException("Error executing " + className + ". Exit code: " + exitCode);
            }
            return new Result(exitCode, readResult(outputStream.toString()));
        } catch (StorageEngineException e) {
            throw e;
        } catch (InvocationTargetException e) {
            throw new StorageEngineException("Error executing " + className, e.getCause());
        } catch (Exception e) {
            throw new StorageEngineException("Error executing " + className, e);
        }
    }
}
