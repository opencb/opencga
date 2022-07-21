/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.test.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.collections4.CollectionUtils;
import org.opencb.commons.utils.PrintUtils;
import org.opencb.opencga.test.cli.options.CommonCommandOptions;
import org.opencb.opencga.test.cli.options.DatasetCommandOptions;
import org.opencb.opencga.test.utils.DatasetTestUtils;
import org.opencb.opencga.test.utils.OpencgaLogger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

public class OpencgaTestConfiguration {


    public static Configuration load(InputStream configurationInputStream) throws IOException {
        PrintUtils.print("Loading configuration: ", PrintUtils.Color.CYAN);

        if (configurationInputStream == null) {
            PrintUtils.println("Configuration file not found", PrintUtils.Color.RED);
            System.exit(-1);
        }
        Configuration configuration = null;
        ObjectMapper objectMapper;
        try {
            objectMapper = new ObjectMapper(new YAMLFactory());
            configuration = objectMapper.readValue(configurationInputStream, Configuration.class);
            loadMutators(configuration);
            if (CollectionUtils.isNotEmpty(DatasetCommandOptions.envs)) {
                disableUnselectedEnvs(configuration);
            }
        } catch (IOException e) {
            e.printStackTrace();
            PrintUtils.println("Configuration file could not be parsed", PrintUtils.Color.RED);
            OpencgaLogger.printLog("Configuration file could not be parsed: " + e.getMessage(), Level.SEVERE);
            System.exit(-1);
        }


        overrideConfigurationParams(configuration);
        OpencgaLogger.setLogLevel(configuration.getLogger().getLogLevel());
        PrintUtils.println(" Configuration load success ", PrintUtils.Color.WHITE);
        return configuration;
    }

    private static void disableUnselectedEnvs(Configuration configuration) {
        List<Environment> envs = new ArrayList<>();
        for (Environment environment : configuration.getEnvs()) {
            for (String envId : DatasetCommandOptions.envs) {
                if (envId.equals(environment.getId())) {
                    envs.add(environment);
                }
            }
        }
        if (CollectionUtils.isNotEmpty(envs)) {
            configuration.setEnvs(envs);
        } else {
            PrintUtils.printError("Envs are not present in config file.");

        }
    }

    private static void loadMutators(Configuration configuration) {
        for (Environment env : configuration.getEnvs()) {
            File envDir = Paths.get(DatasetTestUtils.getInputEnvironmentDirPath(env)).toFile();
            if (envDir.exists()) {
                File mutationsFile = Paths.get(DatasetTestUtils.getInputEnvironmentDirPath(env) + "mutations.yml").toFile();
                if (mutationsFile.exists()) {
                    ObjectMapper objectMapper;
                    Mutator mutator;
                    try {
                        objectMapper = new ObjectMapper(new YAMLFactory());
                        mutator = objectMapper.readValue(mutationsFile, Mutator.class);
                        configuration.setMutator(mutator);
                    } catch (IOException e) {
                        e.printStackTrace();
                        PrintUtils.println("Mutations file could not be parsed", PrintUtils.Color.RED);
                        OpencgaLogger.printLog("Mutations file could not be parsed: " + e.getMessage(), Level.SEVERE);
                        System.exit(-1);
                    }
                }
            } else {
                PrintUtils.printError("Directory " + envDir.getAbsolutePath() + " not exists.");
                System.exit(0);
            }
        }
    }

    private static void overrideConfigurationParams(Configuration configuration) {
        if (DatasetCommandOptions.commonCommandOptions.logLevel != null && !DatasetCommandOptions.commonCommandOptions.logLevel.equals(CommonCommandOptions.logLevel_DEFAULT_VALUE)) {
            configuration.getLogger().setLogLevel(DatasetCommandOptions.commonCommandOptions.logLevel);
        } else {
            configuration.getLogger().setLogLevel(CommonCommandOptions.logLevel_DEFAULT_VALUE);
        }
    }


}
