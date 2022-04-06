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
import org.opencb.commons.utils.PrintUtils;
import org.opencb.opencga.test.cli.options.CommonCommandOptions;
import org.opencb.opencga.test.cli.options.DatasetCommandOptions;
import org.opencb.opencga.test.utils.OpencgaLogger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Level;

public class OpencgaTestConfiguration {


    public static Configuration load(InputStream configurationInputStream) throws IOException {

        if (configurationInputStream == null) {
            PrintUtils.println("Configuration file not found", PrintUtils.Color.RED);
            System.exit(-1);
        }
        Configuration configuration = null;
        ObjectMapper objectMapper;
        try {
            objectMapper = new ObjectMapper(new YAMLFactory());
            configuration = objectMapper.readValue(configurationInputStream, Configuration.class);
        } catch (IOException e) {
            OpencgaLogger.printLog("Configuration file could not be parsed: " + e.getMessage(), Level.SEVERE);

        }

        overrideConfigurationParams(configuration);
        validateConfiguration(configuration);
        OpencgaLogger.setLogLevel(configuration.getLogger().getLogLevel());

        return configuration;
    }

    private static void overrideConfigurationParams(Configuration configuration) {

        if (!DatasetCommandOptions.commonCommandOptions.logLevel.equals(CommonCommandOptions.logLevel_DEFAULT_VALUE)) {
            configuration.getLogger().setLogLevel(DatasetCommandOptions.commonCommandOptions.logLevel);
        }

    }

    private static void validateConfiguration(Configuration configuration) {
        checkReference(configuration);
        checkDataset(configuration);
    }

    private static void checkDataset(Configuration configuration) {

        for (Environment env : configuration.getEnvs()) {
            String separator = "";
            if (!env.getDataset().getPath().endsWith(File.separator)) {
                separator = File.separator;
            }
            checkDirectory(env.getDataset().getPath());
            checkDirectory(env.getDataset().getPath() + separator + "fastq");
            checkDirectory(env.getDataset().getPath() + separator + "templates");
        }
    }

    private static void checkReference(Configuration configuration) {
        for (Environment env : configuration.getEnvs()) {
            if (!checkDirectory(env.getReference().getIndex())) {
                System.err.println("The index must be present.");
                System.exit(-1);
            }
        }

    }


    private static boolean checkDirectory(String dir) {
        if (!Files.exists(Paths.get(dir))) {
            System.err.println("Volume " + dir + " is not present.");
            return false;
        }
        return true;
    }

}
