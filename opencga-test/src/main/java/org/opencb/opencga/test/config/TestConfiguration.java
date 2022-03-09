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
import org.opencb.opencga.test.cli.options.RunCommandOptions;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class TestConfiguration {

    private static Configuration config;
    private String outputDir;

    public static void load(InputStream configurationInputStream) throws IOException {

        System.out.println("Loading configuration.");
        if (configurationInputStream == null) {
            System.out.println("Configuration file not found");
            System.exit(-1);
        }
        Configuration configuration;
        ObjectMapper objectMapper;
        try {
            objectMapper = new ObjectMapper(new YAMLFactory());
            configuration = objectMapper.readValue(configurationInputStream, Configuration.class);
        } catch (IOException e) {
            throw new IOException("Configuration file could not be parsed: " + e.getMessage(), e);
        }

        overrideConfigurationParams();
        validateConfiguration(configuration);
        PrintUtils.println("Configuration loaded.");

        config = configuration;
    }

    private static void overrideConfigurationParams() {

        PrintUtils.println(RunCommandOptions.output);
        PrintUtils.println(String.valueOf(RunCommandOptions.simulate));

    }

    private static void validateConfiguration(Configuration configuration) {
        checkReference(configuration);
        checkDataset(configuration);
    }

    private static void checkDataset(Configuration configuration) {

        for (Env env : configuration.getEnvs()) {
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
        for (Env env : configuration.getEnvs()) {
            if (checkDirectory(env.getReference().getIndex())) {
                if (env.getAligner().getName().toLowerCase().contains("bwa")) {
                    checkIndexFileNames(env.getReference());
                }
            } else {
                System.err.println("The index must be present.");
                System.exit(-1);
            }
        }

    }

    private static void checkIndexFileNames(Reference reference) {
        List<String> indexFiles = findAllFileNamesInFolder(new File(reference.getIndex()));
        String indexPrefix = reference.getPath().substring(reference.getPath().lastIndexOf("/") + 1);
        if (!indexFiles.contains(indexPrefix)) {
            System.err.println("The file" + indexPrefix + " must be present in the index folder.");
        } else if (!indexFiles.contains(indexPrefix + ".amb")
                || !indexFiles.contains(indexPrefix + ".ann")
                || !indexFiles.contains(indexPrefix + ".bwt")
                || !indexFiles.contains(indexPrefix + ".pac")
                || !indexFiles.contains(indexPrefix + ".sa")) {
            System.err.println("The index folder is not complete. Please reindex.");
        }
    }


    public static List<String> findAllFileNamesInFolder(File folder) {

        List<String> res = new ArrayList<>();
        for (File file : folder.listFiles()) {
            if (!file.isDirectory()) {
                res.add(file.getName());
            }
        }
        return res;
    }

    private static boolean checkDirectory(String dir) {
        if (!Files.exists(Paths.get(dir))) {
            System.err.println("Volume " + dir + " is not present.");
            return false;
        }
        return true;
    }

}
