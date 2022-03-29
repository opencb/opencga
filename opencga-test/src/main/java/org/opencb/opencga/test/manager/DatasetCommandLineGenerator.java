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

package org.opencb.opencga.test.manager;

import org.opencb.opencga.test.config.Caller;
import org.opencb.opencga.test.config.Configuration;
import org.opencb.opencga.test.config.Env;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class DatasetCommandLineGenerator {

    private Configuration configuration;

    public DatasetCommandLineGenerator(Configuration configuration) {
        this.configuration = configuration;
    }

    public static List<String> findAllFileNamesInFolder(File folder) {
        List<String> res = new ArrayList<>();
        for (File file : Objects.requireNonNull(folder.listFiles())) {
            if (!file.isDirectory()) {
                res.add(file.getName());
            }
        }
        return res;
    }

    /**
     * Allows to only process a list of given environments.
     *
     * @return CLIs Map as a list of command lines by environment
     * @throws IOException
     */
    public Map<String, List<String>> generateCommandLines() throws IOException {
        // logger.debug("Processing the following environments: {}", environments.toString())
        List<Env> environments = configuration.getEnvs();
        Map<String, List<String>> commandLinesMap = new HashMap<>();
        for (Env environment : environments) {
            List<String> commandLines = this.generateCommandLines(environment);
            commandLinesMap.put(environment.getId(), commandLines);
        }
        return commandLinesMap;
    }

    /**
     * Generate the CLIs for a single environment.
     *
     * @param environment
     * @return
     */
    public List<String> generateCommandLines(Env environment) {
        String alignerCommand = environment.getAligner().getCommand();
        List<String> alignerParams = environment.getAligner().getParams();
        List<Caller> callers = environment.getCallers();
        Map<String, String> callersCommands = new HashMap<>();
        Map<String, List<String>> callersParams = new HashMap<>();
        for (Caller caller : callers) {
            callersCommands.put(caller.getName(), caller.getCommand());
            callersParams.put(caller.getName(), caller.getParams());
        }

        List<String> commandLines = getAlignerCommandLines(environment, alignerCommand, alignerParams);
        for (String caller : callersCommands.keySet()) {
            commandLines.addAll(getVariantCallerCommandLines(environment, callersCommands.get(caller), callersParams.get(caller)));
        }
        return commandLines;
    }

    /**
     * Generate the command lines for Aligner adding params to command and replacing environment variables.
     *
     * @param environment
     * @return
     */
    private List<String> getAlignerCommandLines(Env environment, String command, List<String> params) {
        String param = String.join(" ", params);
        command = command.replace("${PARAMS}", param);
        command = replaceAlignerEnvironmentVariables(environment, command);
        List<String> commandLines = generateReadsCommandLines(environment, command);
        return commandLines;
    }

    /**
     * Generate the command lines for Aligner adding params to command and replacing environment variables.
     *
     * @param environment
     * @return
     */
    private List<String> getVariantCallerCommandLines(Env environment, String command, List<String> params) {
        String param = String.join(" ", params);
        command = command.replace("${PARAMS}", param);
        command = replaceCallerEnvironmentVariables(environment, command);
        List<String> commandLines = generateReadsCommandLines(environment, command);
        return commandLines;
    }

    /***
     *
     * @param environment
     * @param command
     * @return List of command lines with the names of the dataset's fastq files
     */
    private List<String> generateReadsCommandLines(Env environment, String command) {
        List<String> result = new ArrayList<>();
        File datasetDir = new File(environment.getDataset().getPath() + File.separator + "fastq");
        List<String> filenames = findAllFileNamesInFolder(datasetDir);

        Collections.sort(filenames);
        for (int i = 0; i < filenames.size(); i++) {
            command = command.replace("${FASTQ1}", filenames.get(i));
            command = command.replace("${FASTQNAME}", filenames.get(i).substring(0, filenames.get(i).indexOf('.')));
            if (environment.getDataset().isPaired()) {
                command = command.replace("${FASTQ2}", filenames.get(++i));
            }
            result.add(command);
        }
        return result;
    }

    /***
     *
     * @param environment
     * @param command
     * @return Command Line whit replaced values of the environment variables
     */
    private String replaceAlignerEnvironmentVariables(Env environment, String command) {
        command = command.replace("${INDEX}", environment.getReference().getIndex());
        String output = environment.getDataset().getPath();
        String separator = "";
        if (!output.endsWith(File.separator)) {
            separator = File.separator;
        }
        command = command.replace("${OUTPUT}", output + separator + environment.getId() + "/bam/${FASTQNAME}.bam");

        return command;
    }

    /***
     *
     *
     * @param environment
     * @param command
     * @return Command Line whit replaced values of the environment variables
     */
    private String replaceCallerEnvironmentVariables(Env environment, String command) {
        command = command.replace("${INDEX}", environment.getReference().getIndex());
        String output = environment.getDataset().getPath();
        String separator = "";
        if (!output.endsWith(File.separator)) {
            separator = File.separator;
        }
        command = command.replace("${OUTPUT}", output + separator + environment.getId() + "/vcf/${FASTQNAME}.vcf");
        command = command.replace("${BAM}", output + separator + environment.getId() + "/bam/${FASTQNAME}.bam");

        return command;
    }

}
