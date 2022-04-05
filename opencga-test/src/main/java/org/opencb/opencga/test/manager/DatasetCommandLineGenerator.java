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
import org.opencb.opencga.test.config.Environment;
import org.opencb.opencga.test.execution.DataSetExecutionCommand;
import org.opencb.opencga.test.execution.DatasetExecutionPlan;
import org.opencb.opencga.test.utils.DatasetTestUtils;

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
    public List<DatasetExecutionPlan> generateCommandLines() throws IOException {
        //logger.debug("Processing the following environments: {}", environments.toString())
        List<Environment> environments = configuration.getEnvs();
        List<DatasetExecutionPlan> datasetPlanExecutions = new ArrayList<>();
        for (Environment environment : environments) {
            DatasetExecutionPlan datasetPlanExecution = new DatasetExecutionPlan(environment);
            Map<String, List<DataSetExecutionCommand>> commands = new HashMap<>();
            File datasetDir = new File(environment.getDataset().getPath() + File.separator + "fastq");
            List<String> filenames = findAllFileNamesInFolder(datasetDir);
            Collections.sort(filenames);
            for (int i = 0; i < filenames.size(); i++) {
                List<DataSetExecutionCommand> commandLines = new LinkedList<>();
                String filename = filenames.get(i).substring(0, filenames.get(i).indexOf('.'));
                // Generate command line for the Aligner, WARNING!! filenames index is incremented two times if is Paired-End enabled
                String command = getAlignerCommandLine(environment, filename).replace("${FASTQ1}", environment.getDataset().getPath() + "fastq/" + filenames.get(i));
                if (environment.getDataset().isPaired()) {
                    command = command.replace("${FASTQ2}", environment.getDataset().getPath() + "fastq/" + filenames.get(++i));
                }

                commandLines.add(new DataSetExecutionCommand().setCommandLine(command).setImage(environment.getAligner().getImage()));
                // Adding samtools command lines
                List<String> samtoolsCommand = DatasetTestUtils.getSamtoolsCommands(DatasetTestUtils.getEnvironmentOutputDir(environment) + filename);
                for (String c : samtoolsCommand) {
                    commandLines.add(new DataSetExecutionCommand().setCommandLine(c).setImage(environment.getAligner().getImage()));
                }
                // Adding caller command lines
                List<Caller> callers = environment.getCallers();
                for (Caller caller : callers) {
                    String callerCommand = getVariantCallerCommandLine(environment, caller.getCommand(), caller.getParams(), filename);
                    commandLines.add(new DataSetExecutionCommand().setCommandLine(callerCommand).setImage(caller.getImage()));
                }
                commands.put(filename, commandLines);
            }
            datasetPlanExecution.setCommands(commands);
            datasetPlanExecutions.add(datasetPlanExecution);
        }
        return datasetPlanExecutions;
    }

    /**
     * Generate the command lines for Aligner adding params to command and replacing environment variables.
     *
     * @param environment
     * @param command
     * @param params
     * @param filename
     * @return
     */
    private String getVariantCallerCommandLine(Environment environment, String command, List<String> params, String filename) {
        String param = String.join(" ", params);
        command = command.replace("${PARAMS}", param);
        command = command.replace("${INDEX}", environment.getReference().getIndex());
        String output = DatasetTestUtils.getEnvironmentOutputDir(environment);
        command = command.replace("${OUTPUT}", output + "vcf/" + filename + ".vcf");
        command = command.replace("${BAM}", output + "bam/" + filename + ".bam");
        return command;
    }


    /**
     * Generate the command lines for Aligner adding params to command and replacing environment variables.
     *
     * @param environment
     * @return
     */
    private String getAlignerCommandLine(Environment environment, String filename) {
        String param = String.join(" ", environment.getAligner().getParams());
        String command = environment.getAligner().getCommand();
        command = command.replace("${PARAMS}", param);
        command = command.replace("${INDEX}", environment.getReference().getIndex());
        String output = DatasetTestUtils.getEnvironmentOutputDir(environment);
        command = command.replace("${OUTPUT}", output + "bam/${FASTQNAME}.bam");
        command = command.replace("${FASTQNAME}", filename);
        return command;
    }
}
