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

package org.opencb.opencga.test.execution;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.commons.utils.PrintUtils;
import org.opencb.opencga.test.config.Configuration;
import org.opencb.opencga.test.config.Environment;
import org.opencb.opencga.test.config.Mutation;
import org.opencb.opencga.test.config.Variant;
import org.opencb.opencga.test.execution.models.DataSetExecutionCommand;
import org.opencb.opencga.test.execution.models.DatasetExecutionFile;
import org.opencb.opencga.test.execution.models.DatasetExecutionPlan;
import org.opencb.opencga.test.utils.DatasetTestUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LocalDatasetExecutor extends DatasetExecutor {


    public LocalDatasetExecutor(Configuration configuration) {
        super(configuration);
    }

    @Override
    public void execute(List<DatasetExecutionPlan> datasetPlanExecutionList) {

        if (!DatasetTestUtils.areSkippedAllExecutionPlans(datasetPlanExecutionList)) {
            PrintUtils.println("Executing the data set plan executions: ", PrintUtils.Color.CYAN,
                    datasetPlanExecutionList.size() + " plans found.", PrintUtils.Color.WHITE);
        }

        for (DatasetExecutionPlan datasetPlanExecution : datasetPlanExecutionList) {
            Map<String, List<String>> result = new HashMap<>();
            createOutputDirs(datasetPlanExecution.getEnvironment());
            PrintUtils.println("Plan execution for ", PrintUtils.Color.CYAN, datasetPlanExecution.getEnvironment().getId(), PrintUtils.Color.WHITE);
            PrintUtils.println("Executing ", PrintUtils.Color.CYAN, datasetPlanExecution.getDatasetExecutionFiles().size() + " files", PrintUtils.Color.WHITE);
            for (DatasetExecutionFile datasetExecutionFile : datasetPlanExecution.getDatasetExecutionFiles()) {
                List<String> dockerCommands = new ArrayList<>();
                for (DataSetExecutionCommand command : datasetExecutionFile.getCommands()) {
                    dockerCommands.add(DockerUtils.buildMountPathsCommandLine(command.getImage(), command.getCommandLine()));
                }
                result.put(datasetExecutionFile.getInputFilename(), dockerCommands);
            }
            for (String filename : result.keySet()) {
                PrintUtils.println(datasetPlanExecution.getEnvironment().getId(), PrintUtils.Color.CYAN, " - [" + filename + "]\n", PrintUtils.Color.WHITE);
                executeFileScript(datasetPlanExecution.getEnvironment(), result, filename);
            }
        }

        if (CollectionUtils.isNotEmpty(configuration.getMutator())) {
            PrintUtils.println("Writing mutations: ", PrintUtils.Color.CYAN,
                    configuration.getMutator().size() + " mutators found.", PrintUtils.Color.WHITE);
            FilenameFilter filter = (f, name) -> name.endsWith(".vcf");
            for (Mutation mutation : configuration.getMutator()) {
                String filename = mutation.getFile();
                for (Environment environment : configuration.getEnvs()) {
                    File vcfDir = new File(DatasetTestUtils.getVCFOutputDirPath(environment));
                    if (vcfDir.exists()) {
                        String[] files = vcfDir.list(filter);
                        for (int i = 0; i < files.length; i++) {
                            String vcfFilename = files[i].substring(0, files[i].lastIndexOf('.'));
                            if (vcfFilename.equals(filename)) {
                                setVariantsInFile(mutation.getVariants());
                            }
                        }

                    }
                }

            }
        }
    }

    private void setVariantsInFile(List<Variant> variants) {
    }

    private Map<String, List<Variant>> getVariantsMap(List<Mutation> mutations) {
        Map<String, List<Variant>> res = new HashMap<>();
        for (Mutation mutation : mutations) {
            res.put(mutation.getFile(), mutation.getVariants());
        }
        return res;

    }

    private void createOutputDirs(Environment env) {
        createDir(DatasetTestUtils.getEnvironmentDirPath(env));
        createDir(DatasetTestUtils.getEnvironmentOutputDirPath(env));
        createDir(DatasetTestUtils.getVCFOutputDirPath(env));
        createDir(DatasetTestUtils.getExecutionDirPath(env));
        createDir(DatasetTestUtils.getOutputBamDirPath(env));
        File templatesDir = Paths.get(DatasetTestUtils.getInputTemplatesDirPath(env)).toFile();
        if (templatesDir.exists()) {
            try {
                FileUtils.copyDirectory(templatesDir, Paths.get(DatasetTestUtils.getOutputTemplatesDirPath(env)).toFile());
                PrintUtils.println("Directory " + templatesDir.getAbsolutePath() + " copied to " + DatasetTestUtils.getOutputTemplatesDirPath(env), PrintUtils.Color.CYAN);
            } catch (IOException e) {
                PrintUtils.printError("Error creating " + templatesDir.getAbsolutePath());
                System.exit(0);
            }
        } else {
            PrintUtils.printError("Directory " + templatesDir.getAbsolutePath() + " not exists.");
            System.exit(0);
        }
    }

    private void createDir(String dir) {
        if (!new File(dir).exists()) {
            try {
                Files.createDirectory(Paths.get(dir));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void executeFileScript(Environment env, Map<String, List<String>> result, String filename) {
        try {
            Path path = Paths.get(DatasetTestUtils.getExecutionDirPath(env) + filename + ".sh");
            if (path.toFile().exists()) {
                path.toFile().delete();
            }
            Files.createFile(path);
            FileWriter fw = new FileWriter(path.toAbsolutePath().toString(), true);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write("#!/bin/bash");
            bw.newLine();
            for (String command : result.get(filename)) {
                PrintUtils.println("[EXEC] ", PrintUtils.Color.BLUE, command, PrintUtils.Color.WHITE);
                bw.write(command);
                bw.newLine();
            }
            bw.close();
            String[] cmd = {"sh", path.toAbsolutePath().toString(), path.getParent().toString()};
            executeCommand(cmd);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void executeCommand(String[] command) {
        try {
            Process process = Runtime.getRuntime().exec(command);
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            int exitCode = process.waitFor();
            if (exitCode == 0) {
                while ((line = reader.readLine()) != null) {
                    PrintUtils.println(line, PrintUtils.Color.YELLOW);
                }
                PrintUtils.println(command[1] + " execution success!\n\n", PrintUtils.Color.GREEN);
            } else {
                while ((line = reader.readLine()) != null) {
                    PrintUtils.println(line, PrintUtils.Color.RED);
                }
                PrintUtils.println("exitCode ERROR: " + exitCode, PrintUtils.Color.RED);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
