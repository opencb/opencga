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

import org.opencb.commons.utils.DockerUtils;
import org.opencb.commons.utils.PrintUtils;
import org.opencb.opencga.test.config.Environment;
import org.opencb.opencga.test.utils.DatasetTestUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LocalDatasetExecutor implements DatasetExecutor {

    public void execute(List<DatasetExecutionPlan> datasetPlanExecutionList) {

        for (DatasetExecutionPlan datasetPlanExecution : datasetPlanExecutionList) {
            Map<String, List<String>> result = new HashMap<>();
            createOutputDirs(datasetPlanExecution.getEnvironment());
            for (String filename : datasetPlanExecution.getCommands().keySet()) {
                List<String> dockerCommands = new ArrayList<>();
                for (DataSetExecutionCommand command : datasetPlanExecution.getCommands().get(filename)) {
                    dockerCommands.add(DockerUtils.buildMountPathsCommandLine(command.getImage(), command.getCommandLine()));
                }
                result.put(filename, dockerCommands);
            }
            for (String filename : result.keySet()) {
                PrintUtils.println(datasetPlanExecution.getEnvironment().getId(), PrintUtils.Color.GREEN, " - [" + filename + "]\n", PrintUtils.Color.YELLOW);
                executeFileScript(datasetPlanExecution.getEnvironment(), result, filename);
            }
        }
    }

    private void createOutputDirs(Environment env) {
        createDir(DatasetTestUtils.getEnvironmentDir(env));
        createDir(DatasetTestUtils.getEnvironmentOutputDir(env));
        createDir(DatasetTestUtils.getVCFDirPath(env));
        createDir(DatasetTestUtils.getExecutionDirPath(env));
        createDir(DatasetTestUtils.getBamDirPath(env));
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
