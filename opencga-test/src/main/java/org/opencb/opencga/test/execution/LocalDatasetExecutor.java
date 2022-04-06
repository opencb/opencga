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
import org.opencb.opencga.test.config.Caller;
import org.opencb.opencga.test.utils.DatasetTestUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;

public class LocalDatasetExecutor implements DatasetExecutor {

    public void execute(List<DatasetExecutionPlan> datasetPlanExecutionList) {

        for (DatasetExecutionPlan datasetPlanExecution : datasetPlanExecutionList) {
            createDir(DatasetTestUtils.getEnvironmentOutputDir(datasetPlanExecution.getEnvironment()));
            Map<String, List<String>> result = new HashMap<>();
            for (String filename : datasetPlanExecution.getCommands().keySet()) {
                createDir(DatasetTestUtils.getEnvironmentOutputDir(datasetPlanExecution.getEnvironment()) + filename);
                createDir(DatasetTestUtils.getEnvironmentOutputDir(datasetPlanExecution.getEnvironment()) + filename + "/vcf");
                createDir(DatasetTestUtils.getEnvironmentOutputDir(datasetPlanExecution.getEnvironment()) + filename + "/execution");
                for (Caller caller : datasetPlanExecution.getEnvironment().getCallers()) {
                    createDir(DatasetTestUtils.getEnvironmentOutputDir(datasetPlanExecution.getEnvironment()) + filename + "/vcf/" + caller.getName().replaceAll(" ", "_"));
                }
                createDir(DatasetTestUtils.getEnvironmentOutputDir(datasetPlanExecution.getEnvironment()) + filename + "/bam");
                List<String> dockerCommands = new ArrayList<>();
                for (DataSetExecutionCommand command : datasetPlanExecution.getCommands().get(filename)) {
                    dockerCommands.add(DockerUtils.buildMountPathsCommandLine(command.getImage(), command.getCommandLine()));
                }
                result.put(filename, dockerCommands);
            }
            PrintUtils.println(datasetPlanExecution.getEnvironment().getId(), PrintUtils.Color.GREEN);
            for (String filename : result.keySet()) {
                PrintUtils.println("    " + filename, PrintUtils.Color.YELLOW);
                executeFileScript(datasetPlanExecution, result, filename);
            }
        }


    }

    private void createDir(String environmentOutputDir) {
        if (!new File(environmentOutputDir).exists()) {
            try {
                Files.createDirectory(Paths.get(environmentOutputDir));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void executeFileScript(DatasetExecutionPlan datasetPlanExecution, Map<String, List<String>> result, String filename) {
        try {
            Set<PosixFilePermission> ownerWritable = PosixFilePermissions.fromString("rw-r--r--");
            FileAttribute<?> permissions = PosixFilePermissions.asFileAttribute(ownerWritable);
            Path path = Paths.get(DatasetTestUtils.getEnvironmentOutputDir(datasetPlanExecution.getEnvironment()) + "/" + filename + "/execution/" + filename + ".sh");

            if (path.toFile().exists()) {
                path.toFile().delete();
            }
            Files.createFile(path, permissions);
            FileWriter fw = new FileWriter(path.toAbsolutePath().toString(), true);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write("#!/bin/bash");
            bw.newLine();
            for (String command : result.get(filename)) {
                PrintUtils.println("    " + command);
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
            PrintUtils.println(" Executing....   " + command[1], PrintUtils.Color.YELLOW);
            Process process = Runtime.getRuntime().exec(command);
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            int exitCode = process.waitFor();
            PrintUtils.println(" ...Executed", PrintUtils.Color.YELLOW);
            if (exitCode == 0) {
                while ((line = reader.readLine()) != null) {
                    PrintUtils.println(line, PrintUtils.Color.YELLOW);
                }
                PrintUtils.println("Success!", PrintUtils.Color.GREEN);
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
