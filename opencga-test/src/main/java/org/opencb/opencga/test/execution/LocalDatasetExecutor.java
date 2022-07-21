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
import org.opencb.opencga.test.cli.options.DatasetCommandOptions;
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

        if (!configuration.getMutator().isSkip()) {
            mutate();
        } else {
            PrintUtils.println("Writing mutations SKIPPED ", PrintUtils.Color.CYAN);

        }
    }


    public void mutate() {
        if (CollectionUtils.isNotEmpty(configuration.getMutator().getMutations())) {
            PrintUtils.println("Writing mutations: ", PrintUtils.Color.CYAN,
                    configuration.getMutator().getMutations().size() + " mutators found.", PrintUtils.Color.WHITE);
            FilenameFilter filter = (f, name) -> name.endsWith(".vcf");
            for (Mutation mutation : configuration.getMutator().getMutations()) {
                if (!mutation.isSkip()) {
                    String filename = mutation.getFile();
                    for (Environment environment : configuration.getEnvs()) {
                        File vcfDir = new File(DatasetTestUtils.getVCFOutputDirPath(environment));
                        if (vcfDir.exists()) {
                            String[] files = vcfDir.list(filter);
                            for (int i = 0; i < files.length; i++) {
                                //String vcfFilename = files[i].substring(0, files[i].lastIndexOf('.'));
                                if (files[i].equals(filename)) {
                                    PrintUtils.println("Variants found for file: ", PrintUtils.Color.CYAN, files[i], PrintUtils.Color.WHITE);
                                    setVariantsInFile(mutation.getVariants(), new File(vcfDir.getAbsolutePath() + File.separator + filename));
                                }
                            }
                        }
                    }
                }
            }
        } else {
            PrintUtils.println("Writing mutations: ", PrintUtils.Color.CYAN,
                    configuration.getMutator().getMutations().size() + " mutations found.", PrintUtils.Color.WHITE);

        }
    }

    public void setVariantsInFile(List<Variant> variants, File file) {
        //get the variant as String
        //insert it in temp file
        PrintUtils.println("Writing variants: ", PrintUtils.Color.CYAN,
                variants.size() + " variants found.", PrintUtils.Color.WHITE);
        for (Variant variant : variants) {
            if (!variant.isSkip()) {
                insertStringInFile(file, variant);
            }
        }
    }


    public void insertStringInFile(File inFile, Variant variant) {
        String line = getVariantLine(variant);
        //PrintUtils.println("Trying to insert: " + line + " \n", PrintUtils.Color.CYAN, inFile.getAbsolutePath(), PrintUtils.Color.WHITE);
        try {
            Path path = Paths.get(inFile.getAbsolutePath() + "$$$$$$$$.tmp");
            Files.createFile(path);
            FileWriter fw = new FileWriter(path.toAbsolutePath().toString(), true);
            BufferedWriter bw = new BufferedWriter(fw);
            if (inFile.exists()) {
                //PrintUtils.println("File found: ", PrintUtils.Color.CYAN, inFile.getAbsolutePath(), PrintUtils.Color.WHITE);
                FileInputStream fis = new FileInputStream(inFile);
                BufferedReader in = new BufferedReader(new InputStreamReader(fis));
                String vcfLine = "";
                int i = 1;
                boolean inserted = false;
                while ((vcfLine = in.readLine()) != null) {
                    if (!inserted && isVariantLine(vcfLine, variant)) {
                        bw.write(line);
                        bw.newLine();
                        PrintUtils.println("Inserted new line: ", PrintUtils.Color.CYAN, line, PrintUtils.Color.WHITE);
                        PrintUtils.println(inFile.getAbsolutePath(), PrintUtils.Color.CYAN, ":" + i, PrintUtils.Color.WHITE);
                        PrintUtils.println("-----------------", PrintUtils.Color.CYAN);
                        inserted = true;
                    }
                    bw.write(vcfLine);
                    bw.newLine();
                    i++;
                }
                if (!inserted) {
                    bw.write(line);
                    bw.newLine();
                    PrintUtils.println("Inserted new line: ", PrintUtils.Color.CYAN, line, PrintUtils.Color.WHITE);
                    PrintUtils.println(inFile.getAbsolutePath(), PrintUtils.Color.CYAN, ":" + i, PrintUtils.Color.WHITE);
                    PrintUtils.println("-----------------", PrintUtils.Color.CYAN);
                }
                bw.flush();
                bw.close();
                in.close();
                Path targetPath = Paths.get(inFile.getAbsolutePath());
                inFile.delete();
                Files.move(path, targetPath);
            } else {
                PrintUtils.printError("ERROR writing variant to file: " + inFile.getAbsolutePath() + " does not exists");
            }

        } catch (Exception e) {
            e.printStackTrace();
            PrintUtils.printError("ERROR writing variant to file: " + e.getLocalizedMessage());
        }


    }

    private boolean isVariantLine(String thisLine, Variant variant) {
        String[] split = thisLine.split("\\t");
        if (variant.getChromosome().equals(split[0])) {
            if (Integer.parseInt(split[1]) > Integer.parseInt(variant.getPosition())) {
                //  PrintUtils.println("Inserting new line before: " + thisLine, PrintUtils.Color.CYAN);
                return true;
            }
        }
        return false;
    }

    private String getVariantLine(Variant variant) {
        String line = variant.getChromosome() + "\t";
        line += variant.getPosition() + "\t";
        line += variant.getId() + "\t";
        line += variant.getReference() + "\t";
        line += variant.getAlternate() + "\t";
        line += variant.getQuality() + "\t";
        line += variant.getFilter() + "\t";
        line += variant.getInfo() + "\t";
        line += variant.getFormat();
        for (String sample : variant.getSamples()) {
            line += "\t" + sample;
        }
        return line;
    }

    private Map<String, List<Variant>> getVariantsMap(List<Mutation> mutations) {
        Map<String, List<Variant>> res = new HashMap<>();
        for (Mutation mutation : mutations) {
            res.put(mutation.getFile(), mutation.getVariants());
        }
        return res;

    }

    private void createOutputDirs(Environment env) {
        createDir(DatasetTestUtils.getOutputDirPath(env));
        createDir(DatasetTestUtils.getEnvironmentOutputDirPath(env));
        createDir(DatasetTestUtils.getMetadataDirPath(env));
        createDir(DatasetTestUtils.getVCFOutputDirPath(env));
        createDir(DatasetTestUtils.getExecutionDirPath(env));
        createDir(DatasetTestUtils.getEnvironmentExecutionDirPath(env));
        createDir(DatasetTestUtils.getOutputBamDirPath(env));
        copyExecutionMetadata(env);
        copyTemplatesDir(env);
    }

    private void copyTemplatesDir(Environment env) {
        File templatesDir = Paths.get(DatasetTestUtils.getInputTemplatesDirPath(env)).toFile();
        if (templatesDir.exists()) {
            try {
                FileUtils.copyDirectory(templatesDir, Paths.get(DatasetTestUtils.getOutputTemplatesDirPath(env)).toFile());
                PrintUtils.println(PrintUtils.format("Directory ", PrintUtils.Color.CYAN)
                        + PrintUtils.format(templatesDir.getAbsolutePath(), PrintUtils.Color.WHITE)
                        + PrintUtils.format(" copied to ", PrintUtils.Color.CYAN)
                        + PrintUtils.format(DatasetTestUtils.getOutputTemplatesDirPath(env), PrintUtils.Color.WHITE));
            } catch (IOException e) {
                PrintUtils.printError("Error creating " + templatesDir.getAbsolutePath());
                System.exit(0);
            }
        } else {
            PrintUtils.printError("Directory " + templatesDir.getAbsolutePath() + " not exists.");
            System.exit(0);
        }
    }

    private void copyExecutionMetadata(Environment env) {
        File metadataDir = Paths.get(DatasetTestUtils.getMetadataDirPath(env)).toFile();
        File configFile = new File(DatasetCommandOptions.configFile);
        if (metadataDir.exists()) {
            try {
                File metaDataConfigFile = new File(DatasetTestUtils.getMetadataDirPath(env) + "configuration.yml");
                FileUtils.copyFile(configFile, metaDataConfigFile);
            } catch (IOException e) {
                PrintUtils.printError("Error copying " + configFile.getAbsolutePath());
                System.exit(0);
            }

        } else {
            PrintUtils.printError("Directory " + metadataDir.getAbsolutePath() + " not exists.");
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
            Path path = Paths.get(DatasetTestUtils.getEnvironmentExecutionDirPath(env) + filename + ".sh");
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
