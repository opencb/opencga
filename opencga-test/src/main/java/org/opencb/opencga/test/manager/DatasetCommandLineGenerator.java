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

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.opencb.commons.utils.PrintUtils;
import org.opencb.opencga.test.config.Caller;
import org.opencb.opencga.test.config.Configuration;
import org.opencb.opencga.test.config.Environment;
import org.opencb.opencga.test.execution.models.DataSetExecutionCommand;
import org.opencb.opencga.test.execution.models.DatasetExecutionFile;
import org.opencb.opencga.test.execution.models.DatasetExecutionPlan;
import org.opencb.opencga.test.utils.DatasetTestUtils;
import org.opencb.opencga.test.utils.OpencgaLogger;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Level;

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

    public static List<String> findAllBamFileNamesInFolder(File folder) {
        List<String> res = new ArrayList<>();
        for (File file : Objects.requireNonNull(folder.listFiles())) {
            if (!file.isDirectory() && file.getName().endsWith(".sam")) {
                res.add(file.getName());
            }
        }
        return res;
    }

    public static File[] dirListByAscendingDate(File folder, FilenameFilter filter) {
        if (!folder.isDirectory()) {
            return null;
        }
        File files[] = folder.listFiles(filter);
        Arrays.sort(files, (o1, o2) -> Long.valueOf(o2.lastModified()).compareTo
                (o1.lastModified()));
        return files;
    }

    /**
     * Allows to only process a list of given environments.
     *
     * @param resume
     * @return CLIs Map as a list of command lines by environment
     * @throws IOException
     */
    public List<DatasetExecutionPlan> generateCommandLines(boolean resume) throws IOException {
        List<Environment> environments = configuration.getEnvs();
        OpencgaLogger.printLog("Processing the following environments: " + environments.toString(), Level.INFO);
        List<DatasetExecutionPlan> datasetPlanExecutions = new LinkedList<>();
        for (Environment environment : environments) {
            DatasetExecutionPlan datasetPlanExecution = new DatasetExecutionPlan(environment);
            List<DatasetExecutionFile> commands = new LinkedList<>();
            if (DatasetTestUtils.areSkippedAllCallers(environment)) {
                //If callers are disabled input vcf folder should exist
                File vcfDir = Paths.get(DatasetTestUtils.getInputVCFDirPath(environment)).toFile();
                File templatesDir = Paths.get(DatasetTestUtils.getInputTemplatesDirPath(environment)).toFile();
                if (vcfDir.exists()) {
                    FileUtils.copyDirectory(vcfDir, Paths.get(DatasetTestUtils.getVCFOutputDirPath(environment)).toFile());
                    FileUtils.copyDirectory(templatesDir, Paths.get(DatasetTestUtils.getOutputTemplatesDirPath(environment)).toFile());
                    PrintUtils.println("Directory " + templatesDir.getAbsolutePath() + " copied to " + DatasetTestUtils.getOutputTemplatesDirPath(environment), PrintUtils.Color.CYAN);
                    PrintUtils.println("Directory " + vcfDir.getAbsolutePath() + " copied to " + DatasetTestUtils.getVCFOutputDirPath(environment), PrintUtils.Color.CYAN);
                } else {
                    PrintUtils.printError("Directory " + vcfDir.getAbsolutePath() + " not exists.");
                    System.exit(0);
                }
            } else {
                List<String> filenames = new ArrayList<>();
                if (environment.getAligner().isSkip()) {
                    //If aligner are disabled input bam folder must exist
                    File bamDir = Paths.get(DatasetTestUtils.getInputBamDirPath(environment)).toFile();
                    if (bamDir.exists()) {
                        FileUtils.copyDirectory(bamDir, Paths.get(DatasetTestUtils.getOutputBamDirPath(environment)).toFile());
                        PrintUtils.println("Directory " + bamDir.getAbsolutePath() + " copied to " + DatasetTestUtils.getOutputBamDirPath(environment), PrintUtils.Color.CYAN);
                    } else {
                        PrintUtils.printError("Directory " + bamDir.getAbsolutePath() + " not exists.");
                        System.exit(0);
                    }
                    filenames = findAllBamFileNamesInFolder(bamDir);
                } else {
                    File datasetDir = new File(DatasetTestUtils.getInputFastqDirPath(environment));
                    filenames = findAllFileNamesInFolder(datasetDir);
                    Collections.sort(filenames);
                }
                for (int i = 0; i < filenames.size(); i++) {
                    DatasetExecutionFile datasetExecutionFile = new DatasetExecutionFile();
                    List<String> outputFilenames = new ArrayList<>();
                    List<DataSetExecutionCommand> commandLines = new LinkedList<>();
                    String filename = filenames.get(i).substring(0, filenames.get(i).indexOf('.'));
                    filename = filename.replaceAll(" ", "_");
                    if (environment.getAligner() != null && !environment.getAligner().isSkip()) {
                        // Generate command line for the Aligner, WARNING!! filenames index is incremented two times if is Paired-End enabled
                        String command = getAlignerCommandLine(environment, filename).replace("${FASTQ1}", DatasetTestUtils.getInputFastqDirPath(environment) + filenames.get(i));
                        if (environment.getDataset().isPaired()) {
                            command = command.replace("${FASTQ2}", DatasetTestUtils.getInputFastqDirPath(environment) + filenames.get(++i));
                        }
                        commandLines.add(new DataSetExecutionCommand().setCommandLine(command).setImage(environment.getAligner().getImage()));
                        // Adding samtools command lines
                        List<String> samtoolsCommand = DatasetTestUtils.getSamtoolsCommands(DatasetTestUtils.getOutputBamDirPath(environment) + filename);
                        for (String c : samtoolsCommand) {
                            commandLines.add(new DataSetExecutionCommand().setCommandLine(c).setImage(environment.getAligner().getImage()));
                        }
                    }
                    // Adding caller command lines if it isn't skipped
                    List<Caller> callers = environment.getCallers();
                    for (Caller caller : callers) {
                        if (!caller.isSkip()) {
                            String outputFilename = caller.getName().replaceAll(" ", "_") + "_" + filename + ".vcf";
                            outputFilenames.add(outputFilename + ".idx");
                            String callerCommand = getVariantCallerCommandLine(environment, caller.getCommand(), caller.getParams(), filename,
                                    outputFilename);
                            commandLines.add(new DataSetExecutionCommand().setCommandLine(callerCommand).setImage(caller.getImage()));
                        }
                    }

                    datasetExecutionFile.setInputFilename(filename);
                    datasetExecutionFile.setCommands(commandLines);
                    datasetExecutionFile.setOutputFilenames(outputFilenames);
                    if (resume) {
                        if (!isExecutedFile(Paths.get(DatasetTestUtils.getVCFOutputDirPath(environment)).toFile(), outputFilenames)) {
                            commands.add(datasetExecutionFile);
                        }
                    } else {
                        commands.add(datasetExecutionFile);
                    }
                }


                datasetPlanExecution.setDatasetExecutionFiles(commands);
                datasetPlanExecutions.add(datasetPlanExecution);
            }
        }
        return datasetPlanExecutions;
    }

    private boolean isExecutedFile(File dir, List<String> outputFilenames) {
        if (!dir.exists()) {
            return false;
        }
        FilenameFilter filter = (f, name) -> name.endsWith(".idx");
        String[] files = dir.list(filter);
        if (files.length == 0) {
            return false;
        }
        File[] filesSorted = dirListByAscendingDate(dir, filter);
        String lastModifiedFilename = filesSorted[0].getName();
        OpencgaLogger.printLog("Last modified .idx file " + lastModifiedFilename, Level.INFO);
        String[] okFiles = ArrayUtils.remove(files, ArrayUtils.indexOf(files, lastModifiedFilename));
        OpencgaLogger.printLog(ArrayUtils.toString(okFiles), Level.INFO);
        OpencgaLogger.printLog("outputFilenames::: " + outputFilenames, Level.INFO);
        boolean res = true;
        for (String filename : outputFilenames) {
            if (!ArrayUtils.contains(okFiles, filename)) {
                res = false;
            }
        }
        OpencgaLogger.printLog("res::: " + res, Level.INFO);

        return res;
    }

    /**
     * Generate the command lines for Aligner adding params to command and replacing environment variables.
     *
     * @param environment
     * @param command
     * @param params
     * @param filename
     * @param name
     * @return
     */
    private String getVariantCallerCommandLine(Environment environment, String command, List<String> params, String filename, String name) {
        String param = String.join(" ", params);
        command = command.replace("${PARAMS}", param);
        command = command.replace("${INDEX}", environment.getReference().getIndex());
        command = command.replace("${OUTPUT}", DatasetTestUtils.getVCFOutputDirPath(environment) + name);
        command = command.replace("${BAM}", DatasetTestUtils.getOutputBamDirPath(environment) + filename + ".sorted.bam");
        command = command.replace("${REFERENCE.PATH}", environment.getReference().getPath());
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
        command = command.replace("${OUTPUT}", DatasetTestUtils.getOutputBamDirPath(environment) + "${FASTQNAME}.sam");
        command = command.replace("${FASTQNAME}", filename);
        return command;
    }
}
