/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.app.cli.main;

import com.beust.jcommander.ParameterException;
import org.apache.commons.lang3.ArrayUtils;
import org.opencb.opencga.app.cli.CommandExecutor;
import org.opencb.opencga.app.cli.main.executors.*;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by imedina on 27/05/16.
 */
public class OpencgaMain {

    public static final String VERSION = GitRepositoryState.get().getBuildVersion();

    public static void main(String[] args) {

        if (args.length > 3 && "users".equals(args[0]) && "login".equals(args[1])) {
            // Check there is no --help
            boolean passwordRequired = true;
            for (String arg : args) {
                switch (arg) {
                    case "--password":
                    case "-p":
                        Logger logger = LoggerFactory.getLogger(OpencgaMain.class);
                        logger.warn("Argument " + arg + " no longer required. It will be forbidden in future releases.");
                    case "--help":
                    case "-h":
                        passwordRequired = false;
                        break;
                    default:
                        break;
                }
            }
            if (passwordRequired) {
                args = ArrayUtils.addAll(args, "--password");
            }
        }

        OpencgaCliOptionsParser cliOptionsParser = new OpencgaCliOptionsParser();
        try {
            cliOptionsParser.parse(args);
        } catch (ParameterException e) {
            System.err.println(e.getMessage());
            cliOptionsParser.printUsage();
            System.exit(1);
        }

        String parsedCommand = cliOptionsParser.getCommand();
        if (parsedCommand == null || parsedCommand.isEmpty()) {
            if (cliOptionsParser.getGeneralOptions().version) {
                System.out.println("Version " + GitRepositoryState.get().getBuildVersion());
                System.out.println("Git version: " + GitRepositoryState.get().getBranch() + " " + GitRepositoryState.get().getCommitId());
                System.exit(0);
            } else if (cliOptionsParser.getGeneralOptions().help) {
                cliOptionsParser.printUsage();
                System.exit(0);
            } else {
                cliOptionsParser.printUsage();
                System.exit(1);
            }
        } else {
            CommandExecutor commandExecutor = null;
            // Check if any command -h option is present
            if (cliOptionsParser.isHelp()) {
                cliOptionsParser.printUsage();
                System.exit(0);
            } else {
                String parsedSubCommand = cliOptionsParser.getSubCommand();
                if (parsedSubCommand == null || parsedSubCommand.isEmpty()) {
                    cliOptionsParser.printUsage();
                } else {
                    switch (parsedCommand) {
                        case "users":
                            commandExecutor = new UserCommandExecutor(cliOptionsParser.getUserCommandOptions());
                            break;
                        case "projects":
                            commandExecutor = new ProjectCommandExecutor(cliOptionsParser.getProjectCommandOptions());
                            break;
                        case "studies":
                            commandExecutor = new StudyCommandExecutor(cliOptionsParser.getStudyCommandOptions());
                            break;
                        case "files":
                            commandExecutor = new FileCommandExecutor(cliOptionsParser.getFileCommandOptions());
                            break;
                        case "jobs":
                            commandExecutor = new JobCommandExecutor(cliOptionsParser.getJobCommandOptions());
                            break;
                        case "individuals":
                            commandExecutor = new IndividualCommandExecutor(cliOptionsParser.getIndividualCommandOptions());
                            break;
                        case "samples":
                            commandExecutor = new SampleCommandExecutor(cliOptionsParser.getSampleCommandOptions());
                            break;
                        case "cohorts":
                            commandExecutor = new CohortCommandExecutor(cliOptionsParser.getCohortCommandOptions());
                            break;
                        case "panels":
                            commandExecutor = new DiseasePanelCommandExecutor(cliOptionsParser.getDiseasePanelCommandOptions());
                            break;
                        case "families":
                            commandExecutor = new FamilyCommandExecutor(cliOptionsParser.getFamilyCommandOptions());
                            break;
                        case "alignments":
                            commandExecutor = new AlignmentCommandExecutor(cliOptionsParser.getAlignmentCommandOptions());
                            break;
                        case "variant":
                            commandExecutor = new VariantCommandExecutor(cliOptionsParser.getVariantCommandOptions());
                            break;
                        case "clinical":
                            commandExecutor = new ClinicalAnalysisCommandExecutor(cliOptionsParser.getClinicalAnalysisCommandOptions());
                            break;
                        case "variantoperation":
                            commandExecutor = new VariantOperationCommandExecutor(cliOptionsParser.getVariantOperationCommandOptions());
                            break;
                        case "meta":
                            commandExecutor = new MetaCommandExecutor(cliOptionsParser.getMetaCommandOptions());
                            break;
                        default:
                            System.out.printf("ERROR: not valid command passed: '" + parsedCommand + "'");
                            break;
                    }

                    if (commandExecutor != null) {
                        try {
                            commandExecutor.execute();
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.exit(1);
                        }
                    } else {
                        cliOptionsParser.printUsage();
                        System.exit(1);
                    }
                }
            }
        }
    }
}