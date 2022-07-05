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

package org.opencb.opencga.app.cli.internal;

import com.beust.jcommander.ParameterException;
import org.opencb.commons.utils.GitRepositoryState;
import org.opencb.opencga.app.cli.internal.executors.*;
import org.opencb.opencga.app.cli.main.impl.CommandExecutorImpl;

import java.io.IOException;

/**
 * Created by imedina on 03/02/15.
 */
public class InternalMain {

    public static final String VERSION = GitRepositoryState.get().getBuildVersion();

    public static void main(String[] args) {
        System.exit(privateMain(args));
    }

    public static int privateMain(String[] args) {

        InternalCliOptionsParser cliOptionsParser = new InternalCliOptionsParser();
        try {
            cliOptionsParser.parse(args);
        } catch (ParameterException e) {
            System.err.println(e.getMessage());
            cliOptionsParser.printUsage();
            return 1;
        }

        String parsedCommand = cliOptionsParser.getCommand();
        if (parsedCommand == null || parsedCommand.isEmpty()) {
            if (cliOptionsParser.getGeneralOptions().version) {
                System.out.println("Version " + GitRepositoryState.get().getBuildVersion());
                System.out.println("Git version: " + GitRepositoryState.get().getBranch() + " " + GitRepositoryState.get().getCommitId());
                return 0;
            } else if (cliOptionsParser.getGeneralOptions().help) {
                cliOptionsParser.printUsage();
                return 0;
            } else {
                cliOptionsParser.printUsage();
                return 1;
            }
        } else {
            CommandExecutorImpl commandExecutor = null;
            // Check if any command -h option is present
            if (cliOptionsParser.isHelp()) {
                cliOptionsParser.printUsage();
                return 0;
            } else {
                String parsedSubCommand = cliOptionsParser.getSubCommand();
                if (parsedSubCommand == null || parsedSubCommand.isEmpty()) {
                    cliOptionsParser.printUsage();
                } else {
                    switch (parsedCommand) {
//                        case "expression":
//                            commandExecutor = new ExpressionCommandExecutor(cliOptionsParser.getExpressionCommandOptions());
//                            break;
//                        case "functional":
//                            commandExecutor = new FunctionalCommandExecutor(cliOptionsParser.getFunctionalCommandOptions());
//                            break;
                        case "variant":
                            commandExecutor = new VariantInternalCommandExecutor(cliOptionsParser.getVariantCommandOptions());
                            break;
                        case "alignment":
                            commandExecutor = new AlignmentCommandExecutor(cliOptionsParser.getAlignmentCommandOptions());
                            break;
                        case "tools":
                            commandExecutor = new ToolsCommandExecutor(cliOptionsParser.getToolsCommandOptions());
                            break;
                        case "clinical":
                            commandExecutor = new ClinicalCommandExecutor(cliOptionsParser.getClinicalCommandOptions());
                            break;
                        case "files":
                            commandExecutor = new FileCommandExecutor(cliOptionsParser.getFileCommandOptions());
                            break;
                        case "samples":
                            commandExecutor = new SampleCommandExecutor(cliOptionsParser.getSampleCommandOptions());
                            break;
                        case "cohorts":
                            commandExecutor = new CohortCommandExecutor(cliOptionsParser.getCohortCommandOptions());
                            break;
                        case "individuals":
                            commandExecutor = new IndividualCommandExecutor(cliOptionsParser.getIndividualCommandOptions());
                            break;
                        case "families":
                            commandExecutor = new FamilyCommandExecutor(cliOptionsParser.getFamilyCommandOptions());
                            break;
                        case "jobs":
                            commandExecutor = new JobCommandExecutor(cliOptionsParser.getJobCommandOptions());
                            break;
                        case "panels":
                            commandExecutor = new DiseasePanelInternalCommandExecutor(cliOptionsParser.getPanelInternalCommandOptions());
                            break;
                        case "studies":
                            commandExecutor = new StudyCommandExecutor(cliOptionsParser.getStudyCommandOptions());
                            break;
                        default:
                            System.err.printf("ERROR: not valid command passed: '" + parsedCommand + "'");
                            break;
                    }

                    if (commandExecutor != null) {
                        try {
                            commandExecutor.loadConfiguration();
                            commandExecutor.loadStorageConfiguration();
                            commandExecutor.execute();
                        } catch (IOException e) {
                            System.err.println("Configuration files not found: " + e);
                            e.printStackTrace();
                            System.exit(1);
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.exit(1);
                        }
                    } else {
                        cliOptionsParser.printUsage();
                        return 1;
                    }
                }

            }
        }
        return 0;
    }

}
