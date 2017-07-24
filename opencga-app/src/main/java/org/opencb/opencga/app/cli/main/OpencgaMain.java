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
import org.opencb.opencga.app.cli.CommandExecutor;
import org.opencb.opencga.app.cli.main.executors.analysis.AlignmentCommandExecutor;
import org.opencb.opencga.app.cli.main.executors.analysis.VariantCommandExecutor;
import org.opencb.opencga.app.cli.main.executors.catalog.*;
import org.opencb.opencga.core.common.GitRepositoryState;

/**
 * Created by imedina on 27/05/16.
 */
public class OpencgaMain {

    public static final String VERSION = GitRepositoryState.get().getBuildVersion();

    public static void main(String[] args) {

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
                            commandExecutor = new UserCommandExecutor(cliOptionsParser.getUsersCommandOptions());
                            break;
                        case "projects":
                            commandExecutor = new ProjectCommandExecutor(cliOptionsParser.getProjectCommandOptions());
                            break;
                        case "studies":
                            commandExecutor = new StudyCommandExecutor(cliOptionsParser.getStudyCommandOptions());
                            break;
                        case "files":
                            commandExecutor = new FileCommandExecutor(cliOptionsParser.getFileCommands());
                            break;
                        case "jobs":
                            commandExecutor = new JobCommandExecutor(cliOptionsParser.getJobsCommands());
                            break;
                        case "individuals":
                            commandExecutor = new IndividualCommandExecutor(cliOptionsParser.getIndividualsCommands());
                            break;
                        case "samples":
                            commandExecutor = new SampleCommandExecutor(cliOptionsParser.getSampleCommands());
                            break;
                        case "cohorts":
                            commandExecutor = new CohortCommandExecutor(cliOptionsParser.getCohortCommands());
                            break;
                        case "panels":
                            commandExecutor = new PanelCommandExecutor(cliOptionsParser.getPanelCommands());
                            break;
                        case "families":
                            commandExecutor = new FamilyCommandExecutor(cliOptionsParser.getFamilyCommands());
                            break;
                        case "tools":
                            commandExecutor = new ToolCommandExecutor(cliOptionsParser.getToolCommands());
                            break;
                        case "variables":
                            commandExecutor = new VariableCommandExecutor(cliOptionsParser.getVariableCommands());
                            break;
                        case "alignments":
                            commandExecutor = new AlignmentCommandExecutor(cliOptionsParser.getAlignmentCommands());
                            break;
                        case "variant":
                            commandExecutor = new VariantCommandExecutor(cliOptionsParser.getVariantCommands());
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