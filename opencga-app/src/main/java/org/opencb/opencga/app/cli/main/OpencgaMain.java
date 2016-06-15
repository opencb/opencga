/*
 * Copyright 2015 OpenCB
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
import org.opencb.opencga.app.cli.main.executors.*;
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
//                        case "catalog":
//                            commandExecutor = new CatalogCommandExecutor(cliOptionsParser.getCatalogCommandOptions());
//                            break;
                        case "users":
                            commandExecutor = new UsersCommandExecutor(cliOptionsParser.getUsersCommandOptions());
                            break;
//                        case "audit":
//                            commandExecutor = new AuditCommandExecutor(cliOptionsParser.getAuditCommandOptions());
//                            break;
//                        case "server":
//                            commandExecutor = new ServerCommandExecutor(cliOptionsParser.getServerCommandOptions());
//                            break;
                        case "projects":
                            commandExecutor = new ProjectsCommandExecutor(cliOptionsParser.getProjectCommandOptions());
                            break;
                        case "studies":
                            commandExecutor = new StudiesCommandExecutor(cliOptionsParser.getStudyCommandOptions());
                            break;
                        case "files":
                            commandExecutor = new FilesCommandExecutor(cliOptionsParser.getFileCommands());
                            break;
                        case "cohorts":
                            commandExecutor = new CohortsCommandExecutor(cliOptionsParser.getCohortCommands());
                            break;
                        case "samples":
                            commandExecutor = new SamplesCommandExecutor(cliOptionsParser.getSampleCommands());
                            break;
                        case "jobs":
                            commandExecutor = new JobsCommandExecutor(cliOptionsParser.getJobsCommands());
                            break;
                        case "individuals":
                            commandExecutor = new IndividualsCommandExecutor(cliOptionsParser.getIndividualsCommands());
                        case "tools":
                            commandExecutor = new ToolsCommandExecutor(cliOptionsParser.getToolCommands());
                            break;
                        default:
                            System.out.printf("ERROR: not valid command passed: '" + parsedCommand + "'");
                            break;
                    }

                    if (commandExecutor != null) {
//                        commandExecutor.loadConfigurations();
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
