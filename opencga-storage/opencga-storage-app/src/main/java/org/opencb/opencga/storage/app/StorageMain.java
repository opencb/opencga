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

package org.opencb.opencga.storage.app;

import com.beust.jcommander.ParameterException;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.storage.app.cli.CommandExecutor;
import org.opencb.opencga.storage.app.cli.client.executors.AlignmentCommandExecutor;
import org.opencb.opencga.storage.app.cli.client.ClientCliOptionsParser;
import org.opencb.opencga.storage.app.cli.client.executors.VariantCommandExecutor;

import java.io.IOException;

/**
 * Created by imedina on 02/03/15.
 */
public class StorageMain {

    public static void main(String[] args) {
        System.exit(privateMain(args));
    }

    public static int privateMain(String[] args) {
        ClientCliOptionsParser cliOptionsParser = new ClientCliOptionsParser();

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
                System.out.println("Version " + GitRepositoryState.getInstance().getBuildVersion());
                System.out.println("Git version: " + GitRepositoryState.getInstance().getBranch() + " " + GitRepositoryState.getInstance().getCommitId());
//                System.out.println(GitRepositoryState.getInstance());
                return 0;
            } else if (cliOptionsParser.getGeneralOptions().help) {
                cliOptionsParser.printUsage();
                return 0;
            } else {
                cliOptionsParser.printUsage();
                return 1;
            }
        } else {
            CommandExecutor commandExecutor = null;
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
//                        case "feature":
//                            commandExecutor = new IndexAlignmentsCommandExecutor(cliOptionsParser.getIndexAlignmentsCommandOptions());
//                            break;
                        case "alignment":
                            commandExecutor = new AlignmentCommandExecutor(cliOptionsParser.getAlignmentCommandOptions());
                            break;
                        case "variant":
                            commandExecutor = new VariantCommandExecutor(cliOptionsParser.getVariantCommandOptions());
                            break;
                        default:
                            System.out.printf("ERROR: not valid command passed: '" + parsedCommand + "'");
                            break;
                    }

                    if (commandExecutor != null) {
                        try {
                            commandExecutor.loadStorageConfiguration();
                        } catch (IOException ex) {
                            if (commandExecutor.getLogger() == null) {
                                ex.printStackTrace();
                            } else {
                                commandExecutor.getLogger().error("Error reading OpenCGA Storage configuration: " + ex.getMessage());
                            }
                        }
                        try {
                            commandExecutor.execute();
                        } catch (IOException ex) {
                            ex.printStackTrace();
                            return 1;
                        } catch (Exception e) {
                            e.printStackTrace();
                            return 1;
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
