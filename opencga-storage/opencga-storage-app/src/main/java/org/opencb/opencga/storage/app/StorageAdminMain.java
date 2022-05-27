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
import org.opencb.commons.utils.GitRepositoryState;
import org.opencb.opencga.storage.app.cli.CommandExecutor;
import org.opencb.opencga.storage.app.cli.server.AdminCliOptionsParser;
import org.opencb.opencga.storage.app.cli.server.executors.BenchmarkCommandExecutor;
import org.opencb.opencga.storage.app.cli.server.executors.ServerCommandExecutor;

import java.io.IOException;

/**
 * Created by imedina on 02/03/15.
 */
public class StorageAdminMain {

    public static void main(String[] args) {
        System.exit(privateMain(args));
    }

    public static int privateMain(String[] args) {
        AdminCliOptionsParser adminCliOptionsParser = new AdminCliOptionsParser();

        try {
            adminCliOptionsParser.parse(args);
        } catch (ParameterException e) {
            System.err.println(e.getMessage());
            adminCliOptionsParser.printUsage();
            return 1;
        }

        String parsedCommand = adminCliOptionsParser.getCommand();
        if (parsedCommand == null || parsedCommand.isEmpty()) {
            if (adminCliOptionsParser.getGeneralOptions().version) {
                System.out.println("Version " + GitRepositoryState.get().getBuildVersion());
                System.out.println("Git version: " + GitRepositoryState.get().getBranch() + " " + GitRepositoryState.get().getCommitId());
                return 0;
            } else if (adminCliOptionsParser.getGeneralOptions().help) {
                adminCliOptionsParser.printUsage();
                return 0;
            } else {
                adminCliOptionsParser.printUsage();
                return 1;
            }
        } else {
            CommandExecutor commandExecutor = null;
            // Check if any command -h option is present
            if (adminCliOptionsParser.isHelp()) {
                adminCliOptionsParser.printUsage();
                return 0;
            } else {
                String parsedSubCommand = adminCliOptionsParser.getSubCommand();
                if (parsedSubCommand == null || parsedSubCommand.isEmpty()) {
                    adminCliOptionsParser.printUsage();
                } else {
                    switch (parsedCommand) {
                        case "server":
                            commandExecutor = new ServerCommandExecutor(adminCliOptionsParser.getServerCommandOptions());
                            break;
//                        case "grpc":
//                            commandExecutor = new GrpcCommandExecutor(adminCliOptionsParser.getGrpcCommandOptions());
//                            break;
                        case "benchmark":
                            commandExecutor = new BenchmarkCommandExecutor(adminCliOptionsParser.getBenchmarkCommandOptions());
                            break;
                        default:
                            System.out.printf("ERROR: not valid command passed: '" + parsedCommand + "'");
                            break;
                    }

                    if (commandExecutor != null) {
                        try {
                            commandExecutor.loadStorageConfiguration();
                            commandExecutor.execute();
                        } catch (IOException ex) {
                            if (commandExecutor.getLogger() == null) {
                                ex.printStackTrace();
                            } else {
                                commandExecutor.getLogger().error("Error reading OpenCGA Storage configuration: " + ex.getMessage());
                            }
                            return 1;
                        } catch (Exception e) {
                            e.printStackTrace();
                            return 1;
                        }
                    } else {
                        adminCliOptionsParser.printUsage();
                        return 1;
                    }
                }
            }
        }
        return 0;
    }
}
