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

package org.opencb.opencga.storage.app;

import com.beust.jcommander.ParameterException;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.storage.app.cli.CommandExecutor;
import org.opencb.opencga.storage.app.cli.server.GrpcCommandExecutor;
import org.opencb.opencga.storage.app.cli.server.RestCommandExecutor;
import org.opencb.opencga.storage.app.cli.server.ServerCliOptionsParser;

import java.io.IOException;

/**
 * Created by imedina on 02/03/15.
 */
public class StorageServerMain {

    public static void main(String[] args) {
        System.exit(privateMain(args));
    }

    public static int privateMain(String[] args) {
        ServerCliOptionsParser serverCliOptionsParser = new ServerCliOptionsParser();

        try {
            serverCliOptionsParser.parse(args);
        } catch (ParameterException e) {
            System.err.println(e.getMessage());
            serverCliOptionsParser.printUsage();
            return 1;
        }

        String parsedCommand = serverCliOptionsParser.getCommand();
        if (parsedCommand == null || parsedCommand.isEmpty()) {
            if (serverCliOptionsParser.getGeneralOptions().version) {
                System.out.println("Version " + GitRepositoryState.get().getBuildVersion());
                System.out.println("Git version: " + GitRepositoryState.get().getBranch() + " " + GitRepositoryState.get().getCommitId());
                return 0;
            } else if (serverCliOptionsParser.getGeneralOptions().help) {
                serverCliOptionsParser.printUsage();
                return 0;
            } else {
                serverCliOptionsParser.printUsage();
                return 1;
            }
        } else {
            CommandExecutor commandExecutor = null;
            // Check if any command -h option is present
            if (serverCliOptionsParser.isHelp()) {
                serverCliOptionsParser.printUsage();
                return 0;
            } else {
                String parsedSubCommand = serverCliOptionsParser.getSubCommand();
                if (parsedSubCommand == null || parsedSubCommand.isEmpty()) {
                    serverCliOptionsParser.printUsage();
                } else {
                    switch (parsedCommand) {
                        case "rest":
                            commandExecutor = new RestCommandExecutor(serverCliOptionsParser.getRestCommandOptions());
                            break;
                        case "grpc":
                            commandExecutor = new GrpcCommandExecutor(serverCliOptionsParser.getGrpcCommandOptions());
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
                        serverCliOptionsParser.printUsage();
                        return 1;
                    }
                }
            }
        }
        return 0;
    }

}
