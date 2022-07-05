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

package org.opencb.opencga.app.cli.admin;

import com.beust.jcommander.ParameterException;
import org.opencb.commons.utils.GitRepositoryState;
import org.opencb.opencga.app.cli.admin.executors.*;
import org.opencb.opencga.app.cli.main.impl.CommandExecutorImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by imedina on 03/02/15.
 */
public class AdminMain {

    public static final String VERSION = GitRepositoryState.get().getBuildVersion();

    public static void main(String[] args) {

        AdminCliOptionsParser cliOptionsParser = new AdminCliOptionsParser();

        // Add password parameter
        if (args.length > 1) {
            // Check there is no --help
            boolean passwordRequired = true;
            for (String arg : args) {
                switch (arg) {
                    case "--password":
                    case "-p":
                        Logger logger = LoggerFactory.getLogger(AdminMain.class);
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
                args = org.apache.commons.lang3.ArrayUtils.addAll(args, "--password");
            }
        }

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
            CommandExecutorImpl commandExecutor = null;
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
                        case "catalog":
                            commandExecutor = new CatalogCommandExecutor(cliOptionsParser.getCatalogCommandOptions());
                            break;
                        case "users":
                            commandExecutor = new UsersCommandExecutor(cliOptionsParser.getUsersCommandOptions());
                            break;
                        case "audit":
                            commandExecutor = new AuditCommandExecutor(cliOptionsParser.getAuditCommandOptions());
                            break;
                        case "server":
                            commandExecutor = new ServerCommandExecutor(cliOptionsParser.getServerCommandOptions());
                            break;
                        case "meta":
                            commandExecutor = new MetaCommandExecutor(cliOptionsParser.getMetaCommandOptions());
                            break;
                        case "panel":
                            commandExecutor = new PanelCommandExecutor(cliOptionsParser.getPanelCommandOptions());
                            break;
                        case "migration":
                            commandExecutor = new MigrationCommandExecutor(cliOptionsParser.getMigrationCommandOptions());
                            break;
                        default:
                            System.out.printf("ERROR: not valid command passed: '%s'", parsedCommand);
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
                        System.exit(1);
                    }
                }

            }
        }
    }

}
