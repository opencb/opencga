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
import org.opencb.opencga.storage.app.cli.*;

import java.io.IOException;

/**
 * Created by imedina on 02/03/15.
 */
public class StorageMain {

    public static final String VERSION = "0.6.0-SNAPSHOT";

    public static void main(String[] args) {

        CliOptionsParser cliOptionsParser = new CliOptionsParser();
        try {
            cliOptionsParser.parse(args);
        } catch (ParameterException e) {
            System.err.println(e.getMessage());
            cliOptionsParser.printUsage();
            System.exit(1);
        }

        String parsedCommand = cliOptionsParser.getCommand();
        if(parsedCommand == null || parsedCommand.isEmpty()) {
            if(cliOptionsParser.getGeneralOptions().version) {
                System.out.println("Version " + VERSION);
                System.exit(0);
            } else if(cliOptionsParser.getGeneralOptions().help) {
                cliOptionsParser.printUsage();
                System.exit(0);
            } else {
                cliOptionsParser.printUsage();
                System.exit(1);
            }
        } else {
            CommandExecutor commandExecutor = null;
            // Check if any command -h option is present
            if(cliOptionsParser.isHelp()) {
                cliOptionsParser.printUsage();
                System.exit(0);
            } else {
                switch (parsedCommand) {
                    case "create-accessions":
                        commandExecutor = new CreateAccessionsCommandExecutor(cliOptionsParser.getCreateAccessionsCommandOption());
                        break;
                    case "index-alignments":
                        commandExecutor = new IndexAlignmentsCommandExecutor(cliOptionsParser.getIndexAlignmentsCommandOptions());
                        break;
                    case "index-variants":
                        commandExecutor = new IndexVariantsCommandExecutor(cliOptionsParser.getIndexVariantsCommandOptions());
                        break;
                    case "fetch-alignments":
                        commandExecutor = new FetchAlignmentsCommandExecutor(cliOptionsParser.getQueryAlignmentsCommandOptions());
                        break;
                    case "fetch-variants":
                        commandExecutor = new FetchVariantsCommandExecutor(cliOptionsParser.getQueryVariantsCommandOptions());
                        break;
                    case "annotate-variants":
                        commandExecutor = new AnnotateVariantsCommandExecutor(cliOptionsParser.getAnnotateVariantsCommandOptions());
                        break;
                    case "stats-variants":
                        commandExecutor = new StatsVariantsCommandExecutor(cliOptionsParser.getStatsVariantsCommandOptions());
                        break;
                    default:
                        System.out.printf("ERROR: not valid command passed: '" + parsedCommand + "'");
                        break;
                }

                if (commandExecutor != null) {
                    try {
                        commandExecutor.loadStorageConfiguration();
                    } catch (IOException ex) {
                        commandExecutor.getLogger().error("Error reading OpenCGA Storage configuration: " + ex.getMessage());
                        System.exit(1);
                    }
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
