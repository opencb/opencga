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

package org.opencb.opencga.app.cli.analysis;

import com.beust.jcommander.ParameterException;
import org.opencb.opencga.app.cli.CommandExecutor;
import org.opencb.opencga.app.cli.analysis.executors.AlignmentCommandExecutor;
import org.opencb.opencga.app.cli.analysis.executors.VariantCommandExecutor;
import org.opencb.opencga.core.common.GitRepositoryState;

/**
 * Created by imedina on 03/02/15.
 */
public class AnalysisMain {

    public static final String VERSION = GitRepositoryState.get().getBuildVersion();

    public static void main(String[] args) {
        System.exit(privateMain(args));
    }

    public static int privateMain(String[] args) {

        AnalysisCliOptionsParser cliOptionsParser = new AnalysisCliOptionsParser();
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
//                        case "expression":
//                            commandExecutor = new ExpressionCommandExecutor(cliOptionsParser.getExpressionCommandOptions());
//                            break;
//                        case "functional":
//                            commandExecutor = new FunctionalCommandExecutor(cliOptionsParser.getFunctionalCommandOptions());
//                            break;
                        case "variant":
                            commandExecutor = new VariantCommandExecutor(cliOptionsParser.getVariantCommandOptions());
                            break;
                        case "alignment":
                            commandExecutor = new AlignmentCommandExecutor(cliOptionsParser.getAlignmentCommandOptions());
                            break;
                        default:
                            System.err.printf("ERROR: not valid command passed: '" + parsedCommand + "'");
                            break;
                    }

                    if (commandExecutor != null) {
                        if (!commandExecutor.loadConfigurations()) {
                            return 1;
                        }
                        try {
                            commandExecutor.execute();
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
