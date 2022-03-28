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

package org.opencb.opencga.storage.app.cli.server;

import com.beust.jcommander.*;
import org.opencb.commons.utils.CommandLineUtils;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.storage.app.cli.GeneralCliOptions;
import org.opencb.opencga.storage.app.cli.server.options.BenchmarkCommandOptions;
import org.opencb.opencga.storage.app.cli.server.options.ServerCommandOptions;

import java.util.Map;

/**
 * Created by imedina on 02/03/15.
 */
public class AdminCliOptionsParser extends GeneralCliOptions {

    public final ServerCommandOptions serverCommandOptions;
    public final BenchmarkCommandOptions benchmarkCommandOptions;

    public AdminCliOptionsParser() {

        serverCommandOptions = new ServerCommandOptions(this.commonOptions, this.jcommander);
        jcommander.addCommand("server", serverCommandOptions);
        JCommander serverSubCommands = jcommander.getCommands().get("server");
        serverSubCommands.addCommand("rest", serverCommandOptions.restServerCommandOptions);
        serverSubCommands.addCommand("grpc", serverCommandOptions.grpcServerCommandOptions);

        benchmarkCommandOptions = new BenchmarkCommandOptions(this.commonOptions, this.jcommander);
        jcommander.addCommand("benchmark", benchmarkCommandOptions);
        JCommander benchmarkSubCommands = jcommander.getCommands().get("benchmark");
        benchmarkSubCommands.addCommand("variant", benchmarkCommandOptions.variantBenchmarkCommandOptions);
        benchmarkSubCommands.addCommand("alignment", benchmarkCommandOptions.alignmentBenchmarkCommandOptions);
    }

//    //    private final IndexSequenceCommandOptions indexSequenceCommandOptions;
//    private RestCommandOptions restCommandOptions;
//    private GrpcCommandOptions grpcCommandOptions;
//
//
//    public AdminCliOptionsParser() {
//
//        restCommandOptions = new RestCommandOptions();
//        jcommander.addCommand("rest", restCommandOptions);
//        JCommander alignmentSubCommands = jcommander.getCommands().get("rest");
//        alignmentSubCommands.addCommand("start", restCommandOptions.restStartCommandOptions);
//        alignmentSubCommands.addCommand("stop", restCommandOptions.restStopCommandOptions);
//        alignmentSubCommands.addCommand("status", restCommandOptions.restStatusCommandOptions);
//
//        grpcCommandOptions = new GrpcCommandOptions();
//        jcommander.addCommand("grpc", grpcCommandOptions);
//        JCommander variantSubCommands = jcommander.getCommands().get("grpc");
//        variantSubCommands.addCommand("start", grpcCommandOptions.grpcStartCommandOptions);
//        variantSubCommands.addCommand("stop", grpcCommandOptions.grpcStopCommandOptions);
//        variantSubCommands.addCommand("querystatus", grpcCommandOptions.grpcStatusCommandOptions);
//
//    }

    public void printUsage() {
        String parsedCommand = getCommand();
        if (parsedCommand.isEmpty()) {
            System.err.println("");
            System.err.println("Program:     OpenCGA Storage Admin (OpenCB)");
            System.err.println("Version:     " + GitRepositoryState.get().getBuildVersion());
            System.err.println("Git commit:  " + GitRepositoryState.get().getCommitId());
            System.err.println("Description: Big Data platform for processing and analysing NGS data");
            System.err.println("");
            System.err.println("Usage:       opencga-storage-admin.sh [-h|--help] [--version] <command> [options]");
            System.err.println("");
            System.err.println("Commands:");
            printMainUsage();
            System.err.println("");
        } else {
            String parsedSubCommand = getSubCommand();
            if (parsedSubCommand.isEmpty()) {
                System.err.println("");
                System.err.println("Usage:   opencga-storage-admin.sh " + parsedCommand + " [options]");
                System.err.println("");
                System.err.println("Subcommands:");
                printCommands(jcommander.getCommands().get(parsedCommand));
                System.err.println("");
            } else {
                System.err.println("");
                System.err.println("Usage:   opencga-storage-server.sh " + parsedCommand + " " + parsedSubCommand + " [options]");
                System.err.println("");
                System.err.println("Options:");
                CommandLineUtils.printCommandUsage(jcommander.getCommands().get(parsedCommand).getCommands().get(parsedSubCommand));
                System.err.println("");
            }
        }
    }

    private void printMainUsage() {
        for (String s : jcommander.getCommands().keySet()) {
            System.err.printf("%14s  %s\n", s, jcommander.getCommandDescription(s));
        }
    }

    private void printCommands(JCommander commander) {
        for (Map.Entry<String, JCommander> entry : commander.getCommands().entrySet()) {
            System.err.printf("%14s  %s\n", entry.getKey(), commander.getCommandDescription(entry.getKey()));
        }
    }


    public GeneralOptions getGeneralOptions() {
        return generalOptions;
    }

    public CommonOptions getCommonOptions() {
        return commonOptions;
    }

    public ServerCommandOptions getServerCommandOptions() {
        return serverCommandOptions;
    }

    public BenchmarkCommandOptions getBenchmarkCommandOptions() {
        return benchmarkCommandOptions;
    }

    //    public RestCommandOptions getRestCommandOptions() {
//        return restCommandOptions;
//    }
//
//    public GrpcCommandOptions getGrpcCommandOptions() {
//        return grpcCommandOptions;
//    }
}
