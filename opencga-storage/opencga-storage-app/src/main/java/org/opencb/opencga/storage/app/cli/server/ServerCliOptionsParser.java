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

package org.opencb.opencga.storage.app.cli.server;

import com.beust.jcommander.*;
import org.opencb.commons.utils.CommandLineUtils;
import org.opencb.opencga.core.common.GitRepositoryState;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by imedina on 02/03/15.
 */
public class ServerCliOptionsParser {

    private final JCommander jcommander;

    private final GeneralOptions generalOptions;
    private final CommonOptions commonOptions;

    //    private final IndexSequenceCommandOptions indexSequenceCommandOptions;
    private RestCommandOptions restCommandOptions;
    private GrpcCommandOptions grpcCommandOptions;


    public ServerCliOptionsParser() {

        generalOptions = new GeneralOptions();
        jcommander = new JCommander(generalOptions);

        commonOptions = new CommonOptions();

        restCommandOptions = new RestCommandOptions();
        jcommander.addCommand("rest", restCommandOptions);
        JCommander alignmentSubCommands = jcommander.getCommands().get("rest");
        alignmentSubCommands.addCommand("start", restCommandOptions.restStartCommandOptions);
        alignmentSubCommands.addCommand("stop", restCommandOptions.restStopCommandOptions);
        alignmentSubCommands.addCommand("status", restCommandOptions.restStatusCommandOptions);

        grpcCommandOptions = new GrpcCommandOptions();
        jcommander.addCommand("grpc", grpcCommandOptions);
        JCommander variantSubCommands = jcommander.getCommands().get("grpc");
        variantSubCommands.addCommand("start", grpcCommandOptions.grpcStartCommandOptions);
        variantSubCommands.addCommand("stop", grpcCommandOptions.grpcStopCommandOptions);
        variantSubCommands.addCommand("querystatus", grpcCommandOptions.grpcStatusCommandOptions);

    }

    public void parse(String[] args) throws ParameterException {
        jcommander.parse(args);
    }

    public String getCommand() {
        return (jcommander.getParsedCommand() != null) ? jcommander.getParsedCommand() : "";
    }

    public String getSubCommand() {
        String parsedCommand = jcommander.getParsedCommand();
        if (jcommander.getCommands().containsKey(parsedCommand)) {
            String subCommand = jcommander.getCommands().get(parsedCommand).getParsedCommand();
            return subCommand != null ? subCommand: "";
        } else {
            return null;
        }
    }

    public boolean isHelp() {
        String parsedCommand = jcommander.getParsedCommand();
        if (parsedCommand != null) {
            JCommander jCommander = jcommander.getCommands().get(parsedCommand);
            List<Object> objects = jCommander.getObjects();
            if (!objects.isEmpty() && objects.get(0) instanceof CommonOptions) {
                return ((CommonOptions) objects.get(0)).help;
            }
        }
        return getCommonOptions().help;
    }


    public class GeneralOptions {

        @Parameter(names = {"-h", "--help"}, help = true)
        public boolean help;

        @Parameter(names = {"--version"})
        public boolean version;

    }

    /**
     * This class contains all those parameters available for all 'commands'
     */
    public class CommandOptions {

        @Parameter(names = {"-h", "--help"},  description = "This parameter prints this help", help = true)
        public boolean help;

        public JCommander getSubCommand() {
            return jcommander.getCommands().get(getCommand()).getCommands().get(getSubCommand());
        }

        public String getParsedSubCommand() {
            String parsedCommand = jcommander.getParsedCommand();
            if (jcommander.getCommands().containsKey(parsedCommand)) {
                String subCommand = jcommander.getCommands().get(parsedCommand).getParsedCommand();
                return subCommand != null ? subCommand: "";
            } else {
                return "";
            }
        }
    }


    /**
     * This class contains all those common parameters available for all 'subcommands'
     */
    public class CommonOptions {

        @Parameter(names = {"-h", "--help"}, description = "Print this help", help = true)
        public boolean help;

        @Parameter(names = {"-L", "--log-level"}, description = "One of the following: 'error', 'warn', 'info', 'debug', 'trace'")
        public String logLevel = "info";

        @Parameter(names = {"--log-file"}, description = "One of the following: 'error', 'warn', 'info', 'debug', 'trace'")
        public String logFile;

        @Parameter(names = {"-v", "--verbose"}, description = "Increase the verbosity of logs")
        public boolean verbose = false;

        @Parameter(names = {"-C", "--conf"}, description = "Configuration file path.")
        public String configFile;

        @Parameter(names = {"--storage-engine"}, arity = 1, description = "Default storage engine for the server, if not set then is read from storage-configuration.yml")
        public String storageEngine;

        @DynamicParameter(names = "-D", description = "Storage engine specific parameters go here comma separated, ie. -Dmongodb" +
                ".compression=snappy", hidden = false)
        public Map<String, String> params = new HashMap<>(); //Dynamic parameters must be initialized

    }


    /*
     * Alignment (BAM, CRAM) CLI options
     */
    @Parameters(commandNames = {"rest"}, commandDescription = "Implements different tools for working with BAM files")
    public class RestCommandOptions extends CommandOptions {

        RestStartCommandOptions restStartCommandOptions;
        RestStopCommandOptions restStopCommandOptions;
        RestStatusCommandOptions restStatusCommandOptions;

        public RestCommandOptions() {
            this.restStartCommandOptions = new RestStartCommandOptions();
            this.restStopCommandOptions = new RestStopCommandOptions();
            this.restStatusCommandOptions = new RestStatusCommandOptions();
        }
    }

    /*
     * Variant (VCF, BCF) CLI options
     */
    @Parameters(commandNames = {"grpc"}, commandDescription = "Implements different tools for working with gVCF/VCF files")
    public class GrpcCommandOptions extends CommandOptions {

        GrpcStartCommandOptions grpcStartCommandOptions = new GrpcStartCommandOptions();
        GrpcStopCommandOptions grpcStopCommandOptions = new GrpcStopCommandOptions();
        GrpcStatusCommandOptions grpcStatusCommandOptions = new GrpcStatusCommandOptions();

        public GrpcCommandOptions() {
            this.grpcStartCommandOptions = new GrpcStartCommandOptions();
            this.grpcStopCommandOptions = new GrpcStopCommandOptions();
            this.grpcStatusCommandOptions = new GrpcStatusCommandOptions();
        }
    }



    @Parameters(commandNames = {"start"}, commandDescription = "Index alignment file")
    public class RestStartCommandOptions {

        @ParametersDelegate
        public CommonOptions commonOptions = ServerCliOptionsParser.this.commonOptions;

        @Parameter(names = {"--port"}, description = "Port", required = false, arity = 1)
        int port;

        @Parameter(names = {"--auth-manager"}, description = "Port", required = false, arity = 1)
        String authManager;

    }

    @Parameters(commandNames = {"stop"}, commandDescription = "Search over indexed variants")
    public class RestStopCommandOptions {

        @ParametersDelegate
        public CommonOptions commonOptions = ServerCliOptionsParser.this.commonOptions;

        @Parameter(names = {"--port"}, description = "Port", required = false, arity = 1)
        int port;
    }

    @Parameters(commandNames = {"status"}, commandDescription = "Search over indexed variants")
    public class RestStatusCommandOptions {

    }



    @Parameters(commandNames = {"start"}, commandDescription = "Index alignment file")
    public class GrpcStartCommandOptions {

        @ParametersDelegate
        public CommonOptions commonOptions = ServerCliOptionsParser.this.commonOptions;

        @Parameter(names = {"--port"}, description = "Port", required = false, arity = 1)
        int port;

        @Parameter(names = {"--auth-manager"}, description = "Port", required = false, arity = 1)
        String authManager;

    }

    @Parameters(commandNames = {"stop"}, commandDescription = "Search over indexed variants")
    public class GrpcStopCommandOptions {

        @ParametersDelegate
        public CommonOptions commonOptions = ServerCliOptionsParser.this.commonOptions;

        @Parameter(names = {"--port"}, description = "Port", required = false, arity = 1)
        int port;

    }

    @Parameters(commandNames = {"status"}, commandDescription = "Search over indexed variants")
    public class GrpcStatusCommandOptions {

    }


    public void printUsage() {
        String parsedCommand = getCommand();
        if (parsedCommand.isEmpty()) {
            System.err.println("");
            System.err.println("Program:     OpenCGA Storage Server (OpenCB)");
            System.err.println("Version:     " + GitRepositoryState.get().getBuildVersion());
            System.err.println("Git commit:  " + GitRepositoryState.get().getCommitId());
            System.err.println("Description: Big Data platform for processing and analysing NGS data");
            System.err.println("");
            System.err.println("Usage:       opencga-storage-server.sh [-h|--help] [--version] <command> [options]");
            System.err.println("");
            System.err.println("Commands:");
            printMainUsage();
            System.err.println("");
        } else {
            String parsedSubCommand = getSubCommand();
            if (parsedSubCommand.isEmpty()) {
                System.err.println("");
                System.err.println("Usage:   opencga-storage-server.sh " + parsedCommand + " [options]");
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

    public RestCommandOptions getRestCommandOptions() {
        return restCommandOptions;
    }

    public GrpcCommandOptions getGrpcCommandOptions() {
        return grpcCommandOptions;
    }

}
