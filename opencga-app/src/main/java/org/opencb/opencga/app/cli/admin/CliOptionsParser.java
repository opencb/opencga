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

package org.opencb.opencga.app.cli.admin;

import com.beust.jcommander.*;
import org.opencb.commons.utils.CommandLineUtils;
import org.opencb.opencga.core.common.GitRepositoryState;

import java.util.List;
import java.util.Map;

/**
 * Created by imedina on 02/03/15.
 */
public class CliOptionsParser {

    private final JCommander jCommander;

    private final GeneralOptions generalOptions;
    private final CommonCommandOptions commonCommandOptions;

    private CatalogCommandOptions catalogCommandOptions;
    private UsersCommandOptions usersCommandOptions;
    private AuditCommandOptions auditCommandOptions;
    private ToolsCommandOptions toolsCommandOptions;


    public CliOptionsParser() {
        generalOptions = new GeneralOptions();

        jCommander = new JCommander(generalOptions);
        jCommander.setProgramName("opencga-admin.sh");

        commonCommandOptions = new CommonCommandOptions();

        catalogCommandOptions = new CatalogCommandOptions();
        jCommander.addCommand("catalog", catalogCommandOptions);
        JCommander catalogSubCommands = jCommander.getCommands().get("catalog");
        catalogSubCommands.addCommand("install", catalogCommandOptions.installCatalogCommandOptions);
        catalogSubCommands.addCommand("delete", catalogCommandOptions.deleteCatalogCommandOptions);
//        catalogSubCommands.addCommand("query", catalogCommandOptions.queryCatalogCommandOptions);
        catalogSubCommands.addCommand("index", catalogCommandOptions.indexCatalogCommandOptions);
//        catalogSubCommands.addCommand("audit", catalogCommandOptions.auditCatalogCommandOptions);
        catalogSubCommands.addCommand("clean", catalogCommandOptions.cleanCatalogCommandOptions);
        catalogSubCommands.addCommand("stats", catalogCommandOptions.statsCatalogCommandOptions);
        catalogSubCommands.addCommand("dump", catalogCommandOptions.dumpCatalogCommandOptions);
        catalogSubCommands.addCommand("import", catalogCommandOptions.importCatalogCommandOptions);

        usersCommandOptions = new UsersCommandOptions();
        jCommander.addCommand("users", usersCommandOptions);
        JCommander usersSubCommands = jCommander.getCommands().get("users");
        usersSubCommands.addCommand("create", usersCommandOptions.createUserCommandOptions);
        usersSubCommands.addCommand("delete", usersCommandOptions.deleteUserCommandOptions);
        usersSubCommands.addCommand("disk-quota", usersCommandOptions.diskQuotaUserCommandOptions);
        usersSubCommands.addCommand("stats", usersCommandOptions.statsUserCommandOptions);

        auditCommandOptions = new AuditCommandOptions();
        jCommander.addCommand("audit", auditCommandOptions);
        JCommander auditSubCommands = jCommander.getCommands().get("audit");
        auditSubCommands.addCommand("query", auditCommandOptions.queryAuditCommandOptions);
        auditSubCommands.addCommand("stats", auditCommandOptions.statsAuditCommandOptions);


        toolsCommandOptions = new ToolsCommandOptions();
        jCommander.addCommand("tools", toolsCommandOptions);
        JCommander toolsSubCommands = jCommander.getCommands().get("tools");
        toolsSubCommands.addCommand("install", toolsCommandOptions.installToolCommandOptions);
        toolsSubCommands.addCommand("list", toolsCommandOptions.listToolCommandOptions);
        toolsSubCommands.addCommand("show", toolsCommandOptions.showToolCommandOptions);
    }

    public void parse(String[] args) throws ParameterException {
        jCommander.parse(args);
    }

    public String getCommand() {
        return (jCommander.getParsedCommand() != null) ? jCommander.getParsedCommand() : "";
    }

    public String getSubCommand() {
        String parsedCommand = jCommander.getParsedCommand();
        if (jCommander.getCommands().containsKey(parsedCommand)) {
            String subCommand = jCommander.getCommands().get(parsedCommand).getParsedCommand();
            return subCommand != null ? subCommand: "";
        } else {
            return null;
        }
    }

    public boolean isHelp() {
        String parsedCommand = jCommander.getParsedCommand();
        if (parsedCommand != null) {
            JCommander jCommander2 = jCommander.getCommands().get(parsedCommand);
            List<Object> objects = jCommander2.getObjects();
            if (!objects.isEmpty() && objects.get(0) instanceof CliOptionsParser.CommonCommandOptions) {
                return ((CliOptionsParser.CommonCommandOptions) objects.get(0)).help;
            }
        }
        return commonCommandOptions.help;
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
            return jCommander.getCommands().get(getCommand()).getCommands().get(getSubCommand());
        }

        public String getParsedSubCommand() {
            String parsedCommand = jCommander.getParsedCommand();
            if (jCommander.getCommands().containsKey(parsedCommand)) {
                String subCommand = jCommander.getCommands().get(parsedCommand).getParsedCommand();
                return subCommand != null ? subCommand: "";
            } else {
                return "";
            }
        }
    }

    /**
     * This class contains all those common parameters available for all 'subcommands'
     */
    public class CommonCommandOptions {

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

        @Deprecated
        @Parameter(names = {"--storage-engine"}, arity = 1, description = "One of the listed in storage-configuration.yml")
        public String storageEngine;
    }


    /*
     * Catalog CLI options
     */
    @Parameters(commandNames = {"catalog"}, commandDescription = "Implements different tools interact with Catalog database")
    public class CatalogCommandOptions extends CommandOptions {

        InstallCatalogCommandOptions installCatalogCommandOptions;
        DeleteCatalogCommandOptions deleteCatalogCommandOptions;
//        QueryCatalogCommandOptions queryCatalogCommandOptions;
        IndexCatalogCommandOptions indexCatalogCommandOptions;
//        AuditCatalogCommandOptions auditCatalogCommandOptions;
        CleanCatalogCommandOptions cleanCatalogCommandOptions;
        StatsCatalogCommandOptions statsCatalogCommandOptions;
        DumpCatalogCommandOptions dumpCatalogCommandOptions;
        ImportCatalogCommandOptions importCatalogCommandOptions;

        CommonCommandOptions commonOptions = CliOptionsParser.this.commonCommandOptions;

        public CatalogCommandOptions() {
            this.installCatalogCommandOptions = new InstallCatalogCommandOptions();
            this.deleteCatalogCommandOptions = new DeleteCatalogCommandOptions();
//            this.queryCatalogCommandOptions= new QueryCatalogCommandOptions();
            this.indexCatalogCommandOptions = new IndexCatalogCommandOptions();
//            this.auditCatalogCommandOptions = new AuditCatalogCommandOptions();
            this.cleanCatalogCommandOptions = new CleanCatalogCommandOptions();
            this.statsCatalogCommandOptions = new StatsCatalogCommandOptions();
            this.dumpCatalogCommandOptions = new DumpCatalogCommandOptions();
            this.importCatalogCommandOptions = new ImportCatalogCommandOptions();
        }
    }

    /*
     * Users CLI options
     */
    @Parameters(commandNames = {"users"}, commandDescription = "Implements different tools for working with users")
    public class UsersCommandOptions extends CommandOptions {

        CreateUserCommandOptions createUserCommandOptions;
        DeleteUserCommandOptions deleteUserCommandOptions;
        StatsUserCommandOptions statsUserCommandOptions;
        DiskQuotaUserCommandOptions diskQuotaUserCommandOptions;

        CommonCommandOptions commonOptions = CliOptionsParser.this.commonCommandOptions;

        public UsersCommandOptions() {
            this.createUserCommandOptions = new CreateUserCommandOptions();
            this.deleteUserCommandOptions = new DeleteUserCommandOptions();
            this.statsUserCommandOptions = new StatsUserCommandOptions();
            this.diskQuotaUserCommandOptions = new DiskQuotaUserCommandOptions();
        }
    }

    /*
     * Audit CLI options
     */
    @Parameters(commandNames = {"audit"}, commandDescription = "Implements different tools for working with audit")
    public class AuditCommandOptions extends CommandOptions {

        QueryAuditCommandOptions queryAuditCommandOptions;
        StatsAuditCommandOptions statsAuditCommandOptions;

        CommonCommandOptions commonOptions = CliOptionsParser.this.commonCommandOptions;

        public AuditCommandOptions() {
            this.queryAuditCommandOptions= new QueryAuditCommandOptions();
            this.statsAuditCommandOptions= new StatsAuditCommandOptions();
        }
    }


    /*
     * Tools CLI options
     */
    @Parameters(commandNames = {"tools"}, commandDescription = "Implements different tools for working with tools")
    public class ToolsCommandOptions extends CommandOptions {

        InstallToolCommandOptions installToolCommandOptions;
        ListToolCommandOptions listToolCommandOptions;
        ShowToolCommandOptions showToolCommandOptions;

        CommonCommandOptions commonOptions = CliOptionsParser.this.commonCommandOptions;

        public ToolsCommandOptions() {
            this.installToolCommandOptions = new InstallToolCommandOptions();
            this.listToolCommandOptions = new ListToolCommandOptions();
            this.showToolCommandOptions = new ShowToolCommandOptions();
        }
    }


    /**
     * Auxiliary class for Database connection.
     */
    class CatalogDatabaseCommandOptions {

        @Parameter(names = {"-d", "--database"}, description = "DataBase name to load the data, eg. opencga_catalog")
        public String database;

        @Parameter(names = {"-H", "--host"}, description = "DataBase host and port, eg. localhost:27017")
        public String host;

        @Parameter(names = {"-p", "--password"}, description = "Admin password", password = true, arity = 0)
        public String password;
    }



    /*
     *  CATALOG SUB-COMMANDS
     */

    @Parameters(commandNames = {"install"}, commandDescription = "Install Catalog database and collections together with the indexes")
    public class InstallCatalogCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = CliOptionsParser.this.commonCommandOptions;

        @Parameter(names = {"--overwrite"}, description = "Reset the database if exists before installing")
        public boolean overwrite;

    }

    @Parameters(commandNames = {"delete"}, commandDescription = "Delete the Catalog database")
    public class DeleteCatalogCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = CliOptionsParser.this.commonCommandOptions;

    }

//    @Parameters(commandNames = {"query"}, commandDescription = "Query audit data from Catalog database")
//    public class QueryCatalogCommandOptions extends CatalogDatabaseCommandOptions {
//
//        @ParametersDelegate
//        public CommonCommandOptions commonOptions = CliOptionsParser.this.commonCommandOptions;
//
//        @Parameter(names = {"--filter"}, description = "Query filter for data")
//        public String filter;
//    }

    @Parameters(commandNames = {"index"}, commandDescription = "Create the non-existing indices in Catalog database")
    public class IndexCatalogCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = CliOptionsParser.this.commonCommandOptions;

        @Parameter(names = {"--reset"}, description = "Remove existing indexes before creting the new one")
        public boolean reset;
    }

//    @Parameters(commandNames = {"audit"}, commandDescription = "Query audit data from Catalog database")
//    public class AuditCatalogCommandOptions extends CatalogDatabaseCommandOptions {
//
//        @ParametersDelegate
//        public CommonCommandOptions commonOptions = CliOptionsParser.this.commonCommandOptions;
//
//        @Parameter(names = {"--filter"}, description = "A comma-separated list of collections for the stats")
//        public String filter;
//    }

    @Parameters(commandNames = {"clean"}, commandDescription = "Query audit data from Catalog database")
    public class CleanCatalogCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = CliOptionsParser.this.commonCommandOptions;

        @Parameter(names = {"--collections"}, description = "A comma-separated list of collections to clean up")
        public String filter = "ALL";

    }

    @Parameters(commandNames = {"stats"}, commandDescription = "Print some summary stats of Catalog database")
    public class StatsCatalogCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = CliOptionsParser.this.commonCommandOptions;

        @Parameter(names = {"--collections"}, description = "A comma-separated list of collections for the stats")
        public String collections = "ALL";
    }

    @Parameters(commandNames = {"dump"}, commandDescription = "Create a dump of Catalog database")
    public class DumpCatalogCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = CliOptionsParser.this.commonCommandOptions;

        @Parameter(names = {"--collections"}, description = "A comma-separated list of collections to be dumped", arity = 1)
        public String collections = "ALL";
    }

    @Parameters(commandNames = {"import"}, commandDescription = "Create a dump of Catalog database")
    public class ImportCatalogCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = CliOptionsParser.this.commonCommandOptions;

        @Parameter(names = {"--collections"}, description = "A comma-separated list of collections to be imported", arity = 1)
        public String collections = "ALL";
    }



    /*
     * AUDIT SUB-COMMANDS
     */


    @Parameters(commandNames = {"query"}, commandDescription = "Query audit data from Catalog database")
    public class QueryAuditCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = CliOptionsParser.this.commonCommandOptions;

        @Parameter(names = {"--filter"}, description = "Query filter for data")
        public String filter;
    }

    @Parameters(commandNames = {"stats"}, commandDescription = "Print summary stats for an user")
    public class StatsAuditCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = CliOptionsParser.this.commonCommandOptions;
    }


    /*
     * USER SUB-COMMANDS
     */

    @Parameters(commandNames = {"create"}, commandDescription = "Create a new user in Catalog database and the workspace")
    public class CreateUserCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = CliOptionsParser.this.commonCommandOptions;


        @Parameter(names = {"--user-id"}, description = "Full name of the study where the file is classified", required = true, arity = 1)
        public String userId;

    }

    @Parameters(commandNames = {"delete"}, commandDescription = "Delete the user Catalog database entry and the workspace")
    public class DeleteUserCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = CliOptionsParser.this.commonCommandOptions;


        @Parameter(names = {"--user-id"}, description = "Full name of the study where the file is classified", required = true, arity = 1)
        public String userId;

    }


    @Parameters(commandNames = {"disk-quota"}, commandDescription = "Set a new disk quota for an user")
    public class DiskQuotaUserCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = CliOptionsParser.this.commonCommandOptions;


        @Parameter(names = {"--user-id"}, description = "User id to get stats from", required = true, arity = 1)
        public String userId;

        @Parameter(names = {"--quota"}, description = "Disk quota in GB", required = true, arity = 1)
        public int diskQupta;
    }

    @Parameters(commandNames = {"stats"}, commandDescription = "Print summary stats for an user")
    public class StatsUserCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = CliOptionsParser.this.commonCommandOptions;


        @Parameter(names = {"--user-id"}, description = "User id to get stats from", required = true, arity = 1)
        public String userId;

    }




    @Parameters(commandNames = {"install"}, commandDescription = "Install and check a new tool")
    public class InstallToolCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = CliOptionsParser.this.commonCommandOptions;


        @Parameter(names = {"-i", "--input"}, description = "File with the new tool to be installed", required = true, arity = 1)
        public String study;

    }

    @Parameters(commandNames = {"list"}, commandDescription = "Print a summary list of all tools")
    public class ListToolCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = CliOptionsParser.this.commonCommandOptions;


        @Parameter(names = {"--filter"}, description = "Some kind of filter", arity = 1)
        public String study;

    }

    @Parameters(commandNames = {"show"}, commandDescription = "Show a summary of the tool")
    public class ShowToolCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = CliOptionsParser.this.commonCommandOptions;


        @Parameter(names = {"--tool-id"}, description = "Full name of the study where the file is classified", arity = 1)
        public String study;

    }



    public void printUsage() {
        String parsedCommand = getCommand();
        if (parsedCommand.isEmpty()) {
            System.err.println("");
            System.err.println("Program:     OpenCGA Catalog (OpenCB)");
            System.err.println("Version:     " + GitRepositoryState.get().getBuildVersion());
            System.err.println("Git commit:  " + GitRepositoryState.get().getCommitId());
            System.err.println("Description: Big Data platform for processing and analysing NGS data");
            System.err.println("");
            System.err.println("Usage:       opencga-admin.sh [-h|--help] [--version] <command> [options]");
            System.err.println("");
            System.err.println("Commands:");
            printMainUsage();
            System.err.println("");
        } else {
            String parsedSubCommand = getSubCommand();
            if (parsedSubCommand.isEmpty()) {
                System.err.println("");
                System.err.println("Usage:   opencga-admin.sh " + parsedCommand + " <subcommand> [options]");
                System.err.println("");
                System.err.println("Subcommands:");
                printCommands(jCommander.getCommands().get(parsedCommand));
                System.err.println("");
            } else {
                System.err.println("");
                System.err.println("Usage:   opencga-admin.sh " + parsedCommand + " " + parsedSubCommand + " [options]");
                System.err.println("");
                System.err.println("Options:");
                CommandLineUtils.printCommandUsage(jCommander.getCommands().get(parsedCommand).getCommands().get(parsedSubCommand));
                System.err.println("");
            }
        }
    }

    private void printMainUsage() {
        for (String s : jCommander.getCommands().keySet()) {
            System.err.printf("%14s  %s\n", s, jCommander.getCommandDescription(s));
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

    public CommonCommandOptions getCommonOptions() {
        return commonCommandOptions;
    }

    public CatalogCommandOptions getCatalogCommandOptions() {
        return catalogCommandOptions;
    }

    public UsersCommandOptions getUsersCommandOptions() {
        return usersCommandOptions;
    }

    public AuditCommandOptions getAuditCommandOptions() {
        return auditCommandOptions;
    }

    public ToolsCommandOptions getToolsCommandOptions() {
        return toolsCommandOptions;
    }
}
