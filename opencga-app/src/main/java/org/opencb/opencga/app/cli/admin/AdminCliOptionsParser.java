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
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.GeneralCliOptions.GeneralOptions;
import org.opencb.opencga.catalog.models.Account;
import org.opencb.opencga.core.common.GitRepositoryState;

import java.util.List;
import java.util.Map;


/**
 * Created by imedina on 02/03/15.
 */
public class AdminCliOptionsParser {

    private final JCommander jCommander;

    private final GeneralOptions generalOptions;
    private final AdminCommonCommandOptions commonCommandOptions;

    private CatalogCommandOptions catalogCommandOptions;
    private UsersCommandOptions usersCommandOptions;
    private AuditCommandOptions auditCommandOptions;
    private ToolsCommandOptions toolsCommandOptions;
    private ServerCommandOptions serverCommandOptions;


    public AdminCliOptionsParser() {
        generalOptions = new GeneralOptions();

        jCommander = new JCommander(generalOptions);
        jCommander.setProgramName("opencga-admin.sh");

        commonCommandOptions = new AdminCommonCommandOptions();

        catalogCommandOptions = new CatalogCommandOptions();
        jCommander.addCommand("catalog", catalogCommandOptions);
        JCommander catalogSubCommands = jCommander.getCommands().get("catalog");
        catalogSubCommands.addCommand("demo", catalogCommandOptions.demoCatalogCommandOptions);
        catalogSubCommands.addCommand("install", catalogCommandOptions.installCatalogCommandOptions);
        catalogSubCommands.addCommand("delete", catalogCommandOptions.deleteCatalogCommandOptions);
        catalogSubCommands.addCommand("index", catalogCommandOptions.indexCatalogCommandOptions);
        catalogSubCommands.addCommand("clean", catalogCommandOptions.cleanCatalogCommandOptions);
        catalogSubCommands.addCommand("stats", catalogCommandOptions.statsCatalogCommandOptions);
        catalogSubCommands.addCommand("dump", catalogCommandOptions.dumpCatalogCommandOptions);
        catalogSubCommands.addCommand("import", catalogCommandOptions.importCatalogCommandOptions);
        catalogSubCommands.addCommand("daemon", catalogCommandOptions.daemonCatalogCommandOptions);

        usersCommandOptions = new UsersCommandOptions();
        jCommander.addCommand("users", usersCommandOptions);
        JCommander usersSubCommands = jCommander.getCommands().get("users");
        usersSubCommands.addCommand("create", usersCommandOptions.createUserCommandOptions);
        usersSubCommands.addCommand("import", usersCommandOptions.importUserCommandOptions);
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

        serverCommandOptions = new ServerCommandOptions();
        jCommander.addCommand("server", serverCommandOptions);
        JCommander serverSubCommands = jCommander.getCommands().get("server");
        serverSubCommands.addCommand("rest", serverCommandOptions.restServerCommandOptions);
        serverSubCommands.addCommand("grpc", serverCommandOptions.grpcServerCommandOptions);
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
            if (!objects.isEmpty() && objects.get(0) instanceof AdminCommonCommandOptions) {
                return ((AdminCommonCommandOptions) objects.get(0)).help;
            }
        }
        return commonCommandOptions.help;
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
    public class AdminCommonCommandOptions extends GeneralCliOptions.CommonCommandOptions {

        @Parameter(names = {"-p", "--password"}, description = "Admin password", required = false, password = true, arity = 0)
        public String adminPassword;

    }


    /*
     * Catalog CLI options
     */
    @Parameters(commandNames = {"catalog"}, commandDescription = "Implements different tools interact with Catalog database")
    public class CatalogCommandOptions extends CommandOptions {

        DemoCatalogCommandOptions demoCatalogCommandOptions;
        InstallCatalogCommandOptions installCatalogCommandOptions;
        DeleteCatalogCommandOptions deleteCatalogCommandOptions;
        IndexCatalogCommandOptions indexCatalogCommandOptions;
        CleanCatalogCommandOptions cleanCatalogCommandOptions;
        StatsCatalogCommandOptions statsCatalogCommandOptions;
        DumpCatalogCommandOptions dumpCatalogCommandOptions;
        ImportCatalogCommandOptions importCatalogCommandOptions;
        DaemonCatalogCommandOptions daemonCatalogCommandOptions;

        AdminCommonCommandOptions commonOptions = AdminCliOptionsParser.this.commonCommandOptions;

        public CatalogCommandOptions() {
            this.demoCatalogCommandOptions = new DemoCatalogCommandOptions();
            this.installCatalogCommandOptions = new InstallCatalogCommandOptions();
            this.deleteCatalogCommandOptions = new DeleteCatalogCommandOptions();
            this.indexCatalogCommandOptions = new IndexCatalogCommandOptions();
            this.cleanCatalogCommandOptions = new CleanCatalogCommandOptions();
            this.statsCatalogCommandOptions = new StatsCatalogCommandOptions();
            this.dumpCatalogCommandOptions = new DumpCatalogCommandOptions();
            this.importCatalogCommandOptions = new ImportCatalogCommandOptions();
            this.daemonCatalogCommandOptions = new DaemonCatalogCommandOptions();
        }
    }

    /*
     * Users CLI options
     */
    @Parameters(commandNames = {"users"}, commandDescription = "Implements different tools for working with users")
    public class UsersCommandOptions extends CommandOptions {

        CreateUserCommandOptions createUserCommandOptions;
        ImportUserCommandOptions importUserCommandOptions;
        DeleteUserCommandOptions deleteUserCommandOptions;
        StatsUserCommandOptions statsUserCommandOptions;
        DiskQuotaUserCommandOptions diskQuotaUserCommandOptions;

        AdminCommonCommandOptions commonOptions = AdminCliOptionsParser.this.commonCommandOptions;

        public UsersCommandOptions() {
            this.createUserCommandOptions = new CreateUserCommandOptions();
            this.importUserCommandOptions = new ImportUserCommandOptions();
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

        AdminCommonCommandOptions commonOptions = AdminCliOptionsParser.this.commonCommandOptions;

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

        AdminCommonCommandOptions commonOptions = AdminCliOptionsParser.this.commonCommandOptions;

        public ToolsCommandOptions() {
            this.installToolCommandOptions = new InstallToolCommandOptions();
            this.listToolCommandOptions = new ListToolCommandOptions();
            this.showToolCommandOptions = new ShowToolCommandOptions();
        }
    }

    /*
     * Server CLI options
     */
    @Parameters(commandNames = {"server"}, commandDescription = "Manage REST and gRPC servers")
    public class ServerCommandOptions extends CommandOptions {

        RestServerCommandOptions restServerCommandOptions;
        GrpcServerCommandOptions grpcServerCommandOptions;

        AdminCommonCommandOptions commonOptions = AdminCliOptionsParser.this.commonCommandOptions;

        public ServerCommandOptions() {
            this.restServerCommandOptions = new RestServerCommandOptions();
            this.grpcServerCommandOptions = new GrpcServerCommandOptions();
        }
    }



    /**
     * Auxiliary class for Database connection.
     */
    class CatalogDatabaseCommandOptions {

        @Parameter(names = {"-d", "--database"}, description = "Database name for the catalog metadata, eg. opencga_catalog. If not present is read from catalog-configuration.yml")
        @Deprecated
        public String database;

        @Parameter(names = {"--database-host"}, description = "Database host and port, eg. localhost:27017. If not present is read from catalog-configuration.yml")
        public String databaseHost;

        @Parameter(names = {"--database-user"}, description = "Database user name. If not present is read from catalog-configuration.yml")
        public String databaseUser;

        @Parameter(names = {"--database-password"}, description = "Database password. If not present is read from catalog-configuration.yml", password = true, arity = 0)
        public String databasePassword;
    }



    /*
     *  CATALOG SUB-COMMANDS
     */

    @Parameters(commandNames = {"demo"}, commandDescription = "Install and populate a catalog database with demonstration purposes.")
    public class DemoCatalogCommandOptions {

        @ParametersDelegate
        public AdminCommonCommandOptions commonOptions = AdminCliOptionsParser.this.commonCommandOptions;

        @Parameter(names = {"--database-name"}, description = "Database name for the catalog metadata. If not present, it will be set to opencga_catalog_demo")
        public String database;

        @Parameter(names = {"--force"}, description = "If this parameters is set, it will override the database installation.")
        public boolean force;

    }

    @Parameters(commandNames = {"install"}, commandDescription = "Install Catalog database and collections together with the indexes")
    public class InstallCatalogCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AdminCommonCommandOptions commonOptions = AdminCliOptionsParser.this.commonCommandOptions;

//        @Parameter(names = {"--overwrite"}, description = "Reset the database if exists before installing")
//        public boolean overwrite;

    }

    @Parameters(commandNames = {"delete"}, commandDescription = "Delete the Catalog database")
    public class DeleteCatalogCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AdminCommonCommandOptions commonOptions = AdminCliOptionsParser.this.commonCommandOptions;

    }

    @Parameters(commandNames = {"index"}, commandDescription = "Create the non-existing indices in Catalog database")
    public class IndexCatalogCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AdminCommonCommandOptions commonOptions = AdminCliOptionsParser.this.commonCommandOptions;

        @Parameter(names = {"--reset"}, description = "Remove existing indexes before creting the new one")
        public boolean reset;
    }

    @Parameters(commandNames = {"clean"}, commandDescription = "Query audit data from Catalog database")
    public class CleanCatalogCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AdminCommonCommandOptions commonOptions = AdminCliOptionsParser.this.commonCommandOptions;

        @Parameter(names = {"--collections"}, description = "A comma-separated list of collections to clean up")
        public String filter = "ALL";

    }

    @Parameters(commandNames = {"stats"}, commandDescription = "Print some summary stats of Catalog database")
    public class StatsCatalogCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AdminCommonCommandOptions commonOptions = AdminCliOptionsParser.this.commonCommandOptions;

        @Parameter(names = {"--collections"}, description = "A comma-separated list of collections for the stats")
        public String collections = "ALL";
    }

    @Parameters(commandNames = {"dump"}, commandDescription = "Create a dump of Catalog database")
    public class DumpCatalogCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AdminCommonCommandOptions commonOptions = AdminCliOptionsParser.this.commonCommandOptions;

        @Parameter(names = {"--collections"}, description = "A comma-separated list of collections to be dumped", arity = 1)
        public String collections = "ALL";
    }

    @Parameters(commandNames = {"import"}, commandDescription = "Create a dump of Catalog database")
    public class ImportCatalogCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AdminCommonCommandOptions commonOptions = AdminCliOptionsParser.this.commonCommandOptions;

        @Parameter(names = {"--collections"}, description = "A comma-separated list of collections to be imported", arity = 1)
        public String collections = "ALL";
    }

    @Parameters(commandNames = {"daemon"}, commandDescription = "Start and stop Catalog daemons")
    public class DaemonCatalogCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AdminCommonCommandOptions commonOptions = AdminCliOptionsParser.this.commonCommandOptions;

        @Parameter(names = {"--start"}, description = "File with the new tool to be installed", arity = 0)
        public boolean start;

        @Parameter(names = {"--stop"}, description = "File with the new tool to be installed", arity = 0)
        public boolean stop;
    }



    /*
     * AUDIT SUB-COMMANDS
     */


    @Parameters(commandNames = {"query"}, commandDescription = "Query audit data from Catalog database")
    public class QueryAuditCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AdminCommonCommandOptions commonOptions = AdminCliOptionsParser.this.commonCommandOptions;

        @Parameter(names = {"--filter"}, description = "Query filter for data")
        public String filter;
    }

    @Parameters(commandNames = {"stats"}, commandDescription = "Print summary stats for an user")
    public class StatsAuditCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AdminCommonCommandOptions commonOptions = AdminCliOptionsParser.this.commonCommandOptions;
    }


    /*
     * USER SUB-COMMANDS
     */

    @Parameters(commandNames = {"create"}, commandDescription = "Create a new user with a default project in Catalog database and the workspace")
    public class CreateUserCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AdminCommonCommandOptions commonOptions = AdminCliOptionsParser.this.commonCommandOptions;

        @Parameter(names = {"-u", "--user-id"}, description = "User id", required = true, arity = 1)
        public String userId;

        @Parameter(names = {"--user-name"}, description = "User name", required = true, arity = 1)
        public String userName;

//        @Parameter(names = {"--user-password"}, description = "User password", required = true, password = true, arity = 0)
        @Parameter(names = {"--user-password"}, description = "User password", required = true, arity = 1)
        public String userPassword;

        @Parameter(names = {"--user-email"}, description = "User email", required = true, arity = 1)
        public String userEmail;

        @Parameter(names = {"--user-organization"}, description = "User organization", required = false, arity = 1)
        public String userOrganization;

        @Parameter(names = {"--user-DiskQuota"}, description = "User disk quota", required = false, arity = 1)
        public Long userDiskQuota;

        @Parameter(names = {"--project-name"}, description = "Project name. Default: Default", required = false, arity = 1)
        public String projectName;

        @Parameter(names = {"--project-alias"}, description = "Project alias: Default: default", required = false, arity = 1)
        public String projectAlias;

        @Parameter(names = {"--project-description"}, description = "Project description.", required = false, arity = 1)
        public String projectDescription;

        @Parameter(names = {"--project-organization"}, description = "Project organization", required = false, arity = 1)
        public String projectOrganization;

    }

    @Parameters(commandNames = {"import"}, commandDescription = "Import users from an authentication origin into catalog")
    public class ImportUserCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AdminCommonCommandOptions commonOptions = AdminCliOptionsParser.this.commonCommandOptions;

        @Parameter(names = {"-u", "--users"}, description = "Comma separated list of user ids", required = true, arity = 1)
        public String users;

        @Parameter(names = {"-g", "--groups"}, description = "Comma separated list of group ids [PENDING]", arity = 1)
        public String groups;

        @Parameter(names = {"--auth-origin"}, description = "Authentication id (as defined in the catalog configuration file) of the origin"
                + " to be used to import users from.", arity = 1, required = true)
        public String authOrigin;

        @Parameter(names = {"--type"}, description = "User account type of the users to be imported (guest or full).", arity = 1)
        public String type = Account.FULL;

        @Parameter(names = {"--expiration-date"}, description = "Expiration date (DD/MM/YYYY). By default, 1 year starting from the "
                + "import day", arity = 1)
        public String expDate;

        @Parameter(names = {"--studies"}, description = "Comma separated list of studies where the imported users will be synchronized "
                + "with their belonging groups. If empty, users will not have any special permission.", arity = 1)
        public String studies;
    }

    @Parameters(commandNames = {"delete"}, commandDescription = "Delete the user Catalog database entry and the workspace")
    public class DeleteUserCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AdminCommonCommandOptions commonOptions = AdminCliOptionsParser.this.commonCommandOptions;


        @Parameter(names = {"-u", "--user-id"}, description = "Full name of the study where the file is classified", required = true, arity = 1)
        public String userId;

    }


    @Parameters(commandNames = {"disk-quota"}, commandDescription = "Set a new disk quota for an user")
    public class DiskQuotaUserCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AdminCommonCommandOptions commonOptions = AdminCliOptionsParser.this.commonCommandOptions;


        @Parameter(names = {"-u", "--user-id"}, description = "User id to get stats from", required = true, arity = 1)
        public String userId;

        @Parameter(names = {"--quota"}, description = "Disk quota in GB", required = true, arity = 1)
        public long diskQuota;
    }

    @Parameters(commandNames = {"stats"}, commandDescription = "Print summary stats for an user")
    public class StatsUserCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AdminCommonCommandOptions commonOptions = AdminCliOptionsParser.this.commonCommandOptions;


        @Parameter(names = {"-u", "--user-id"}, description = "User id to get stats from", required = true, arity = 1)
        public String userId;

    }


    /*
     * TOOL SUB-COMMANDS
     */
    @Parameters(commandNames = {"install"}, commandDescription = "Install and check a new tool")
    public class InstallToolCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AdminCommonCommandOptions commonOptions = AdminCliOptionsParser.this.commonCommandOptions;


        @Parameter(names = {"-i", "--input"}, description = "File with the new tool to be installed", required = true, arity = 1)
        public String study;

    }

    @Parameters(commandNames = {"list"}, commandDescription = "Print a summary list of all tools")
    public class ListToolCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AdminCommonCommandOptions commonOptions = AdminCliOptionsParser.this.commonCommandOptions;


        @Parameter(names = {"--filter"}, description = "Some kind of filter", arity = 1)
        public String study;

    }

    @Parameters(commandNames = {"show"}, commandDescription = "Show a summary of the tool")
    public class ShowToolCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AdminCommonCommandOptions commonOptions = AdminCliOptionsParser.this.commonCommandOptions;


        @Parameter(names = {"--tool-id"}, description = "Full name of the study where the file is classified", arity = 1)
        public String study;

    }


    /*
     * SERVER SUB-COMMANDS
     */
    @Parameters(commandNames = {"rest"}, commandDescription = "Install and check a new tool")
    public class RestServerCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AdminCommonCommandOptions commonOptions = AdminCliOptionsParser.this.commonCommandOptions;

        @Parameter(names = {"--start"}, description = "File with the new tool to be installed", arity = 0)
        public boolean start;

        @Parameter(names = {"--stop"}, description = "File with the new tool to be installed", arity = 0)
        public boolean stop;
    }

    @Parameters(commandNames = {"grpc"}, commandDescription = "Print a summary list of all tools")
    public class GrpcServerCommandOptions extends CatalogDatabaseCommandOptions {

        @ParametersDelegate
        public AdminCommonCommandOptions commonOptions = AdminCliOptionsParser.this.commonCommandOptions;

        @Parameter(names = {"--start"}, description = "File with the new tool to be installed", arity = 0)
        public boolean start;

        @Parameter(names = {"--stop"}, description = "File with the new tool to be installed", arity = 0)
        public boolean stop;
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

    public AdminCommonCommandOptions getCommonOptions() {
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

    public ServerCommandOptions getServerCommandOptions() {
        return serverCommandOptions;
    }
}
