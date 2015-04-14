package org.opencb.opencga.app.cli.main;

import com.beust.jcommander.*;
import com.beust.jcommander.converters.IParameterSplitter;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.beans.File;
import org.opencb.opencga.catalog.beans.Study;

import java.util.*;

/**
 * Created by jacobo on 29/09/14.
 */
public class OptionsParser {

    private final JCommander jcommander;

    private final GeneralOptions generalOptions;
    private final CommonOptions commonOptions;

    private final UserAndPasswordOptions userAndPasswordOptions;

    private final UserCommands userCommands;
    private final ProjectCommands projectCommands;
    private final StudyCommands studyCommands;
    private final FileCommands fileCommands;
    private final ToolCommands toolCommands;
    final CohortCommands cohortCommands;
    final SampleCommands sampleCommands;

    public final CommandShareResource commandShareResource;


    public OptionsParser(boolean interactive) {
        generalOptions = new GeneralOptions();

        jcommander = new JCommander(generalOptions);

        commonOptions = new CommonOptions();
        userAndPasswordOptions = new UserAndPasswordOptions();
        commandShareResource = new CommandShareResource();

        userCommands = new UserCommands(jcommander);
        projectCommands = new ProjectCommands(jcommander);
        studyCommands = new StudyCommands(jcommander);
        fileCommands = new FileCommands(jcommander);
        cohortCommands = new CohortCommands(jcommander);
        sampleCommands = new SampleCommands(jcommander);
        toolCommands = new ToolCommands(jcommander);


        if (interactive) { //Add interactive commands
//            jcommander.addCommand(new HelpCommands());
            jcommander.addCommand(new ExitCommands());
        }
    }

    public void parse(String[] args) throws ParameterException {
        jcommander.parse(args);
    }

    public String getCommand() {
        String parsedCommand = jcommander.getParsedCommand();
        return parsedCommand != null ? parsedCommand: "";
    }

    public String getSubCommand() {
        String parsedCommand = jcommander.getParsedCommand();
        if (jcommander.getCommands().containsKey(parsedCommand)) {
            String subCommand = jcommander.getCommands().get(parsedCommand).getParsedCommand();
            return subCommand != null ? subCommand: "";
        } else {
            return "";
        }
    }

    public void printUsage(){
        if(!getCommand().isEmpty()) {
            if(!getSubCommand().isEmpty()){
//                usage(getCommand(), getSubcommand());
                jcommander.getCommands().get(getCommand()).usage(getSubCommand());
            } else {
//                jcommander.usage(getCommand());
                new JCommander(jcommander.getCommands().get(getCommand()).getObjects().get(0)).usage();
                System.err.println("Available commands");
                printUsage(jcommander.getCommands().get(getCommand()));
            }
        } else {
            new JCommander(generalOptions).usage();
            System.err.println("Available commands");
            printUsage(jcommander);
        }
    }

    private void printUsage(JCommander commander) {
        int gap = 10;
        int leftGap = 1;
        for (String s : commander.getCommands().keySet()) {
            if (gap < s.length() + leftGap) {
                gap = s.length() + leftGap;
            }
        }
        for (Map.Entry<String, JCommander> entry : commander.getCommands().entrySet()) {
            System.err.printf("%" + gap + "s    %s\n", entry.getKey(), commander.getCommandDescription(entry.getKey()));
        }
    }

    public GeneralOptions getGeneralOptions() {
        return generalOptions;
    }

    public CommonOptions getCommonOptions() {
        return commonOptions;
    }

    public UserAndPasswordOptions getUserAndPasswordOptions() {
        return userAndPasswordOptions;
    }

    public UserCommands getUserCommands() {
        return userCommands;
    }

    public ProjectCommands getProjectCommands() {
        return projectCommands;
    }

    public StudyCommands getStudyCommands() {
        return studyCommands;
    }

    public FileCommands getFileCommands() {
        return fileCommands;
    }

    public ToolCommands getToolCommands() {
        return toolCommands;
    }

    public class GeneralOptions {
        @Parameter(names = {"-h", "--help"}, help = true)
        public boolean help;

        @Parameter(names = {"-V", "--version"})
        public boolean version;

        @Parameter(names = {"-i", "--interactive"})
        public boolean interactive;
    }


    class UserAndPasswordOptions {
        @Parameter(names = {"-u", "--user"}, description = "UserId", required = false, arity = 1)
        String user;

        @Parameter(names = {"-p", "--password"}, description = "Password", arity = 1, required = false,  password = false)
        String password;

        @Parameter(names = {"-sid", "--session-id"}, description = "SessionId", arity = 1, required = false)
        String sessionId;
    }

    enum OutputFormat {IDS, ID_CSV, NAME_ID_MAP, ID_LIST, RAW, PRETTY_JSON, PLAIN_JSON}

    class CommonOptions {
        @Parameter(names = {"-C", "--conf"}, description = "This parameter sets the level of the logging", required = false, arity = 1)
        public String conf;

        @Parameter(names = {"-h", "--help"}, help = true)
        public boolean help;

        @Parameter(names = {"--verbose"}, description = "log-level to debug")
        boolean verbose;

        @Parameter(names = {"--log-level"}, description = "This parameter sets the level of the logging. One of {error, warn, info, debug, trace}")
        String logLevel = null; // TODO add validation?

        @DynamicParameter(names = "-D", description = "Dynamic parameters go here", hidden = true)
        Map<String, String> dynamic = new HashMap<String, String>();

        @Parameter(names = {"--include"}, description = "", required = false, arity = 1)
        public String include;

        @Parameter(names = {"--exclude"}, description = "", required = false, arity = 1)
        public String exclude;

        @Parameter(names = {"--metadata"}, description = "Include metadata information", required = false, arity = 1)
        public boolean metadata = false;

        @Parameter(names = {"--output-format"}, description = "Output format. one of {IDS, ID_CSV, NAME_ID_MAP, ID_LIST, RAW, PRETTY_JSON, PLAIN_JSON}", required = false, arity = 1)
        OutputFormat outputFormat = OutputFormat.PRETTY_JSON;

        QueryOptions getQueryOptions() {
            QueryOptions queryOptions = new QueryOptions(dynamic, false);
            if (include != null && !include.isEmpty()) {
                queryOptions.add("include", include);
            }
            if (exclude != null && !exclude.isEmpty()) {
                queryOptions.add("exclude", exclude);
            }
            return queryOptions;
        }
    }

    @Parameters(commandNames = {"help"}, commandDescription = "Description")
    class HelpCommands {
    }


    @Parameters(commandNames = {"exit"}, commandDescription = "Description")
    class ExitCommands {
    }

    @Parameters(commandNames = {"users"}, commandDescription = "User commands")
    class UserCommands {

        final CreateCommand createCommand;
        final InfoCommand infoCommand;
        final ListCommand listCommand;
        final LoginCommand loginCommand;
        final LogoutCommand logoutCommand;

        public UserCommands(JCommander jcommander) {
            jcommander.addCommand(this);
            JCommander users = jcommander.getCommands().get("users");
            users.addCommand(createCommand = new CreateCommand());
            users.addCommand(infoCommand = new InfoCommand());
            users.addCommand(listCommand = new ListCommand());
            users.addCommand(loginCommand = new LoginCommand());
            users.addCommand(logoutCommand = new LogoutCommand());
        }

        @ParametersDelegate
        CommonOptions cOpt = commonOptions;

        @Parameters(commandNames = {"create"}, commandDescription = "Create new user for OpenCGA-Catalog")
        class CreateCommand {

//            @ParametersDelegate
//            UserAndPasswordOptions up = userAndPasswordOptions;

            @Parameter(names = {"-u", "--user"}, description = "UserId", required = false, arity = 1)
            String user;

            @Parameter(names = {"-p", "--password"}, description = "Password", arity = 1, required = false,  password = false)
            String password;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"-n", "--name"}, description = "User name", required = true, arity = 1)
            String name;

            @Parameter(names = {"-e", "--email"}, description = "Email", required = true, arity = 1)
            String email;

            @Parameter(names = {"-o", "--organization"}, description = "Organization", required = false, arity = 1)
            String organization;
        }

        @Parameters(commandNames = {"info"}, commandDescription = "Get user's information")
        class InfoCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

        }


        @Parameters(commandNames = {"list"}, commandDescription = "List all projects and studies from a selected user")
        class ListCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"-L", "--level"}, description = "Descend only level directories deep.", arity = 1)
            public int level = Integer.MAX_VALUE;

            @Parameter(names = {"-R", "--recursive"}, description = "List subdirectories recursively", arity = 0)
            public boolean recursive = false;

        }

        @Parameters(commandNames = {"login"}, commandDescription = "Login as user and return its sessionId")
        public class LoginCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

//            @Parameter(names = {"-p", "--password"}, description = "Password read from the console", arity = 0, required = false,  password = true)
//            String password;
        }

        @Parameters(commandNames = {"logout"}, commandDescription = "End user session")
        public class LogoutCommand {

//            @Parameter(names = {"-u", "--user"}, description = "UserId", required = false, arity = 1)
//            String user;

            @Parameter(names = {"--session-id", "-sid"}, description = "SessionId", required = true, arity = 1)
            public String sessionId;
        }
    }


    @Parameters(commandNames = {"projects"}, commandDescription = "Project commands")
    class ProjectCommands {

        final CreateCommand createCommand;
        final InfoCommand infoCommand;

        @ParametersDelegate
        CommonOptions cOpt = commonOptions;

        public ProjectCommands(JCommander jcommander) {
            jcommander.addCommand(this);
            JCommander projects = jcommander.getCommands().get("projects");
            projects.addCommand(this.createCommand = new CreateCommand());
            projects.addCommand(this.infoCommand = new InfoCommand());
//        projects.addCommand(commandShareResource);
        }

        @Parameters(commandNames = {"create"}, commandDescription = "Create new project")
        class CreateCommand {

            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"-n", "--name"}, description = "Project name", required = true, arity = 1)
            String name;

            @Parameter(names = {"-a", "--alias"}, description = "Alias", required = true, arity = 1)
            String alias;

            @Parameter(names = {"-d", "--description"}, description = "Description", required = true, arity = 1)
            String description;

            @Parameter(names = {"-o", "--organization"}, description = "Organization", required = false, arity = 1)
            String organization;
        }

        @Parameters(commandNames = {"info"}, commandDescription = "Get project information")
        class InfoCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"-id", "--project-id"}, description = "Project identifier", required = true, arity = 1)
            String id;

        }
    }

    @Parameters(commandNames = {"studies"}, commandDescription = "Study commands")
    class StudyCommands {

        final CreateCommand createCommand;
        final InfoCommand infoCommand;

        public StudyCommands(JCommander jcommander) {
            jcommander.addCommand(this);
            JCommander studies = jcommander.getCommands().get("studies");
            studies.addCommand(createCommand = new CreateCommand());
            studies.addCommand(infoCommand = new InfoCommand());
            studies.addCommand(commandShareResource);

        }

        @Parameters(commandNames = {"create"}, commandDescription = "Create new study")
        class CreateCommand {

            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"--project-id"}, description = "Project identifier", required = true, arity = 1)
            String projectId;

            @Parameter(names = {"-n", "--name"}, description = "Study name", required = true, arity = 1)
            String name;

            @Parameter(names = {"-a", "--alias"}, description = "alias", required = true, arity = 1)
            String alias;

            @Parameter(names = {"-t", "--type"}, description = "Type", required = false, arity = 1)
            Study.Type type = Study.Type.CASE_CONTROL;

            @Parameter(names = {"-d", "--description"}, description = "Organization", required = false, arity = 1)
            String description;

            @Parameter(names = {"--uri"}, description = "URI for the folder where to place the study files. Must be a correct URI.", required = false, arity = 1)
            String uri;
        }

        @Parameters(commandNames = {"info"}, commandDescription = "Get study information")
        class InfoCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"-id", "--study-id"}, description = "Study identifier", required = true, arity = 1)
            String id;
        }
    }



    @Parameters(commandNames = {"files"}, commandDescription = "File commands")
    class FileCommands {

        final CreateCommand createCommand;
        final CreateFolderCommand createFolderCommand;
        final InfoCommand infoCommand;
        final SearchCommand searchCommand;
        final ListCommand listCommand;
        final IndexCommand indexCommand;
        final StatsCommand statsCommand;
        final AnnotationCommand annotationCommand;

        public FileCommands(JCommander jcommander) {
            jcommander.addCommand(this);
            JCommander files = jcommander.getCommands().get("files");
            files.addCommand(this.createCommand = new CreateCommand());
            files.addCommand(this.createFolderCommand = new CreateFolderCommand());
            files.addCommand(this.infoCommand = new InfoCommand());
            files.addCommand(this.searchCommand = new SearchCommand());
            files.addCommand(this.listCommand = new ListCommand());
            files.addCommand(this.indexCommand = new IndexCommand());
            files.addCommand(this.statsCommand = new StatsCommand());
            files.addCommand(this.annotationCommand = new AnnotationCommand());
//        files.addCommand(commandShareResource);
        }

        @Parameters(commandNames = {"create"}, commandDescription = "Create file")
        class CreateCommand {

            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"-i", "--input"}, description = "Input file", required = true, arity = 1)
            String inputFile;

            @Parameter(names = {"-s", "--study-id"}, description = "studyId", required = true, arity = 1)
            String studyId;

            @Parameter(names = {"--path"}, description = "Directory where to create the file", required = false, arity = 1)
            String path  = "";

            @Parameter(names = {"-d", "--description"}, description = "Description", required = false, arity = 1)
            String description;


            @Parameter(names = {"-f", "--format"}, description = "one of {PLAIN, GZIP, BINARY, EXECUTABLE, IMAGE}. See catalog.beans.File.Format", required = false, arity = 1)
            File.Format format = File.Format.PLAIN;

            @Parameter(names = {"-b", "--bioformat"}, description = "See catalog.beans.File.Bioformat for more info", required = false, arity = 1)
            File.Bioformat bioformat = File.Bioformat.NONE;

            @Parameter(names = {"-P", "--parents"}, description = "Create parent directories if needed", required = false)
            boolean parents;

            @Parameter(names = {"-m", "--move"}, description = "Move file instead of copy", required = false, arity = 0)
            boolean move;

            @Parameter(names = {"-ch", "--checksum"}, description = "Calculate checksum", required = false, arity = 0)
            boolean calculateChecksum = false;
        }

        @Parameters(commandNames = {"create-folder"}, commandDescription = "Create Folder")
        class CreateFolderCommand {

            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"--path"}, description = "New folder path", required = true, arity = 1)
            String path  = "";

            @Parameter(names = {"-s", "--studyId"}, description = "studyId", required = true, arity = 1)
            String studyId;

            @Parameter(names = {"-P", "--parents"}, description = "Create parent directories if needed", required = false)
            boolean parents;
        }

        @Parameters(commandNames = {"info"}, commandDescription = "Get file information")
        class InfoCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"-id", "--file-id"}, description = "File id", required = true, arity = 1)
            String id;
        }

        @Parameters(commandNames = {"search"}, commandDescription = "Search files")
        class SearchCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"--study-id"}, description = "Study id", required = true, arity = 1)
            String studyId;
//            @Parameter(names = {"--name"}, description = "Exact file name", required = false, arity = 1)
//            String name;
//            @Parameter(names = {"--path"}, description = "Exact file path", required = false, arity = 1)
//            String path;
            @Parameter(names = {"--name"}, description = "File name. Use regex pattern", required = false, arity = 1)
            String name;

            @Parameter(names = {"--directory"}, description = "Directory path (study relative). Use regex pattern", required = false, arity = 1)
            String directory;

            @Parameter(names = {"--type"}, description = "File type. CSV", required = false, arity = 1)
            List<File.Type> types;

            @Parameter(names = {"--bioformat"}, description = "File bioformat. CSV", required = false, arity = 1)
            List<File.Bioformat> bioformats;

            @Parameter(names = {"--status"}, description = "File status. CSV", required = false, arity = 1)
            List<File.Status> status;

        }

        @Parameters(commandNames = {"list"}, commandDescription = "List files in folder")
        class ListCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"-id", "--file-id"}, description = "Folder id", required = true, arity = 1)
            String id;

            @Parameter(names = {"-L", "--level"}, description = "Descend only level directories deep.", arity = 1)
            public int level = Integer.MAX_VALUE;

            @Parameter(names = {"-R", "--recursive"}, description = "List subdirectories recursively", arity = 0)
            public boolean recursive = false;
        }

        @Parameters(commandNames = {"index"}, commandDescription = "Index file in the selected StorageEngine")
        class IndexCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"-id", "--file-id"}, description = "File id", required = true, arity = 1)
            String id;

            @Parameter(names = {"--storage-engine"}, description = "", required = false, arity = 1)
            String storageEngine;

            @Parameter(names = {"-o", "--outdir-id"}, description = "Directory ID where to create the file", required = false, arity = 1)
            String outdir = "";

            @Parameter(names = {"--enqueue"}, description = "Enqueue the job to be launched by the execution manager", arity = 0)
            boolean enqueue;

            @Parameter(names = {"--indexed-file-id"}, description = "Use an existing indexed file instead of creating a new file in Catalog.")
            String indexedFileId;
            
            @Parameter(names = {"--database"}, description = "Specify the database name instead of using the default database.")
            String dbName;

            @Parameter(description = " -- {opencga-storage internal parameter. Use your head}") //Wil contain args after "--"
            public List<String> dashDashParameters;
        }

        @Parameters(commandNames = {"stats-variants"}, commandDescription = "Calculate variant stats for a set of cohorts.")
        class StatsCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"-id", "--file-id"}, description = "File id", required = true, arity = 1)
            String id;

            @Parameter(names = {"--cohort-id"}, description = "CSV for all cohort-id to calculate stats", required = true, arity = 1)
            List<Integer> cohortIds;

            @Parameter(names = {"--enqueue"}, description = "Enqueue the job to be launched by the execution manager", arity = 0)
            boolean enqueue;

            @Parameter(description = " -- {opencga-storage internal parameter. Use your head}") //Wil contain args after "--"
            public List<String> dashDashParameters;
        }

        @Parameters(commandNames = {"annotate-variants"}, commandDescription = "Annotate variants")
        class AnnotationCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"-id", "--file-id"}, description = "File id", required = true, arity = 1)
            String id;

            @Parameter(names = {"--enqueue"}, description = "Enqueue the job to be launched by the execution manager", arity = 0)
            boolean enqueue;

            @Parameter(description = " -- {opencga-storage internal parameter. Use your head}") //Wil contain args after "--"
            public List<String> dashDashParameters;
        }
    }

    @Parameters(commandNames = {"cohorts"}, commandDescription = "Cohorts methods")
    public class CohortCommands {
        final InfoCommand infoCommand;
        final CreateCommand createCommand;
        final SamplesCommand samplesCommand;

        public CohortCommands(JCommander jcommander) {
            jcommander.addCommand(this);
            JCommander files = jcommander.getCommands().get("cohorts");
            files.addCommand(this.infoCommand = new InfoCommand());
            files.addCommand(this.createCommand = new CreateCommand());
            files.addCommand(this.samplesCommand = new SamplesCommand());
        }

        @Parameters(commandNames = {"info"}, commandDescription = "Get cohort information")
        class InfoCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"-id", "--cohort-id"}, description = "Cohort id", required = true, arity = 1)
            int id;
        }

        @Parameters(commandNames = {"create"}, commandDescription = "Create a cohort")
        class CreateCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"--study-id"}, description = "Study id", required = true, arity = 1)
            String studyId;

            @Parameter(names = {"--name"}, description = "cohort name", required = false, arity = 1)
            String name;

            @Parameter(names = {"--variable-set-id"}, description = "VariableSetId", required = false, arity = 1)
            int variableSetId;

            @Parameter(names = {"--description"}, description = "cohort description", required = false, arity = 1)
            String description;

            @Parameter(names = {"--sample-ids"}, description = "Sample ids for the cohort (CSV)", required = false, arity = 1)
            String sampleIds;

            @Parameter(names = {"--variable"}, description = "Categorical variable name to use to create cohorts", required = false, arity = 1)
            String variable;
        }

        @Parameters(commandNames = {"samples"}, commandDescription = "List samples belonging to a cohort")
        class SamplesCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"-id", "--cohort-id"}, description = "Cohort id", required = true, arity = 1)
            int id;
        }
    }
    public static class SemiColonParameterSplitter implements IParameterSplitter {

        public List<String> split(String value) {
            return Arrays.asList(value.split(";"));
        }

    }
    public static class NoSplitter implements IParameterSplitter {

        public List<String> split(String value) {
            return Arrays.asList(value.split(";"));
        }

    }
    @Parameters(commandNames = {"samples"}, commandDescription = "Samples commands")
    public class SampleCommands {

        final InfoCommand infoCommand;
        final SearchCommand searchCommand;
        final LoadCommand loadCommand;

        public SampleCommands(JCommander jcommander) {
            jcommander.addCommand(this);
            JCommander files = jcommander.getCommands().get("samples");
            files.addCommand(this.infoCommand = new InfoCommand());
            files.addCommand(this.searchCommand = new SearchCommand());
            files.addCommand(this.loadCommand = new LoadCommand());
//            files.addCommand(this.samplesCommand = new SamplesCommand());
        }

        @Parameters(commandNames = {"info"}, commandDescription = "Get samples information")
        class InfoCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"-id", "--sample-id"}, description = "Sample id", required = true, arity = 1)
            int id;
        }

        @Parameters(commandNames = {"search"}, commandDescription = "Search samples")
        class SearchCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"--study-id"}, description = "Study id", required = true, arity = 1)
            String studyId;

            @Parameter(names = {"--variable-set-id"}, description = "VariableSetId", required = false, arity = 1)
            String variableSetId;

            @Parameter(names = {"--name"}, description = "Sample names (CSV)", required = false, arity = 1)
            String sampleNames;

            @Parameter(names = {"-id", "--sample-id"}, description = "Sample ids (CSV)", required = false, arity = 1)
            String sampleIds;

            @Parameter(names = {"-a", "--annotation"}, description = "SampleAnnotations values. <variableName>:<annotationValue>(,<annotationValue>)*", required = false, arity = 1, splitter = SemiColonParameterSplitter.class)
            List<String> annotation;
        }

        @Parameters(commandNames = {"load"}, commandDescription = "Load samples from a pedigree file")
        class LoadCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"--study-id"}, description = "Study id where to load samples", required = true, arity = 1)
            String studyId;

            @Parameter(names = {"--variable-set-id"}, description = "VariableSetId that represents the pedigree file", required = false, arity = 1)
            int variableSetId;

            @Parameter(names = {"--pedigree-id"}, description = "Pedigree file id already loaded in OpenCGA", required = false, arity = 1)
            String pedigreeFileId;

//            @Parameter(names = {"--pedigree-file"}, description = "Pedigree file not loaded in OpenCGA", required = false, arity = 1)
//            String pedigreeFile;
        }
    }


    @Parameters(commandNames = {"tools"}, commandDescription = "Tools commands")
    class ToolCommands {

        final CreateCommand createCommand;
        final InfoCommand infoCommand;

        public ToolCommands(JCommander jcommander) {
            jcommander.addCommand(this);
            JCommander tools = jcommander.getCommands().get("tools");
            tools.addCommand(this.createCommand = new CreateCommand());
            tools.addCommand(this.infoCommand = new InfoCommand());
        }

        @Parameters(commandNames = {"create"}, commandDescription = "Register external tool into catalog")
        class CreateCommand {

            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"-a", "--alias"}, description = "alias", required = true, arity = 1)
            String alias;

            @Parameter(names = {"-d", "--description"}, description = "Tool description", required = false, arity = 1)
            String description = "";

//            @Parameter(names = {"-n", "--name"}, description = "Name", required = true, arity = 1)
//            String name;

            @Parameter(names = {"-P", "--path"}, description = "Path", required = true, arity = 1)
            String path;

            @Parameter(names = {"--open"}, description = "Allow other users to use the tool", required = false, arity = 0)
            boolean openTool = false;
        }

        @Parameters(commandNames = {"info"}, commandDescription = "Get file information")
        class InfoCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"-id", "--tool-id"}, description = "Tool id", required = true, arity = 1)
            int id;
        }
    }

    @Parameters(commandNames = {"share"}, commandDescription = "Share resource")
    class CommandShareResource {
        @ParametersDelegate
        UserAndPasswordOptions up = userAndPasswordOptions;

        @ParametersDelegate
        CommonOptions cOpt = commonOptions;

        @Parameter(names = {"-id"}, description = "Unique identifier", required = true, arity = 1)
        public String id;

        @Parameter(names = {"-U"}, description = "User to share", required = true, arity = 1)
        public String user;

        @Parameter(names = {"-r"}, description = "Read", required = true, arity = 1)
        public boolean read;

        @Parameter(names = {"-w"}, description = "Write", required = true, arity = 1)
        public boolean write;

        @Parameter(names = {"-x"}, description = "Execute", required = true, arity = 1)
        public boolean execute;

        @Parameter(names = {"-d"}, description = "Delete", required = true, arity = 1)
        public boolean delete;
    }


}
