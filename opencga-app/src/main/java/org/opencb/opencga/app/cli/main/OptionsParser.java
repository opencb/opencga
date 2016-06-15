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

package org.opencb.opencga.app.cli.main;

import com.beust.jcommander.*;
import com.beust.jcommander.converters.IParameterSplitter;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.models.Cohort;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Study;

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
    private final JobsCommands jobsCommands;
    private final ToolCommands toolCommands;
    final CohortCommands cohortCommands;
    final SampleCommands sampleCommands;

//    public final CommandShareResource commandShareResource;


    public OptionsParser(boolean interactive) {
        generalOptions = new GeneralOptions();

        jcommander = new JCommander(generalOptions);

        commonOptions = new CommonOptions();
        userAndPasswordOptions = new UserAndPasswordOptions();
//        commandShareResource = new CommandShareResource();

        userCommands = new UserCommands(jcommander);
        projectCommands = new ProjectCommands(jcommander);
        studyCommands = new StudyCommands(jcommander);
        fileCommands = new FileCommands(jcommander);
        cohortCommands = new CohortCommands(jcommander);
        sampleCommands = new SampleCommands(jcommander);
        jobsCommands = new JobsCommands(jcommander);
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

    public JobsCommands getJobsCommands() {
        return jobsCommands;
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

        @Parameter(names = {"-hp", "--hidden-password"}, description = "Password", arity = 1, required = false,  password = true)
        String hiddenPassword;

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

    class BasicCommand {
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

            @Parameter(names = {"-U", "--show-uris"}, description = "Show uris from linked files and folders", arity = 0)
            public boolean uries = false;

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

            @Parameter(names = {"--session-id", "-sid"}, description = "SessionId", required = false, arity = 1)
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

            @Parameter(names = {"-d", "--description"}, description = "Description", required = false, arity = 1)
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
        final ResyncCommand resyncCommand;
        final ListCommand listCommand;
        final CheckCommand checkCommand;
        final StatusCommand statusCommand;
        final AnnotationCommand annotationCommand;

        public StudyCommands(JCommander jcommander) {
            jcommander.addCommand(this);
            JCommander studies = jcommander.getCommands().get("studies");
            studies.addCommand(createCommand = new CreateCommand());
            studies.addCommand(infoCommand = new InfoCommand());
            studies.addCommand(resyncCommand = new ResyncCommand());
            studies.addCommand(listCommand = new ListCommand());
            studies.addCommand(checkCommand = new CheckCommand());
            studies.addCommand(statusCommand = new StatusCommand());
            studies.addCommand(this.annotationCommand = new AnnotationCommand());
//            studies.addCommand(commandShareResource);
        }

        abstract class BaseStudyCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"-id", "--study-id"}, description = "Study identifier", required = true, arity = 1)
            String id;
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
            Study.Type type = Study.Type.COLLECTION;

            @Parameter(names = {"-d", "--description"}, description = "Organization", required = false, arity = 1)
            String description;

            @Parameter(names = {"--uri"}, description = "URI for the folder where to place the study files. Must be a correct URI.", required = false, arity = 1)
            String uri;

            @Parameter(names = {"--datastore"}, description = "Configure place to store different files. One datastore per bioformat. <bioformat>:<storageEngineName>:<database_name>")
            List<String> datastores;
            
            @Parameter(names = {"--aggregation-type"}, description = "Set the study as aggregated of type {NONE, BASIC, EVS, EXAC}")
            VariantSource.Aggregation aggregated = VariantSource.Aggregation.NONE;
        }

        @Parameters(commandNames = {"resync"}, commandDescription = "Scans the study folder to find changes")
        class ResyncCommand extends BaseStudyCommand {
            @Parameter(names = {"-ch", "--checksum"}, description = "Calculate checksum", required = false, arity = 0)
            boolean calculateChecksum = false;
        }

        @Parameters(commandNames = {"check-files"}, commandDescription = "Check if files in study are correctly tracked.")
        class CheckCommand extends BaseStudyCommand {

            @Parameter(names = {"-ch", "--checksum"}, description = "Calculate checksum", required = false, arity = 0)
            boolean calculateChecksum = false;
        }

        @Parameters(commandNames = {"info"}, commandDescription = "Get study information")
        class InfoCommand  extends BaseStudyCommand {}

        @Parameters(commandNames = {"list"}, commandDescription = "List files in folder")
        class ListCommand extends BaseStudyCommand {
            @Parameter(names = {"-L", "--level"}, description = "Descend only level directories deep.", arity = 1)
            public int level = Integer.MAX_VALUE;

            @Parameter(names = {"-R", "--recursive"}, description = "List subdirectories recursively", arity = 0)
            public boolean recursive = false;

            @Parameter(names = {"-U", "--show-uris"}, description = "Show uris from linked files and folders", arity = 0)
            public boolean uries = false;
        }

        @Parameters(commandNames = {"status"}, commandDescription = "Scans the study folder to find untracked or missing files")
        class StatusCommand extends BaseStudyCommand {}

        @Parameters(commandNames = {"annotate-variants"}, commandDescription = "Annotate variants")
        class AnnotationCommand extends BaseStudyCommand {

            @Parameter(names = {"-o", "--outdir-id"}, description = "Directory ID where to create the file", required = true, arity = 1)
            String outdir = "";

            @Parameter(names = {"--enqueue"}, description = "Enqueue the job to be launched by the execution manager", arity = 0)
            boolean enqueue;

            @Parameter(description = " -- {opencga-storage internal parameter. Use your head}") //Wil contain args after "--"
            public List<String> dashDashParameters;
        }
    }



    @Parameters(commandNames = {"files"}, commandDescription = "File commands")
    class FileCommands {

        final CreateCommand createCommand;
        final CreateFolderCommand createFolderCommand;
        final LinkCommand linkCommand;
        final RelinkCommand relinkCommand;
        final RefreshCommand refreshCommand;
        final UploadCommand uploadCommand;
        final InfoCommand infoCommand;
        final SearchCommand searchCommand;
        final ListCommand listCommand;
        final IndexCommand indexCommand;

        public FileCommands(JCommander jcommander) {
            jcommander.addCommand(this);
            JCommander files = jcommander.getCommands().get("files");
            files.addCommand(this.createCommand = new CreateCommand());
            files.addCommand(this.createFolderCommand = new CreateFolderCommand());
            files.addCommand(this.uploadCommand = new UploadCommand());
            files.addCommand(this.infoCommand = new InfoCommand());
            files.addCommand(this.listCommand = new ListCommand());
            files.addCommand(this.linkCommand = new LinkCommand());
            files.addCommand(this.relinkCommand = new RelinkCommand());
            files.addCommand(this.refreshCommand = new RefreshCommand());
            files.addCommand(this.searchCommand = new SearchCommand());
            files.addCommand(this.indexCommand = new IndexCommand());
//        files.addCommand(commandShareResource);
        }

        class BaseFileCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"-id", "--file-id"}, description = "File id", required = true, arity = 1)
            String id;
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

            @Parameter(names = {"-f", "--format"}, description = "one of {PLAIN, GZIP, BINARY, EXECUTABLE, IMAGE}. See catalog.models.File.Format", required = false, arity = 1)
            File.Format format = File.Format.PLAIN;

            @Parameter(names = {"-b", "--bioformat"}, description = "See catalog.models.File.Bioformat for more info", required = false, arity = 1)
            File.Bioformat bioformat = File.Bioformat.NONE;

            @Parameter(names = {"-P", "--parents"}, description = "Create parent directories if needed", required = false)
            boolean parents;

            @Parameter(names = {"-m", "--move"}, description = "Move file instead of copy", required = false, arity = 0)
            boolean move;

            @Parameter(names = {"-ch", "--checksum"}, description = "Calculate checksum", required = false, arity = 0)
            boolean calculateChecksum = false;
        }

        @Parameters(commandNames = {"upload"}, commandDescription = "Attach a physical file to a catalog entry file.")
        class UploadCommand extends BaseFileCommand{

            @Parameter(names = {"-i", "--input"}, description = "Input file", required = true, arity = 1)
            String inputFile;

            @Parameter(names = {"--replace"}, description = "Replace the existing attached file. ALERT: The existing file will be removed", required = false, arity = 0)
            boolean replace = false;

            @Parameter(names = {"-m", "--move"}, description = "Move file instead of copy", required = false, arity = 0)
            boolean move = false;

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

            @Parameter(names = {"-s", "--study-id"}, description = "studyId", required = true, arity = 1)
            String studyId;

            @Parameter(names = {"-P", "--parents"}, description = "Create parent directories if needed", required = false)
            boolean parents;
        }

        @Parameters(commandNames = {"link"}, commandDescription = "Link an external file into catalog.")
        class LinkCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"-i", "--input"}, description = "File location", required = true, arity = 1)
            String inputFile;

            @Parameter(names = {"-s", "--study-id"}, description = "Study identifier", required = true, arity = 1)
            String studyId;

            @Parameter(names = {"-d", "--description"}, description = "Description", required = false, arity = 1)
            String description;

            @Parameter(names = {"--path"}, description = "New folder path", required = false, arity = 1)
            String path  = "";

            @Parameter(names = {"-P", "--parents"}, description = "Create parent directories if needed", required = false)
            boolean parents;

            @Parameter(names = {"-ch", "--checksum"}, description = "Calculate checksum", required = false, arity = 0)
            boolean calculateChecksum = false;
        }

        @Parameters(commandNames = {"relink"}, commandDescription = "Change file location. Provided file must be either STAGED or an external file")
        class RelinkCommand extends BaseFileCommand {
            @Parameter(names = {"-i", "--input"}, description = "File location", required = true, arity = 1)
            String inputFile;

            @Parameter(names = {"-ch", "--checksum"}, description = "Calculate checksum", required = false, arity = 0)
            boolean calculateChecksum = false;
        }

        @Parameters(commandNames = {"info"}, commandDescription = "Get file information")
        class InfoCommand extends BaseFileCommand {}

        @Parameters(commandNames = {"search"}, commandDescription = "Search files")
        class SearchCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"-s", "--study-id"}, description = "Study id", required = true, arity = 1)
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
            List<String> status;

        }

        @Parameters(commandNames = {"list"}, commandDescription = "List files in folder")
        class ListCommand extends BaseFileCommand {
            @Parameter(names = {"-L", "--level"}, description = "Descend only level directories deep.", arity = 1)
            public int level = Integer.MAX_VALUE;

            @Parameter(names = {"-R", "--recursive"}, description = "List subdirectories recursively", arity = 0)
            public boolean recursive = false;

            @Parameter(names = {"-U", "--show-uris"}, description = "Show uris from linked files and folders", arity = 0)
            public boolean uries = false;
        }

        @Parameters(commandNames = {"refresh"}, commandDescription = "Refresh metadata from the selected file or folder. Print updated files.")
        class RefreshCommand extends BaseFileCommand { }

        @Parameters(commandNames = {"index"}, commandDescription = "Index file in the selected StorageEngine")
        class IndexCommand extends BaseFileCommand {
            @Parameter(names = {"-o", "--outdir-id"}, description = "Directory ID where to create the file", required = false, arity = 1)
            String outdir = "";

            @Parameter(names = {"--enqueue"}, description = "Enqueue the job to be launched by the execution manager", arity = 0)
            boolean enqueue;

            @Parameter(names = "--transform", description = "Run only the transform phase")
            boolean transform = false;

            @Parameter(names = "--load", description = "Run only the load phase")
            boolean load = false;

            @Parameter(names = "--calculate-stats", description = "Calculate stats for cohort ALL", arity = 0)
            boolean calculateStats = false;

            @Parameter(names = "--annotate", description = "Annotate new variants", arity = 0)
            boolean annotate = false;

            @Parameter(description = " -- {opencga-storage internal parameter. Use your head}") //Wil contain args after "--"
            public List<String> dashDashParameters;
        }

    }

    @Parameters(commandNames = {"cohorts"}, commandDescription = "Cohorts methods")
    public class CohortCommands {
        final InfoCommand infoCommand;
        final CreateCommand createCommand;
        final SamplesCommand samplesCommand;
        final StatsCommand statsCommand;

        public CohortCommands(JCommander jcommander) {
            jcommander.addCommand(this);
            JCommander files = jcommander.getCommands().get("cohorts");
            files.addCommand(this.infoCommand = new InfoCommand());
            files.addCommand(this.createCommand = new CreateCommand());
            files.addCommand(this.samplesCommand = new SamplesCommand());
            files.addCommand(this.statsCommand = new StatsCommand());
        }

        @Parameters(commandNames = {InfoCommand.COMMAND_NAME}, commandDescription = "Get cohort information")
        class InfoCommand {

            public static final String COMMAND_NAME = "info";

            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"-id", "--cohort-id"}, description = "Cohort id", required = true, arity = 1)
            long id;
        }

        @Parameters(commandNames = {CreateCommand.COMMAND_NAME}, commandDescription = "Create a cohort")
        class CreateCommand {

            public static final String COMMAND_NAME = "create";

            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"--study-id"}, description = "Study id", required = true, arity = 1)
            String studyId;

            @Parameter(names = {"--name"}, description = "cohort name", required = false, arity = 1)
            String name;

            @Parameter(names = {"--variable-set"}, description = "VariableSetId", required = false, arity = 1)
            String variableSet;

            @Parameter(names = {"--description"}, description = "cohort description", required = false, arity = 1)
            String description;

            @Parameter(names = {"--sample-ids"}, description = "Sample ids for the cohort (CSV)", required = false, arity = 1)
            String sampleIds;

            @Parameter(names = {"--variable"}, description = "Categorical variable name to use to create cohorts", required = false, arity = 1)
            String variable;

            @Parameter(names = {"--type"}, description = "Cohort type", required = false, arity = 1)
            Cohort.Type type;

            @Parameter(names = {"--from-aggregation-mapping-file"}, description = "If the study is aggregated, basic cohorts without samples may be extracted from the mapping file", required = false, arity = 1)
            String tagmap = null;
        }

        @Parameters(commandNames = {SamplesCommand.COMMAND_NAME}, commandDescription = "List samples belonging to a cohort")
        class SamplesCommand {

            public static final String COMMAND_NAME = "samples";

            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"-id", "--cohort-id"}, description = "Cohort id", required = true, arity = 1)
            long id;
        }

        @Parameters(commandNames = {StatsCommand.COMMAND_NAME}, commandDescription = "Calculate variant stats for a set of cohorts.")
        class StatsCommand {

            public static final String COMMAND_NAME = "calculate-stats";

            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"-id", "--cohort-id"}, description = "CSV Cohort id list", required = false)
            List<Long> cohortIds;

            @Parameter(names = {"-o", "--outdir-id"}, description = "Directory ID where to create the file", required = false, arity = 1)
            String outdir = "";

            @Parameter(names = {"--enqueue"}, description = "Enqueue the job to be launched by the execution manager", arity = 0)
            boolean enqueue;
            
            @Parameter(names = {"--aggregation-mapping-file"}, description = "File containing population names mapping in an aggregated VCF file")
            String tagmap = null;
            
            @Parameter(description = " -- {opencga-storage internal parameter. Use your head}") //Wil contain args after "--"
            public List<String> dashDashParameters;
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
        final DeleteCommand deleteCommand;

        public SampleCommands(JCommander jcommander) {
            jcommander.addCommand(this);
            JCommander files = jcommander.getCommands().get("samples");
            files.addCommand(this.infoCommand = new InfoCommand());
            files.addCommand(this.searchCommand = new SearchCommand());
            files.addCommand(this.loadCommand = new LoadCommand());
            files.addCommand(this.deleteCommand = new DeleteCommand());
//            files.addCommand(this.samplesCommand = new SamplesCommand());
        }

        class BaseSampleCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"-id", "--sample-id"}, description = "Sample id", required = true, arity = 1)
            long id;
        }

        @Parameters(commandNames = {"info"}, commandDescription = "Get samples information")
        class InfoCommand extends BaseSampleCommand {
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

            @Parameter(names = {"--variable-set-id"}, description = "VariableSetId that represents the pedigree file", required = false, arity = 1)
            long variableSetId;

            @Parameter(names = {"--pedigree-id"}, description = "Pedigree file id already loaded in OpenCGA", required = true, arity = 1)
            String pedigreeFileId;
        }

        @Parameters(commandNames = {"delete"}, commandDescription = "Deletes the selected sample")
        class DeleteCommand extends BaseSampleCommand {
        }
    }

    @Parameters(commandNames = {"jobs"}, commandDescription = "Jobs commands")
    class JobsCommands {

        final InfoCommand infoCommand;
        final DoneJobCommand doneJobCommand;
        final StatusCommand statusCommand;
        final RunJobCommand runJobCommand;

        public JobsCommands(JCommander jcommander) {
            jcommander.addCommand(this);
            JCommander tools = jcommander.getCommands().get("jobs");
            tools.addCommand(this.infoCommand = new InfoCommand());
            tools.addCommand(this.doneJobCommand = new DoneJobCommand());
            tools.addCommand(this.statusCommand = new StatusCommand());
            tools.addCommand(this.runJobCommand = new RunJobCommand());
        }

        @Parameters(commandNames = {"info"}, commandDescription = "Get job information")
        class InfoCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"-id", "--job-id"}, description = "Job id", required = true, arity = 1)
            long id;
        }

        @Parameters(commandNames = {"finished"}, commandDescription = "Notify catalog that a job have finished.")
        class DoneJobCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"-id", "--job-id"}, description = "Job id", required = true, arity = 1)
            long id;

            @Parameter(names = {"--error"}, description = "Job finish with error", required = false, arity = 0)
            boolean error;

            @Parameter(names = {"--force"}, description = "Force finish job. Ignore if the job was PREPARED, QUEUED or RUNNING", required = false, arity = 0)
            boolean force;

            @Parameter(names = {"--discart-output"}, description = "Discart generated files. Temporal output directory will be deleted.", required = false, arity = 0)
            boolean discardOutput;
        }

        @Parameters(commandNames = {"status"}, commandDescription = "Get the status of all running jobs.")
        class StatusCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"--study-id"}, description = "Study id", required = false, arity = 1)
            String studyId;
        }

        @Parameters(commandNames = {"run"}, commandDescription = "Executes a job.")
        class RunJobCommand {

            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"-t", "--tool-id"}, description = "", required = true, arity = 1)
            String toolId;

            @Parameter(names = {"-s", "--study-id"}, description = "Study id", required = true, arity = 1)
            String studyId;

            @Parameter(names = {"-o", "--outdir"}, description = "Output directory", required = true, arity = 1)
            String outdir;

            @Parameter(names = {"-e", "--execution"}, description = "", required = false, arity = 1)
            String execution;

            @Parameter(names = {"-n", "--name"}, description = "", required = true, arity = 1)
            String name;

            @Parameter(names = {"-d", "--description"}, description = "", required = false, arity = 1)
            String description;

            @DynamicParameter(names = "-P", description = "Parameters", hidden = false)
            ObjectMap params = new ObjectMap();

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

        @Parameters(commandNames = {"info"}, commandDescription = "Get tool information")
        class InfoCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"-id", "--tool-id"}, description = "Tool id", required = true, arity = 1)
            String id;
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
