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
import org.opencb.commons.utils.CommandLineUtils;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.admin.AdminCliOptionsParser;
import org.opencb.opencga.app.cli.main.options.ProjectCommandOptions;
import org.opencb.opencga.app.cli.main.options.StudyCommandOptions;
import org.opencb.opencga.app.cli.main.options.UserCommandOptions;
import org.opencb.opencga.catalog.models.Cohort;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.core.common.GitRepositoryState;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by imedina on AdminMain.
 */
public class OpencgaCliOptionsParser {

    private final JCommander jCommander;

    private final GeneralOptions generalOptions;
    private final OpencgaCommonCommandOptions commonCommandOptions;

    protected final UserAndPasswordOptions userAndPasswordOptions;

    private UserCommandOptions usersCommandOptions;
    private ProjectCommandOptions projectCommandOptions;
    private StudyCommandOptions studyCommandOptions;
    private FileCommandsOptions fileCommands;
    private JobsCommandsOptions jobsCommands;
    private IndividualsCommandsOptions individualsCommandsOptions;
    private ToolCommandsOptions toolCommands;
    private CohortCommandsOptions cohortCommands;
    private SampleCommandsOptions sampleCommands;

//    public final CommandShareResource commandShareResource;

    public OpencgaCliOptionsParser() {
        this(false);
    }

    public OpencgaCliOptionsParser(boolean interactive) {
        generalOptions = new GeneralOptions();

        jCommander = new JCommander(generalOptions);

        commonCommandOptions = new OpencgaCommonCommandOptions();
        userAndPasswordOptions = new UserAndPasswordOptions();
//        commandShareResource = new CommandShareResource();

        usersCommandOptions = new UserCommandOptions(this.commonCommandOptions, this.jCommander);
        jCommander.addCommand("users", usersCommandOptions);
        JCommander userSubCommands = jCommander.getCommands().get("users");
        userSubCommands.addCommand("create", usersCommandOptions.createUserCommandOptions);
        userSubCommands.addCommand("info", usersCommandOptions.infoCommandOptions);
        userSubCommands.addCommand("list", usersCommandOptions.listCommandOptions);
        userSubCommands.addCommand("login", usersCommandOptions.loginCommandOptions);
        userSubCommands.addCommand("logout", usersCommandOptions.logoutCommandOptions);

        projectCommandOptions = new ProjectCommandOptions(this.commonCommandOptions, jCommander);
        jCommander.addCommand("projects", projectCommandOptions);
        JCommander projectSubCommands = jCommander.getCommands().get("projects");
        projectSubCommands.addCommand("create", projectCommandOptions.createCommandOptions);
        projectSubCommands.addCommand("info", projectCommandOptions.infoCommandOptions);
        projectSubCommands.addCommand("studies", projectCommandOptions.studiesCommandOptions);
        projectSubCommands.addCommand("update", projectCommandOptions.updateCommandOptions);
        projectSubCommands.addCommand("delete", projectCommandOptions.deleteCommandOptions);

        studyCommandOptions = new StudyCommandOptions(this.commonCommandOptions, jCommander);
        jCommander.addCommand("studies", studyCommandOptions);
        JCommander studySubCommands = jCommander.getCommands().get("studies");
        studySubCommands.addCommand("create", studyCommandOptions.createCommandOptions);
        studySubCommands.addCommand("info", studyCommandOptions.infoCommandOptions);
        studySubCommands.addCommand("list", studyCommandOptions.listCommandOptions);
        studySubCommands.addCommand("resync", studyCommandOptions.resyncCommandOptions);
        studySubCommands.addCommand("check-files", studyCommandOptions.checkCommandOptions);
        studySubCommands.addCommand("status", studyCommandOptions.statusCommandOptions);
        studySubCommands.addCommand("annotate-variants", studyCommandOptions.annotationCommandOptions);

        fileCommands = new FileCommandsOptions(jCommander);
        jobsCommands = new JobsCommandsOptions(jCommander);
        individualsCommandsOptions = new IndividualsCommandsOptions(jCommander);
        toolCommands = new ToolCommandsOptions(jCommander);
        cohortCommands = new CohortCommandsOptions(jCommander);
        sampleCommands = new SampleCommandsOptions(jCommander);


        if (interactive) { //Add interactive commands
//            jCommander.addCommand(new HelpCommands());
            jCommander.addCommand(new ExitCommands());
        }
    }

//    public void parse(String[] args) throws ParameterException {
//        jCommander.parse(args);
//    }
//
//    public String getCommand() {
//        String parsedCommand = jCommander.getParsedCommand();
//        return parsedCommand != null ? parsedCommand: "";
//    }
//
//    public String getSubCommand() {
//        String parsedCommand = jCommander.getParsedCommand();
//        if (jCommander.getCommands().containsKey(parsedCommand)) {
//            String subCommand = jCommander.getCommands().get(parsedCommand).getParsedCommand();
//            return subCommand != null ? subCommand: "";
//        } else {
//            return "";
//        }
//    }

//    public void printUsage(){
//        if(!getCommand().isEmpty()) {
//            if(!getSubCommand().isEmpty()){
////                usage(getCommand(), getSubcommand());
//                jCommander.getCommands().get(getCommand()).usage(getSubCommand());
//            } else {
////                jCommander.usage(getCommand());
//                new JCommander(jCommander.getCommands().get(getCommand()).getObjects().get(0)).usage();
//                System.err.println("Available commands");
//                printUsage(jCommander.getCommands().get(getCommand()));
//            }
//        } else {
//            new JCommander(generalOptions).usage();
//            System.err.println("Available commands");
//            printUsage(jCommander);
//        }
//    }

//    private void printUsage(JCommander commander) {
//        int gap = 10;
//        int leftGap = 1;
//        for (String s : commander.getCommands().keySet()) {
//            if (gap < s.length() + leftGap) {
//                gap = s.length() + leftGap;
//            }
//        }
//        for (Map.Entry<String, JCommander> entry : commander.getCommands().entrySet()) {
//            System.err.printf("%" + gap + "s    %s\n", entry.getKey(), commander.getCommandDescription(entry.getKey()));
//        }
//    }

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
            if (!objects.isEmpty() && objects.get(0) instanceof AdminCliOptionsParser.AdminCommonCommandOptions) {
                return ((AdminCliOptionsParser.AdminCommonCommandOptions) objects.get(0)).help;
            }
        }
        return commonCommandOptions.help;
    }




    public class GeneralOptions {
        @Parameter(names = {"-h", "--help"}, help = true)
        public boolean help;

        @Parameter(names = {"-V", "--version"})
        public boolean version;

        @Parameter(names = {"-i", "--interactive"})
        public boolean interactive;
    }

    /**
     * This class contains all those parameters available for all 'commands'
     */
    public class CommandOptions {

//        @Parameter(names = {"-h", "--help"},  description = "This parameter prints this help", help = true)
//        public boolean help;

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

    public class UserAndPasswordOptions {

        @Parameter(names = {"-u", "--user"}, description = "UserId", required = false, arity = 1)
        public String user;

        @Parameter(names = {"-p", "--password"}, description = "Password", arity = 1, required = false,  password = true)
        public String password;

        @Deprecated
        @Parameter(names = {"-hp", "--hidden-password"}, description = "Password", arity = 1, required = false,  password = true)
        public String hiddenPassword;

        @Parameter(names = {"-sid", "--session-id"}, description = "SessionId", arity = 1, required = false, hidden = true)
        public String sessionId;
    }

    enum OutputFormat {IDS, ID_CSV, NAME_ID_MAP, ID_LIST, RAW, PRETTY_JSON, PLAIN_JSON}

    //    class CommonOptions {
    public static class OpencgaCommonCommandOptions extends GeneralCliOptions.CommonCommandOptions {

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

    @Parameters(commandNames = {"files"}, commandDescription = "File commands")
    public class FileCommandsOptions extends CommandOptions{

        public final CreateCommand createCommand;
        public final CreateFolderCommand createFolderCommand;
        public final InfoCommand infoCommand;
        public final DownloadCommand downloadCommand;
        public final GrepCommand grepCommand;
        public final SearchCommand searchCommand;
        public final ListCommand listCommand;
        public final IndexCommand indexCommand;
        public final AlignamentsCommand alignamentsCommand;
        public final FetchCommand fetchCommand;
        //final VariantsCommand variantsCommand;
        public final ShareCommand shareCommand;
        public final UpdateCommand updateCommand;
        public final RelinkCommand relinkCommand;
        public final DeleteCommand deleteCommand;
        public final RefreshCommand refreshCommand;
        public final UnlinkCommand unlink;
        public final LinkCommand linkCommand;
        public final UploadCommand uploadCommand;



        public OpencgaCliOptionsParser.OpencgaCommonCommandOptions commonOptions = OpencgaCliOptionsParser.this.commonCommandOptions;
        public FileCommandsOptions(JCommander jcommander) {
            jcommander.addCommand(this);
            JCommander files = jcommander.getCommands().get("files");
            files.addCommand(this.createCommand = new CreateCommand());
            files.addCommand(this.createFolderCommand = new CreateFolderCommand());
            files.addCommand(this.infoCommand = new InfoCommand());
            files.addCommand(this.downloadCommand = new DownloadCommand());
            files.addCommand(this.grepCommand = new GrepCommand());
            files.addCommand(this.searchCommand = new SearchCommand());
            files.addCommand(this.listCommand = new ListCommand());
            files.addCommand(this.indexCommand = new IndexCommand());
            files.addCommand(this.alignamentsCommand = new AlignamentsCommand());
            files.addCommand(this.fetchCommand = new FetchCommand());
            files.addCommand(this.shareCommand = new ShareCommand());
            files.addCommand(this.updateCommand = new UpdateCommand());
            files.addCommand(this.relinkCommand = new RelinkCommand());
            files.addCommand(this.deleteCommand = new DeleteCommand());
            files.addCommand(this.refreshCommand = new RefreshCommand());
            files.addCommand(this.unlink = new UnlinkCommand());
            files.addCommand(this.linkCommand = new LinkCommand());
            files.addCommand(this.uploadCommand = new UploadCommand());


//        files.addCommand(commandShareResource);
        }

        public class BaseFileCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            OpencgaCommonCommandOptions cOpt = commonCommandOptions;

            @Parameter(names = {"-id", "--file-id"}, description = "File id", required = true, arity = 1)
            public String id;
        }

        @Parameters(commandNames = {"create"}, commandDescription = "Create file")
        public class CreateCommand {

            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            OpencgaCommonCommandOptions cOpt = commonCommandOptions;

            @Parameter(names = {"-i", "--input"}, description = "Input file", required = true, arity = 1)
            String inputFile;

            @Parameter(names = {"-s", "--study-id"}, description = "studyId", required = true, arity = 1)
            String studyId;

            @Parameter(names = {"--path"}, description = "Directory where to create the file", required = false, arity = 1)
            public String path  = "";

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

        @Parameters(commandNames = {"create-folder"}, commandDescription = "Create Folder")
        class CreateFolderCommand {

            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            OpencgaCommonCommandOptions cOpt = commonCommandOptions;

            @Parameter(names = {"--path"}, description = "New folder path", required = true, arity = 1)
            String path  = "";

            @Parameter(names = {"-s", "--study-id"}, description = "studyId", required = true, arity = 1)
            String studyId;

            @Parameter(names = {"-P", "--parents"}, description = "Create parent directories if needed", required = false)
            boolean parents = true;
        }

        @Parameters(commandNames = {"info"}, commandDescription = "Get file information")
        public class InfoCommand extends BaseFileCommand {}

        @Parameters(commandNames = {"download"}, commandDescription = "Download file")
        class DownloadCommand extends BaseFileCommand {}

        @Parameters(commandNames = {"grep"}, commandDescription = "Get file information")
        class GrepCommand extends BaseFileCommand {
            @Parameter(names = {"--pattern"}, description = "Pattern", required = false, arity = 1)
            String pattern  = "";

            @Parameter(names = {"-ic", "--ignoreCase"}, description = "ignoreCase", required = false, arity = 0)
            boolean ignoreCase = false;

            @Parameter(names = {"-m", "--multi"}, description = "multi", required = false, arity = 0)
            boolean multi = true;
        }

        @Parameters(commandNames = {"search"}, commandDescription = "Search files")
        class SearchCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            OpencgaCommonCommandOptions cOpt = commonCommandOptions;

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
        public class ListCommand extends BaseFileCommand {
            @Parameter(names = {"--level"}, description = "Descend only level directories deep.", arity = 1)
            public int level = Integer.MAX_VALUE;

            @Parameter(names = {"-R", "--recursive"}, description = "List subdirectories recursively", arity = 0)
            public boolean recursive = false;

            @Parameter(names = {"-U", "--show-uris"}, description = "Show uris from linked files and folders", arity = 0)
            public boolean uries = false;
        }

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

        @Parameters(commandNames = {"alignaments"}, commandDescription = "Fetch alignments from a BAM file")
        class AlignamentsCommand extends BaseFileCommand { }


        @Parameters(commandNames = {"fetch"}, commandDescription = "File fetch")
        class FetchCommand extends BaseFileCommand {
            @Parameter(names = {"--region"}, description = "Region", required = true, arity = 1)
            String region;

            @Parameter(names = {"--view_as_pairs"}, description = "View_as_pairs", required = false, arity = 0)
            boolean view_as_pairs = false;

            @Parameter(names = {"--include_coverage"}, description = "Include_coverage", required = false, arity = 0)
            boolean include_coverage = true;

            @Parameter(names = {"--process_differences"}, description = "Process_differences", required = false, arity = 0)
            boolean process_differences = true;

            @Parameter(names = {"--histogram"}, description = "Histogram", required = false, arity = 0)
            boolean histogram = false;

            @Parameter(names = {"--groupBy"}, description = "GroupBy: [ct, gene, ensemblGene]", required = false, arity = 1)
            String groupBy;

            @Parameter(names = {"--variantSource"}, description = "VariantSource", required = false, arity = 0)
            boolean variantSource = false;

            @Parameter(names = {"--interval"}, description = "Interval", required = false, arity = 1)
            String interval;

        }

        @Parameters(commandNames = {"share"}, commandDescription = "Share file with other user")
        class ShareCommand  {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            OpencgaCommonCommandOptions cOpt = commonCommandOptions;

            @Parameter(names = {"-fids","--fileIds"}, description = "fileIds", required = true)
            String fileIds;

            @Parameter(names = {"-uids","--userIds"}, description = "User you want to share the file with. Accepts: '{userId}', '@{groupId}' or '*'", required = true)
            String userIds;

            @Parameter(names = {"--unshare"}, description = "Remove the previous AclEntry", required = false, arity = 0)
            boolean unshare = false;

            @Parameter(names = {"--read"}, description = "Read permission", required = false, arity = 0)
            boolean read = false;

            @Parameter(names = {"--write"}, description = "Write permission", required = false, arity = 0)
            boolean write = false;

            @Parameter(names = {"--delete"}, description = "Delete permission", required = false, arity = 0)
            boolean delete = false;

        }


        @Parameters(commandNames = {"update"}, commandDescription = "Modify file")
        class UpdateCommand extends BaseFileCommand { }

        @Parameters(commandNames = {"relink"}, commandDescription = "Change file location. Provided file must be either STAGED or an external file")
        class RelinkCommand extends BaseFileCommand {
            @Parameter(names = {"-i", "--input"}, description = "File location", required = true, arity = 1)
            String inputFile;


            @Parameter(names = {"-ch", "--checksum"}, description = "Calculate checksum", required = false, arity = 0)
            boolean calculateChecksum = false;
        }


        @Parameters(commandNames = {"delete"}, commandDescription = "Delete file")
        class DeleteCommand extends BaseFileCommand { }


        @Parameters(commandNames = {"refresh"}, commandDescription = "Refresh metadata from the selected file or folder. Print updated files.")
        class RefreshCommand extends BaseFileCommand { }

        @Parameters(commandNames = {"unlink"}, commandDescription = "Unlink an external file from catalog")
        class UnlinkCommand extends BaseFileCommand { }


        @Parameters(commandNames = {"link"}, commandDescription = "Link an external file into catalog.")
        public class LinkCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            OpencgaCommonCommandOptions cOpt = commonCommandOptions;

            // @Parameter(names = {"-i", "--input"}, description = "File location", required = true, arity = 1)
            // String inputFile;
            @Parameter(names = {"-uri"}, description = "File location", required = true, arity = 1)
            public String uri;

            @Parameter(names = {"-s", "--study-id"}, description = "Study identifier", required = true, arity = 1)
            public String studyId;

            @Parameter(names = {"-d", "--description"}, description = "Description", required = false, arity = 1)
            String description;

            @Parameter(names = {"--path"}, description = "New folder path", required = false, arity = 1)
            public String path  = "";

            @Parameter(names = {"-P", "--parents"}, description = "Create parent directories if needed", required = false)
            boolean parents;

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



    }

    @Parameters(commandNames = {"jobs"}, commandDescription = "Jobs commands")
    public class JobsCommandsOptions extends CommandOptions{

        final InfoCommand infoCommand;
        final DoneJobCommand doneJobCommand;
        final StatusCommand statusCommand;
        final RunJobCommand runJobCommand;

        public OpencgaCliOptionsParser.OpencgaCommonCommandOptions commonOptions = OpencgaCliOptionsParser.this.commonCommandOptions;
        public JobsCommandsOptions(JCommander jcommander) {
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
            OpencgaCommonCommandOptions cOpt = commonCommandOptions;

            @Parameter(names = {"-id", "--job-id"}, description = "Job id", required = true, arity = 1)
            long id;
        }

        @Parameters(commandNames = {"finished"}, commandDescription = "Notify catalog that a job have finished.")
        class DoneJobCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            OpencgaCommonCommandOptions cOpt = commonCommandOptions;

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
            OpencgaCommonCommandOptions cOpt = commonCommandOptions;

            @Parameter(names = {"--study-id"}, description = "Study id", required = false, arity = 1)
            String studyId;
        }

        @Parameters(commandNames = {"run"}, commandDescription = "Executes a job.")
        class RunJobCommand {

            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            OpencgaCommonCommandOptions cOpt = commonCommandOptions;

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


    @Parameters(commandNames = {"individuals"}, commandDescription = "Individuals commands")
    public class IndividualsCommandsOptions extends CommandOptions{

        final CreateCommand createCommand;
        final InfoCommand infoCommand;
        final SearchCommand searchCommand;
        final AnnotateCommand annotateCommand;
        final UpdateCommand updateCommand;
        final DeleteCommand deleteCommand;


        public OpencgaCliOptionsParser.OpencgaCommonCommandOptions commonOptions = OpencgaCliOptionsParser.this.commonCommandOptions;
        public IndividualsCommandsOptions(JCommander jcommander) {
            jcommander.addCommand(this);
            JCommander tools = jcommander.getCommands().get("individuals");
            tools.addCommand(this.createCommand = new CreateCommand());
            tools.addCommand(this.infoCommand = new InfoCommand());
            tools.addCommand(this.searchCommand = new SearchCommand());
            tools.addCommand(this.annotateCommand = new AnnotateCommand());
            tools.addCommand(this.updateCommand = new UpdateCommand());
            tools.addCommand(this.deleteCommand = new DeleteCommand());

        }

        @Parameters(commandNames = {"create"}, commandDescription = "Create sample.")
        class CreateCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            OpencgaCommonCommandOptions cOpt = commonCommandOptions;

            @Parameter(names = {"-s", "--study-id"}, description = "StudyId", required = true, arity = 1)
            String studyId;

            @Parameter(names = {"-n", "--name"}, description = "Name", required = true, arity = 1)
            String name;

            @Parameter(names = {"--family"}, description = "Family", required = false, arity = 1)
            String family  = "";

            @Parameter(names = {"--fatherId"}, description = "FatherId", required = false, arity = 1)
            Integer fatherId;

            @Parameter(names = {"--motherId"}, description = "MotherId", required = false, arity = 1)
            Integer motherId;

            @Parameter(names = {"--gender"}, description = "Gender", required = false)
            String gender;
        }


        class BaseIndividualsCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            OpencgaCommonCommandOptions cOpt = commonCommandOptions;

            @Parameter(names = {"-id", "--individual-id"}, description = "Individual id", required = true, arity = 1)
            Integer id;
        }

        @Parameters(commandNames = {"info"}, commandDescription = "Get individual information")
        class InfoCommand extends BaseIndividualsCommand{ }

        @Parameters(commandNames = {"search"}, commandDescription = "Search for individuals")
        class SearchCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            OpencgaCommonCommandOptions cOpt = commonCommandOptions;

            @Parameter(names = {"-s", "--study-id"}, description = "studyId", required = true, arity = 1)
            String studyId;

//            @Parameter(names = {"-id", "--individual-id"}, description = "Id", required = false, arity = 1)
//            String id;

            @Parameter(names = {"--name"}, description = "name", required = false, arity = 1)
            String name;

            @Parameter(names = {"--fatherId"}, description = "fatherId", required = false, arity = 1)
            String fatherId;

            @Parameter(names = {"--motherId"}, description = "motherId", required = false, arity = 1)
            String motherId;

            @Parameter(names = {"--family"}, description = "family", required = false, arity = 1)
            String family;

            @Parameter(names = {"--gender"}, description = "gender", required = false, arity = 1)
            String gender;

            @Parameter(names = {"--race"}, description = "race", required = false, arity = 1)
            String race;

            @Parameter(names = {"--species"}, description = "species", required = false, arity = 1)
            String species;

            @Parameter(names = {"--population"}, description = "population", required = false, arity = 1)
            String population;

            @Parameter(names = {"--variableSetId"}, description = "variableSetId", required = false, arity = 1)
            Integer variableSetId;

            @Parameter(names = {"--annotationSetId"}, description = "annotationSetId", required = false, arity = 1)
            String annotationSetId;

            @Parameter(names = {"--annotation"}, description = "annotation", required = false, arity = 1)
            String annotation;
        }

        @Parameters(commandNames = {"annotate"}, commandDescription = "Annotate an individual")
        class AnnotateCommand extends BaseIndividualsCommand{
            @Parameter(names = {"-aN","--annotateSetName"}, description = "Annotation set name. Must be unique",
                    required = true, arity = 1)
            String annotateSetName;

            @Parameter(names = {"--variableSetId"}, description = "variableSetId", required = false, arity = 1)
            Integer id;

            @Parameter(names = {"--update"}, description = "Update an already existing AnnotationSet", required = false,
                    arity = 0)
            boolean update;

            @Parameter(names = {"--delete"}, description = "Delete an AnnotationSet", required = false, arity = 0)
            boolean delete;
        }

        @Parameters(commandNames = {"update"}, commandDescription = "Update individual information")
        class UpdateCommand extends BaseIndividualsCommand{

//            @Parameter(names = {"-id", "--individual-id"}, description = "Id", required = false, arity = 1)
//            String id;

            @Parameter(names = {"--name"}, description = "name", required = false, arity = 1)
            String name;

            @Parameter(names = {"--fatherId"}, description = "FatherId", required = false, arity = 1)
            Integer fatherId;

            @Parameter(names = {"--motherId"}, description = "MotherId", required = false, arity = 1)
            Integer motherId;

            @Parameter(names = {"--family"}, description = "Family", required = false, arity = 1)
            String family;

            @Parameter(names = {"--gender"}, description = "Gender", required = false)
            String gender;

            @Parameter(names = {"--race"}, description = "race", required = false, arity = 1)
            String race;

        }

        @Parameters(commandNames = {"delete"}, commandDescription = "Delete individual information")
        class DeleteCommand extends BaseIndividualsCommand{ }
    }
    @Parameters(commandNames = {"cohorts"}, commandDescription = "Cohorts methods")
    public class CohortCommandsOptions extends CommandOptions{
        public final CreateCommand createCommand;
        public final InfoCommand infoCommand;
        public final SamplesCommand samplesCommand;
        public final StatsCommand statsCommand;

        public OpencgaCliOptionsParser.OpencgaCommonCommandOptions commonOptions = OpencgaCliOptionsParser.this.commonCommandOptions;
        public CohortCommandsOptions(JCommander jcommander) {
            jcommander.addCommand(this);
            JCommander files = jcommander.getCommands().get("cohorts");
            files.addCommand(this.createCommand = new CreateCommand());
            files.addCommand(this.infoCommand = new InfoCommand());
            files.addCommand(this.samplesCommand = new SamplesCommand());
            files.addCommand(this.statsCommand = new StatsCommand());
        }
        @Parameters(commandNames = {CreateCommand.COMMAND_NAME}, commandDescription = "Create a cohort")
        public class CreateCommand {

            public static final String COMMAND_NAME = "create";

            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            OpencgaCommonCommandOptions cOpt = commonCommandOptions;

            @Parameter(names = {"--study-id"}, description = "Study id", required = true, arity = 1)
            public String studyId;

            @Parameter(names = {"--name"}, description = "cohort name", required = false, arity = 1)
            public String name;

            @Parameter(names = {"--variable-set-id"}, description = "VariableSetId", required = false, arity = 1)
            long variableSetId;

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
        @Parameters(commandNames = {InfoCommand.COMMAND_NAME}, commandDescription = "Get cohort information")
        public class InfoCommand {

            public static final String COMMAND_NAME = "info";

            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            OpencgaCommonCommandOptions cOpt = commonCommandOptions;

            @Parameter(names = {"-id", "--cohort-id"}, description = "Cohort id", required = true, arity = 1)
            public long id;
        }



        @Parameters(commandNames = {SamplesCommand.COMMAND_NAME}, commandDescription = "List samples belonging to a cohort")
        public class SamplesCommand {

            public static final String COMMAND_NAME = "samples";

            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            OpencgaCommonCommandOptions cOpt = commonCommandOptions;

            @Parameter(names = {"-id", "--cohort-id"}, description = "Cohort id", required = true, arity = 1)
            public long id;
        }

        @Parameters(commandNames = {StatsCommand.COMMAND_NAME}, commandDescription = "Calculate variant stats for a set of cohorts.")
        class StatsCommand {

            public static final String COMMAND_NAME = "calculate-stats";

            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            OpencgaCommonCommandOptions cOpt = commonCommandOptions;

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
    public class SampleCommandsOptions extends CommandOptions{

        final LoadCommand loadCommand;
        final InfoCommand infoCommand;
        final SearchCommand searchCommand;
        final DeleteCommand deleteCommand;

        public OpencgaCliOptionsParser.OpencgaCommonCommandOptions commonOptions = OpencgaCliOptionsParser.this.commonCommandOptions;
        public SampleCommandsOptions(JCommander jcommander) {
            jcommander.addCommand(this);
            JCommander files = jcommander.getCommands().get("samples");
            files.addCommand(this.loadCommand = new LoadCommand());
            files.addCommand(this.infoCommand = new InfoCommand());
            files.addCommand(this.searchCommand = new SearchCommand());
            files.addCommand(this.deleteCommand = new DeleteCommand());
//            files.addCommand(this.samplesCommand = new SamplesCommand());
        }

        class BaseSampleCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            OpencgaCommonCommandOptions cOpt = commonCommandOptions;

            @Parameter(names = {"-id", "--sample-id"}, description = "Sample id", required = true, arity = 1)
            long id;
        }

        @Parameters(commandNames = {"load"}, commandDescription = "Load samples from a pedigree file")
        class LoadCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            OpencgaCommonCommandOptions cOpt = commonCommandOptions;

            @Parameter(names = {"--variable-set-id"}, description = "VariableSetId that represents the pedigree file", required = false, arity = 1)
            long variableSetId;

            @Parameter(names = {"--pedigree-id"}, description = "Pedigree file id already loaded in OpenCGA", required = true, arity = 1)
            String pedigreeFileId;
        }
        @Parameters(commandNames = {"info"}, commandDescription = "Get samples information")
        class InfoCommand extends BaseSampleCommand {
        }

        @Parameters(commandNames = {"search"}, commandDescription = "Search samples")
        class SearchCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            OpencgaCommonCommandOptions cOpt = commonCommandOptions;

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



        @Parameters(commandNames = {"delete"}, commandDescription = "Deletes the selected sample")
        class DeleteCommand extends BaseSampleCommand {
        }
    }



    @Parameters(commandNames = {"tools"}, commandDescription = "Tools commands")
    public class ToolCommandsOptions extends CommandOptions{

        final CreateCommand createCommand;
        final InfoCommand infoCommand;

        public OpencgaCliOptionsParser.OpencgaCommonCommandOptions commonOptions = OpencgaCliOptionsParser.this.commonCommandOptions;
        public ToolCommandsOptions(JCommander jcommander) {
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
            OpencgaCommonCommandOptions cOpt = commonCommandOptions;

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
            OpencgaCommonCommandOptions cOpt = commonCommandOptions;

            @Parameter(names = {"-id", "--tool-id"}, description = "Tool id", required = true, arity = 1)
            String id;
        }
    }

    @Parameters(commandNames = {"share"}, commandDescription = "Share resource")
    class CommandShareResource {
        @ParametersDelegate
        UserAndPasswordOptions up = userAndPasswordOptions;

        @ParametersDelegate
        OpencgaCommonCommandOptions cOpt = commonCommandOptions;

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

    public void printUsage() {
        String parsedCommand = getCommand();
        if (parsedCommand.isEmpty()) {
            System.err.println("");
            System.err.println("Program:     OpenCGA (OpenCB)");
            System.err.println("Version:     " + GitRepositoryState.get().getBuildVersion());
            System.err.println("Git commit:  " + GitRepositoryState.get().getCommitId());
            System.err.println("Description: Big Data platform for processing and analysing NGS data");
            System.err.println("");
            System.err.println("Usage:       opencga.sh [-h|--help] [--version] <command> [options]");
            System.err.println("");
            System.err.println("Commands:");
            printMainUsage();
            System.err.println("");
        } else {
            String parsedSubCommand = getSubCommand();
            if (parsedSubCommand.isEmpty()) {
                System.err.println("");
                System.err.println("Usage:   opencga.sh " + parsedCommand + " <subcommand> [options]");
                System.err.println("");
                System.err.println("Subcommands:");
                printCommands(jCommander.getCommands().get(parsedCommand));
                System.err.println("");
            } else {
                System.err.println("");
                System.err.println("Usage:   opencga.sh " + parsedCommand + " " + parsedSubCommand + " [options]");
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

    public OpencgaCommonCommandOptions getCommonCommandOptions() {
        return commonCommandOptions;
    }

    public UserAndPasswordOptions getUserAndPasswordOptions() {
        return userAndPasswordOptions;
    }

    public UserCommandOptions getUsersCommandOptions() {
        return usersCommandOptions;
    }

    public ProjectCommandOptions getProjectCommandOptions() {
        return projectCommandOptions;
    }

    public StudyCommandOptions getStudyCommandOptions() {
        return studyCommandOptions;
    }

    public FileCommandsOptions getFileCommands() {
        return fileCommands;
    }


    public JobsCommandsOptions getJobsCommands() {
        return jobsCommands;
    }
    public IndividualsCommandsOptions getIndividualsCommands() {
        return individualsCommandsOptions;
    }
    public ToolCommandsOptions getToolCommands() {
        return toolCommands;
    }
    public CohortCommandsOptions getCohortCommands() {
        return cohortCommands;
    }
    public SampleCommandsOptions getSampleCommands() {
        return sampleCommands;
    }
}
