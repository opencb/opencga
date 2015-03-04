package org.opencb.opencga.app.cli.main;

import com.beust.jcommander.*;
import org.opencb.opencga.catalog.beans.File;

import java.util.HashMap;
import java.util.Map;

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
        toolCommands = new ToolCommands(jcommander);


        if (interactive) { //Add interactive commands
//            jcommander.addCommand(new HelpCommands());
            jcommander.addCommand(new ExitCommands());
        }
    }

    public void parse(String[] args) throws ParameterException{
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
                System.out.println("Available commands");
                printUsage(jcommander.getCommands().get(getCommand()));
            }
        } else {
            new JCommander(generalOptions).usage();
            System.out.println("Available commands");
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
            System.out.printf("%" + gap + "s    %s\n", entry.getKey(), commander.getCommandDescription(entry.getKey()));
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
    }

    class CommonOptions {
        @Parameter(names = {"-C", "--conf"}, description = "This parameter set the level of the logging", required = false, arity = 1)
        public String conf;

        @Parameter(names = {"-h", "--help"}, help = true)
        public boolean help;

        @Parameter(names = {"-v", "--verbose"}, description = "This parameter set the level of the logging", required = false, arity = 1)
        public boolean verbose;

        @Parameter(names = {"-L", "--log-level"}, description = "This parameter set the level of the logging", required = false, arity = 1)
        public int logLevel;

        @DynamicParameter(names = "-D", description = "Dynamic parameters go here", hidden = true)
        Map<String, String> dynamic = new HashMap<String, String>();

    }

    @Parameters(commandNames = {"help"}, hidden = true, commandDescription = "Description")
    class HelpCommands {
    }


    @Parameters(commandNames = {"exit"}, hidden = true, commandDescription = "Description")
    class ExitCommands {
    }

    @Parameters(commandNames = {"users"}, commandDescription = "Description")
    class UserCommands {

        final CreateCommand createCommand;
        final InfoCommand infoCommand;
        final ListCommand lsitCommand;
        final LoginCommand loginCommand;
        final LogoutCommand logoutCommand;

        public UserCommands(JCommander jcommander) {
            jcommander.addCommand(this);
            JCommander users = jcommander.getCommands().get("users");
            users.addCommand(createCommand = new CreateCommand());
            users.addCommand(infoCommand = new InfoCommand());
            users.addCommand(lsitCommand = new ListCommand());
            users.addCommand(loginCommand = new LoginCommand());
            users.addCommand(logoutCommand = new LogoutCommand());
        }

        @ParametersDelegate
        CommonOptions cOpt = commonOptions;

        @Parameters(commandNames = {"create"}, commandDescription = "Description")
        class CreateCommand {

            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"--name"}, description = "User name", required = true, arity = 1)
            String name;

            @Parameter(names = {"--email"}, description = "Email", required = true, arity = 1)
            String email;

            @Parameter(names = {"--organization"}, description = "Organization", required = true, arity = 1)
            String organization;
        }

        @Parameters(commandNames = {"info"}, commandDescription = "Description")
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

            @Parameter(names = {"--studies"}, description = "", arity = 0)
            public boolean studies;

            @Parameter(names = {"--files"}, description = "", arity = 0)
            public boolean files;

        }

        @Parameters(commandNames = {"login"}, commandDescription = "Description")
        public class LoginCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

        }

        @Parameters(commandNames = {"logout"}, commandDescription = "Description")
        public class LogoutCommand {

            @Parameter(names = {"-u", "--user"}, description = "UserId", required = false, arity = 1)
            String user;

            @Parameter(names = {"--sessionId", "-sid"}, description = "SessionId", required = true, arity = 1)
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

            @Parameter(names = {"-o", "--org", "--organization"}, description = "Organization", required = true, arity = 1)
            String organization;
        }

        @Parameters(commandNames = {"info"}, commandDescription = "Get project info")
        class InfoCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"-i", "--id"}, description = "Project identifier", required = true, arity = 1)
            String id;

        }
    }

    @Parameters(commandNames = {"studies"}, commandDescription = "Description")
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

        @Parameters(commandNames = {"create"}, commandDescription = "Description")
        class CreateCommand {

            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"--pid", "--projectId"}, description = "Project identifier", required = true, arity = 1)
            String projectId;

            @Parameter(names = {"--name"}, description = "Study name", required = true, arity = 1)
            String name;

            @Parameter(names = {"--alias"}, description = "alias", required = true, arity = 1)
            String alias;

            @Parameter(names = {"--type"}, description = "Type", required = true, arity = 1)
            String type;

            @Parameter(names = {"--description"}, description = "Organization", required = true, arity = 1)
            String description;
        }

        @Parameters(commandNames = {"info"}, commandDescription = "Description")
        class InfoCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"--id"}, description = "Study id", required = true, arity = 1)
            String id;
        }
    }



    @Parameters(commandNames = {"files"}, commandDescription = "File methods")
    class FileCommands {

        final CreateCommand createCommand;
        final CreateFolderCommand createFolderCommand;
        final InfoCommand infoCommand;
        final ListCommand listCommand;
        final IndexCommand indexCommand;

        public FileCommands(JCommander jcommander) {
            jcommander.addCommand(this);
            JCommander files = jcommander.getCommands().get("files");
            files.addCommand(this.createCommand = new CreateCommand());
            files.addCommand(this.createFolderCommand = new CreateFolderCommand());
            files.addCommand(this.infoCommand = new InfoCommand());
            files.addCommand(this.listCommand = new ListCommand());
            files.addCommand(this.indexCommand = new IndexCommand());
//        files.addCommand(commandShareResource);
        }

        @Parameters(commandNames = {"create"}, commandDescription = "Create file")
        class CreateCommand {

            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"-i", "--input"}, description = "Input file", required = true, arity = 1)
            String file;

            @Parameter(names = {"-s", "--studyId"}, description = "studyId", required = true, arity = 1)
            String studyId;

            @Parameter(names = {"-o", "--outdir"}, description = "Directory where to create the file", required = false, arity = 1)
            String path  = "";

            @Parameter(names = {"-d", "--description"}, description = "Description", required = false, arity = 1)
            String description;

            @Parameter(names = {"-f", "--format"}, description = "format", required = false, arity = 1)
            File.Format format = File.Format.PLAIN;

            @Parameter(names = {"-b", "--bioformat"}, description = "bioformat", required = false, arity = 1)
            File.Bioformat bioformat = File.Bioformat.NONE;

            @Parameter(names = {"-P", "--parents"}, description = "parents", required = false)
            boolean parents;

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

            @Parameter(names = {"-P", "--parents"}, description = "parents", required = false)
            boolean parents;
        }

        @Parameters(commandNames = {"info"}, commandDescription = "Get file information")
        class InfoCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"--id"}, description = "File id", required = true, arity = 1)
            String id;
        }

        @Parameters(commandNames = {"list"}, commandDescription = "List files")
        class ListCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"--id"}, description = "Folder id", required = true, arity = 1)
            String id;
        }

        @Parameters(commandNames = {"index"}, commandDescription = "Index files")
        class IndexCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"--id"}, description = "File id", required = true, arity = 1)
            String id;

            @Parameter(names = {"--storage-engine"}, description = "", required = false, arity = 1)
            String storageEngine;

            @Parameter(names = {"-o", "--outdir"}, description = "Directory where to create the file", required = false, arity = 1)
            String outdir = "";

        }
    }



    @Parameters(commandNames = {"tools"}, commandDescription = "Tools methods")
    class ToolCommands {

        final CreateCommand createCommand;
        final InfoCommand infoCommand;

        public ToolCommands(JCommander jcommander) {
            jcommander.addCommand(this);
            JCommander tools = jcommander.getCommands().get("tools");
            tools.addCommand(this.createCommand = new CreateCommand());
            tools.addCommand(this.infoCommand = new InfoCommand());
        }

        @Parameters(commandNames = {"create"}, commandDescription = "Regist existint tool in catalog")
        class CreateCommand {

            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"-a", "--alias"}, description = "alias", required = true, arity = 1)
            String alias;

            @Parameter(names = {"-d", "--description"}, description = "Description", required = false, arity = 1)
            String description = "";

//            @Parameter(names = {"-n", "--name"}, description = "Name", required = true, arity = 1)
//            String name;

            @Parameter(names = {"-P", "--path"}, description = "Path", required = true, arity = 1)
            String path;

            @Parameter(names = {"--open"}, description = "Set usability", required = false, arity = 0)
            boolean openTool = false;
        }

        @Parameters(commandNames = {"info"}, commandDescription = "Get file information")
        class InfoCommand {
            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"--id"}, description = "Tool id", required = true, arity = 1)
            int id;
        }
    }

    @Parameters(commandNames = {"share"}, commandDescription = "Share resource")
    class CommandShareResource {
        @ParametersDelegate
        UserAndPasswordOptions up = userAndPasswordOptions;

        @Parameter(names = {"--id"}, description = "Unique identifier", required = true, arity = 1)
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
