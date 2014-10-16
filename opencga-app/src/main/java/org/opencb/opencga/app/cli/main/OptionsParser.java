package org.opencb.opencga.app.cli.main;

import com.beust.jcommander.*;

/**
 * Created by jacobo on 29/09/14.
 */
public class OptionsParser {

    private final JCommander jcommander;

    private final GeneralOptions generalOptions;
    private final CommonOptions commonOptions;

    private final UserAndPasswordOptions userAndPasswordOptions;

    private UserCommands userCommands;
    private ProjectCommands projectCommands;
    private StudyCommands studyCommands;
    private FileCommands fileCommands;

    public final CommandShareResource commandShareResource;



    public OptionsParser() {
        generalOptions = new GeneralOptions();

        jcommander = new JCommander(generalOptions);

        commonOptions = new CommonOptions();
        userAndPasswordOptions = new UserAndPasswordOptions();

        userCommands = new UserCommands();
        projectCommands = new ProjectCommands();
        studyCommands = new StudyCommands();
        fileCommands = new FileCommands();

        commandShareResource = new CommandShareResource();

        jcommander.addCommand(userCommands);
        JCommander users = jcommander.getCommands().get("users");
        users.addCommand(userCommands.createCommand);
        users.addCommand(userCommands.infoCommand);
//        users.addCommand(commandUserLogin = new CommandUser.CommandUserLogin());
//        users.addCommand(commandUserLogout = new CommandUser.CommandUserLogout());


        jcommander.addCommand(projectCommands);
        JCommander projects = jcommander.getCommands().get("projects");
        projects.addCommand(projectCommands.createCommand);
        projects.addCommand(projectCommands.infoCommand);
//        projects.addCommand(commandShareResource);

        jcommander.addCommand(studyCommands);
        JCommander studies = jcommander.getCommands().get("studies");
        studies.addCommand(studyCommands.createCommand);
        studies.addCommand(studyCommands.infoCommand);
        studies.addCommand(commandShareResource);


        jcommander.addCommand(fileCommands);
        JCommander files = jcommander.getCommands().get("files");
        files.addCommand(fileCommands.createCommand);
        files.addCommand(fileCommands.infoCommand);
        files.addCommand(fileCommands.listCommand);
//        files.addCommand(commandFileList = new CommandFile.CommandFileList());
//        files.addCommand(commandShareResource);

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
                jcommander.usage(getCommand());
            }
        } else {
            jcommander.usage();
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


    public class GeneralOptions {
        @Parameter(names = {"-h", "--help"}, help = true)
        public boolean help;

        @Parameter(names = {"-V", "--version"})
        public boolean version;
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

    }



    @Parameters(commandNames = {"users"}, commandDescription = "Description")
    class UserCommands {

        CreateCommand createCommand = new CreateCommand();
        InfoCommand infoCommand = new InfoCommand();
        LoginCommand loginCommand = new LoginCommand();
        LogoutCommand logoutCommand = new LogoutCommand();

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

        CreateCommand createCommand = new CreateCommand();
        InfoCommand infoCommand = new InfoCommand();

        @ParametersDelegate
        CommonOptions cOpt = commonOptions;

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

        CreateCommand createCommand = new CreateCommand();
        InfoCommand infoCommand = new InfoCommand();

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

        CreateCommand createCommand = new CreateCommand();
        InfoCommand infoCommand = new InfoCommand();
        ListCommand listCommand = new ListCommand();

        @Parameters(commandNames = {"create"}, commandDescription = "Create file")
        class CreateCommand {

            @ParametersDelegate
            UserAndPasswordOptions up = userAndPasswordOptions;

            @ParametersDelegate
            CommonOptions cOpt = commonOptions;

            @Parameter(names = {"-i", "--input"}, description = "Input file", required = true, arity = 1)
            public String file;

            @Parameter(names = {"-o", "--outdir"}, description = "Directory where to create the file", required = false, arity = 1)
            public String path  = "";

            @Parameter(names = {"-d", "--description"}, description = "Organization", required = true, arity = 1)
            public String description;

            @Parameter(names = {"-s", "--studyId"}, description = "studyId", required = true, arity = 1)
            public String studyId;

            @Parameter(names = {"-f", "--format"}, description = "format", required = true, arity = 1)
            public String format;

            @Parameter(names = {"-b", "--bioformat"}, description = "bioformat", required = true, arity = 1)
            public String bioformat;

            @Parameter(names = {"-P", "--parents"}, description = "parents", required = false)
            public boolean parents;
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
