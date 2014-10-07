package org.opencb.opencga.app.cli.main;

import com.beust.jcommander.*;

import java.io.File;
import java.io.InputStream;

/**
 * Created by jacobo on 29/09/14.
 */
public class OptionsParser {


    private final JCommander jcommander;
    public final GeneralOptions generalOptions;
    public final CommandUser commandUser;
    public final CommandUser.CommandUserCreate commandUserCreate;
    public final CommandUser.CommandUserInfo commandUserInfo;
    public final CommandUser.CommandUserLogin commandUserLogin;
    public final CommandUser.CommandUserLogout commandUserLogout;
    public final CommandProject commandProject;
    public final CommandProject.CommandProjectCreate commandProjectCreate;
    public final CommandProject.CommandProjectInfo commandProjectInfo;
    public final CommandStudy commandStudy;
    public final CommandStudy.CommandStudyInfo commandStudyInfo;
    public final CommandStudy.CommandStudyCreate commandStudyCreate;
    public final CommandFile commandFile;
    public final CommandFile.CommandFileCreate commandFileCreate;
    public final CommandFile.CommandFileList commandFileList;
    public final CommandFile.CommandFileInfo commandFileInfo;

    public final CommandShareResource commandShareResource;


    public OptionsParser() {
        generalOptions = new GeneralOptions();
        jcommander = new JCommander(generalOptions);
        commandUser = new CommandUser();
        commandProject = new CommandProject();
        commandStudy = new CommandStudy();
        commandFile = new CommandFile();
        commandShareResource = new CommandShareResource();
        jcommander.addCommand(commandUser);
        jcommander.addCommand(commandProject);
        jcommander.addCommand(commandStudy);
        jcommander.addCommand(commandFile);

        JCommander users = jcommander.getCommands().get("users");
        users.addCommand(commandUserCreate = new CommandUser.CommandUserCreate());
        users.addCommand(commandUserInfo = new CommandUser.CommandUserInfo());
        users.addCommand(commandUserLogin = new CommandUser.CommandUserLogin());
        users.addCommand(commandUserLogout = new CommandUser.CommandUserLogout());

        JCommander projects = jcommander.getCommands().get("projects");
        projects.addCommand(commandProjectCreate = new CommandProject.CommandProjectCreate());
        projects.addCommand(commandProjectInfo = new CommandProject.CommandProjectInfo());
        projects.addCommand(commandShareResource);

        JCommander studies = jcommander.getCommands().get("studies");
        studies.addCommand(commandStudyInfo = new CommandStudy.CommandStudyInfo());
        studies.addCommand(commandStudyCreate = new CommandStudy.CommandStudyCreate());
        studies.addCommand(commandShareResource);

        JCommander files = jcommander.getCommands().get("files");
        files.addCommand(commandFileCreate = new CommandFile.CommandFileCreate());
        files.addCommand(commandFileInfo = new CommandFile.CommandFileInfo());
        files.addCommand(commandFileList = new CommandFile.CommandFileList());

//        files.addCommand(commandShareResource);

    }

    public void parse(String[] args) throws ParameterException{
        jcommander.parse(args);
    }

    public String getCommand(){
        String parsedCommand = jcommander.getParsedCommand();
        return parsedCommand != null? parsedCommand: "";
    }

    public String getSubcommand(){
        String parsedCommand = jcommander.getParsedCommand();
        if (jcommander.getCommands().containsKey(parsedCommand)) {
            String subcommand = jcommander.getCommands().get(parsedCommand).getParsedCommand();
            return subcommand != null? subcommand: "";
        } else {
            return "";
        }
    }

    public void printUsage(){
        if(!getCommand().isEmpty()) {
            if(!getSubcommand().isEmpty()){
//                usage(getCommand(), getSubcommand());
                jcommander.getCommands().get(getCommand()).usage(getSubcommand());
            } else {
                jcommander.usage(getCommand());
            }
        } else {
            jcommander.usage();
        }
    }

//    public void usage(){
//        jcommander.usage();
//    }
//
//    public void usage(String command){
//        jcommander.usage(command);
//    }
//
//    public void usage(String command, String subcommand){
//        jcommander.getCommands().get(command).usage(subcommand);
//    }

    public class GeneralOptions {
        @Parameter(names = {"--help", "-h"}, help = true)
        public boolean help;
    }

    static class UserPassword{
        @Parameter(names = {"-u", "--user"}, description = "UserId", required = false, arity = 1)
        String user;

        @Parameter(names = {"-p", "--password"}, description = "Password", arity = 1, required = false,  password = false)
        String password;
    }

    @Parameters(commandNames = {"users"}, commandDescription = "Description")
    static class CommandUser {

        @Parameters(commandNames = {"create"}, commandDescription = "Description")
        static class CommandUserCreate {

            @ParametersDelegate
            public UserPassword up = new UserPassword();

            @Parameter(names = {"--name"}, description = "User name", required = true, arity = 1)
            String name;

            @Parameter(names = {"--email"}, description = "Email", required = true, arity = 1)
            String email;

            @Parameter(names = {"--organization"}, description = "Organization", required = true, arity = 1)
            String organization;
        }

        @Parameters(commandNames = {"info"}, commandDescription = "Description")
        static class CommandUserInfo {
            @ParametersDelegate
            public UserPassword up = new UserPassword();
        }

        @Parameters(commandNames = {"login"}, commandDescription = "Description")
        static public class CommandUserLogin {
            @ParametersDelegate
            public UserPassword up = new UserPassword();
        }

        @Parameters(commandNames = {"logout"}, commandDescription = "Description")
        static public class CommandUserLogout {

            @Parameter(names = {"-u", "--user"}, description = "UserId", required = false, arity = 1)
            String user;

            @Parameter(names = {"--sessionId", "-sid"}, description = "SessionId", required = true, arity = 1)
            public String sessionId;
        }
    }


    @Parameters(commandNames = {"projects"}, commandDescription = "Project commands")
    static class CommandProject {

        @Parameters(commandNames = {"create"}, commandDescription = "Create new project")
        static class CommandProjectCreate {

            @ParametersDelegate
            public UserPassword up = new UserPassword();

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
        static class CommandProjectInfo {
            @ParametersDelegate
            public UserPassword up = new UserPassword();

            @Parameter(names = {"-i", "--id"}, description = "Project identifier", required = true, arity = 1)
            String id;

        }
    }

    @Parameters(commandNames = {"studies"}, commandDescription = "Description")
    static class CommandStudy {

        @Parameters(commandNames = {"create"}, commandDescription = "Description")
        static class CommandStudyCreate {

            @ParametersDelegate
            public UserPassword up = new UserPassword();

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
        static class CommandStudyInfo {
            @ParametersDelegate
            public UserPassword up = new UserPassword();

            @Parameter(names = {"--id"}, description = "Study id", required = true, arity = 1)
            String id;
        }
    }



    @Parameters(commandNames = {"files"}, commandDescription = "File methods")
    static class CommandFile {

        @Parameters(commandNames = {"create"}, commandDescription = "Create file")
        static class CommandFileCreate {

            @ParametersDelegate
            public UserPassword up = new UserPassword();

            @Parameter(names = {"--input"}, description = "Input file", required = true, arity = 1)
            public String file;

            @Parameter(names = {"--outdir"}, description = "Directory where to create the file", required = false, arity = 1)
            public String path  = "";

            @Parameter(names = {"--description"}, description = "Organization", required = true, arity = 1)
            public String description;

            @Parameter(names = {"--studyId"}, description = "studyId", required = true, arity = 1)
            public String studyId;

            @Parameter(names = {"--format"}, description = "format", required = true, arity = 1)
            public String format;

            @Parameter(names = {"--bioformat"}, description = "bioformat", required = true, arity = 1)
            public String bioformat;

            @Parameter(names = {"--parents"}, description = "parents", required = false, arity = 1)
            public boolean parents;
        }

        @Parameters(commandNames = {"list"}, commandDescription = "List files")
        static class CommandFileList {
            @ParametersDelegate
            public UserPassword up = new UserPassword();

            @Parameter(names = {"--id"}, description = "Folder id", required = true, arity = 1)
            String id;
        }

        @Parameters(commandNames = {"info"}, commandDescription = "Get file information")
        static class CommandFileInfo {
            @ParametersDelegate
            public UserPassword up = new UserPassword();

            @Parameter(names = {"--id"}, description = "File id", required = true, arity = 1)
            String id;
        }
    }

    @Parameters(commandNames = {"share"}, commandDescription = "Share resource")
    static class CommandShareResource {
        @ParametersDelegate
        public UserPassword up = new UserPassword();

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
