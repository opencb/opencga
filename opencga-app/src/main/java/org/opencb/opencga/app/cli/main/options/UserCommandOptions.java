package org.opencb.opencga.app.cli.main.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser.OpencgaCommonCommandOptions;

/**
 * Created by pfurio on 13/06/16.
 */
@Parameters(commandNames = {"users"}, commandDescription = "User commands")
public class UserCommandOptions {

    public CreateCommandOptions createCommandOptions;
    public InfoCommandOptions infoCommandOptions;
    public ProjectsCommandOptions projectsCommandOptions;
    public LoginCommandOptions loginCommandOptions;
    public LogoutCommandOptions logoutCommandOptions;
    public JCommander jCommander;

    public OpencgaCommonCommandOptions commonCommandOptions;

    public UserCommandOptions(OpencgaCommonCommandOptions commonCommandOptions, JCommander jCommander) {
        this.commonCommandOptions = commonCommandOptions;
        this.jCommander = jCommander;

        this.createCommandOptions = new CreateCommandOptions();
        this.infoCommandOptions = new InfoCommandOptions();
        this.projectsCommandOptions = new ProjectsCommandOptions();
        this.loginCommandOptions = new LoginCommandOptions();
        this.logoutCommandOptions = new LogoutCommandOptions();
    }

    public JCommander getjCommander() {
        return jCommander;
    }

    public class BaseUserCommand {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-u", "--user-id"}, description = "User id",  required = true, arity = 1)
        public String user;

    }

    @Parameters(commandNames = {"create"}, commandDescription = "Create new user for OpenCGA-Catalog")
    public class CreateCommandOptions extends BaseUserCommand {

        @Parameter(names = {"--user-name"}, description = "User name", required = true, arity = 1)
        public String userName;

        @Parameter(names = {"--user-password"}, description = "User password", required = true,  password = true, arity = 0)
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

    @Parameters(commandNames = {"info"}, commandDescription = "Get user's information")
    public class InfoCommandOptions extends BaseUserCommand {

        @Parameter(names = {"--last-activity"}, description = "If matches with the user's last activity, return " +
                "an empty QueryResult", arity = 1, required = false)
        public String lastActivity;
    }


    @Parameters(commandNames = {"projects"}, commandDescription = "List all projects and studies from a selected user")
    public class ProjectsCommandOptions {
        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = UserCommandOptions.this.commonCommandOptions;
//
//        @Parameter(names = {"--level"}, description = "Descend only level directories deep.", arity = 1)
//        public int level = Integer.MAX_VALUE;
//
//        @Parameter(names = {"-R", "--recursive"}, description = "List subdirectories recursively", arity = 0)
//        public boolean recursive = false;
//
//        @Parameter(names = {"-U", "--show-uris"}, description = "Show uris from linked files and folders", arity = 0)
//        public boolean uris = false;

    }

    @Parameters(commandNames = {"login"}, commandDescription = "Login as user and return its sessionId")
    public class LoginCommandOptions {

        @Parameter(names = {"-u", "--user"}, description = "UserId", required = false, arity = 1)
        public String user;

        @Parameter(names = {"-p", "--password"}, description = "Password", arity = 1, required = false, password = true)
        public String password;

        @Parameter(names = {"-S","--session-id"}, description = "SessionId", arity = 1, required = false, hidden = true)
        public String sessionId;

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = UserCommandOptions.this.commonCommandOptions;
    }

    @Parameters(commandNames = {"logout"}, commandDescription = "End user session")
    public class LogoutCommandOptions {
        @Parameter(names = {"--session-id", "-S"}, description = "SessionId", required = false, arity = 1)
        public String sessionId;
    }


}
