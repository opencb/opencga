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

    public CreateUserCommandOptions createUserCommandOptions;
    public InfoCommandOptions infoCommandOptions;
    public ListCommandOptions listCommandOptions;
    public LoginCommandOptions loginCommandOptions;
    public LogoutCommandOptions logoutCommandOptions;
    public JCommander jCommander;

    public OpencgaCommonCommandOptions commonCommandOptions;

    public UserCommandOptions(OpencgaCommonCommandOptions commonCommandOptions, JCommander jCommander) {
        this.commonCommandOptions = commonCommandOptions;
        this.jCommander = jCommander;

        this.createUserCommandOptions = new CreateUserCommandOptions();
        this.infoCommandOptions = new InfoCommandOptions();
        this.listCommandOptions = new ListCommandOptions();
        this.loginCommandOptions = new LoginCommandOptions();
        this.logoutCommandOptions = new LogoutCommandOptions();
    }

    public JCommander getjCommander() {
        return jCommander;
    }

    @Parameters(commandNames = {"create"}, commandDescription = "Create new user for OpenCGA-Catalog")
    public class CreateUserCommandOptions {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = UserCommandOptions.this.commonCommandOptions;

        @Parameter(names = {"-u", "--user"}, description = "User id", arity = 1)
        public String user;

        @Parameter(names = {"-p", "--password"}, description = "Password", arity = 1, password = true)
        public String password;

        @Parameter(names = {"-n", "--name"}, description = "User name", required = true, arity = 1)
        public String name;

        @Parameter(names = {"-e", "--email"}, description = "Email", required = true, arity = 1)
        public String email;

        @Parameter(names = {"-o", "--organization"}, description = "Organization", required = false, arity = 1)
        public String organization;

    }

    @Parameters(commandNames = {"info"}, commandDescription = "Get user's information")
    public class InfoCommandOptions {
        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = UserCommandOptions.this.commonCommandOptions;
    }


    @Parameters(commandNames = {"list"}, commandDescription = "List all projects and studies from a selected user")
    public class ListCommandOptions {
        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = UserCommandOptions.this.commonCommandOptions;

        @Parameter(names = {"--level"}, description = "Descend only level directories deep.", arity = 1)
        public int level = Integer.MAX_VALUE;

        @Parameter(names = {"-R", "--recursive"}, description = "List subdirectories recursively", arity = 0)
        public boolean recursive = false;

        @Parameter(names = {"-U", "--show-uris"}, description = "Show uris from linked files and folders", arity = 0)
        public boolean uries = false;

    }

    @Parameters(commandNames = {"login"}, commandDescription = "Login as user and return its sessionId")
    public class LoginCommandOptions {

        @Parameter(names = {"-u", "--user"}, description = "UserId", required = false, arity = 1)
        public String user;

        @Parameter(names = {"-p", "--password"}, description = "Password", arity = 1, required = false,  password = true)
        public String password;

        @Deprecated
        @Parameter(names = {"-hp", "--hidden-password"}, description = "Password", arity = 1, required = false,  password = true)
        public String hiddenPassword;

        @Parameter(names = {"-sid", "--session-id"}, description = "SessionId", arity = 1, required = false, hidden = true)
        public String sessionId;

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = UserCommandOptions.this.commonCommandOptions;
    }

    @Parameters(commandNames = {"logout"}, commandDescription = "End user session")
    public class LogoutCommandOptions {
        @Parameter(names = {"--session-id", "-sid"}, description = "SessionId", required = false, arity = 1)
        public String sessionId;
    }


}
