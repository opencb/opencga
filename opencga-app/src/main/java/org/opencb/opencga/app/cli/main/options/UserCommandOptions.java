/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.app.cli.main.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.GeneralCliOptions.NumericOptions;

import static org.opencb.opencga.app.cli.GeneralCliOptions.CommonCommandOptions;
import static org.opencb.opencga.app.cli.GeneralCliOptions.DataModelOptions;

/**
 * Created by pfurio on 13/06/16.
 */
@Parameters(commandNames = {"users"}, commandDescription = "User commands")
public class UserCommandOptions {

    public CreateCommandOptions createCommandOptions;
    public InfoCommandOptions infoCommandOptions;
    public UpdateCommandOptions updateCommandOptions;
    public ChangePasswordCommandOptions changePasswordCommandOptions;
    public ProjectsCommandOptions projectsCommandOptions;
    public LoginCommandOptions loginCommandOptions;
    public LogoutCommandOptions logoutCommandOptions;
    public TemplateCommandOptions templateCommandOptions;

    public JCommander jCommander;
    public CommonCommandOptions commonCommandOptions;
    public DataModelOptions commonDataModelOptions;
    public NumericOptions commonNumericOptions;

    protected static final String DEPRECATED = "[DEPRECATED] ";

    public UserCommandOptions(CommonCommandOptions commonCommandOptions, DataModelOptions dataModelOptions, NumericOptions numericOptions,
                              JCommander jCommander) {

        this.commonCommandOptions = commonCommandOptions;
        this.commonDataModelOptions = dataModelOptions;
        this.commonNumericOptions = numericOptions;
        this.jCommander = jCommander;

        this.createCommandOptions = new CreateCommandOptions();
        this.infoCommandOptions = new InfoCommandOptions();
        this.updateCommandOptions = new UpdateCommandOptions();
        this.changePasswordCommandOptions = new ChangePasswordCommandOptions();
        this.projectsCommandOptions = new ProjectsCommandOptions();
        this.loginCommandOptions = new LoginCommandOptions();
        this.logoutCommandOptions = new LogoutCommandOptions();
        this.templateCommandOptions = new TemplateCommandOptions();
    }

    public JCommander getjCommander() {
        return jCommander;
    }

    public class BaseUserCommand {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-u", "--user"}, description = "User id, this must be unique in OpenCGA",  required = true, arity = 1)
        public String user;

    }

    public class NotRequiredUserParam {

        @Parameter(names = {"-u", "--user"}, description = "User id, this must be unique in OpenCGA",  required = false, arity = 1)
        public String user;

    }

    @Parameters(commandNames = {"create"}, commandDescription = "Create a new user")
    public class CreateCommandOptions extends BaseUserCommand {

        @Parameter(names = {"-n", "--name"}, description = "User name", required = true, arity = 1)
        public String name;

        @Parameter(names = {"-p", "--password"}, description = "User password", required = true,  password = true, arity = 1)
        public String password;

        @Parameter(names = {"-e", "--email"}, description = "User email", required = true, arity = 1)
        public String email;

        @Parameter(names = {"-o", "--organization"}, description = "User organization", arity = 1)
        public String organization;
    }

    @Parameters(commandNames = {"info"}, commandDescription = "Get complete information of the user together with owned and shared projects"
            + " and studies")
    public class InfoCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public DataModelOptions dataModelOptions = commonDataModelOptions;

        @ParametersDelegate
        public NotRequiredUserParam userParam = new NotRequiredUserParam();

        @Deprecated
        @Parameter(names = {"--last-modified"}, description = "[DEPRECATED] If matches with the user's last activity, return " +
                "an empty QueryResult", arity = 1, required = false)
        public String lastModified;
    }

    @Parameters(commandNames = {"update"}, commandDescription = "Update some user attributes")
    public class UpdateCommandOptions extends BaseUserCommand {

        @Parameter(names = {"-n", "--name"}, description = DEPRECATED + "Use --json instead.", arity = 1)
        public String name;

        @Parameter(names = {"-e", "--email"}, description = DEPRECATED + "Use --json instead.", arity = 1)
        public String email;

        @Parameter(names = {"-o", "--organization"}, description = DEPRECATED + "Use --json instead.", arity = 1)
        public String organization;

        @Parameter(names = {"--attributes"}, description = DEPRECATED + "Use --json instead.", arity = 1)
        public String attributes;

        @Parameter(names = {"--json"}, description = "JSON file containing the user fields to be updated", required = true, arity = 1)
        public String json;
    }

    @Parameters(commandNames = {"password"}, commandDescription = "Change the user's password")
    public class ChangePasswordCommandOptions {

        @Parameter(names = {"-u", "--user"}, description = "User id", arity = 1, required = true)
        public String user;

        @Parameter(names = {"--password"}, description = "Old password", arity = 1, required = true)
        public String password;

        @Parameter(names = {"--new-password"}, description = "New password", arity = 1, required = true)
        public String npassword;
    }

    @Parameters(commandNames = {"projects"}, commandDescription = "List all projects and studies belonging to the user")
    public class ProjectsCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public DataModelOptions dataModelOptions = commonDataModelOptions;

        @ParametersDelegate
        public NumericOptions numericOptions = commonNumericOptions;

        @ParametersDelegate
        public NotRequiredUserParam userParam = new NotRequiredUserParam();

    }

    @Parameters(commandNames = {"login"}, commandDescription = "Get identified and gain access to the system")
    public class LoginCommandOptions extends BaseUserCommand {

        @Parameter(names = {"-p", "--password"}, description = "User password", arity = 0, required = true, password = true, hidden = true)
        public String password;

    }

    @Parameters(commandNames = {"logout"}, commandDescription = "End user session")
    public class LogoutCommandOptions {

        @Parameter(names = {"-S", "--session-id"}, description = "SessionId", required = false, arity = 1, hidden = true)
        public String sessionId;

    }

    @Parameters(commandNames = {"load-template"}, commandDescription = "Load data from a template")
    public class TemplateCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-f", "--file"}, arity = 1, required = true, description = "Template file")
        public String file;
    }


}
