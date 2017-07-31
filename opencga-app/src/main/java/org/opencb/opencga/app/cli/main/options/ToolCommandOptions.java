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
import org.opencb.opencga.app.cli.GeneralCliOptions;

/**
 * Created by sgallego on 6/15/16.
 */
@Parameters(commandNames = {"tools"}, commandDescription = "Tools commands")
public class ToolCommandOptions {

    public InfoCommandOptions infoCommandOptions;
    public SearchCommandOptions searchCommandOptions;
    public HelpCommandOptions helpCommandOptions;
    public UpdateCommandOptions updateCommandOptions;
    public DeleteCommandOptions deleteCommandOptions;

    public JCommander jCommander;
    public GeneralCliOptions.CommonCommandOptions commonCommandOptions;

    public ToolCommandOptions(GeneralCliOptions.CommonCommandOptions commonCommandOptions, JCommander jCommander) {
        this.commonCommandOptions = commonCommandOptions;
        this.jCommander = jCommander;

        this.infoCommandOptions = new InfoCommandOptions();
        this.searchCommandOptions = new SearchCommandOptions();
        this.helpCommandOptions = new HelpCommandOptions();
        this.updateCommandOptions = new UpdateCommandOptions();
        this.deleteCommandOptions = new DeleteCommandOptions();

    }

    public class BaseToolsCommand {
        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--id"}, description = "Tool id", required = true, arity = 1)
        public String id;
    }


    @Parameters(commandNames = {"info"}, commandDescription = "Get tool information")
    public class InfoCommandOptions extends BaseToolsCommand {

        @Parameter(names = {"--include"}, description = "Comma separated list of fields to be included in the response", arity = 1)
        public String include;

        @Parameter(names = {"--exclude"}, description = "Comma separated list of fields to be excluded from the response", arity = 1)
        public String exclude;

        @Parameter(names = {"--execution"}, description = "execution", required = false, arity = 1)
        public String execution;
    }

    @Parameters(commandNames = {"search"}, commandDescription = "Search tools")
    public class SearchCommandOptions {

        @Parameter(names = {"--include"}, description = "Comma separated list of fields to be included in the response", arity = 1)
        public String include;

        @Parameter(names = {"--exclude"}, description = "Comma separated list of fields to be excluded from the response", arity = 1)
        public String exclude;

        @Parameter(names = {"--skip"}, description = "Number of results to skip", arity = 1)
        public String skip;

        @Parameter(names = {"--limit"}, description = "Maximum number of results to be returned", arity = 1)
        public String limit;

        @Parameter(names = {"--ids"}, description = "Comma separated list of ids", required = false, arity = 1)
        public String id;

        @Parameter(names = {"--user-id"}, description = "UserId", required = false, arity = 1)
        public String userId;

        @Parameter(names = {"--alias"}, description = "alias", required = false, arity = 1)
        public String alias;

        @Parameter(names = {"--count"}, description = "Total number of results.", required = false, arity = 0)
        public boolean count;
    }

    @Parameters(commandNames = {"help"}, commandDescription = "Tool help")
    public class HelpCommandOptions extends BaseToolsCommand {

        @Parameter(names = {"--execution"}, description = "execution", required = false, arity = 1)
        public String execution;
    }

    @Parameters(commandNames = {"update"}, commandDescription = "Update some user attributes")
    public class UpdateCommandOptions extends BaseToolsCommand {

    }

    @Parameters(commandNames = {"delete"}, commandDescription = "Delete a tool")
    public class DeleteCommandOptions extends BaseToolsCommand {

    }

}
