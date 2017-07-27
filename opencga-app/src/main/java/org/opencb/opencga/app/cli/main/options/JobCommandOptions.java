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
import org.opencb.opencga.app.cli.main.options.commons.AclCommandOptions;

import static org.opencb.opencga.app.cli.GeneralCliOptions.*;

/**
 * Created by sgallego on 6/14/16.
 */
@Parameters(commandNames = {"jobs"}, commandDescription = "Jobs commands")
public class JobCommandOptions {

    public CreateCommandOptions createCommandOptions;
    public InfoCommandOptions infoCommandOptions;
    public SearchCommandOptions searchCommandOptions;
    public VisitCommandOptions visitCommandOptions;
    public DeleteCommandOptions deleteCommandOptions;
    public GroupByCommandOptions groupByCommandOptions;

    public AclCommandOptions.AclsCommandOptions aclsCommandOptions;
    public AclCommandOptions.AclsUpdateCommandOptions aclsUpdateCommandOptions;

    public JCommander jCommander;
    public CommonCommandOptions commonCommandOptions;
    public DataModelOptions commonDataModelOptions;
    public NumericOptions commonNumericOptions;

    public JobCommandOptions(CommonCommandOptions commonCommandOptions, DataModelOptions dataModelOptions, NumericOptions numericOptions,
                             JCommander jCommander) {

        this.commonCommandOptions = commonCommandOptions;
        this.commonDataModelOptions = dataModelOptions;
        this.commonNumericOptions = numericOptions;
        this.jCommander = jCommander;

        this.createCommandOptions = new CreateCommandOptions();
        this.infoCommandOptions = new InfoCommandOptions();
        this.searchCommandOptions = new SearchCommandOptions();
        this.visitCommandOptions = new VisitCommandOptions();
        this.deleteCommandOptions = new DeleteCommandOptions();
        this.groupByCommandOptions = new GroupByCommandOptions();

        AclCommandOptions aclCommandOptions = new AclCommandOptions(commonCommandOptions);
        this.aclsCommandOptions = aclCommandOptions.getAclsCommandOptions();
        this.aclsUpdateCommandOptions = aclCommandOptions.getAclsUpdateCommandOptions();
    }

    public class BaseJobCommand extends StudyOption {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--job"}, description = "Job id", required = true, arity = 1)
        public String job;

    }

    @Parameters(commandNames = {"create"}, commandDescription = "Create a job in catalog (register an already executed job)")
    public class CreateCommandOptions extends StudyOption {

        @ParametersDelegate
        CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-n", "--name"}, description = "Job name", required = true, arity = 1)
        public String name;

        @Parameter(names = {"--tool-name"}, description = "Tool name", required = true, arity = 1)
        public String toolName;

        @Parameter(names = {"-d", "--description"}, description = "Job description", arity = 1)
        public String description;

        @Parameter(names = {"--execution"}, description = "Execution", arity = 1)
        public String execution;

        @Parameter(names = {"--start-time"}, description = "Start time of the job", arity = 1)
        public long startTime;

        @Parameter(names = {"--end-time"}, description = "End time of the job", arity = 1)
        public long endTime;

        @Parameter(names = {"--command-line"}, description = "Command line", required = true, arity = 1)
        public String commandLine;

        @Parameter(names = {"--output-directory"}, description = "Directory (previously registered in catalog) where the output is stored",
                required = true, arity = 1)
        public String outDir;

        @Parameter(names = {"--input"}, description = "Comma separated list of file ids used as input of the job", arity = 1)
        public String input;

        @Parameter(names = {"--output"}, description = "Comma separated list of file ids used as output of the job", arity = 1)
        public String output;
    }

    @Parameters(commandNames = {"info"}, commandDescription = "Get job information")
    public class InfoCommandOptions extends BaseJobCommand {

        @ParametersDelegate
        public DataModelOptions dataModelOptions = commonDataModelOptions;

    }


    @Parameters(commandNames = {"search"}, commandDescription = "Search job")
    public class SearchCommandOptions extends StudyOption {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public DataModelOptions dataModelOptions = commonDataModelOptions;

        @ParametersDelegate
        public NumericOptions numericOptions = commonNumericOptions;

        @Parameter(names = {"-n", "--name"}, description = "Comma separated list of names.", required = false, arity = 1)
        public String name;

        @Parameter(names = {"--tool-name"}, description = "Tool name.", required = false, arity = 1)
        public String toolName;

        @Parameter(names = {"--status"}, description = "Status.", required = false, arity = 1)
        public String status;

        @Parameter(names = {"--owner-id"}, description = "Owner id.", required = false, arity = 1)
        public String ownerId;

        @Parameter(names = {"--date"}, description = "Creation date.", required = false, arity = 1)
        public String date;

        @Parameter(names = {"--input-files"}, description = "Comma separated list of input file ids.", required = false, arity = 1)
        public String inputFiles;

        @Parameter(names = {"--output-files"}, description = "Comma separated list of output file ids.", required = false, arity = 1)
        public String outputFiles;

    }

    @Parameters(commandNames = {"visit"}, commandDescription = "Increment job visits")
    public class VisitCommandOptions extends BaseJobCommand {

    }

    @Parameters(commandNames = {"delete"}, commandDescription = "Delete job")
    public class DeleteCommandOptions extends BaseJobCommand {

        @Parameter(names = {"--delete-files"}, description = "Delete files, default:true", required = false, arity = 0)
        public boolean deleteFiles = true;
    }

    @Parameters(commandNames = {"group-by"}, commandDescription = "GroupBy job")
    public class GroupByCommandOptions extends StudyOption {

        @ParametersDelegate
        CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-f", "--fields"}, description = "Comma separated list of fields by which to group by.", required = true, arity = 1)
        public String fields;

        @Deprecated
        @Parameter(names = {"--ids"}, description = "[DEPRECATED] Comma separated list of ids.", required = false, arity = 1)
        public String id;

        @Parameter(names = {"-n", "--name"}, description = "Comma separated list of names.", required = false, arity = 1)
        public String name;

        @Parameter(names = {"--path"}, description = "Path.", required = false, arity = 1)
        public String path;

        @Parameter(names = {"--status"}, description = "Status.", required = false, arity = 1)
        public String status;

        @Parameter(names = {"--owner-id"}, description = "Owner id.", required = false, arity = 1)
        public String ownerId;

        @Parameter(names = {"--creation-date"}, description = "Creation date.", required = false, arity = 1)
        public String creationDate;

        @Deprecated
        @Parameter(names = {"-d", "--description"}, description = "Description", required = false, arity = 1)
        public String description;

        @Parameter(names = {"--attributes"}, description = "Attributes", required = false, arity = 1)
        public String attributes;
    }

}
