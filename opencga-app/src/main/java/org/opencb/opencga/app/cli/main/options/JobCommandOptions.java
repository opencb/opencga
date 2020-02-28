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
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.common.Enums;

import java.util.List;

import static org.opencb.opencga.app.cli.GeneralCliOptions.*;
import static org.opencb.opencga.core.models.common.Enums.ExecutionStatus.RUNNING;

/**
 * Created by sgallego on 6/14/16.
 */
@Parameters(commandNames = {"jobs"}, commandDescription = "Jobs commands")
public class JobCommandOptions {

    public CreateCommandOptions createCommandOptions;
    public InfoCommandOptions infoCommandOptions;
    public SearchCommandOptions searchCommandOptions;
    public TopCommandOptions topCommandOptions;
    public LogCommandOptions logCommandOptions;
    public DeleteCommandOptions deleteCommandOptions;

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
        this.topCommandOptions = new TopCommandOptions();
        this.logCommandOptions = new LogCommandOptions();
        this.deleteCommandOptions = new DeleteCommandOptions();

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

        @Parameter(names = {"--id"}, description = "Job id", required = true, arity = 1)
        public String id;

        @Parameter(names = {"--tool-id"}, description = "Tool id", required = true, arity = 1)
        public String toolId;

        @Parameter(names = {"-d", "--description"}, description = "Job description", arity = 1)
        public String description;

        @Parameter(names = {"--command-line"}, description = "Command line", arity = 1)
        public String commandLine;

        @Parameter(names = {"--priority"}, description = "Priority", arity = 1)
        public Enums.Priority priority;

        @Parameter(names = {"--creation-date"}, description = "Creation date", arity = 1)
        public String creationDate;

        @Parameter(names = {"--status"}, description = "Job status", arity = 1)
        public Enums.ExecutionStatus executionStatus;

        @Parameter(names = {"--output-directory"}, description = "Directory (previously registered in catalog) where the output is stored",
                required = true, arity = 1)
        public String outDir;

        @Parameter(names = {"--input"}, description = "Comma separated list of file ids used as input of the job", arity = 1)
        public List<String> input;

        @Parameter(names = {"--output"}, description = "Comma separated list of file ids used as output of the job", arity = 1)
        public List<String> output;

        @Parameter(names = {"--tags"}, description = "Comma separated list of tags", arity = 1)
        public List<String> tags;
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

        @Parameter(names = {"--id"}, description = "Comma separated list of job ids.", required = false, arity = 1)
        public String id;

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

    @Parameters(commandNames = {"top"}, commandDescription = "Provide a view of jobs activity in real time.")
    public class TopCommandOptions extends StudyOption {
        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-d", "--delay"}, description = "Delay between iterations in seconds", required = false, arity = 1)
        public int delay = 2;

        @Parameter(names = { "--iterations"}, description = "Exit after N iterations", required = false, arity = 1)
        public Integer iterations;

        @Parameter(names = {"-n", "--jobs"}, description = "Number of jobs to print", required = false, arity = 1)
        public int jobsLimit = 20;
    }

    @Parameters(commandNames = {"log"}, commandDescription = "Provide a view of jobs activity in real time.")
    public class LogCommandOptions extends StudyOption {
        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--job"}, description = ParamConstants.JOB_ID_DESCRIPTION + " or 'running' to print all running jobs.")
        public String job = RUNNING.toLowerCase();

        @Parameter(names = {"--type"}, description = "Log file to be shown (stdout or stderr)")
        public String type = "stderr";

        @Parameter(names = {"-f", "--follow"}, description = "Output appended data as the file grows", arity = 0)
        public boolean follow;

        @Parameter(names = {"-n", "--tail"}, description = "Output the last lines NUM lines.", arity = 1)
        public Integer tailLines;

        @Parameter(names = {"-d", "--delay"}, description = "Delay between iterations in seconds", required = false, arity = 1)
        public int delay = 2;
    }

    @Parameters(commandNames = {"delete"}, commandDescription = "Delete job")
    public class DeleteCommandOptions extends BaseJobCommand {

        @Parameter(names = {"--delete-files"}, description = "Delete files, default:true", required = false, arity = 0)
        public boolean deleteFiles = true;
    }

}
