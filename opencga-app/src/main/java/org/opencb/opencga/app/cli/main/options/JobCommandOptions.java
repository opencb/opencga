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
    public RetryCommandOptions retryCommandOptions;
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
        this.retryCommandOptions = new RetryCommandOptions();
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

    @Parameters(commandNames = {"retry"}, commandDescription = "Relaunch a failed job")
    public class RetryCommandOptions extends StudyOption {

        @ParametersDelegate
        CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--job", "--job-to-retry"}, description = ParamConstants.JOB_ID_DESCRIPTION + " of the job to retry", required = true, arity = 1)
        public String jobToRetry;

        @Parameter(names = {"--id"}, description = ParamConstants.JOB_ID_CREATION_DESCRIPTION, arity = 1)
        public String id;

        @Parameter(names = {"-d", "--description"}, description = ParamConstants.JOB_DESCRIPTION_DESCRIPTION, arity = 1)
        public String description;

        @Parameter(names = {"--depends-on"}, description = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION, arity = 1)
        public List<String> jobDependsOn;

        @Parameter(names = {"--tags"}, description = ParamConstants.JOB_TAGS_DESCRIPTION, arity = 1)
        public List<String> jobTags;
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

        @Parameter(names = {"--id"}, description = ParamConstants.JOB_ID_CREATION_DESCRIPTION, arity = 1)
        public String id;

        @Parameter(names = {"--other-studies"}, description = ParamConstants.OTHER_STUDIES_FLAG_DESCRIPTION, arity = 0)
        public Boolean otherStudies;

        @Parameter(names = {"--tool-id"}, description = ParamConstants.JOB_TOOL_ID_DESCRIPTION, arity = 1)
        public String toolId;

        @Parameter(names = {"--user-id"}, description = ParamConstants.JOB_USER_DESCRIPTION, arity = 1)
        public String userId;

        @Parameter(names = {"--priority"}, description = ParamConstants.JOB_PRIORITY_DESCRIPTION, arity = 1)
        public String priority;

        @Parameter(names = {"--internal-status"}, description = ParamConstants.JOB_STATUS_DESCRIPTION, arity = 1)
        public String internalStatus;

        @Parameter(names = {"--creation-date"}, description = ParamConstants.CREATION_DATE_DESCRIPTION, arity = 1)
        public String creationDate;

        @Parameter(names = {"--modification-date"}, description = ParamConstants.MODIFICATION_DATE_DESCRIPTION, arity = 1)
        public String modificationDate;

        @Parameter(names = {"--execution-start"}, description = ParamConstants.JOB_EXECUTION_START_DESCRIPTION, arity = 1)
        public String executionStart;

        @Parameter(names = {"--execution-end"}, description = ParamConstants.JOB_EXECUTION_END_DESCRIPTION, arity = 1)
        public String executionEnd;

        @Parameter(names = {"--visited"}, description = ParamConstants.JOB_VISITED_DESCRIPTION, arity = 0)
        public Boolean visited;

        @Parameter(names = {"--tags"}, description = ParamConstants.JOB_TAGS_DESCRIPTION, arity = 1)
        public String tags;

        @Parameter(names = {"--input-files"}, description = ParamConstants.JOB_INPUT_FILES_DESCRIPTION, arity = 1)
        public String inputFiles;

        @Parameter(names = {"--output-files"}, description = ParamConstants.JOB_OUTPUT_FILES_DESCRIPTION, arity = 1)
        public String outputFiles;

        @Parameter(names = {"--acl"}, description = ParamConstants.ACL_DESCRIPTION, arity = 1)
        public String acl;

        @Parameter(names = {"--release"}, description = ParamConstants.RELEASE_DESCRIPTION, arity = 1)
        public Integer release;

        @Parameter(names = {"--deleted"}, description = ParamConstants.DELETED_DESCRIPTION, arity = 0)
        public Boolean deleted;
    }

    @Parameters(commandNames = {"top"}, commandDescription = "Provide a view of jobs activity in real time.")
    public class TopCommandOptions extends StudyOption {
        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-d", "--delay"}, description = "Delay between iterations in seconds", arity = 1)
        public int delay = 2;

        @Parameter(names = {"--plain"}, description = "Plain representation, without dependencies.", arity = 0)
        public boolean plain;

        @Parameter(names = {"--columns"}, description = "Output columns to print." +
                " [ID, TOOL_ID, STATUS, EVENTS, STUDY, SUBMISSION, PRIORITY, RUNNING_TIME, START, END, INPUT, OUTPUT, OUTPUT_DIRECTORY]")
        public String columns;

        @Parameter(names = { "--iterations"}, description = "Exit after N iterations", arity = 1)
        public Integer iterations;

        @Parameter(names = {"-n", "--jobs"}, description = "Number of jobs to print", arity = 1)
        public Integer jobsLimit;

        @Parameter(names = {"--tool-id"}, description = ParamConstants.JOB_TOOL_ID_DESCRIPTION, arity = 1)
        public String toolId;

        @Parameter(names = {"--user-id"}, description = ParamConstants.JOB_USER_DESCRIPTION, arity = 1)
        public String userId;

        @Parameter(names = {"--priority"}, description = ParamConstants.JOB_PRIORITY_DESCRIPTION, arity = 1)
        public String priority;

        @Parameter(names = {"--internal-status"}, description = ParamConstants.JOB_STATUS_DESCRIPTION, arity = 1)
        public String internalStatus;
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

        @Parameter(names = {"-d", "--delay"}, description = "Delay between iterations in seconds", arity = 1)
        public int delay = 2;
    }

    @Parameters(commandNames = {"delete"}, commandDescription = "Delete job")
    public class DeleteCommandOptions extends BaseJobCommand {

    }

}
