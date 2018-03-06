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
import org.opencb.opencga.core.models.GroupParams;
import org.opencb.opencga.core.models.MemberParams;
import org.opencb.opencga.core.models.acls.AclParams;

import static org.opencb.opencga.app.cli.GeneralCliOptions.*;

/**
 * Created by pfurio on 13/06/16.
 */
@Parameters(commandNames = {"studies"}, commandDescription = "Study commands")
public class StudyCommandOptions {

    public CreateCommandOptions createCommandOptions;
    public InfoCommandOptions infoCommandOptions;
    public FilesCommandOptions filesCommandOptions;
    public ScanFilesCommandOptions scanFilesCommandOptions;
    public ResyncFilesCommandOptions resyncFilesCommandOptions;
    public StatusCommandOptions statusCommandOptions;
    public SearchCommandOptions searchCommandOptions;
    public UpdateCommandOptions updateCommandOptions;
    public DeleteCommandOptions deleteCommandOptions;
    public SummaryCommandOptions summaryCommandOptions;
    public JobsCommandOptions jobsCommandOptions;
    public SamplesCommandOptions samplesCommandOptions;
    public HelpCommandOptions helpCommandOptions;

    public GroupsCommandOptions groupsCommandOptions;
    public GroupsCreateCommandOptions groupsCreateCommandOptions;
    public GroupsDeleteCommandOptions groupsDeleteCommandOptions;
    public GroupsUpdateCommandOptions groupsUpdateCommandOptions;
    public MemberGroupUpdateCommandOptions memberGroupUpdateCommandOptions;
    public AdminsGroupUpdateCommandOptions adminsGroupUpdateCommandOptions;

    public AclsCommandOptions aclsCommandOptions;
    public AclsUpdateCommandOptions aclsUpdateCommandOptions;

    public JCommander jCommander;
    public CommonCommandOptions commonCommandOptions;
    public DataModelOptions commonDataModelOptions;
    public NumericOptions commonNumericOptions;

    public StudyCommandOptions(CommonCommandOptions commonCommandOptions, DataModelOptions dataModelOptions, NumericOptions numericOptions,
                               JCommander jCommander) {

        this.commonCommandOptions = commonCommandOptions;
        this.commonDataModelOptions = dataModelOptions;
        this.commonNumericOptions = numericOptions;
        this.jCommander = jCommander;

        this.createCommandOptions = new CreateCommandOptions();
        this.infoCommandOptions = new InfoCommandOptions();
        this.filesCommandOptions = new FilesCommandOptions();
        this.scanFilesCommandOptions = new ScanFilesCommandOptions();
        this.resyncFilesCommandOptions = new ResyncFilesCommandOptions();
        this.statusCommandOptions = new StatusCommandOptions();
        this.searchCommandOptions = new SearchCommandOptions();
        this.updateCommandOptions = new UpdateCommandOptions();
        this.deleteCommandOptions = new DeleteCommandOptions();
        this.summaryCommandOptions = new SummaryCommandOptions();
        this.jobsCommandOptions = new JobsCommandOptions();
        this.samplesCommandOptions = new SamplesCommandOptions();
        this.helpCommandOptions = new HelpCommandOptions();

        this.groupsCommandOptions = new GroupsCommandOptions();
        this.groupsCreateCommandOptions = new GroupsCreateCommandOptions();
        this.groupsDeleteCommandOptions = new GroupsDeleteCommandOptions();
        this.groupsUpdateCommandOptions = new GroupsUpdateCommandOptions();
        this.memberGroupUpdateCommandOptions = new MemberGroupUpdateCommandOptions();
        this.adminsGroupUpdateCommandOptions = new AdminsGroupUpdateCommandOptions();

        this.aclsCommandOptions = new AclsCommandOptions();
        this.aclsUpdateCommandOptions = new AclsUpdateCommandOptions();
    }

    public abstract class BaseStudyCommand extends StudyOption {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

    }

    @Parameters(commandNames = {"create"}, commandDescription = "Create new study")
    public class CreateCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-p", "--project"}, description = "Project identifier, this parameter is optional when only one project exist",
                arity = 1)
        public String project;

        @Parameter(names = {"-n", "--name"}, description = "Study name", required = true, arity = 1)
        public String name;

        @Parameter(names = {"-a", "--alias"}, description = "Study alias", required = true, arity = 1)
        public String alias;

        @Parameter(names = {"-t", "--type"}, description = "Type of study, ej.CASE_CONTROL,CASE_SET,...", arity = 1)
        public String type = "CASE_CONTROL";

        @Parameter(names = {"-d", "--description"}, description = "Description", arity = 1)
        public String description;
    }

    @Parameters(commandNames = {"info"}, commandDescription = "Get study information")
    public class InfoCommandOptions extends BaseStudyCommand {

        @ParametersDelegate
        public DataModelOptions dataModelOptions = commonDataModelOptions;
    }

    @Parameters(commandNames = {"search"}, commandDescription = "Search studies")
    public class SearchCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public DataModelOptions dataModelOptions = commonDataModelOptions;

        @ParametersDelegate
        public NumericOptions numericOptions = commonNumericOptions;

        @Parameter(names = {"-p", "--project"}, description = "Project id or alias", arity = 1)
        public String project;

        @Parameter(names = {"-n", "--name"}, description = "Study name.", arity = 1)
        public String name;

        @Parameter(names = {"-a", "--alias"}, description = "Study alias.", arity = 1)
        public String alias;

        @Parameter(names = {"-t", "--type"}, description = "Type of study, ej.CASE_CONTROL,CASE_SET,...", arity = 1)
        public String type;

        @Parameter(names = {"--creation-date"}, description = "Creation date.", arity = 1)
        public String creationDate;

        @Parameter(names = {"--status"}, description = "Status.", arity = 1)
        public String status;

        @Parameter(names = {"--attributes"}, description = "Attributes.", arity = 1)
        public String attributes;

        @Parameter(names = {"--nattributes"}, description = "Numerical attributes.", arity = 1)
        public String nattributes;

        @Parameter(names = {"--battributes"}, description = "Boolean attributes.", arity = 0)
        public String battributes;


    }

    @Parameters(commandNames = {"scan-files"}, commandDescription = "Scans the study folder to find untracked or missing files")
    public class ScanFilesCommandOptions extends BaseStudyCommand {

    }

    @Parameters(commandNames = {"resync-files"}, commandDescription = "Scans the study folder to find untracked or missing files and "
            + "updates the status")
    public class ResyncFilesCommandOptions extends BaseStudyCommand {

    }


    @Parameters(commandNames = {"files"}, commandDescription = "Fetch files from a study")
    public class FilesCommandOptions extends BaseStudyCommand {

        @ParametersDelegate
        public DataModelOptions dataModelOptions = commonDataModelOptions;

        @ParametersDelegate
        public NumericOptions numericOptions = commonNumericOptions;

        @Deprecated
        @Parameter(names = {"--file"}, description = "[DEPRECATED] File id", arity = 1)
        public String file;

        @Parameter(names = {"-n", "--name"}, description = "Name", arity = 1)
        public String name;

        @Deprecated
        @Parameter(names = {"--path"}, description = "[DEPRECATED] Path", arity = 1)
        public String path;

        @Parameter(names = {"-t", "--file-type"}, description = "Comma separated Type values. For existing Types see files/help", arity = 1)
        public String type = "FILE";

        @Parameter(names = {"-b", "--bioformat"}, description = "Comma separated Bioformat values. For existing Bioformats see files/help",
                arity = 1)
        public String bioformat;

        @Parameter(names = {"--format"}, description = "Comma separated Format values. For existing Formats see files/help", arity = 1)
        public String format;

        @Parameter(names = {"--status"}, description = "Status", arity = 1)
        public String status;

        @Parameter(names = {"--directory"}, description = "Directory", arity = 1)
        public String directory;

        @Parameter(names = {"--creation-date"}, description = "Creation date.", arity = 1)
        public String creationDate;

        @Parameter(names = {"--modification-date"}, description = "Modification Date.", arity = 1)
        public String modificationDate;

        @Deprecated
        @Parameter(names = {"--description"}, description = "Description", arity = 1)
        public String description;

        @Parameter(names = {"--size"}, description = "Filter by size of the file", arity = 1)
        public String size;

        @Parameter(names = {"--sample-ids"}, description = "Comma separated sampleIds", arity = 1)
        public String sampleIds;

        @Parameter(names = {"--job-id"}, description = "Job Id", arity = 1)
        public String jobId;

        @Parameter(names = {"--attributes"}, description = "Attributes.", arity = 1)
        public String attributes;

        @Parameter(names = {"--nattributes"}, description = "Numerical attributes.", arity = 1)
        public String nattributes;

        @Parameter(names = {"-e", "--external"}, description = "Whether to fetch external linked files", arity = 0)
        public boolean external;
    }


    @Parameters(commandNames = {"status"}, commandDescription = "Scans the study folder to find untracked or missing files")
    public class StatusCommandOptions extends BaseStudyCommand {

    }

    @Parameters(commandNames = {"update"}, commandDescription = "Update the attributes of a study")
    public class UpdateCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"-n", "--name"}, description = "Study name", arity = 1)
        public String name;

        @Parameter(names = {"-t", "--type"}, description = "Type of study, ej.CASE_CONTROL,CASE_SET,...", arity = 1)
        public String type;

        @Parameter(names = {"-d", "--description"}, description = "Organization", arity = 1)
        public String description;

        @Parameter(names = {"--stats"}, description = "Stats", arity = 1)
        public String stats;

        @Parameter(names = {"--attributes"}, description = "Attributes", arity = 1)
        public String attributes;

    }

    @Parameters(commandNames = {"delete"}, commandDescription = "[PENDING] Delete a study")
    public class DeleteCommandOptions extends BaseStudyCommand {

    }

    @Parameters(commandNames = {"summary"}, commandDescription = "Summary with the general stats of a study")
    public class SummaryCommandOptions extends BaseStudyCommand {

    }

    @Parameters(commandNames = {"jobs"}, commandDescription = "Study jobs information")
    public class JobsCommandOptions extends BaseStudyCommand {

        @ParametersDelegate
        public DataModelOptions dataModelOptions = commonDataModelOptions;

        @ParametersDelegate
        public NumericOptions numericOptions = commonNumericOptions;

        @Parameter(names = {"-n", "--name"}, description = "Job name", arity = 1)
        public String name;

        @Parameter(names = {"--tool-name"}, description = "Tool name", arity = 1)
        public String toolName;

        @Parameter(names = {"--status"}, description = "Job status", arity = 1)
        public String status;

        @Parameter(names = {"--owner-id"}, description = "User that created the job", arity = 1)
        public String ownerId;

        @Parameter(names = {"--date"}, description = "Creation date of the job", arity = 1)
        public String date;

        @Deprecated
        @Parameter(names = {"--input-files"}, description = "[DEPRECATED] Comma separated list of input file ids", arity = 1)
        public String inputFiles;

        @Deprecated
        @Parameter(names = {"--output-files"}, description = "[DEPRECATED] Comma separated list of output file ids", arity = 1)
        public String outputFiles;

    }

    @Parameters(commandNames = {"samples"}, commandDescription = "Study samples information")
    public class SamplesCommandOptions extends BaseStudyCommand {

        @ParametersDelegate
        public DataModelOptions dataModelOptions = commonDataModelOptions;

        @ParametersDelegate
        public NumericOptions numericOptions = commonNumericOptions;

        @Parameter(names = {"-n", "--name"}, description = "Sample name", arity = 1)
        public String name;

        @Parameter(names = {"--source"}, description = "Source of the sample", arity = 1)
        public String source;

        @Parameter(names = {"-d", "--description"}, description = "Sample description", arity = 1)
        public String description;

        @Parameter(names = {"--individual"}, description = "Individual id", arity = 1)
        public String individual;

        @Parameter(names = {"--annotation"}, description = "Annotation", arity = 1)
        public String annotation;

    }

    @Parameters(commandNames = {"groups"}, commandDescription = "Return the groups present in the studies")
    public class GroupsCommandOptions extends BaseStudyCommand {
        @Parameter(names = {"--name"}, description = "Group name. If present, it will fetch only information of the group provided.",
                arity = 1)
        public String group;
    }

    @Parameters(commandNames = {"help"}, commandDescription = "Help [PENDING]")
    public class HelpCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

    }

    @Parameters(commandNames = {"groups-create"}, commandDescription = "Create a group")
    public class GroupsCreateCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"--name"}, description = "Group name.", required = true, arity = 1)
        public String groupId;

        @Parameter(names = {"--users"}, description = "Comma separated list of members that will form the group", arity = 1)
        public String users;

    }

    @Parameters(commandNames = {"groups-delete"}, commandDescription = "Delete group")
    public class GroupsDeleteCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"--name"}, description = "Group name", required = true, arity = 1)
        public String groupId;

    }

    @Parameters(commandNames = {"groups-update"}, commandDescription = "Updates the members of the group")
    public class GroupsUpdateCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"--name"}, description = "Group name", required = true, arity = 1)
        public String groupId;

        @Parameter(names = {"--users"}, description = "Comma separated list of users", required = true, arity = 1)
        public String users;

        @Parameter(names = {"--action"}, description = "Action to be performed over users (ADD, SET, REMOVE)", required = true, arity = 1)
        public GroupParams.Action action;
    }

    @Parameters(commandNames = {"members-update"}, commandDescription = "Add/Remove users to access the study")
    public class MemberGroupUpdateCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"--users"}, description = "Comma separated list of users", required = true, arity = 1)
        public String users;

        @Parameter(names = {"--action"}, description = "Action to be performed over users (ADD, REMOVE)", required = true, arity = 1)
        public MemberParams.Action action;
    }

    @Parameters(commandNames = {"admins-update"}, commandDescription = "Add/Remove administrative users to the study")
    public class AdminsGroupUpdateCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"--users"}, description = "Comma separated list of users", required = true, arity = 1)
        public String users;

        @Parameter(names = {"--action"}, description = "Action to be performed over users (ADD, REMOVE)", required = true, arity = 1)
        public MemberParams.Action action;
    }

    @Parameters(commandNames = {"acl"}, commandDescription = "Return the acls set for the resource")
    public class AclsCommandOptions extends BaseStudyCommand {
        @Parameter(names = {"-m", "--member"}, description = "Member id  ('userId', '@groupId' or '*'). If provided, only returns "
                + "acls given to the member.", arity = 1)
        public String memberId;
    }

    @Parameters(commandNames = {"acl-update"}, commandDescription = "Update the permissions set for a member")
    public class AclsUpdateCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"-m", "--member"}, description = "Member id  ('userId', '@groupId' or '*')", required = true, arity = 1)
        public String memberId;

        @Parameter(names = {"-p", "--permissions"}, description = "Comma separated list of accepted permissions for the resource",
                arity = 1)
        public String permissions;

        @Parameter(names = {"-a", "--action"}, description = "Action to be applied with the permissions (SET, ADD, REMOVE or RESET)",
                arity = 1)
        public AclParams.Action action = AclParams.Action.SET;

        @Parameter(names = {"--template"}, description = "Template of permissions to be used (admin, analyst or view_only)", arity = 1)
        public String template;
    }
}
