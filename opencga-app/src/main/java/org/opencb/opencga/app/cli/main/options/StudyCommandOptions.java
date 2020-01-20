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
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.models.AclParams;
import org.opencb.opencga.core.models.study.GroupParams;
import org.opencb.opencga.core.models.study.Study;

import static org.opencb.opencga.app.cli.GeneralCliOptions.*;

/**
 * Created by pfurio on 13/06/16.
 */
@Parameters(commandNames = {"studies"}, commandDescription = "Study commands")
public class StudyCommandOptions {

    public CreateCommandOptions createCommandOptions;
    public InfoCommandOptions infoCommandOptions;
//    public ScanFilesCommandOptions scanFilesCommandOptions;
//    public ResyncFilesCommandOptions resyncFilesCommandOptions;
//    public StatusCommandOptions statusCommandOptions;
    public SearchCommandOptions searchCommandOptions;
    public UpdateCommandOptions updateCommandOptions;
//    public DeleteCommandOptions deleteCommandOptions;
    public StatsCommandOptions statsCommandOptions;

    public GroupsCommandOptions groupsCommandOptions;
    public GroupsCreateCommandOptions groupsCreateCommandOptions;
    public GroupsDeleteCommandOptions groupsDeleteCommandOptions;
    public GroupsUpdateCommandOptions groupsUpdateCommandOptions;

    public VariableSetsCommandOptions variableSetsCommandOptions;
    public VariableSetsUpdateCommandOptions variableSetsUpdateCommandOptions;
    public VariablesUpdateCommandOptions variablesUpdateCommandOptions;

    public AclsCommandOptions aclsCommandOptions;
    public AclsUpdateCommandOptions aclsUpdateCommandOptions;

    public JCommander jCommander;
    public CommonCommandOptions commonCommandOptions;
    public DataModelOptions commonDataModelOptions;
    public NumericOptions commonNumericOptions;

    protected static final String DEPRECATED = "[DEPRECATED] ";

    public StudyCommandOptions(CommonCommandOptions commonCommandOptions, DataModelOptions dataModelOptions, NumericOptions numericOptions,
                               JCommander jCommander) {

        this.commonCommandOptions = commonCommandOptions;
        this.commonDataModelOptions = dataModelOptions;
        this.commonNumericOptions = numericOptions;
        this.jCommander = jCommander;

        this.createCommandOptions = new CreateCommandOptions();
        this.infoCommandOptions = new InfoCommandOptions();
//        this.scanFilesCommandOptions = new ScanFilesCommandOptions();
//        this.resyncFilesCommandOptions = new ResyncFilesCommandOptions();
        this.searchCommandOptions = new SearchCommandOptions();
        this.updateCommandOptions = new UpdateCommandOptions();
        this.statsCommandOptions = new StatsCommandOptions();

        this.groupsCommandOptions = new GroupsCommandOptions();
        this.groupsCreateCommandOptions = new GroupsCreateCommandOptions();
        this.groupsDeleteCommandOptions = new GroupsDeleteCommandOptions();
        this.groupsUpdateCommandOptions = new GroupsUpdateCommandOptions();

        this.variableSetsCommandOptions = new VariableSetsCommandOptions();
        this.variableSetsUpdateCommandOptions = new VariableSetsUpdateCommandOptions();
        this.variablesUpdateCommandOptions = new VariablesUpdateCommandOptions();

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

        @Parameter(names = {"--id"}, description = "Study id", required = true, arity = 1)
        public String id;

        @Parameter(names = {"-a", "--alias"}, description = DEPRECATED + "Replaced by 'id'", arity = 1)
        public String alias;

        @Parameter(names = {"-t", "--type"}, description = "Type of study, ej.CASE_CONTROL,CASE_SET,...", arity = 1)
        public Study.Type type = Study.Type.CASE_CONTROL;

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

        @Parameter(names = {"--id"}, description = "Study id.", arity = 1)
        public String id;

        @Parameter(names = {"-n", "--name"}, description = "Study name.", arity = 1)
        public String name;

        @Parameter(names = {"-a", "--alias"}, description = "Study alias.", arity = 1)
        public String alias;

        @Parameter(names = {"-t", "--type"}, description = "Type of study, ej.CASE_CONTROL,CASE_SET,...", arity = 1)
        public Study.Type type;

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

    @Parameters(commandNames = {"update"}, commandDescription = "Update a study")
    public class UpdateCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-s", "--study"}, description = "Study [[user@]project:]study.", arity = 1, required = true)
        public String study;

        @Parameter(names = {"-n", "--name"}, description = "Study name", arity = 1)
        public String name;

        @Parameter(names = {"-t", "--type"}, description = "Type of study, ej.CASE_CONTROL,CASE_SET,...", arity = 1)
        public Study.Type type;

        @Parameter(names = {"-d", "--description"}, description = "Organization", arity = 1)
        public String description;

        @Parameter(names = {"--stats"}, description = "Stats", arity = 1)
        public String stats;

        @Parameter(names = {"--attributes"}, description = "Attributes", arity = 1)
        public String attributes;

    }

    @Parameters(commandNames = {"stats"}, commandDescription = "General stats of a study")
    public class StatsCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"default"}, description = "Calculate default stats")
        public boolean defaultStats;

        @Parameter(names = {"--file-fields"}, description = "List of file fields separated by semicolons, e.g.: studies;type. "
                + "For nested fields use >>, e.g.: studies>>biotype;typ", arity = 1)
        public String fileFields;

        @Parameter(names = {"--family-fields"}, description = "List of family fields separated by semicolons, e.g.: studies;type. "
                + "For nested fields use >>, e.g.: studies>>biotype;typ", arity = 1)
        public String familyFields;

        @Parameter(names = {"--individual-fields"}, description = "List of individual fields separated by semicolons, e.g.: studies;type. "
                + "For nested fields use >>, e.g.: studies>>biotype;typ", arity = 1)
        public String individualFields;

        @Parameter(names = {"--sample-fields"}, description = "List of sample fields separated by semicolons, e.g.: studies;type. "
                + "For nested fields use >>, e.g.: studies>>biotype;typ", arity = 1)
        public String sampleFields;

        @Parameter(names = {"--cohort-fields"}, description = "List of cohort fields separated by semicolons, e.g.: studies;type. "
                + "For nested fields use >>, e.g.: studies>>biotype;typ", arity = 1)
        public String cohortFields;
    }

    @Parameters(commandNames = {"groups"}, commandDescription = "Return the groups present in the studies")
    public class GroupsCommandOptions extends BaseStudyCommand {
        @Parameter(names = {"--name"}, description = "Group name. If present, it will fetch only information of the group provided.",
                arity = 1)
        public String group;
    }

    @Parameters(commandNames = {"groups-create"}, commandDescription = "Create a group")
    public class GroupsCreateCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"--id"}, description = "Group id.", required = true, arity = 1)
        public String groupId;

        @Parameter(names = {"--name"}, description = "Group name.", arity = 1)
        public String groupName;

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
        public GroupParams.Action action = GroupParams.Action.ADD;
    }

    @Parameters(commandNames = {"variable-sets"}, commandDescription = "Return the variable sets of a study")
    public class VariableSetsCommandOptions extends BaseStudyCommand {
        @Parameter(names = {"--variable-set"}, description = "Id of the variable set to be retrieved. If no id is passed, it will fetch "
                + "all the variable sets of the study.", arity = 1)
        public String variableSet;
    }

    @Parameters(commandNames = {"variable-sets-update"}, commandDescription = "Create or remove a variable set")
    public class VariableSetsUpdateCommandOptions extends BaseStudyCommand {
        @Parameter(names = {"--action"}, description = "Action to be performed: ADD or REMOVE a variable set.", arity = 1)
        public ParamUtils.BasicUpdateAction action;

        @Parameter(names = {"--variable-set"}, description = "JSON file containing the variable set to be created or removed.",
                required = true, arity = 1)
        public String variableSet;
    }

    @Parameters(commandNames = {"variable-sets-variables-update"}, commandDescription = "Add or remove variables to a variable set")
    public class VariablesUpdateCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"--action"}, description = "Action to be performed: ADD or REMOVE a variable.", arity = 1)
        public ParamUtils.BasicUpdateAction action;

        @Parameter(names = {"--variable-set"}, description = "Variable set id", required = true, arity = 1)
        public String variableSet;

        @Parameter(names = {"--variable"}, description = "JSON file containing the variable to be added or removed to the variable set.",
                required = true, arity = 1)
        public String variable;
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
