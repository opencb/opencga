package org.opencb.opencga.app.cli.main.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser.OpencgaCommonCommandOptions;

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

    public AclsCommandOptions aclsCommandOptions;
    public AclsCreateCommandOptions aclsCreateCommandOptions;
    public AclsMemberDeleteCommandOptions aclsMemberDeleteCommandOptions;
    public AclsMemberInfoCommandOptions aclsMemberInfoCommandOptions;
    public AclsMemberUpdateCommandOptions aclsMemberUpdateCommandOptions;


    public JCommander jCommander;
    public OpencgaCommonCommandOptions commonCommandOptions;

    public JobCommandOptions(OpencgaCommonCommandOptions commonCommandOptions, JCommander jCommander) {

        this.commonCommandOptions = commonCommandOptions;
        this.jCommander = jCommander;

        this.createCommandOptions = new CreateCommandOptions();
        this.infoCommandOptions = new InfoCommandOptions();
        this.searchCommandOptions = new SearchCommandOptions();
        this.visitCommandOptions = new VisitCommandOptions();
        this.deleteCommandOptions = new DeleteCommandOptions();
        this.groupByCommandOptions = new GroupByCommandOptions();

        this.aclsCommandOptions = new AclsCommandOptions();
        this.aclsCreateCommandOptions = new AclsCreateCommandOptions();
        this.aclsMemberDeleteCommandOptions = new AclsMemberDeleteCommandOptions();
        this.aclsMemberInfoCommandOptions = new AclsMemberInfoCommandOptions();
        this.aclsMemberUpdateCommandOptions = new AclsMemberUpdateCommandOptions();
    }

    public class BaseJobCommand {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--job-id"}, description = "Job id", required = true, arity = 1)
        public String id;
    }

    @Parameters(commandNames = {"create"}, commandDescription = "Create a cohort")
    public class CreateCommandOptions {

        @ParametersDelegate
        OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--study-id"}, description = "Study id", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"--name"}, description = "Job name", required = true, arity = 1)
        public String name;

        @Parameter(names = {"--tool-id"}, description = "Tool Id", required = true, arity = 1)
        public String toolId;

        @Parameter(names = {"--execution"}, description = "Execution", required = false, arity = 1)
        public String execution;

        @Parameter(names = {"--description"}, description = "Job description", required = false, arity = 1)
        public String description;
    }

    @Parameters(commandNames = {"info"}, commandDescription = "Get job information")
    public class InfoCommandOptions extends BaseJobCommand {
    }


    @Parameters(commandNames = {"search"}, commandDescription = "Search job")
    public class SearchCommandOptions {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--study-id"}, description = "Study id", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"--job-id"}, description = "Job id", required = false, arity = 1)
        public String jobId;

        @Parameter(names = {"--name"}, description = "Comma separated list of names.", required = false, arity = 1)
        public String name;

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

        @Parameter(names = {"--limit"}, description = "Max number of results", required = false, arity = 1)
        public String limit;

        @Parameter(names = {"--skip"}, description = "Offset.", required = false, arity = 1)
        public String skip;
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
    public class GroupByCommandOptions {

        @ParametersDelegate
        OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--by"},
                description = "Comma separated list of fields by which to group by.",
                required = true, arity = 1)
        public String by;

        @Parameter(names = {"--study-id"}, description = "Study id", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"--id"}, description = "Comma separated list of ids.",
                required = false, arity = 1)
        public String id;

        @Parameter(names = {"--name"}, description = "Comma separated list of names.", required = false, arity = 1)
        public String name;

        @Parameter(names = {"--path"}, description = "Path.", required = false, arity = 1)
        public String path;

        @Parameter(names = {"--status"}, description = "Status.", required = false, arity = 1)
        public String status;

        @Parameter(names = {"--owner-id"}, description = "Owner id.", required = false, arity = 1)
        public String ownerId;

        @Parameter(names = {"--creation-date"}, description = "Creation date.", required = false, arity = 1)
        public String creationDate;

        @Parameter(names = {"-d", "--description"}, description = "Description", required = false, arity = 1)
        public String description;

        @Parameter(names = {"--attributes"}, description = "Attributes", required = false, arity = 1)
        public String attributes;
    }


    @Parameters(commandNames = {"acl"}, commandDescription = "Return the acl of the job [PENDING]")
    public class AclsCommandOptions extends BaseJobCommand {
    }

    @Parameters(commandNames = {"acl-create"}, commandDescription = "Define a set of permissions for a list of users or groups [PENDING]")
    public class AclsCreateCommandOptions extends BaseJobCommand {

        @Parameter(names = {"--members"},
                description = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'", required = true, arity = 1)
        public String members;

        @Parameter(names = {"--permissions"}, description = "Comma separated list of cohort permissions", required = true, arity = 1)
        public String permissions;

        @Parameter(names = {"--template-id"}, description = "Template of permissions to be used (admin, analyst or locked)",
                required = false, arity = 1)
        public String templateId;
    }

    @Parameters(commandNames = {"acl-member-delete"},
            commandDescription = "Delete all the permissions granted for the user or group [PENDING]")
    public class AclsMemberDeleteCommandOptions extends BaseJobCommand {

        @Parameter(names = {"--member-id"}, description = "Member id", required = true, arity = 1)
        public String memberId;
    }

    @Parameters(commandNames = {"acl-member-info"},
            commandDescription = "Return the set of permissions granted for the user or group [PENDING]")
    public class AclsMemberInfoCommandOptions extends BaseJobCommand {

        @Parameter(names = {"--member-id"}, description = "Member id", required = true, arity = 1)
        public String memberId;
    }

    @Parameters(commandNames = {"acl-member-update"},
            commandDescription = "Update the set of permissions granted for the user or group [PENDING]")
    public class AclsMemberUpdateCommandOptions extends BaseJobCommand {

        @Parameter(names = {"--member-id"}, description = "Member id", required = true, arity = 1)
        public String memberId;

        @Parameter(names = {"--add-permissions"}, description = "Comma separated list of permissions to add", required = false, arity = 1)
        public String addPermissions;

        @Parameter(names = {"--remove-permissions"}, description = "Comma separated list of permissions to remove",
                required = false, arity = 1)
        public String removePermissions;

        @Parameter(names = {"--set-permissions"}, description = "Comma separated list of permissions to set", required = false, arity = 1)
        public String setPermissions;
    }

}
