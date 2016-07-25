package org.opencb.opencga.app.cli.main.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser.OpencgaCommonCommandOptions;
import org.opencb.opencga.catalog.models.Cohort;
import org.opencb.opencga.catalog.models.Study;

/**
 * Created by sgallego on 6/14/16.
 */
@Parameters(commandNames = {"cohorts"}, commandDescription = "Cohorts commands")
public class CohortCommandOptions {

    public CreateCommandOptions createCommandOptions;
    public InfoCommandOptions infoCommandOptions;
    public SamplesCommandOptions samplesCommandOptions;
    public AnnotateCommandOptions annotateCommandOptions;
    public UpdateCommandOptions updateCommandOptions;
    public DeleteCommandOptions deleteCommandOptions;
    public StatsCommandOptions statsCommandOptions;
    public GroupByCommandOptions groupByCommandOptions;

    public AclsCommandOptions aclsCommandOptions;
    public AclsCreateCommandOptions aclsCreateCommandOptions;
    public AclsMemberDeleteCommandOptions aclsMemberDeleteCommandOptions;
    public AclsMemberInfoCommandOptions aclsMemberInfoCommandOptions;
    public AclsMemberUpdateCommandOptions aclsMemberUpdateCommandOptions;

    public AnnotationSetsAllInfoCommandOptions annotationSetsAllInfoCommandOptions;
    public AnnotationSetsSearchCommandOptions annotationSetsSearchCommandOptions;
    public AnnotationSetsInfoCommandOptions annotationSetsInfoCommandOptions;
    public AnnotationSetsDeleteCommandOptions annotationSetsDeleteCommandOptions;

    public JCommander jCommander;
    public OpencgaCommonCommandOptions commonCommandOptions;

    public CohortCommandOptions(OpencgaCommonCommandOptions commonCommandOptions, JCommander jCommander) {

        this.commonCommandOptions = commonCommandOptions;
        this.jCommander = jCommander;

        this.createCommandOptions = new CreateCommandOptions();
        this.infoCommandOptions = new InfoCommandOptions();
        this.samplesCommandOptions = new SamplesCommandOptions();
        this.annotateCommandOptions = new AnnotateCommandOptions();
        this.updateCommandOptions = new UpdateCommandOptions();
        this.deleteCommandOptions = new DeleteCommandOptions();
        this.statsCommandOptions = new StatsCommandOptions();
        this.groupByCommandOptions = new GroupByCommandOptions();

        this.annotationSetsAllInfoCommandOptions = new AnnotationSetsAllInfoCommandOptions();
        this.annotationSetsSearchCommandOptions = new AnnotationSetsSearchCommandOptions();
        this.annotationSetsInfoCommandOptions = new AnnotationSetsInfoCommandOptions();
        this.annotationSetsDeleteCommandOptions = new AnnotationSetsDeleteCommandOptions();

        this.aclsCommandOptions = new AclsCommandOptions();
        this.aclsCreateCommandOptions = new AclsCreateCommandOptions();
        this.aclsMemberDeleteCommandOptions = new AclsMemberDeleteCommandOptions();
        this.aclsMemberInfoCommandOptions = new AclsMemberInfoCommandOptions();
        this.aclsMemberUpdateCommandOptions = new AclsMemberUpdateCommandOptions();
    }

    public class BaseCohortsCommand {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--cohorts-id"}, description = "Cohorts id", required = true, arity = 1)
        public String id;
    }

    @Parameters(commandNames = {"create"}, commandDescription = "Create a cohort")
    public class CreateCommandOptions {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--study-id"}, description = "Study id", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"--name"}, description = "cohort name", required = true, arity = 1)
        public String name;

        @Parameter(names = {"--type"}, description = "Cohort type", required = false, arity = 1)
        public Study.Type type;

        @Parameter(names = {"--variable-set-id"}, description = "VariableSetId", required = false, arity = 1)
        public String variableSetId;

        @Parameter(names = {"--description"}, description = "cohort description", required = false, arity = 1)
        public String description;

        @Parameter(names = {"--sample-ids"}, description = "Sample ids for the cohort (CSV)",
                required = false, arity = 1)
        public String sampleIds;

        @Parameter(names = {"--variable"}, description = "Categorical variable name to use to create cohorts, must go together the "
                + "parameter variable-set-id",
                required = false, arity = 1)
        public String variable;
    }

    @Parameters(commandNames = {"info"}, commandDescription = "Get cohort information")
    public class InfoCommandOptions extends BaseCohortsCommand {
    }

    @Parameters(commandNames = {"samples"}, commandDescription = "List samples belonging to a cohort")
    public class SamplesCommandOptions extends BaseCohortsCommand {

        @Parameter(names = {"--limit"}, description = "Max number of results", required = false, arity = 1)
        public String limit;

        @Parameter(names = {"--skip"}, description = "Offset.", required = false, arity = 1)
        public String skip;

        @Parameter(names = {"--count"}, description = "Total number of results.", required = false, arity = 0)
        public boolean count;
    }

    @Parameters(commandNames = {"stats"},
            commandDescription = "Calculate variant stats for a set of cohorts.")
    public class StatsCommandOptions extends BaseCohortsCommand {

        @Parameter(names = {"--calculate"}, description = "Calculate cohort stats", arity = 0)
        public boolean calculate;

        @Parameter(names = {"--delete"}, description = "Delete stats [PENDING]", arity = 0)
        public boolean delete;

        @Parameter(names = {"--log"}, description = "Log level", required = false, arity = 1)
        public String log = "";

        @Parameter(names = {"-o", "--outdir-id"}, description = "Directory ID where to create the file",
                required = false, arity = 1)
        public String outdirId = "";

    }

    @Parameters(commandNames = {"annotate"}, commandDescription = "Annotate cohort")
    public class AnnotateCommandOptions extends BaseCohortsCommand {

        @Parameter(names = {"--annotate-set-name"}, description = "Annotation set name. Must be unique for the cohort",
                required = true, arity = 1)
        public String annotateSetName;

        @Parameter(names = {"--variableSetId"}, description = "VariableSetIdt", required = true, arity = 1)
        public String variableSetId;

        @Parameter(names = {"--update"}, description = "Update an already existing AnnotationSet, default: false",
                required = false, arity = 0)
        public boolean update;

        @Parameter(names = {"--delete"}, description = "Delete an AnnotationSet, default:false",
                required = false, arity = 0)
        public boolean delete;
    }

    @Parameters(commandNames = {"update"}, commandDescription = "Update cohort")
    public class UpdateCommandOptions extends BaseCohortsCommand {

        @Parameter(names = {"--name"}, description = "Cohort set name.",
                required = false, arity = 1)
        public String name;

        @Parameter(names = {"--creation-date"}, description = "Creation date", required = false, arity = 1)
        public String creationDate;

        @Parameter(names = {"--description"}, description = "Description", required = false, arity = 1)
        public String description;

        @Parameter(names = {"--samples"},
                description = "Comma separated values of sampleIds. Will replace all existing sampleIds",
                required = true, arity = 0)
        public String samples;
    }

    @Parameters(commandNames = {"delete"}, commandDescription = "Delete cohort")
    public class DeleteCommandOptions extends BaseCohortsCommand {
    }

    @Parameters(commandNames = {"group-by"}, commandDescription = "GroupBy cohort")
    public class GroupByCommandOptions {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

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

        @Parameter(names = {"--type"}, description = "Comma separated Type values.", required = false, arity = 1)
        public String type;

        @Parameter(names = {"--status"}, description = "Status.", required = false, arity = 1)
        public String status;

        @Parameter(names = {"--creation-date"}, description = "Creation date.", required = false, arity = 1)
        public String creationDate;

        @Parameter(names = {"--sample-ids"}, description = "Sample ids", required = false, arity = 1)
        public String sampleIds;

        @Parameter(names = {"-d", "--description"}, description = "Description", required = false, arity = 1)
        public String description;

        @Parameter(names = {"--attributes"}, description = "Attributes", required = false, arity = 1)
        public String attributes;

        @Parameter(names = {"--nattributes"}, description = "numerical attributes", required = false, arity = 1)
        public String nattributes;
    }

    @Parameters(commandNames = {"annotation-sets-all-info"}, commandDescription = "Annotate sample")
    public class AnnotationSetsAllInfoCommandOptions extends BaseCohortsCommand {

        @Parameter(names = {"--as-map"}, description = "As-map, default:true", required = false, arity = 0)
        public boolean asMap = true;
    }

    @Parameters(commandNames = {"annotation-sets-search"}, commandDescription = "Annotate sample")
    public class AnnotationSetsSearchCommandOptions extends BaseCohortsCommand {

        @Parameter(names = {"--annotation-set-name"}, description = "Annotation set name.", required = true, arity = 1)
        public String annotationSetName;

        @Parameter(names = {"--variableSetId"}, description = "VariableSetIdt", required = true, arity = 1)
        public String variableSetId;

        @Parameter(names = {"--annotation"}, description = "Annotation.",  required = false, arity = 1)
        public String annotation;
    }

    @Parameters(commandNames = {"annotation-sets-delete"}, commandDescription = "Annotate sample")
    public class AnnotationSetsDeleteCommandOptions extends BaseCohortsCommand {

        @Parameter(names = {"--annotation-set-name"}, description = "Annotation set name.", required = true, arity = 1)
        public String annotationSetName;

        @Parameter(names = {"--variableSetId"}, description = "VariableSetIdt", required = true, arity = 1)
        public String variableSetId;

        @Parameter(names = {"--annotation"}, description = "Annotation.",  required = false, arity = 1)
        public String annotation;
    }

    @Parameters(commandNames = {"annotation-sets-info"}, commandDescription = "Annotate sample")
    public class AnnotationSetsInfoCommandOptions extends BaseCohortsCommand {

        @Parameter(names = {"--annotation-set-name"}, description = "Annotation set name.",
                required = true, arity = 1)
        public String annotationSetName;

        @Parameter(names = {"--variableSetId"}, description = "VariableSetIdt", required = true, arity = 1)
        public String variableSetId;

        @Parameter(names = {"--annotation"}, description = "Annotation.",  required = false, arity = 1)
        public String annotation;

        @Parameter(names = {"--as-map"}, description = "As-map, default:true", required = false, arity = 0)
        public boolean asMap = true;
    }

    @Parameters(commandNames = {"acl"}, commandDescription = "Return the acl of the cohort [PENDING]")
    public class AclsCommandOptions extends BaseCohortsCommand {
    }

    @Parameters(commandNames = {"acl-create"}, commandDescription = "Define a set of permissions for a list of users or groups [PENDING]")
    public class AclsCreateCommandOptions extends BaseCohortsCommand {

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
    public class AclsMemberDeleteCommandOptions extends BaseCohortsCommand {

        @Parameter(names = {"--member-id"}, description = "Member id", required = true, arity = 1)
        public String memberId;
    }

    @Parameters(commandNames = {"acl-member-info"},
            commandDescription = "Return the set of permissions granted for the user or group [PENDING]")
    public class AclsMemberInfoCommandOptions extends BaseCohortsCommand {

        @Parameter(names = {"--member-id"}, description = "Member id", required = true, arity = 1)
        public String memberId;
    }

    @Parameters(commandNames = {"acl-member-update"},
            commandDescription = "Update the set of permissions granted for the user or group [PENDING]")
    public class AclsMemberUpdateCommandOptions extends BaseCohortsCommand {

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
