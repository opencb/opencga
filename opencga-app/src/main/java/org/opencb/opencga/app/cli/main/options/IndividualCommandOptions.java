package org.opencb.opencga.app.cli.main.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser.OpencgaCommonCommandOptions;

/**
 * Created by sgallego on 6/14/16.
 */
@Parameters(commandNames = {"individuals"}, commandDescription = "Individuals commands")
public class IndividualCommandOptions {


    public CreateCommandOptions createCommandOptions;
    public InfoCommandOptions infoCommandOptions;
    public SearchCommandOptions searchCommandOptions;
    public UpdateCommandOptions updateCommandOptions;
    public DeleteCommandOptions deleteCommandOptions;
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

    public IndividualCommandOptions(OpencgaCommonCommandOptions commonCommandOptions, JCommander jCommander) {

        this.commonCommandOptions = commonCommandOptions;
        this.jCommander = jCommander;

        this.createCommandOptions = new CreateCommandOptions();
        this.infoCommandOptions = new InfoCommandOptions();
        this.searchCommandOptions = new SearchCommandOptions();
        this.updateCommandOptions = new UpdateCommandOptions();
        this.deleteCommandOptions = new DeleteCommandOptions();
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

    public class BaseIndividualsCommand {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--individual-id"}, description = "Individual id", required = true, arity = 1)
        public String id;
    }


    @Parameters(commandNames = {"create"}, commandDescription = "Create sample.")
    public class CreateCommandOptions {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-s", "--study-id"}, description = "StudyId", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"-n", "--name"}, description = "Name", required = true, arity = 1)
        public String name;

        @Parameter(names = {"--family"}, description = "Family", required = false, arity = 1)
        public String family = "";

        @Parameter(names = {"--father-id"}, description = "FatherId", required = false, arity = 1)
        public String fatherId;

        @Parameter(names = {"--mother-id"}, description = "MotherId", required = false, arity = 1)
        public String motherId;

        @Parameter(names = {"--gender"}, description = "Gender", required = false)
        public String gender;
    }


    @Parameters(commandNames = {"info"}, commandDescription = "Get individual information")
    public class InfoCommandOptions extends BaseIndividualsCommand {
    }

    @Parameters(commandNames = {"search"}, commandDescription = "Search for individuals")
    public class SearchCommandOptions {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-s", "--study-id"}, description = "studyId", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"--individual-id"}, description = "Id", required = false, arity = 1)
        public String id;

        @Parameter(names = {"--name"}, description = "name", required = false, arity = 1)
        public String name;

        @Parameter(names = {"--father-id"}, description = "fatherId", required = false, arity = 1)
        public String fatherId;

        @Parameter(names = {"--mother-id"}, description = "motherId", required = false, arity = 1)
        public String motherId;

        @Parameter(names = {"--family"}, description = "family", required = false, arity = 1)
        public String family;

        @Parameter(names = {"--gender"}, description = "gender", required = false, arity = 1)
        public String gender;

        @Parameter(names = {"--race"}, description = "race", required = false, arity = 1)
        public String race;

        @Parameter(names = {"--species"}, description = "species", required = false, arity = 1)
        public String species;

        @Parameter(names = {"--population"}, description = "population", required = false, arity = 1)
        public String population;

        @Parameter(names = {"--variable-set-id"}, description = "variableSetId", required = false, arity = 1)
        public String variableSetId;

        @Parameter(names = {"--annotation-set-name"}, description = "Annotation set name.", required = true, arity = 1)
        public String annotationSetName;

        @Parameter(names = {"--annotation"}, description = "annotation", required = false, arity = 1)
        public String annotation;

        @Parameter(names = {"--limit"}, description = "Max number of results", required = false, arity = 1)
        public String limit;

        @Parameter(names = {"--skip"}, description = "Offset.", required = false, arity = 1)
        public String skip;

        @Parameter(names = {"--count"}, description = "Total number of results.", required = false, arity = 0)
        public boolean count;
    }

   /* @Parameters(commandNames = {"annotate"}, commandDescription = "Annotate an individual")
    public class AnnotateCommandOptions extends BaseIndividualsCommand {

        @Parameter(names = {"--annotateSetName"}, description = "Annotation set name. Must be unique",
                required = true, arity = 1)
        public String annotateSetName;

        @Parameter(names = {"--variable-set-id"}, description = "variableSetId", required = false, arity = 1)
        public Integer id;

        @Parameter(names = {"--update"}, description = "Update an already existing AnnotationSet", required = false,
                arity = 0)
        public boolean update;

        @Parameter(names = {"--delete"}, description = "Delete an AnnotationSet", required = false, arity = 0)
        public boolean delete;
    }*/

    @Parameters(commandNames = {"update"}, commandDescription = "Update individual information")
    public class UpdateCommandOptions extends BaseIndividualsCommand {

//            @Parameter(names = {"-id", "--individual-id"}, description = "Id", required = false, arity = 1)
//            String id;

        @Parameter(names = {"--name"}, description = "name", required = false, arity = 1)
        public String name;

        @Parameter(names = {"--father-id"}, description = "FatherId", required = false, arity = 1)
        public String fatherId;

        @Parameter(names = {"--mother-id"}, description = "MotherId", required = false, arity = 1)
        public String motherId;

        @Parameter(names = {"--family"}, description = "Family", required = false, arity = 1)
        public String family;

        @Parameter(names = {"--gender"}, description = "Gender", required = false)
        public String gender;

        @Parameter(names = {"--race"}, description = "race", required = false, arity = 1)
        public String race;

    }

    @Parameters(commandNames = {"delete"}, commandDescription = "Delete individual information")
    public class DeleteCommandOptions extends BaseIndividualsCommand {
    }


    @Parameters(commandNames = {"group-by"}, commandDescription = "GroupBy cohort")
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

        @Parameter(names = {"--name"}, description = "Comma separated list of names.", required = false, arity = 0)
        public String name;

        @Parameter(names = {"--father-id"}, description = "FatherId", required = false, arity = 1)
        public String fatherId;

        @Parameter(names = {"--mother-id"}, description = "MotherId", required = false, arity = 1)
        public String motherId;

        @Parameter(names = {"--family"}, description = "Family", required = false, arity = 1)
        public String family;

        @Parameter(names = {"--gender"}, description = "Gender", required = false)
        public String gender;

        @Parameter(names = {"--race"}, description = "race", required = false, arity = 1)
        public String race;

        @Parameter(names = {"--species"}, description = "species", required = false, arity = 1)
        public String species;

        @Parameter(names = {"--population"}, description = "population", required = false, arity = 1)
        public String population;

        @Parameter(names = {"--variable-set-id"}, description = "Variable set ids", required = false, arity = 1)
        public String variableSetId;

        @Parameter(names = {"--annotation-set-name"}, description = "Annotation set name.", required = false, arity = 0)
        public String annotationSetName;

        @Parameter(names = {"--annotation"}, description = "Annotation", required = false, arity = 1)
        public String annotation;
    }

    @Parameters(commandNames = {"annotation-sets-all-info"}, commandDescription = "Annotate sample")
    public class AnnotationSetsAllInfoCommandOptions extends BaseIndividualsCommand {

        @Parameter(names = {"--as-map"}, description = "As-map, default:true", required = false, arity = 0)
        public boolean asMap = true;
    }

    @Parameters(commandNames = {"annotation-sets-search"}, commandDescription = "Annotate sample")
    public class AnnotationSetsSearchCommandOptions extends BaseIndividualsCommand {

        @Parameter(names = {"--annotation-set-name"}, description = "Annotation set name.",
                required = true, arity = 1)
        public String annotationSetName;

        @Parameter(names = {"--variableSetId"}, description = "VariableSetIdt", required = true, arity = 1)
        public String variableSetId;

        @Parameter(names = {"--annotation"}, description = "Annotation.",  required = false, arity = 1)
        public String annotation;
    }

    @Parameters(commandNames = {"annotation-sets-delete"}, commandDescription = "Annotate sample")
    public class AnnotationSetsDeleteCommandOptions extends BaseIndividualsCommand {

        @Parameter(names = {"--annotation-set-name"}, description = "Annotation set name.",
                required = true, arity = 1)
        public String annotationSetName;

        @Parameter(names = {"--variableSetId"}, description = "VariableSetIdt", required = true, arity = 1)
        public String variableSetId;

        @Parameter(names = {"--annotation"}, description = "Annotation.",  required = false, arity = 1)
        public String annotation;
    }

    @Parameters(commandNames = {"annotation-sets-info"}, commandDescription = "Annotate sample")
    public class AnnotationSetsInfoCommandOptions extends BaseIndividualsCommand {

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

    @Parameters(commandNames = {"acl"}, commandDescription = "Return the acl of the individual [PENDING]")
    public class AclsCommandOptions extends BaseIndividualsCommand {
    }

    @Parameters(commandNames = {"acl-create"}, commandDescription = "Define a set of permissions for a list of users or groups [PENDING]")
    public class AclsCreateCommandOptions extends BaseIndividualsCommand {

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
    public class AclsMemberDeleteCommandOptions extends BaseIndividualsCommand {

        @Parameter(names = {"--member-id"}, description = "Member id", required = true, arity = 1)
        public String memberId;
    }

    @Parameters(commandNames = {"acl-member-info"},
            commandDescription = "Return the set of permissions granted for the user or group [PENDING]")
    public class AclsMemberInfoCommandOptions extends BaseIndividualsCommand {

        @Parameter(names = {"--member-id"}, description = "Member id", required = true, arity = 1)
        public String memberId;
    }

    @Parameters(commandNames = {"acl-member-update"},
            commandDescription = "Update the set of permissions granted for the user or group [PENDING]")
    public class AclsMemberUpdateCommandOptions extends BaseIndividualsCommand {

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
