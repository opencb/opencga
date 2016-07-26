package org.opencb.opencga.app.cli.main.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser.OpencgaCommonCommandOptions;
import org.opencb.opencga.app.cli.main.options.commons.AclCommandOptions;

/**
 * Created by sgallego on 6/14/16.
 */
@Parameters(commandNames = {"samples"}, commandDescription = "Samples commands")
public class SampleCommandOptions {

    public CreateCommandOptions createCommandOptions;
    public LoadCommandOptions loadCommandOptions;
    public InfoCommandOptions infoCommandOptions;
    public SearchCommandOptions searchCommandOptions;
    public UpdateCommandOptions updateCommandOptions;
    public DeleteCommandOptions deleteCommandOptions;
    public GroupByCommandOptions groupByCommandOptions;

    public AnnotationSetsAllInfoCommandOptions annotationSetsAllInfoCommandOptions;
    public AnnotationSetsSearchCommandOptions annotationSetsSearchCommandOptions;
    public AnnotationSetsInfoCommandOptions annotationSetsInfoCommandOptions;
    public AnnotationSetsDeleteCommandOptions annotationSetsDeleteCommandOptions;

    public AclCommandOptions.AclsCommandOptions aclsCommandOptions;
    public AclCommandOptions.AclsCreateCommandOptions aclsCreateCommandOptions;
    public AclCommandOptions.AclsMemberDeleteCommandOptions aclsMemberDeleteCommandOptions;
    public AclCommandOptions.AclsMemberInfoCommandOptions aclsMemberInfoCommandOptions;
    public AclCommandOptions.AclsMemberUpdateCommandOptions aclsMemberUpdateCommandOptions;


    public JCommander jCommander;
    public OpencgaCommonCommandOptions commonCommandOptions;

    AclCommandOptions aclCommandOptions;

    public SampleCommandOptions(OpencgaCommonCommandOptions commonCommandOptions, JCommander jCommander) {
        this.commonCommandOptions = commonCommandOptions;
        this.jCommander = jCommander;

        this.createCommandOptions = new CreateCommandOptions();
        this.loadCommandOptions = new LoadCommandOptions();
        this.infoCommandOptions = new InfoCommandOptions();
        this.searchCommandOptions = new SearchCommandOptions();
        this.updateCommandOptions = new UpdateCommandOptions();
        this.deleteCommandOptions = new DeleteCommandOptions();
        this.groupByCommandOptions = new GroupByCommandOptions();

        this.annotationSetsAllInfoCommandOptions = new AnnotationSetsAllInfoCommandOptions();
        this.annotationSetsSearchCommandOptions = new AnnotationSetsSearchCommandOptions();
        this.annotationSetsInfoCommandOptions = new AnnotationSetsInfoCommandOptions();
        this.annotationSetsDeleteCommandOptions = new AnnotationSetsDeleteCommandOptions();

        aclCommandOptions = new AclCommandOptions(commonCommandOptions);
//        this.aclsCommandOptions = new AclsCommandOptions();
//        this.aclsCreateCommandOptions = new AclsCreateCommandOptions();
//        this.aclsMemberDeleteCommandOptions = new AclsMemberDeleteCommandOptions();
//        this.aclsMemberInfoCommandOptions = new AclsMemberInfoCommandOptions();
//        this.aclsMemberUpdateCommandOptions = new AclsMemberUpdateCommandOptions();
        this.aclsCommandOptions = aclCommandOptions.getAclsCommandOptions();
        this.aclsCreateCommandOptions = aclCommandOptions.getAclsCreateCommandOptions();
        this.aclsMemberDeleteCommandOptions = aclCommandOptions.getAclsMemberDeleteCommandOptions();
        this.aclsMemberInfoCommandOptions = aclCommandOptions.getAclsMemberInfoCommandOptions();
        this.aclsMemberUpdateCommandOptions = aclCommandOptions.getAclsMemberUpdateCommandOptions();
    }

    public class BaseSampleCommand {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--sample-id"}, description = "Sample id", required = true, arity = 1)
        public String id;
    }

    @Parameters(commandNames = {"create"}, commandDescription = "Create a sample")
    public class CreateCommandOptions {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--study-id"}, description = "Study id", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"--name"}, description = "Sample name", required = true, arity = 1)
        public String name;

        @Parameter(names = {"--source"}, description = "Source", required = false, arity = 1)
        public String source;

        @Parameter(names = {"--description"}, description = "Sample description", required = false, arity = 1)
        public String description;
    }

    @Parameters(commandNames = {"load"}, commandDescription = "Load samples from a pedigree file")
    public class LoadCommandOptions {

        @Parameter(names = {"--study-id"}, description = "Study id", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"--file-id"}, description = "File id already loaded in OpenCGA", required = true, arity = 1)
        public String fileId;

        @Parameter(names = {"--variable-set-id"}, description = "VariableSetId that represents the pedigree file", arity = 1)
        public String variableSetId;
    }

    @Parameters(commandNames = {"info"}, commandDescription = "Get samples information")
    public class InfoCommandOptions extends BaseSampleCommand {
    }

    @Parameters(commandNames = {"search"}, commandDescription = "Search samples")
    public class SearchCommandOptions {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--study-id"}, description = "Study id", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"--id"}, description = "Comma separated list of ids.", required = false, arity = 1)
        public String id;

        @Parameter(names = {"--name"}, description = "Comma separated list of names.", required = false, arity = 1)
        public String name;

        @Parameter(names = {"--source"}, description = "Source.", required = false, arity = 1)
        public String source;

        @Parameter(names = {"--indivual-id"}, description = "Indivudual id.", required = false, arity = 1)
        public String individualId;

        @Parameter(names = {"--annotation-set-name"}, description = "Annotation set name.", required = false, arity = 1)
        public String annotationSetName;

        @Parameter(names = {"--variable-set-id"}, description = "Variable set id.", required = false, arity = 1)
        public String variableSetId;

        @Parameter(names = {"--annotation"}, description = "Annotation.", required = false, arity = 1)
        public String annotation;

        @Parameter(names = {"--limit"}, description = "Max number of results", required = false, arity = 1)
        public String limit;

        @Parameter(names = {"--skip"}, description = "Offset.", required = false, arity = 1)
        public String skip;

        @Parameter(names = {"--count"}, description = "Total number of results.", required = false, arity = 0)
        public boolean count;

    /*    @Parameter(names = {"--variable-set-id"}, description = "VariableSetId", required = false, arity = 1)
        String variableSetId;

        @Parameter(names = {"--name"}, description = "Sample names (CSV)", required = false, arity = 1)
        String sampleNames;

        @Parameter(names = {"-id", "--sample-id"}, description = "Sample ids (CSV)", required = false, arity = 1)
        String sampleIds;

        @Parameter(names = {"-a", "--annotation"},
                description = "SampleAnnotations values. <variableName>:<annotationValue>(,<annotationValue>)*",
                required = false, arity = 1, splitter = SemiColonParameterSplitter.class)
        List<String> annotation;*/
    }


    @Parameters(commandNames = {"update"}, commandDescription = "Update cohort")
    public class UpdateCommandOptions extends BaseSampleCommand {

        @Parameter(names = {"--name"}, description = "Cohort set name.", required = false, arity = 1)
        public String name;

        @Parameter(names = {"--source"}, description = "Source", required = true, arity = 1)
        public String source;

        @Parameter(names = {"--description"}, description = "Description", required = true, arity = 0)
        public String description;

        @Parameter(names = {"--individual-id"}, description = "Indivudual id", required = true, arity = 0)
        public String individualId;

    }


    @Parameters(commandNames = {"group-by"}, commandDescription = "GroupBy cohort")
    public class GroupByCommandOptions {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--by"}, description = "Comma separated list of fields by which to group by.", required = true, arity = 1)
        public String by;

        @Parameter(names = {"--study-id"}, description = "Study id", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"--id"}, description = "Comma separated list of ids.", required = false, arity = 1)
        public String id;

        @Parameter(names = {"--name"}, description = "Comma separated list of names.", required = false, arity = 0)
        public String name;

        @Parameter(names = {"--source"}, description = "Source.", required = false, arity = 0)
        public String source;

        @Parameter(names = {"--individual-id"}, description = "Individual id.", required = false, arity = 0)
        public String individualId;

        @Parameter(names = {"--annotation-set-name"}, description = "Annotation set name.", required = false, arity = 0)
        public String annotationSetName;

        @Parameter(names = {"--variable-set-id"}, description = "Variable set ids", required = false, arity = 1)
        public String variableSetId;

        @Parameter(names = {"--annotation"}, description = "Annotation", required = false, arity = 1)
        public String annotation;
    }

    @Parameters(commandNames = {"delete"}, commandDescription = "Deletes the selected sample")
    public class DeleteCommandOptions extends BaseSampleCommand {
    }

    @Parameters(commandNames = {"annotation-sets-all-info"}, commandDescription = "Annotate sample")
    public class AnnotationSetsAllInfoCommandOptions extends BaseSampleCommand {

        @Parameter(names = {"--as-map"}, description = "As-map, default:true", required = false, arity = 0)
        public boolean asMap = true;
    }

    @Parameters(commandNames = {"annotation-sets-search"}, commandDescription = "Annotate sample")
    public class AnnotationSetsSearchCommandOptions extends BaseSampleCommand {

        @Parameter(names = {"--annotation-set-name"}, description = "Annotation set name.",
                required = true, arity = 1)
        public String annotationSetName;

        @Parameter(names = {"--variableSetId"}, description = "VariableSetIdt", required = true, arity = 1)
        public String variableSetId;

        @Parameter(names = {"--annotation"}, description = "Annotation.",  required = false, arity = 1)
        public String annotation;
    }

    @Parameters(commandNames = {"annotation-sets-delete"}, commandDescription = "Annotate sample")
    public class AnnotationSetsDeleteCommandOptions extends BaseSampleCommand {

        @Parameter(names = {"--annotation-set-name"}, description = "Annotation set name.",
                required = true, arity = 1)
        public String annotationSetName;

        @Parameter(names = {"--variableSetId"}, description = "VariableSetIdt", required = true, arity = 1)
        public String variableSetId;

        @Parameter(names = {"--annotation"}, description = "Annotation.",  required = false, arity = 1)
        public String annotation;
    }

    @Parameters(commandNames = {"annotation-sets-info"}, commandDescription = "Annotate sample")
    public class AnnotationSetsInfoCommandOptions extends BaseSampleCommand {

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



//    @Parameters(commandNames = {"acl"}, commandDescription = "Return the acl of the sample [PENDING]")
//    public class AclsCommandOptions extends BaseSampleCommand {
//    }
//
//    @Parameters(commandNames = {"acl-create"}, commandDescription = "Define a set of permissions for a list of users or groups [PENDING]")
//    public class AclsCreateCommandOptions extends BaseSampleCommand {
//
//        @Parameter(names = {"--members"},
//                description = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'", required = true, arity = 1)
//        public String members;
//
//        @Parameter(names = {"--permissions"}, description = "Comma separated list of cohort permissions", required = true, arity = 1)
//        public String permissions;
//
//        @Parameter(names = {"--template-id"}, description = "Template of permissions to be used (admin, analyst or locked)",
//                required = false, arity = 1)
//        public String templateId;
//    }
//
//    @Parameters(commandNames = {"acl-member-delete"},
//            commandDescription = "Delete all the permissions granted for the user or group [PENDING]")
//    public class AclsMemberDeleteCommandOptions extends BaseSampleCommand {
//
//        @Parameter(names = {"--member-id"}, description = "Member id", required = true, arity = 1)
//        public String memberId;
//    }
//
//    @Parameters(commandNames = {"acl-member-info"},
//            commandDescription = "Return the set of permissions granted for the user or group [PENDING]")
//    public class AclsMemberInfoCommandOptions extends BaseSampleCommand {
//
//        @Parameter(names = {"--member-id"}, description = "Member id", required = true, arity = 1)
//        public String memberId;
//    }
//
//    @Parameters(commandNames = {"acl-member-update"},
//            commandDescription = "Update the set of permissions granted for the user or group [PENDING]")
//    public class AclsMemberUpdateCommandOptions extends BaseSampleCommand {
//
//        @Parameter(names = {"--member-id"}, description = "Member id", required = true, arity = 1)
//        public String memberId;
//
//        @Parameter(names = {"--add-permissions"}, description = "Comma separated list of permissions to add", required = false, arity = 1)
//        public String addPermissions;
//
//        @Parameter(names = {"--remove-permissions"}, description = "Comma separated list of permissions to remove",
//                required = false, arity = 1)
//        public String removePermissions;
//
//        @Parameter(names = {"--set-permissions"}, description = "Comma separated list of permissions to set", required = false, arity = 1)
//        public String setPermissions;
//    }
}
