package org.opencb.opencga.app.cli.main.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser.OpencgaCommonCommandOptions;
import org.opencb.opencga.app.cli.main.options.commons.AclCommandOptions;
import org.opencb.opencga.app.cli.main.options.commons.AnnotationCommandOptions;

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

    public AclCommandOptions.AclsCommandOptions aclsCommandOptions;
    public AclCommandOptions.AclsCreateCommandOptions aclsCreateCommandOptions;
    public AclCommandOptions.AclsMemberDeleteCommandOptions aclsMemberDeleteCommandOptions;
    public AclCommandOptions.AclsMemberInfoCommandOptions aclsMemberInfoCommandOptions;
    public AclCommandOptions.AclsMemberUpdateCommandOptions aclsMemberUpdateCommandOptions;

    public AnnotationCommandOptions.AnnotationSetsCreateCommandOptions annotationCreateCommandOptions;
    public AnnotationCommandOptions.AnnotationSetsAllInfoCommandOptions annotationAllInfoCommandOptions;
    public AnnotationCommandOptions.AnnotationSetsSearchCommandOptions annotationSearchCommandOptions;
    public AnnotationCommandOptions.AnnotationSetsDeleteCommandOptions annotationDeleteCommandOptions;
    public AnnotationCommandOptions.AnnotationSetsInfoCommandOptions annotationInfoCommandOptions;
    public AnnotationCommandOptions.AnnotationSetsUpdateCommandOptions annotationUpdateCommandOptions;

    public JCommander jCommander;
    public OpencgaCommonCommandOptions commonCommandOptions;

    private AclCommandOptions aclCommandOptions;
    private AnnotationCommandOptions annotationCommandOptions;

    public IndividualCommandOptions(OpencgaCommonCommandOptions commonCommandOptions, JCommander jCommander) {

        this.commonCommandOptions = commonCommandOptions;
        this.jCommander = jCommander;

        this.createCommandOptions = new CreateCommandOptions();
        this.infoCommandOptions = new InfoCommandOptions();
        this.searchCommandOptions = new SearchCommandOptions();
        this.updateCommandOptions = new UpdateCommandOptions();
        this.deleteCommandOptions = new DeleteCommandOptions();
        this.groupByCommandOptions = new GroupByCommandOptions();

        this.annotationCommandOptions = new AnnotationCommandOptions(commonCommandOptions);
        this.annotationCreateCommandOptions = this.annotationCommandOptions.getCreateCommandOptions();
        this.annotationAllInfoCommandOptions = this.annotationCommandOptions.getAllInfoCommandOptions();
        this.annotationSearchCommandOptions = this.annotationCommandOptions.getSearchCommandOptions();
        this.annotationDeleteCommandOptions = this.annotationCommandOptions.getDeleteCommandOptions();
        this.annotationInfoCommandOptions = this.annotationCommandOptions.getInfoCommandOptions();
        this.annotationUpdateCommandOptions = this.annotationCommandOptions.getUpdateCommandOptions();

        this.aclCommandOptions = new AclCommandOptions(commonCommandOptions);
        this.aclsCommandOptions = aclCommandOptions.getAclsCommandOptions();
        this.aclsCreateCommandOptions = aclCommandOptions.getAclsCreateCommandOptions();
        this.aclsMemberDeleteCommandOptions = aclCommandOptions.getAclsMemberDeleteCommandOptions();
        this.aclsMemberInfoCommandOptions = aclCommandOptions.getAclsMemberInfoCommandOptions();
        this.aclsMemberUpdateCommandOptions = aclCommandOptions.getAclsMemberUpdateCommandOptions();
    }

    public class BaseIndividualsCommand {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--id"}, description = "Individual id", required = true, arity = 1)
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
}
