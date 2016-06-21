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
    public AnnotateCommandOptions annotateCommandOptions;
    public UpdateCommandOptions updateCommandOptions;
    public DeleteCommandOptions deleteCommandOptions;
    public ShareCommandOptions shareCommandOptions;
    public UnshareCommandOptions unshareCommandOptions;
    public GroupByCommandOptions groupByCommandOptions;


    public JCommander jCommander;
    public OpencgaCommonCommandOptions commonCommandOptions;

    public IndividualCommandOptions(OpencgaCommonCommandOptions commonCommandOptions, JCommander jCommander) {
        this.commonCommandOptions = commonCommandOptions;
        this.jCommander = jCommander;

        this.createCommandOptions = new CreateCommandOptions();
        this.infoCommandOptions = new InfoCommandOptions();
        this.searchCommandOptions = new SearchCommandOptions();
        this.annotateCommandOptions = new AnnotateCommandOptions();
        this.updateCommandOptions = new UpdateCommandOptions();
        this.deleteCommandOptions = new DeleteCommandOptions();
        this.shareCommandOptions = new ShareCommandOptions();
        this.unshareCommandOptions = new UnshareCommandOptions();
        this.groupByCommandOptions = new GroupByCommandOptions();

    }

    class BaseIndividualsCommand {
        @ParametersDelegate
        OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--individual-id"}, description = "Individual id", required = true, arity = 1)
        Integer id;
    }


    @Parameters(commandNames = {"create"}, commandDescription = "Create sample.")
    class CreateCommandOptions {

        @ParametersDelegate
        OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-s", "--study-id"}, description = "StudyId", required = true, arity = 1)
        String studyId;

        @Parameter(names = {"-n", "--name"}, description = "Name", required = true, arity = 1)
        String name;

        @Parameter(names = {"--family"}, description = "Family", required = false, arity = 1)
        String family = "";

        @Parameter(names = {"--fatherId"}, description = "FatherId", required = false, arity = 1)
        Integer fatherId;

        @Parameter(names = {"--motherId"}, description = "MotherId", required = false, arity = 1)
        Integer motherId;

        @Parameter(names = {"--gender"}, description = "Gender", required = false)
        String gender;
    }


    @Parameters(commandNames = {"info"}, commandDescription = "Get individual information")
    class InfoCommandOptions extends BaseIndividualsCommand {
    }

    @Parameters(commandNames = {"search"}, commandDescription = "Search for individuals")
    class SearchCommandOptions {

        @ParametersDelegate
        OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-s", "--study-id"}, description = "studyId", required = true, arity = 1)
        String studyId;

        @Parameter(names = {"--individual-id"}, description = "Id", required = false, arity = 1)
        String id;

        @Parameter(names = {"--name"}, description = "name", required = false, arity = 1)
        String name;

        @Parameter(names = {"--fatherId"}, description = "fatherId", required = false, arity = 1)
        String fatherId;

        @Parameter(names = {"--motherId"}, description = "motherId", required = false, arity = 1)
        String motherId;

        @Parameter(names = {"--family"}, description = "family", required = false, arity = 1)
        String family;

        @Parameter(names = {"--gender"}, description = "gender", required = false, arity = 1)
        String gender;

        @Parameter(names = {"--race"}, description = "race", required = false, arity = 1)
        String race;

        @Parameter(names = {"--species"}, description = "species", required = false, arity = 1)
        String species;

        @Parameter(names = {"--population"}, description = "population", required = false, arity = 1)
        String population;

        @Parameter(names = {"--variableSetId"}, description = "variableSetId", required = false, arity = 1)
        Integer variableSetId;

        @Parameter(names = {"--annotationSetId"}, description = "annotationSetId", required = false, arity = 1)
        String annotationSetId;

        @Parameter(names = {"--annotation"}, description = "annotation", required = false, arity = 1)
        String annotation;
    }

    @Parameters(commandNames = {"annotate"}, commandDescription = "Annotate an individual")
    class AnnotateCommandOptions extends BaseIndividualsCommand {
        @Parameter(names = {"--annotateSetName"}, description = "Annotation set name. Must be unique",
                required = true, arity = 1)
        String annotateSetName;

        @Parameter(names = {"--variableSetId"}, description = "variableSetId", required = false, arity = 1)
        Integer id;

        @Parameter(names = {"--update"}, description = "Update an already existing AnnotationSet", required = false,
                arity = 0)
        boolean update;

        @Parameter(names = {"--delete"}, description = "Delete an AnnotationSet", required = false, arity = 0)
        boolean delete;
    }

    @Parameters(commandNames = {"update"}, commandDescription = "Update individual information")
    class UpdateCommandOptions extends BaseIndividualsCommand {

//            @Parameter(names = {"-id", "--individual-id"}, description = "Id", required = false, arity = 1)
//            String id;

        @Parameter(names = {"--name"}, description = "name", required = false, arity = 1)
        String name;

        @Parameter(names = {"--fatherId"}, description = "FatherId", required = false, arity = 1)
        Integer fatherId;

        @Parameter(names = {"--motherId"}, description = "MotherId", required = false, arity = 1)
        Integer motherId;

        @Parameter(names = {"--family"}, description = "Family", required = false, arity = 1)
        String family;

        @Parameter(names = {"--gender"}, description = "Gender", required = false)
        String gender;

        @Parameter(names = {"--race"}, description = "race", required = false, arity = 1)
        String race;

    }

    @Parameters(commandNames = {"delete"}, commandDescription = "Delete individual information")
    class DeleteCommandOptions extends BaseIndividualsCommand {
    }


    @Parameters(commandNames = {"share"}, commandDescription = "Share cohort")
    public class ShareCommandOptions{
        @ParametersDelegate
        OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--individual-ids"}, description = "Individuals ids", required = true, arity = 1)
        String individualIds;

        @Parameter(names = {"--members"},
                description = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'",
                required = true, arity = 1)
        String members;

        @Parameter(names = {"--permission"}, description = "Comma separated list of cohort permissions",
                required = false, arity = 1)
        String permission;

        @Parameter(names = {"--override"}, description = "Boolean indicating whether to allow the change" +
                " of permissions in case any member already had any, default:false", required = false, arity = 0)
        boolean override;
    }

    @Parameters(commandNames = {"unshare"}, commandDescription = "Share cohort")
    public class UnshareCommandOptions {
        @ParametersDelegate
        OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--individual-ids"}, description = "Individuals ids", required = true, arity = 1)
        String individualIds;

        @Parameter(names = {"--members"},
                description = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'",
                required = true, arity = 1)
        String members;

        @Parameter(names = {"--permission"}, description = "Comma separated list of cohort permissions",
                required = false, arity = 1)
        String permission;

    }

    @Parameters(commandNames = {"group-by"}, commandDescription = "GroupBy cohort")
    public class GroupByCommandOptions {
        @ParametersDelegate
        OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--by"},
                description = "Comma separated list of fields by which to group by.",
                required = true, arity = 1)
        String by;

        @Parameter(names = {"--study-id"}, description = "Study id", required = true, arity = 1)
        String studyId;


        @Parameter(names = {"--id"}, description = "Comma separated list of ids.",
                required = false, arity = 1)
        String id;

        @Parameter(names = {"--name"}, description = "Comma separated list of names.", required = false, arity = 0)
        String name;

        @Parameter(names = {"--fatherId"}, description = "FatherId", required = false, arity = 1)
        Integer fatherId;

        @Parameter(names = {"--motherId"}, description = "MotherId", required = false, arity = 1)
        Integer motherId;

        @Parameter(names = {"--family"}, description = "Family", required = false, arity = 1)
        String family;

        @Parameter(names = {"--gender"}, description = "Gender", required = false)
        String gender;

        @Parameter(names = {"--race"}, description = "race", required = false, arity = 1)
        String race;

        @Parameter(names = {"--species"}, description = "species", required = false, arity = 1)
        String species;

        @Parameter(names = {"--population"}, description = "population", required = false, arity = 1)
        String population;

        @Parameter(names = {"--variable-set-id"}, description = "Variable set ids", required = false, arity = 1)
        Integer variableSetId;

        @Parameter(names = {"--annotation-set-id"}, description = "Annotation Set Id.", required = false, arity = 0)
        String annotationSetId;

        @Parameter(names = {"--annotation"}, description = "Annotation", required = false, arity = 1)
        String annotation;


    }
}
