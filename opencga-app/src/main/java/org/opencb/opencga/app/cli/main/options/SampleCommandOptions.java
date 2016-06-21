package org.opencb.opencga.app.cli.main.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser.OpencgaCommonCommandOptions;

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
    public ShareCommandOptions shareCommandOptions;
    public UnshareCommandOptions unshareCommandOptions;
    public GroupByCommandOptions groupByCommandOptions;
    public AnnotateCommandOptions annotateCommandOptions;

    public JCommander jCommander;
    public OpencgaCommonCommandOptions commonCommandOptions;

    public SampleCommandOptions(OpencgaCommonCommandOptions commonCommandOptions, JCommander jCommander) {
        this.commonCommandOptions = commonCommandOptions;
        this.jCommander = jCommander;

        this.createCommandOptions = new CreateCommandOptions();
        this.loadCommandOptions = new LoadCommandOptions();
        this.infoCommandOptions = new InfoCommandOptions();
        this.searchCommandOptions = new SearchCommandOptions();
        this.updateCommandOptions = new UpdateCommandOptions();
        this.deleteCommandOptions = new DeleteCommandOptions();
        this.shareCommandOptions = new ShareCommandOptions();
        this.unshareCommandOptions = new UnshareCommandOptions();
        this.groupByCommandOptions = new GroupByCommandOptions();
        this.annotateCommandOptions = new AnnotateCommandOptions();
    }

    class BaseSampleCommand {

        @ParametersDelegate
        OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--sample-id"}, description = "Sample id", required = true, arity = 1)
        public Integer id;
    }

    @Parameters(commandNames = {"create"}, commandDescription = "Create a cohort")
    public class CreateCommandOptions {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--study-id"}, description = "Study id", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"--name"}, description = "cohort name", required = true, arity = 1)
        public String name;

        @Parameter(names = {"--source"}, description = "Source", required = false, arity = 1)
        public String source;

        @Parameter(names = {"--description"}, description = "Sample description", required = false, arity = 1)
        public String description;
    }

    @Parameters(commandNames = {"load"}, commandDescription = "Load samples from a pedigree file")
    class LoadCommandOptions {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--field-id"}, description = "File id already loaded in OpenCGA", required = true, arity = 1)
        public String fieldId;

        @Parameter(names = {"--variable-set-id"}, description = "VariableSetId that represents the pedigree file", arity = 1)
        public int variableSetId;
    }

    @Parameters(commandNames = {"info"}, commandDescription = "Get samples information")
    class InfoCommandOptions extends BaseSampleCommand {
    }

    @Parameters(commandNames = {"search"}, commandDescription = "Search samples")
    class SearchCommandOptions {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--study-id"}, description = "Study id", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"--id"}, description = "Comma separated list of ids.", required = false, arity = 1)
        public String id;

        @Parameter(names = {"--name"}, description = "Comma separated list of names.", required = false, arity = 0)
        public String name;

        @Parameter(names = {"--source"}, description = "Source.", required = false, arity = 0)
        public String source;

        @Parameter(names = {"--indivual-id"}, description = "Indivudual id.", required = false, arity = 0)
        public String individualId;

        @Parameter(names = {"--annotation-set-id"}, description = "Annotation set id.", required = false, arity = 0)
        public String annotationSetId;

        @Parameter(names = {"--variable-set-id"}, description = "Annotation set id.", required = false, arity = 0)
        public String variableSetId;

        @Parameter(names = {"--annotation"}, description = "Annotation.", required = false, arity = 0)
        public String annotation;


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

    @Parameters(commandNames = {"share"}, commandDescription = "Share cohort")
    public class ShareCommandOptions {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--sample-ids"}, description = "Sample ids", required = true, arity = 1)
        public String sampleIds;

        @Parameter(names = {"--members"}, description = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'",
                required = true, arity = 1)
        public String members;

        @Parameter(names = {"--permission"}, description = "Comma separated list of cohort permissions", required = false, arity = 1)
        public String permission;

        @Parameter(names = {"--override"}, description = "Boolean indicating whether to allow the change"
                + " of permissions in case any member already had any, default:false", required = false, arity = 0)
        public boolean override;
    }

    @Parameters(commandNames = {"unshare"}, commandDescription = "Share cohort")
    public class UnshareCommandOptions {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--sample-ids"}, description = "Sample ids", required = true, arity = 1)
        public String sampleIds;

        @Parameter(names = {"--members"}, description = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'",
                required = true, arity = 1)
        public String members;

        @Parameter(names = {"--permission"}, description = "Comma separated list of cohort permissions", required = false, arity = 1)
        public String permission;
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

        @Parameter(names = {"--annotation-set-id"}, description = "Annotation Set Id.", required = false, arity = 0)
        public String annotationSetId;

        @Parameter(names = {"--variable-set-id"}, description = "Variable set ids", required = false, arity = 1)
        public String variableSetId;

        @Parameter(names = {"--annotation"}, description = "Annotation", required = false, arity = 1)
        public String annotation;
    }

    @Parameters(commandNames = {"delete"}, commandDescription = "Deletes the selected sample")
    class DeleteCommandOptions extends BaseSampleCommand {
    }

    /*public static class SemiColonParameterSplitter implements IParameterSplitter {

        public List<String> split(String value) {
            return Arrays.asList(value.split(";"));
        }

    }*/

    @Parameters(commandNames = {"annotate"}, commandDescription = "Annotate sample")
    class AnnotateCommandOptions extends BaseSampleCommand {

        @Parameter(names = {"--annotate-set-name"}, description = "Annotation set name. Must be unique for the cohort",
                required = true, arity = 1)
        public String annotateSetName;

        @Parameter(names = {"--variableSetId"}, description = "VariableSetIdt", required = true, arity = 1)
        public String variableSetId;

        @Parameter(names = {"--update"}, description = "Update an already existing AnnotationSet, default: false", arity = 0)
        public boolean update;

        @Parameter(names = {"--delete"}, description = "Delete an AnnotationSet, default:false", required = false, arity = 0)
        public boolean delete;
    }

}
