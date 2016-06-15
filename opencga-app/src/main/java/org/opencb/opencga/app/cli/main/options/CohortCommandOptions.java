package org.opencb.opencga.app.cli.main.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser.OpencgaCommonCommandOptions;
import org.opencb.opencga.catalog.models.Cohort;

import java.util.List;

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
    public UnshareCommandOptions unshareCommandOptions;
    public StatsCommandOptions statsCommandOptions;
    public ShareCommandOptions shareCommandOptions;
    public GroupByCommandOptions groupByCommandOptions;

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
        this.unshareCommandOptions = new UnshareCommandOptions();
        this.statsCommandOptions = new StatsCommandOptions();
        this.shareCommandOptions = new ShareCommandOptions();
        this.groupByCommandOptions = new GroupByCommandOptions();
    }

    class BaseCohortsCommand {
        @ParametersDelegate
        OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-id", "--cohorts-id"}, description = "Cohorts id", required = true, arity = 1)
        Integer id;
    }

    @Parameters(commandNames = {"create"}, commandDescription = "Create a cohort")
    public class CreateCommandOptions {

        @ParametersDelegate
        OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--study-id"}, description = "Study id", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"--name"}, description = "cohort name", required = false, arity = 1)
        public String name;

        @Parameter(names = {"--variable-set-id"}, description = "VariableSetId", required = false, arity = 1)
        long variableSetId;

        @Parameter(names = {"--description"}, description = "cohort description", required = false, arity = 1)
        String description;

        @Parameter(names = {"--sample-ids"}, description = "Sample ids for the cohort (CSV)", required = false, arity = 1)
        String sampleIds;

        @Parameter(names = {"--variable"}, description = "Categorical variable name to use to create cohorts", required = false, arity = 1)
        String variable;

        @Parameter(names = {"--type"}, description = "Cohort type", required = false, arity = 1)
        Cohort.Type type;

        @Parameter(names = {"--from-aggregation-mapping-file"}, description = "If the study is aggregated, basic cohorts without samples may be extracted from the mapping file", required = false, arity = 1)
        String tagmap = null;
    }

    @Parameters(commandNames = {"info"}, commandDescription = "Get cohort information")
    public class InfoCommandOptions {

        @ParametersDelegate
        OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-id", "--cohort-id"}, description = "Cohort id", required = true, arity = 1)
        public long id;
    }

    @Parameters(commandNames = {"samples"}, commandDescription = "List samples belonging to a cohort")
    public class SamplesCommandOptions extends BaseCohortsCommand { }

    @Parameters(commandNames = {"calculate-stats"}, commandDescription = "Calculate variant stats for a set of cohorts.")
    class StatsCommandOptions extends BaseCohortsCommand{

        @Parameter(names = {"-o", "--outdir-id"}, description = "Directory ID where to create the file", required = false, arity = 1)
        String outdir = "";

        @Parameter(names = {"--enqueue"}, description = "Enqueue the job to be launched by the execution manager", arity = 0)
        boolean enqueue;

        @Parameter(names = {"--aggregation-mapping-file"}, description = "File containing population names mapping in an aggregated VCF file")
        String tagmap = null;

        @Parameter(description = " -- {opencga-storage internal parameter. Use your head}") //Wil contain args after "--"
        public List<String> dashDashParameters;
    }

    @Parameters(commandNames = {"annotate"}, commandDescription = "Annotate cohort")
    public class AnnotateCommandOptions extends BaseCohortsCommand {
        @Parameter(names = {"--annotateSetName"}, description = "Annotation set name. Must be unique for the cohort",required = true, arity = 1)
        String annotateSetName;

        @Parameter(names = {"--variableSetId"}, description = "VariableSetIdt",required = true, arity = 1)
        String variableSetId;

        @Parameter(names = {"--update"}, description = "Update an already existing AnnotationSet",required = true, arity = 0)
        boolean update;

        @Parameter(names = {"--delete"}, description = "Delete an AnnotationSet",required = true, arity = 0)
        boolean delete;


    }

    @Parameters(commandNames = {"update"}, commandDescription = "Update cohort")
    public class UpdateCommandOptions extends BaseCohortsCommand {

        //TODO

        @Parameter(names = {"--annotateSetName"}, description = "Annotation set name. Must be unique for the cohort",required = true, arity = 1)
        String annotateSetName;

        @Parameter(names = {"--variableSetId"}, description = "VariableSetIdt",required = true, arity = 1)
        String variableSetId;

        @Parameter(names = {"--update"}, description = "Update an already existing AnnotationSet",required = true, arity = 0)
        boolean update;

        @Parameter(names = {"--delete"}, description = "Delete an AnnotationSet",required = true, arity = 0)
        boolean delete;


    }

    @Parameters(commandNames = {"delete"}, commandDescription = "Delete cohort")
    public class DeleteCommandOptions extends BaseCohortsCommand {

        //TODO
        @Parameter(names = {"--annotateSetName"}, description = "Annotation set name. Must be unique for the cohort",required = true, arity = 1)
        String annotateSetName;

        @Parameter(names = {"--variableSetId"}, description = "VariableSetIdt",required = true, arity = 1)
        String variableSetId;

        @Parameter(names = {"--update"}, description = "Update an already existing AnnotationSet",required = true, arity = 0)
        boolean update;

        @Parameter(names = {"--delete"}, description = "Delete an AnnotationSet",required = true, arity = 0)
        boolean delete;


    }

    @Parameters(commandNames = {"unshare"}, commandDescription = "Unshare cohort")
    public class UnshareCommandOptions extends BaseCohortsCommand {
        //TODO
        @Parameter(names = {"--annotateSetName"}, description = "Annotation set name. Must be unique for the cohort",required = true, arity = 1)
        String annotateSetName;

        @Parameter(names = {"--variableSetId"}, description = "VariableSetIdt",required = true, arity = 1)
        String variableSetId;

        @Parameter(names = {"--update"}, description = "Update an already existing AnnotationSet",required = true, arity = 0)
        boolean update;

        @Parameter(names = {"--delete"}, description = "Delete an AnnotationSet",required = true, arity = 0)
        boolean delete;


    }

    @Parameters(commandNames = {"share"}, commandDescription = "Share cohort")
    public class ShareCommandOptions extends BaseCohortsCommand {
        //TODO
    }

    @Parameters(commandNames = {"groupBy"}, commandDescription = "GroupBy cohort")
    public class GroupByCommandOptions extends BaseCohortsCommand {
        //TODO
    }
}
