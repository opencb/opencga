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

    public class BaseCohortsCommand {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--cohorts-id"}, description = "Cohorts id", required = true, arity = 1)
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

        @Parameter(names = {"--type"}, description = "Cohort type", required = false, arity = 1)
        public Study.Type type;

        @Parameter(names = {"--variable-set-id"}, description = "VariableSetId", required = false, arity = 1)
        public Integer variableSetId;

        @Parameter(names = {"--description"}, description = "cohort description", required = false, arity = 1)
        public String description;

        @Parameter(names = {"--sample-ids"}, description = "Sample ids for the cohort (CSV)",
                required = false, arity = 1)
        public String sampleIds;

        @Parameter(names = {"--variable"}, description = "Categorical variable name to use to create cohorts, must go together the "
                + "parameter variable-set-id",
                required = false, arity = 1)
        public String variable;


        //  @Parameter(names = {"--from-aggregation-mapping-file"}, description = "If the study is aggregated, basic cohorts without
        // samples may be extracted from the mapping file", required = false, arity = 1)
        //  String tagmap = null;
    }

    @Parameters(commandNames = {"info"}, commandDescription = "Get cohort information")
    public class InfoCommandOptions extends BaseCohortsCommand {
    }

    @Parameters(commandNames = {"samples"}, commandDescription = "List samples belonging to a cohort")
    public class SamplesCommandOptions extends BaseCohortsCommand {
    }

    @Parameters(commandNames = {"calculate-stats"},
            commandDescription = "Calculate variant stats for a set of cohorts.")
    class StatsCommandOptions extends BaseCohortsCommand {

        @Parameter(names = {"--calculate"}, description = "Calculate cohort stats", arity = 0)
        public boolean calculate;

        @Parameter(names = {"--delete"}, description = "Delete stats [PENDING]", arity = 0)
        public boolean delete;

        @Parameter(names = {"--log"}, description = "Log level", required = false, arity = 1)
        public String log = "";

        @Parameter(names = {"-o", "--outdir-id"}, description = "Directory ID where to create the file",
                required = false, arity = 1)
        public String outdir = "";

      /*  @Parameter(names = {"--enqueue"}, description = "Enqueue the job to be launched by the execution manager", arity = 0)
        boolean enqueue;

        @Parameter(names = {"--aggregation-mapping-file"}, description = "File containing population names mapping in an aggregated VCF
        file")
        String tagmap = null;

        @Parameter(description = " -- {opencga-storage internal parameter. Use your head}") //Wil contain args after "--"
        public List<String> dashDashParameters;*/
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

    @Parameters(commandNames = {"unshare"}, commandDescription = "Unshare cohort")
    public class UnshareCommandOptions {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-ids", "--cohort-ids"}, description = "Cohorts ids", required = true, arity = 1)
        public String ids;

        @Parameter(names = {"--members"},
                description = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'",
                required = true, arity = 1)
        public String members;

        @Parameter(names = {"--permission"}, description = "Comma separated list of cohort permissions",
                required = false, arity = 1)
        public String permission;
    }

    @Parameters(commandNames = {"share"}, commandDescription = "Share cohort")
    public class ShareCommandOptions {
        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-ids", "--cohort-ids"}, description = "Cohorts ids", required = true, arity = 1)
        public String cohortids;

        @Parameter(names = {"--members"},
                description = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'",
                required = true, arity = 1)
        public String members;

        @Parameter(names = {"--permission"}, description = "Comma separated list of cohort permissions",
                required = false, arity = 1)
        public String permission;

        @Parameter(names = {"--override"}, description = "Boolean indicating whether to allow the change" +
                " of permissions in case any member already had any, default:false", required = false, arity = 0)
        public boolean override;
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
}
