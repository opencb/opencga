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
    public ShareCommandOptions shareCommandOptions;
    public UnshareCommandOptions unshareCommandOptions;
    public GroupByCommandOptions groupByCommandOptions;

    // public DoneJobCommandOptions doneJobCommandOptions;
    // public StatusCommandOptions statusCommandOptions;
    // public RunJobCommandOptions runJobCommandOptions;

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
        this.unshareCommandOptions = new UnshareCommandOptions();
        this.shareCommandOptions = new ShareCommandOptions();
        this.groupByCommandOptions = new GroupByCommandOptions();
        //    this.runJobCommandOptions = new RunJobCommandOptions();

    }

    public class BaseJobCommand {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--job-id"}, description = "Job id", required = true, arity = 1)
        public Integer id;
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
        OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--study-id"}, description = "Study id", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"--job-id"}, description = "Job id", required = false, arity = 1)
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

        @Parameter(names = {"--modification-date"}, description = "Modification date.", required = false, arity = 1)
        public String modificationDate;

        @Parameter(names = {"-d", "--description"}, description = "Description", required = false, arity = 1)
        public String description;

        @Parameter(names = {"--job-id"}, description = "Job id", required = false, arity = 1)
        public String jobId;

        @Parameter(names = {"--attributes"}, description = "Attributes", required = false, arity = 1)
        public String attributes;

        @Parameter(names = {"--nattributes"}, description = "numerical attributes", required = false, arity = 1)
        public String nattributes;
    }

    @Parameters(commandNames = {"visit"}, commandDescription = "Increment job visits")
    class VisitCommandOptions extends BaseJobCommand {
    }

    @Parameters(commandNames = {"delete"}, commandDescription = "Delete job")
    class DeleteCommandOptions extends BaseJobCommand {

        @Parameter(names = {"--delete-files"}, description = "Delete files, default:true", required = false, arity = 0)
        boolean deleteFiles;
    }


    @Parameters(commandNames = {"share"}, commandDescription = "Share cohort")
    public class ShareCommandOptions {

        @ParametersDelegate
        OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--job-ids"}, description = "Jobs ids", required = true, arity = 1)
        public String jobsids;

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

    @Parameters(commandNames = {"unshare"}, commandDescription = "Unshare cohort")
    public class UnshareCommandOptions {

        @ParametersDelegate
        OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--job-ids"}, description = "Jobs ids", required = true, arity = 1)
        public String jobsids;

        @Parameter(names = {"--members"},
                description = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'",
                required = true, arity = 1)
        public String members;

        @Parameter(names = {"--permission"}, description = "Comma separated list of cohort permissions",
                required = false, arity = 1)
        public String permission;
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

        @Parameter(names = {"--modification-date"}, description = "Modification date.", required = false, arity = 1)
        public String modificationDate;

        @Parameter(names = {"-d", "--description"}, description = "Description", required = false, arity = 1)
        public String description;

        @Parameter(names = {"--attributes"}, description = "Attributes", required = false, arity = 1)
        String attributes;
    }



  /*  @Parameters(commandNames = {"finished"}, commandDescription = "Notify catalog that a job have finished.")
    class DoneJobCommandOptions {


        @ParametersDelegate
        OpencgaCommonCommandOptions cOpt = commonCommandOptions;

        @Parameter(names = {"-id", "--job-id"}, description = "Job id", required = true, arity = 1)
        long id;

        @Parameter(names = {"--error"}, description = "Job finish with error", required = false, arity = 0)
        boolean error;

        @Parameter(names = {"--force"}, description = "Force finish job. Ignore if the job was PREPARED, QUEUED or RUNNING", required =
        false, arity = 0)
        boolean force;

        @Parameter(names = {"--discart-output"}, description = "Discart generated files. Temporal output directory will be deleted.",
        required = false, arity = 0)
        boolean discardOutput;
    }

    @Parameters(commandNames = {"status"}, commandDescription = "Get the status of all running jobs.")
    class StatusCommandOptions {


        @ParametersDelegate
        OpencgaCommonCommandOptions cOpt = commonCommandOptions;

        @Parameter(names = {"--study-id"}, description = "Study id", required = false, arity = 1)
        String studyId;
    }

    @Parameters(commandNames = {"run"}, commandDescription = "Executes a job.")
    class RunJobCommandOptions {



        @ParametersDelegate
        OpencgaCommonCommandOptions cOpt = commonCommandOptions;

        @Parameter(names = {"-t", "--tool-id"}, description = "", required = true, arity = 1)
        String toolId;

        @Parameter(names = {"-s", "--study-id"}, description = "Study id", required = true, arity = 1)
        String studyId;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory", required = true, arity = 1)
        String outdir;

        @Parameter(names = {"-e", "--execution"}, description = "", required = false, arity = 1)
        String execution;

        @Parameter(names = {"-n", "--name"}, description = "", required = true, arity = 1)
        String name;

        @Parameter(names = {"-d", "--description"}, description = "", required = false, arity = 1)
        String description;

        @DynamicParameter(names = "-P", description = "Parameters", hidden = false)
        ObjectMap params = new ObjectMap();

    }*/

}
