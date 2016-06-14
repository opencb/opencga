package org.opencb.opencga.app.cli.main.options;

import com.beust.jcommander.*;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser.OpencgaCommonCommandOptions;

/**
 * Created by sgallego on 6/14/16.
 */
@Parameters(commandNames = {"projects"}, commandDescription = "Project commands")
public class JobCommandOptions {


    public InfoCommandOptions infoCommandOptions;
    public DoneJobCommandOptions doneJobCommandOptions;
    public StatusCommandOptions statusCommandOptions;
    public RunJobCommandOptions runJobCommandOptions;

    public JCommander jCommander;
    public OpencgaCommonCommandOptions commonCommandOptions;

    public JobCommandOptions(OpencgaCommonCommandOptions commonCommandOptions, JCommander jCommander) {
        this.commonCommandOptions = commonCommandOptions;
        this.jCommander = jCommander;

        this.infoCommandOptions = new InfoCommandOptions();
        this.doneJobCommandOptions = new DoneJobCommandOptions();
        this.statusCommandOptions = new StatusCommandOptions();
        this.runJobCommandOptions = new RunJobCommandOptions();

    }

    class BaseJobCommand {
        @ParametersDelegate
        OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-id", "--job-id"}, description = "Job id", required = true, arity = 1)
        long id;
    }


    @Parameters(commandNames = {"info"}, commandDescription = "Get job information")
    class InfoCommandOptions {


        @ParametersDelegate
        OpencgaCommonCommandOptions cOpt = commonCommandOptions;

        @Parameter(names = {"-id", "--job-id"}, description = "Job id", required = true, arity = 1)
        long id;
    }

    @Parameters(commandNames = {"finished"}, commandDescription = "Notify catalog that a job have finished.")
    class DoneJobCommandOptions {


        @ParametersDelegate
        OpencgaCommonCommandOptions cOpt = commonCommandOptions;

        @Parameter(names = {"-id", "--job-id"}, description = "Job id", required = true, arity = 1)
        long id;

        @Parameter(names = {"--error"}, description = "Job finish with error", required = false, arity = 0)
        boolean error;

        @Parameter(names = {"--force"}, description = "Force finish job. Ignore if the job was PREPARED, QUEUED or RUNNING", required = false, arity = 0)
        boolean force;

        @Parameter(names = {"--discart-output"}, description = "Discart generated files. Temporal output directory will be deleted.", required = false, arity = 0)
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

    }

}
