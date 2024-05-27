package org.opencb.opencga.app.cli.internal.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameters;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.internal.InternalCliOptionsParser;

@Parameters(commandNames = {"job"}, commandDescription = "Implement several job tasks")
public class JobCommandOptions {

    public final static String OUTDIR_PARAM_NAME = "outdir";

    public GeneralCliOptions.CommonCommandOptions jobCommandOptions;
    public final InternalCliOptionsParser.JobOptions internalJobOptions;
    public JCommander jCommander;

    public JobCommandOptions(GeneralCliOptions.CommonCommandOptions jobCommandOptions, JCommander jCommander) {
        this.jobCommandOptions = jobCommandOptions;
        this.internalJobOptions = new InternalCliOptionsParser.JobOptions();
        this.jCommander = jCommander;

    }

}
