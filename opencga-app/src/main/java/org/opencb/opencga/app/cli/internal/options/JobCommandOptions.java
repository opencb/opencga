package org.opencb.opencga.app.cli.internal.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.core.api.ParamConstants;

@Parameters(commandNames = {"job"}, commandDescription = "Implement several job tasks")
public class JobCommandOptions {

    public final static String OUTDIR_PARAM_NAME = "outdir";

    public SecondaryIndex secondaryIndex;

    public GeneralCliOptions.CommonCommandOptions jobCommandOptions;
    public JCommander jCommander;

    public JobCommandOptions(GeneralCliOptions.CommonCommandOptions jobCommandOptions, JCommander jCommander) {
        this.jobCommandOptions = jobCommandOptions;
        this.jCommander = jCommander;

        this.secondaryIndex = new SecondaryIndex();
    }

    @Parameters(commandNames = {"secondary-index"}, commandDescription = "Creates a secondary index for jobs using a search engine")
    public class SecondaryIndex {
        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = jobCommandOptions;

        @Parameter(names = {"-s", "--" + ParamConstants.STUDY_PARAM}, description = "Study [[user@]project:]study.", required = false,
                arity = 1)
        public String studyId;

        @Parameter(names = {"-o", "--" + OUTDIR_PARAM_NAME}, description = "Directory where output files will be saved", required = true,
                arity = 1)
        public String outDir;
    }

}
