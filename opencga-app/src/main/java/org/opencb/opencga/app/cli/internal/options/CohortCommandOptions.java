package org.opencb.opencga.app.cli.internal.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.core.api.ParamConstants;

@Parameters(commandNames = {"cohort"}, commandDescription = "Implement several cohort tasks")
public class CohortCommandOptions {

    public final static String OUTDIR_PARAM_NAME = "outdir";

    public SecondaryIndex secondaryIndex;

    public GeneralCliOptions.CommonCommandOptions cohortCommandOptions;
    public JCommander jCommander;

    public CohortCommandOptions(GeneralCliOptions.CommonCommandOptions cohortCommandOptions, JCommander jCommander) {
        this.cohortCommandOptions = cohortCommandOptions;
        this.jCommander = jCommander;

        this.secondaryIndex = new SecondaryIndex();
    }

    @Parameters(commandNames = {"secondary-index"}, commandDescription = "Creates a secondary index for cohorts using a search engine")
    public class SecondaryIndex {
        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = cohortCommandOptions;

        @Parameter(names = {"-s", "--" + ParamConstants.STUDY_PARAM}, description = "Study [[user@]project:]study.", required = false,
                arity = 1)
        public String studyId;

        @Parameter(names = {"-o", "--" + OUTDIR_PARAM_NAME}, description = "Directory where output files will be saved", required = true,
                arity = 1)
        public String outDir;
    }
}
