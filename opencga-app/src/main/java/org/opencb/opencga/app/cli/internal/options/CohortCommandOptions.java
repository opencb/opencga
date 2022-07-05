package org.opencb.opencga.app.cli.internal.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.commons.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.internal.InternalCliOptionsParser;
import org.opencb.opencga.core.api.ParamConstants;

@Parameters(commandNames = {"cohort"}, commandDescription = "Implement several cohort tasks")
public class CohortCommandOptions {

    public final static String OUTDIR_PARAM_NAME = "outdir";
    public final InternalCliOptionsParser.JobOptions internalJobOptions;
    public SecondaryIndex secondaryIndex;
    public TsvLoad tsvLoad;
    public GeneralCliOptions.CommonCommandOptions cohortCommandOptions;
    public JCommander jCommander;

    public CohortCommandOptions(GeneralCliOptions.CommonCommandOptions cohortCommandOptions, JCommander jCommander) {
        this.cohortCommandOptions = cohortCommandOptions;
        this.internalJobOptions = new InternalCliOptionsParser.JobOptions();
        this.jCommander = jCommander;

        this.secondaryIndex = new SecondaryIndex();
        this.tsvLoad = new TsvLoad();
    }

    @Parameters(commandNames = {"secondary-index"}, commandDescription = "Creates a secondary index for cohorts using a search engine")
    public class SecondaryIndex {
        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = cohortCommandOptions;

        @ParametersDelegate
        public InternalCliOptionsParser.JobOptions jobOptions = internalJobOptions;

        @Parameter(names = {"-s", "--" + ParamConstants.STUDY_PARAM}, description = "Study [[user@]project:]study.", required = false,
                arity = 1)
        public String studyId;

        @Parameter(names = {"-o", "--" + OUTDIR_PARAM_NAME}, description = "Directory where output files will be saved", required = true,
                arity = 1)
        public String outDir;
    }

    @Parameters(commandNames = {"tsv-load"}, commandDescription = "Load annotations from a TSV file")
    public class TsvLoad {
        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = cohortCommandOptions;

        @ParametersDelegate
        public InternalCliOptionsParser.JobOptions jobOptions = internalJobOptions;

        @Parameter(names = {"-s", "--" + ParamConstants.STUDY_PARAM}, description = "Study [[user@]project:]study.", required = true,
                arity = 1)
        public String studyId;

        @Parameter(names = {"--file"}, description = "Path to the TSV file.", required = true, arity = 1)
        public String filePath;

        @Parameter(names = {"--variable-set-id"}, description = ParamConstants.VARIABLE_SET_DESCRIPTION, required = true, arity = 1)
        public String variableSetId;

        @Parameter(names = {"--annotation-set-id"}, description = "AnnotationSet id that will be given to the new annotations.",
                required = true, arity = 1)
        public String annotationSetId;

        @Parameter(names = {"-o", "--" + OUTDIR_PARAM_NAME}, description = "Directory where output files will be saved", required = true,
                arity = 1)
        public String outDir;
    }
}
