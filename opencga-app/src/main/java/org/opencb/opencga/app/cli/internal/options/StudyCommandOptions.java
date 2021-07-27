package org.opencb.opencga.app.cli.internal.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.analysis.templates.TemplateRunner;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.internal.InternalCliOptionsParser;
import org.opencb.opencga.core.api.ParamConstants;

@Parameters(commandNames = {"study"}, commandDescription = "Implement several study tasks")
public class StudyCommandOptions {

    public final static String OUTDIR_PARAM_NAME = "outdir";

    public final GeneralCliOptions.CommonCommandOptions commonCommandOptions;
    public final InternalCliOptionsParser.JobOptions internalJobOptions;
    public JCommander jCommander;

    public TemplateLoader templateLoader;

    public StudyCommandOptions(GeneralCliOptions.CommonCommandOptions commonCommandOptions, JCommander jCommander) {
        this.commonCommandOptions = commonCommandOptions;
        this.internalJobOptions = new InternalCliOptionsParser.JobOptions();
        this.jCommander = jCommander;

        this.templateLoader = new TemplateLoader();
    }

    @Parameters(commandNames = {TemplateLoader.TEMPLATE_RUN_COMMAND}, commandDescription = "Load data in OpenCGA from templates")
    public class TemplateLoader {
        public static final String TEMPLATE_RUN_COMMAND = TemplateRunner.ID + "-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public InternalCliOptionsParser.JobOptions jobOptions = internalJobOptions;

        @Parameter(names = {"-s", "--" + ParamConstants.STUDY_PARAM}, description = "Study [[user@]project:]study.", arity = 1)
        public String studyId;

        @Parameter(names = {"-o", "--" + OUTDIR_PARAM_NAME}, description = "Directory where output files will be saved", required = true,
                arity = 1)
        public String outdir;

        @Parameter(names = {"--id"}, description = "Template id.", required = true, arity = 1)
        public String templateId;

        @Parameter(names = {"--overwrite"}, description = "Overwrite study metadata.", arity = 0)
        public boolean overwrite;

        @Parameter(names = {"--resume"}, description = "Resume study metadata ingestion.", arity = 0)
        public boolean resume;
    }

}
