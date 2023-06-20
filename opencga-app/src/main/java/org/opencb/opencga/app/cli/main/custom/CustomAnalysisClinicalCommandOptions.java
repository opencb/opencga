package org.opencb.opencga.app.cli.main.custom;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.analysis.clinical.ClinicalAnalysisLoadTask;

import static org.opencb.opencga.app.cli.GeneralCliOptions.CommonCommandOptions;

/**
 * This class contains methods for the Files command line. OpenCGA version: 2.2.0-SNAPSHOT PATH: /{apiVersion}/files
 */
@Parameters(commandNames = {"studies"}, commandDescription = "Studies commands")
public class CustomAnalysisClinicalCommandOptions {

    public JCommander jCommander;
    public CommonCommandOptions commonCommandOptions;
    public LoadCommandOptions loadCommandOptions;


    public CustomAnalysisClinicalCommandOptions(CommonCommandOptions commonCommandOptions, JCommander jCommander) {

        this.jCommander = jCommander;
        this.commonCommandOptions = commonCommandOptions;
        this.loadCommandOptions = new LoadCommandOptions();
    }

    @Parameters(commandNames = ClinicalAnalysisLoadTask.ID, commandDescription = ClinicalAnalysisLoadTask.DESCRIPTION)
    public class LoadCommandOptions {
        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--study", "-s"}, description = "Study [[user@]project:]study where study and project can be either the ID or" +
                " UUID", required = false, arity = 1)
        public String study;

        @Parameter(names = {"-i", "--input"}, description = "GZIP file containing the clinical analyses", required = true, arity = 1)
        public String file;

        @Parameter(names = {"--jobId"}, description = "Job id.", arity = 1)
        public String jobId;

        @Parameter(names = {"--jobDependsOn"}, description = "Job depends on.", arity = 1)
        public String jobDependsOn;

        @Parameter(names = {"--jobDescription"}, description = "Job description.", arity = 1)
        public String jobDescription;

        @Parameter(names = {"--jobTags"}, description = "Job tags.", arity = 1)
        public String jobTags;
    }
}