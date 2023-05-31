package org.opencb.opencga.app.cli.main.custom;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import static org.opencb.opencga.app.cli.GeneralCliOptions.CommonCommandOptions;

/**
 * This class contains methods for the Files command line. OpenCGA version: 2.2.0-SNAPSHOT PATH: /{apiVersion}/files
 */
@Parameters(commandNames = {"studies"}, commandDescription = "Studies commands")
public class CustomStudiesCommandOptions {

    public JCommander jCommander;
    public CommonCommandOptions commonCommandOptions;

    public CustomStudiesCommandOptions(CommonCommandOptions commonCommandOptions, JCommander jCommander) {

        this.jCommander = jCommander;
        this.commonCommandOptions = commonCommandOptions;
    }

    @Parameters(commandNames = {"templates-upload"}, commandDescription = "Upload template")
    public class UploadTemplatesCommandOptions {
        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--study", "-s"}, description = "Study [[user@]project:]study where study and project can be either the ID or" +
                " UUID", required = false, arity = 1)
        public String study;

        @Parameter(names = {"-i", "--input"}, description = "ZIP file containing the template files or folder", required = true, arity = 1)
        public String file;
    }

    @Parameters(commandNames = {"templates-run"}, commandDescription = "Execute template")
    public class RunTemplatesCommandOptions {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--study", "-s"}, description = "Study [[user@]project:]study where study and project can be either the ID or" +
                " UUID", required = false, arity = 1)
        public String study;

        @Parameter(names = {"--id"}, description = "Template id", required = true, arity = 1)
        public String id;

        @Parameter(names = {"--overwrite"}, description = "Overwrite study metadata.", arity = 0)
        public boolean overwrite;

        @Parameter(names = {"--resume"}, description = "Resume study metadata ingestion.", arity = 0)
        public boolean resume;

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