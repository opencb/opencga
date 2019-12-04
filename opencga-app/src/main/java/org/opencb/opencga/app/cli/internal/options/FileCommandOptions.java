package org.opencb.opencga.app.cli.internal.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.GeneralCliOptions;

@Parameters(commandNames = {"file"}, commandDescription = "Implement several file tasks")
public class FileCommandOptions {

    public final static String STUDY_PARAM_NAME = "study";
    public final static String OUTDIR_PARAM_NAME = "outdir";

    public DeleteCommandOptions deleteCommandOptions;
    public UnlinkCommandOptions unlinkCommandOptions;

    public GeneralCliOptions.CommonCommandOptions fileCommonOptions;
    public JCommander jCommander;

    public FileCommandOptions(GeneralCliOptions.CommonCommandOptions fileCommonCommandOptions, JCommander jCommander) {
        this.fileCommonOptions = fileCommonCommandOptions;
        this.jCommander = jCommander;

        this.deleteCommandOptions = new DeleteCommandOptions();
        this.unlinkCommandOptions = new UnlinkCommandOptions();
    }

    @Parameters(commandNames = {"delete"}, commandDescription = "Delete file task")
    public class DeleteCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = fileCommonOptions;

        @Parameter(names = {"-s", "--" + STUDY_PARAM_NAME}, description = "Study [[user@]project:]study.", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"--files"}, description = "Comma separated list of files", required = true, arity = 1)
        public String files;

        @Parameter(names = {"--skip-trash"}, description = "Flag indicating to skip trash bin and completely delete the file. It will"
                + " not be possible to recover the file.", arity = 1)
        public boolean skipTrash;

        @Parameter(names = {"-o", "--" + OUTDIR_PARAM_NAME}, description = "Directory where output files will be saved", required = true,
                arity = 1)
        public String outDir;
    }

    @Parameters(commandNames = {"unlink"}, commandDescription = "Unlink file task")
    public class UnlinkCommandOptions {
        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = fileCommonOptions;

        @Parameter(names = {"-s", "--" + STUDY_PARAM_NAME}, description = "Study [[user@]project:]study.", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"--files"}, description = "Comma separated list of files", required = true, arity = 1)
        public String files;

        @Parameter(names = {"-o", "--" + OUTDIR_PARAM_NAME}, description = "Directory where output files will be saved", required = true,
                arity = 1)
        public String outDir;
    }

}
