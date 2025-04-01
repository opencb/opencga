package org.opencb.opencga.app.cli.internal.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.internal.InternalCliOptionsParser;
import org.opencb.opencga.core.api.ParamConstants;

import java.util.List;

@Parameters(commandNames = {"file"}, commandDescription = "Implement several file tasks")
public class FileCommandOptions {

    public final static String OUTDIR_PARAM_NAME = "outdir";

    public DeleteCommandOptions deleteCommandOptions;
    public UnlinkCommandOptions unlinkCommandOptions;
    public FetchCommandOptions fetchCommandOptions;
    public PostlinkCommandOptions postlinkCommandOptions;
    public TsvLoad tsvLoad;

    public final GeneralCliOptions.CommonCommandOptions fileCommonOptions;
    public final InternalCliOptionsParser.JobOptions internalJobOptions;
    public JCommander jCommander;

    public FileCommandOptions(GeneralCliOptions.CommonCommandOptions fileCommonCommandOptions, JCommander jCommander) {
        this.fileCommonOptions = fileCommonCommandOptions;
        this.internalJobOptions = new InternalCliOptionsParser.JobOptions();
        this.jCommander = jCommander;

        this.deleteCommandOptions = new DeleteCommandOptions();
        this.unlinkCommandOptions = new UnlinkCommandOptions();
        this.fetchCommandOptions = new FetchCommandOptions();
        this.postlinkCommandOptions = new PostlinkCommandOptions();
        this.tsvLoad = new TsvLoad();
    }

    @Parameters(commandNames = {"delete"}, commandDescription = "Delete file task")
    public class DeleteCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = fileCommonOptions;

        @ParametersDelegate
        public InternalCliOptionsParser.JobOptions jobOptions = internalJobOptions;

        @Parameter(names = {"-s", "--" + ParamConstants.STUDY_PARAM}, description = "Study [[organization@]project:]study.", required = true,
                arity = 1)
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

        @ParametersDelegate
        public InternalCliOptionsParser.JobOptions jobOptions = internalJobOptions;

        @Parameter(names = {"-s", "--" + ParamConstants.STUDY_PARAM}, description = "Study [[organization@]project:]study.", required = true,
                arity = 1)
        public String studyId;

        @Parameter(names = {"--files"}, description = "Comma separated list of files", required = true, arity = 1)
        public String files;

        @Parameter(names = {"-o", "--" + OUTDIR_PARAM_NAME}, description = "Directory where output files will be saved", required = true,
                arity = 1)
        public String outDir;
    }

    @Parameters(commandNames = {"postlink"}, commandDescription = "Postlink sample association")
    public class PostlinkCommandOptions {
        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = fileCommonOptions;

        @ParametersDelegate
        public InternalCliOptionsParser.JobOptions jobOptions = internalJobOptions;

        @Parameter(names = {"-s", "--" + ParamConstants.STUDY_PARAM}, description = "Study [[organization@]project:]study.", required = true,
                arity = 1)
        public String studyId;

        @Parameter(names = {"--files"}, description = "List of files to associate samples.", arity = 1)
        public List<String> files;

        @Parameter(names = {"--batch-size"}, description = "Samples update batch size")
        public Integer batchSize;

        @Parameter(names = {"-o", "--" + OUTDIR_PARAM_NAME}, description = "Directory where output files will be saved", required = true,
                arity = 1)
        public String outDir;
    }

    @Parameters(commandNames = {"fetch"}, commandDescription = "Fetch file")
    public class FetchCommandOptions {
        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = fileCommonOptions;

        @ParametersDelegate
        public InternalCliOptionsParser.JobOptions jobOptions = internalJobOptions;

        @Parameter(names = {"-s", "--" + ParamConstants.STUDY_PARAM}, description = "Study [[organization@]project:]study.", required = true,
                arity = 1)
        public String studyId;

        @Parameter(names = {"--" + ParamConstants.FILE_PATH_PARAM},
                description = "Folder path where the downloaded file will be registered", required = true, arity = 1)
        public String path;

        @Parameter(names = {"--resource"}, description = "File resource", arity = 1)
        public Boolean resource;

        @Parameter(names = {"--url"}, description = "External url where the file to be registered can be downloaded from", required = true,
                arity = 1)
        public String url;

        @Parameter(names = {"-o", "--" + OUTDIR_PARAM_NAME}, description = "Directory where output files will be saved", required = true,
                arity = 1)
        public String outDir;
    }

    @Parameters(commandNames = {"tsv-load"}, commandDescription = "Load annotations from a TSV file")
    public class TsvLoad {
        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = fileCommonOptions;

        @ParametersDelegate
        public InternalCliOptionsParser.JobOptions jobOptions = internalJobOptions;

        @Parameter(names = {"-s", "--" + ParamConstants.STUDY_PARAM}, description = "Study [[organization@]project:]study.", required = true,
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
