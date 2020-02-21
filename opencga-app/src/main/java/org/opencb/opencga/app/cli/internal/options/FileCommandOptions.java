package org.opencb.opencga.app.cli.internal.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.core.api.ParamConstants;

@Parameters(commandNames = {"file"}, commandDescription = "Implement several file tasks")
public class FileCommandOptions {

    public final static String OUTDIR_PARAM_NAME = "outdir";

    public DeleteCommandOptions deleteCommandOptions;
    public UnlinkCommandOptions unlinkCommandOptions;
    public FetchCommandOptions fetchCommandOptions;
    public SecondaryIndex secondaryIndex;
    public TsvLoad tsvLoad;

    public GeneralCliOptions.CommonCommandOptions fileCommonOptions;
    public JCommander jCommander;

    public FileCommandOptions(GeneralCliOptions.CommonCommandOptions fileCommonCommandOptions, JCommander jCommander) {
        this.fileCommonOptions = fileCommonCommandOptions;
        this.jCommander = jCommander;

        this.deleteCommandOptions = new DeleteCommandOptions();
        this.unlinkCommandOptions = new UnlinkCommandOptions();
        this.fetchCommandOptions = new FetchCommandOptions();
        this.secondaryIndex = new SecondaryIndex();
        this.tsvLoad = new TsvLoad();
    }

    @Parameters(commandNames = {"delete"}, commandDescription = "Delete file task")
    public class DeleteCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = fileCommonOptions;

        @Parameter(names = {"-s", "--" + ParamConstants.STUDY_PARAM}, description = "Study [[user@]project:]study.", required = true,
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

        @Parameter(names = {"-s", "--" + ParamConstants.STUDY_PARAM}, description = "Study [[user@]project:]study.", required = true,
                arity = 1)
        public String studyId;

        @Parameter(names = {"--files"}, description = "Comma separated list of files", required = true, arity = 1)
        public String files;

        @Parameter(names = {"-o", "--" + OUTDIR_PARAM_NAME}, description = "Directory where output files will be saved", required = true,
                arity = 1)
        public String outDir;
    }

    @Parameters(commandNames = {"unlink"}, commandDescription = "Unlink file task")
    public class FetchCommandOptions {
        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = fileCommonOptions;

        @Parameter(names = {"-s", "--" + ParamConstants.STUDY_PARAM}, description = "Study [[user@]project:]study.", required = true,
                arity = 1)
        public String studyId;

        @Parameter(names = {"--" + ParamConstants.FILE_PATH_PARAM},
                description = "Folder path where the downloaded file will be registered", required = true, arity = 1)
        public String path;

        @Parameter(names = {"--url"}, description = "External url where the file to be registered can be downloaded from", required = true,
                arity = 1)
        public String url;

        @Parameter(names = {"-o", "--" + OUTDIR_PARAM_NAME}, description = "Directory where output files will be saved", required = true,
                arity = 1)
        public String outDir;
    }

    @Parameters(commandNames = {"secondary-index"}, commandDescription = "Creates a secondary index for files using a search engine")
    public class SecondaryIndex {
        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = fileCommonOptions;

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
        public GeneralCliOptions.CommonCommandOptions commonOptions = fileCommonOptions;

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
