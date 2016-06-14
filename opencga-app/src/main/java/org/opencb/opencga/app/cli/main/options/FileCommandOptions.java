package org.opencb.opencga.app.cli.main.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser.OpencgaCommonCommandOptions;
import org.opencb.opencga.catalog.models.File;

import java.util.List;

/**
 * Created by sgallego on 6/14/16.
 */
@Parameters(commandNames = {"projects"}, commandDescription = "Project commands")
public class FileCommandOptions {

    public CreateCommandOptions createCommandOptions;
    public CreateFolderCommandOptions createFolderCommandOptions;
    public InfoCommandOptions infoCommandOptions;
    public DownloadCommandOptions downloadCommandOptions;
    public GrepCommandOptions grepCommandOptions;
    public SearchCommandOptions searchCommandOptions;
    public ListCommandOptions listCommandOptions;
    public IndexCommandOptions indexCommandOptions;
    public AlignamentsCommandOptions alignamentsCommandOptions;
    public FetchCommandOptions fetchCommandOptions;
    //final VariantsCommand variantsCommand;
    public ShareCommandOptions shareCommandOptions;
    public UpdateCommandOptions updateCommandOptions;
    public RelinkCommandOptions relinkCommandOptions;
    public DeleteCommandOptions deleteCommandOptions;
    public RefreshCommandOptions refreshCommandOptions;
    public UnlinkCommandOptions unlinkCommandOptions;
    public LinkCommandOptions linkCommandOptions;
    public UploadCommandOptions uploadCommandOptions;

    public JCommander jCommander;
    public OpencgaCommonCommandOptions commonCommandOptions;

    public FileCommandOptions(OpencgaCommonCommandOptions commonCommandOptions, JCommander jCommander) {
        this.commonCommandOptions = commonCommandOptions;
        this.jCommander = jCommander;


        this.createCommandOptions = new CreateCommandOptions();
        this.createFolderCommandOptions = new CreateFolderCommandOptions();
        this.infoCommandOptions = new InfoCommandOptions();
        this.downloadCommandOptions = new DownloadCommandOptions();
        this.grepCommandOptions = new GrepCommandOptions();
        this.searchCommandOptions = new SearchCommandOptions();
        this.listCommandOptions = new ListCommandOptions();
        this.indexCommandOptions = new IndexCommandOptions();
        this.alignamentsCommandOptions = new AlignamentsCommandOptions();
        this.fetchCommandOptions = new FetchCommandOptions();
        this.shareCommandOptions = new ShareCommandOptions();
        this.updateCommandOptions = new UpdateCommandOptions();
        this.relinkCommandOptions = new RelinkCommandOptions();
        this.deleteCommandOptions = new DeleteCommandOptions();
        this.refreshCommandOptions = new RefreshCommandOptions();
        this.unlinkCommandOptions = new UnlinkCommandOptions();
        this.linkCommandOptions = new LinkCommandOptions();
        this.uploadCommandOptions = new UploadCommandOptions();

    }

    class BaseFileCommand {
        @ParametersDelegate
        OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-id", "--file-id"}, description = "File id", required = true, arity = 1)
        public String id;
    }

    @Parameters(commandNames = {"create"}, commandDescription = "Create file")
    public class CreateCommandOptions {

        @ParametersDelegate
        OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "Input file", required = true, arity = 1)
        String inputFile;

        @Parameter(names = {"-s", "--study-id"}, description = "studyId", required = true, arity = 1)
        String studyId;

        @Parameter(names = {"--path"}, description = "Directory where to create the file", required = false, arity = 1)
        public String path  = "";

        @Parameter(names = {"-d", "--description"}, description = "Description", required = false, arity = 1)
        String description;

        @Parameter(names = {"-f", "--format"}, description = "one of {PLAIN, GZIP, BINARY, EXECUTABLE, IMAGE}. See catalog.models.File.Format", required = false, arity = 1)
        File.Format format = File.Format.PLAIN;

        @Parameter(names = {"-b", "--bioformat"}, description = "See catalog.models.File.Bioformat for more info", required = false, arity = 1)
        File.Bioformat bioformat = File.Bioformat.NONE;

        @Parameter(names = {"-P", "--parents"}, description = "Create parent directories if needed", required = false)
        boolean parents;

        @Parameter(names = {"-m", "--move"}, description = "Move file instead of copy", required = false, arity = 0)
        boolean move;

        @Parameter(names = {"-ch", "--checksum"}, description = "Calculate checksum", required = false, arity = 0)
        boolean calculateChecksum = false;
    }

    @Parameters(commandNames = {"create-folder"}, commandDescription = "Create Folder")
    class CreateFolderCommandOptions {

        @ParametersDelegate
        OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--path"}, description = "New folder path", required = true, arity = 1)
        String path  = "";

        @Parameter(names = {"-s", "--study-id"}, description = "studyId", required = true, arity = 1)
        String studyId;

        @Parameter(names = {"-P", "--parents"}, description = "Create parent directories if needed", required = false)
        boolean parents = true;
    }

    @Parameters(commandNames = {"info"}, commandDescription = "Get file information")
    public class InfoCommandOptions extends BaseFileCommand {}

    @Parameters(commandNames = {"download"}, commandDescription = "Download file")
    class DownloadCommandOptions extends BaseFileCommand {}

    @Parameters(commandNames = {"grep"}, commandDescription = "Get file information")
    class GrepCommandOptions extends BaseFileCommand {
        @Parameter(names = {"--pattern"}, description = "Pattern", required = false, arity = 1)
        String pattern  = "";

        @Parameter(names = {"-ic", "--ignoreCase"}, description = "ignoreCase", required = false, arity = 0)
        boolean ignoreCase = false;

        @Parameter(names = {"-m", "--multi"}, description = "multi", required = false, arity = 0)
        boolean multi = true;
    }

    @Parameters(commandNames = {"search"}, commandDescription = "Search files")
    class SearchCommandOptions {

        @ParametersDelegate
        OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-s", "--study-id"}, description = "Study id", required = true, arity = 1)
        String studyId;
        //            @Parameter(names = {"--name"}, description = "Exact file name", required = false, arity = 1)
//            String name;
//            @Parameter(names = {"--path"}, description = "Exact file path", required = false, arity = 1)
//            String path;
        @Parameter(names = {"--name"}, description = "File name. Use regex pattern", required = false, arity = 1)
        String name;

        @Parameter(names = {"--directory"}, description = "Directory path (study relative). Use regex pattern", required = false, arity = 1)
        String directory;

        @Parameter(names = {"--type"}, description = "File type. CSV", required = false, arity = 1)
        List<File.Type> types;

        @Parameter(names = {"--bioformat"}, description = "File bioformat. CSV", required = false, arity = 1)
        List<File.Bioformat> bioformats;

        @Parameter(names = {"--status"}, description = "File status. CSV", required = false, arity = 1)
        List<String> status;

    }

    @Parameters(commandNames = {"list"}, commandDescription = "List files in folder")
    public class ListCommandOptions extends BaseFileCommand {
        @Parameter(names = {"--level"}, description = "Descend only level directories deep.", arity = 1)
        public int level = Integer.MAX_VALUE;

        @Parameter(names = {"-R", "--recursive"}, description = "List subdirectories recursively", arity = 0)
        public boolean recursive = false;

        @Parameter(names = {"-U", "--show-uris"}, description = "Show uris from linked files and folders", arity = 0)
        public boolean uries = false;
    }

    @Parameters(commandNames = {"index"}, commandDescription = "Index file in the selected StorageEngine")
    class IndexCommandOptions extends BaseFileCommand {
        @Parameter(names = {"-o", "--outdir-id"}, description = "Directory ID where to create the file", required = false, arity = 1)
        String outdir = "";

        @Parameter(names = {"--enqueue"}, description = "Enqueue the job to be launched by the execution manager", arity = 0)
        boolean enqueue;

        @Parameter(names = "--transform", description = "Run only the transform phase")
        boolean transform = false;

        @Parameter(names = "--load", description = "Run only the load phase")
        boolean load = false;

        @Parameter(names = "--calculate-stats", description = "Calculate stats for cohort ALL", arity = 0)
        boolean calculateStats = false;

        @Parameter(names = "--annotate", description = "Annotate new variants", arity = 0)
        boolean annotate = false;

        @Parameter(description = " -- {opencga-storage internal parameter. Use your head}") //Wil contain args after "--"
        public List<String> dashDashParameters;
    }

    @Parameters(commandNames = {"alignaments"}, commandDescription = "Fetch alignments from a BAM file")
    class AlignamentsCommandOptions extends BaseFileCommand { }


    @Parameters(commandNames = {"fetch"}, commandDescription = "File fetch")
    class FetchCommandOptions extends BaseFileCommand {
        @Parameter(names = {"--region"}, description = "Region", required = true, arity = 1)
        String region;

        @Parameter(names = {"--view_as_pairs"}, description = "View_as_pairs", required = false, arity = 0)
        boolean view_as_pairs = false;

        @Parameter(names = {"--include_coverage"}, description = "Include_coverage", required = false, arity = 0)
        boolean include_coverage = true;

        @Parameter(names = {"--process_differences"}, description = "Process_differences", required = false, arity = 0)
        boolean process_differences = true;

        @Parameter(names = {"--histogram"}, description = "Histogram", required = false, arity = 0)
        boolean histogram = false;

        @Parameter(names = {"--groupBy"}, description = "GroupBy: [ct, gene, ensemblGene]", required = false, arity = 1)
        String groupBy;

        @Parameter(names = {"--variantSource"}, description = "VariantSource", required = false, arity = 0)
        boolean variantSource = false;

        @Parameter(names = {"--interval"}, description = "Interval", required = false, arity = 1)
        String interval;

    }

    @Parameters(commandNames = {"share"}, commandDescription = "Share file with other user")
    class ShareCommandOptions  {

        @ParametersDelegate
        OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-fids","--fileIds"}, description = "fileIds", required = true)
        String fileIds;

        @Parameter(names = {"-uids","--userIds"}, description = "User you want to share the file with. Accepts: '{userId}', '@{groupId}' or '*'", required = true)
        String userIds;

        @Parameter(names = {"--unshare"}, description = "Remove the previous AclEntry", required = false, arity = 0)
        boolean unshare = false;

        @Parameter(names = {"--read"}, description = "Read permission", required = false, arity = 0)
        boolean read = false;

        @Parameter(names = {"--write"}, description = "Write permission", required = false, arity = 0)
        boolean write = false;

        @Parameter(names = {"--delete"}, description = "Delete permission", required = false, arity = 0)
        boolean delete = false;

    }


    @Parameters(commandNames = {"update"}, commandDescription = "Modify file")
    class UpdateCommandOptions extends BaseFileCommand { }

    @Parameters(commandNames = {"relink"}, commandDescription = "Change file location. Provided file must be either STAGED or an external file")
    class RelinkCommandOptions extends BaseFileCommand {
        @Parameter(names = {"-i", "--input"}, description = "File location", required = true, arity = 1)
        String inputFile;


        @Parameter(names = {"-ch", "--checksum"}, description = "Calculate checksum", required = false, arity = 0)
        boolean calculateChecksum = false;
    }


    @Parameters(commandNames = {"delete"}, commandDescription = "Delete file")
    class DeleteCommandOptions extends BaseFileCommand { }


    @Parameters(commandNames = {"refresh"}, commandDescription = "Refresh metadata from the selected file or folder. Print updated files.")
    class RefreshCommandOptions extends BaseFileCommand { }

    @Parameters(commandNames = {"unlink"}, commandDescription = "Unlink an external file from catalog")
    class UnlinkCommandOptions extends BaseFileCommand { }


    @Parameters(commandNames = {"link"}, commandDescription = "Link an external file into catalog.")
    public class LinkCommandOptions {

        @ParametersDelegate
        OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        // @Parameter(names = {"-i", "--input"}, description = "File location", required = true, arity = 1)
        // String inputFile;
        @Parameter(names = {"-uri"}, description = "File location", required = true, arity = 1)
        public String uri;

        @Parameter(names = {"-s", "--study-id"}, description = "Study identifier", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"-d", "--description"}, description = "Description", required = false, arity = 1)
        String description;

        @Parameter(names = {"--path"}, description = "New folder path", required = false, arity = 1)
        public String path  = "";

        @Parameter(names = {"-P", "--parents"}, description = "Create parent directories if needed", required = false)
        boolean parents;

        @Parameter(names = {"-ch", "--checksum"}, description = "Calculate checksum", required = false, arity = 0)
        boolean calculateChecksum = false;
    }

    @Parameters(commandNames = {"upload"}, commandDescription = "Attach a physical file to a catalog entry file.")
    class UploadCommandOptions extends BaseFileCommand{

        @Parameter(names = {"-i", "--input"}, description = "Input file", required = true, arity = 1)
        String inputFile;

        @Parameter(names = {"--replace"}, description = "Replace the existing attached file. ALERT: The existing file will be removed", required = false, arity = 0)
        boolean replace = false;

        @Parameter(names = {"-m", "--move"}, description = "Move file instead of copy", required = false, arity = 0)
        boolean move = false;

        @Parameter(names = {"-ch", "--checksum"}, description = "Calculate checksum", required = false, arity = 0)
        boolean calculateChecksum = false;

    }


}
