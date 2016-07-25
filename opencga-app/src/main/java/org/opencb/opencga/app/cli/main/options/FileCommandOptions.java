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
@Parameters(commandNames = {"files"}, commandDescription = "Files commands")
public class FileCommandOptions {

    public JCommander jCommander;
    public OpencgaCommonCommandOptions commonCommandOptions;

    public CopyCommandOptions copyCommandOptions;
    public CreateFolderCommandOptions createFolderCommandOptions;
    public InfoCommandOptions infoCommandOptions;
    public DownloadCommandOptions downloadCommandOptions;
    public GrepCommandOptions grepCommandOptions;
    public SearchCommandOptions searchCommandOptions;
    public ListCommandOptions listCommandOptions;
    public IndexCommandOptions indexCommandOptions;
    public AlignmentCommandOptions alignmentCommandOptions;
    public FetchCommandOptions fetchCommandOptions;
    //final VariantsCommand variantsCommand;
    public UpdateCommandOptions updateCommandOptions;
    public UploadCommandOptions uploadCommandOptions;
    public DeleteCommandOptions deleteCommandOptions;
    public LinkCommandOptions linkCommandOptions;
    public RelinkCommandOptions relinkCommandOptions;
    public UnlinkCommandOptions unlinkCommandOptions;
    public RefreshCommandOptions refreshCommandOptions;
    public GroupByCommandOptions groupByCommandOptions;

    public FileCommandOptions(OpencgaCommonCommandOptions commonCommandOptions, JCommander jCommander) {

        this.commonCommandOptions = commonCommandOptions;
        this.jCommander = jCommander;

        this.copyCommandOptions = new CopyCommandOptions();
        this.createFolderCommandOptions = new CreateFolderCommandOptions();
        this.infoCommandOptions = new InfoCommandOptions();
        this.downloadCommandOptions = new DownloadCommandOptions();
        this.grepCommandOptions = new GrepCommandOptions();
        this.searchCommandOptions = new SearchCommandOptions();
        this.listCommandOptions = new ListCommandOptions();
        this.indexCommandOptions = new IndexCommandOptions();
        this.alignmentCommandOptions = new AlignmentCommandOptions();
        this.fetchCommandOptions = new FetchCommandOptions();
        this.updateCommandOptions = new UpdateCommandOptions();
        this.relinkCommandOptions = new RelinkCommandOptions();
        this.deleteCommandOptions = new DeleteCommandOptions();
        this.refreshCommandOptions = new RefreshCommandOptions();
        this.unlinkCommandOptions = new UnlinkCommandOptions();
        this.linkCommandOptions = new LinkCommandOptions();
        this.uploadCommandOptions = new UploadCommandOptions();
        this.groupByCommandOptions = new GroupByCommandOptions();
    }

    public class BaseFileCommand {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--file-id"}, description = "File id", required = true, arity = 1)
        public String id;
    }


    @Parameters(commandNames = {"copy"}, commandDescription = "Copy a file or folder")
    public class CopyCommandOptions {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--path"}, description = "Directory where to create the file", required = false, arity = 1)
        public String path;

        @Parameter(names = {"-i","--input"}, description = "Input file", required = true, arity = 1)
        public String inputFile;

        @Parameter(names = {"-s", "--study-id"}, description = "studyId", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"-d", "--description"}, description = "Description", required = false, arity = 1)
        public String description;

        @Parameter(names = {"-f", "--format"}, description = "one of {PLAIN, GZIP, BINARY, EXECUTABLE, IMAGE}. See File.Format", arity = 1)
        public File.Format format = File.Format.PLAIN;

        @Parameter(names = {"-b", "--bioformat"}, description = "See File.Bioformat for more info", required = false, arity = 1)
        public File.Bioformat bioformat = File.Bioformat.NONE;

        @Parameter(names = {"-P", "--parents"}, description = "Create parent directories if needed", required = false)
        public boolean parents;

        @Parameter(names = {"-m", "--move"}, description = "Move file instead of copy", required = false, arity = 0)
        public boolean move;

        @Parameter(names = {"--checksum"}, description = "Calculate checksum", required = false, arity = 0)
        public boolean calculateChecksum;
    }


    @Parameters(commandNames = {"create-folder"}, commandDescription = "Create Folder")
    public class CreateFolderCommandOptions {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--path"}, description = "New folder path", required = true, arity = 1)
        public String path = "";

        @Parameter(names = {"-s", "--study-id"}, description = "studyId", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"-P", "--parents"}, description = "Create parent directories if needed", required = false)
        public boolean parents = true;
    }


    @Parameters(commandNames = {"info"}, commandDescription = "Get file information")
    public class InfoCommandOptions extends BaseFileCommand {
    }


    @Parameters(commandNames = {"download"}, commandDescription = "Download file")
    class DownloadCommandOptions extends BaseFileCommand {
    }


    @Parameters(commandNames = {"grep"}, commandDescription = "Get file information")
    public class GrepCommandOptions extends BaseFileCommand {

        @Parameter(names = {"--pattern"}, description = "Pattern", required = false, arity = 1)
        public String pattern;

        @Parameter(names = {"--ignore-case"}, description = "ignoreCase", required = false, arity = 0)
        public boolean ignoreCase;

        @Parameter(names = {"-m", "--multi"}, description = "multi", required = false, arity = 0)
        public boolean multi;
    }

    @Parameters(commandNames = {"search"}, commandDescription = "Search files")
    public class SearchCommandOptions {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-s", "--study-id"}, description = "Study id", required = true, arity = 1)
        public String studyId;
        //            @Parameter(names = {"--name"}, description = "Exact file name", required = false, arity = 1)
//            public String name;
//            @Parameter(names = {"--path"}, description = "Exact file path", required = false, arity = 1)
//            public String path;
        @Parameter(names = {"--name"}, description = "File name. Use regex pattern", required = false, arity = 1)
        public String name;

        @Parameter(names = {"--directory"}, description = "Directory path (study relative). Use regex pattern", required = false, arity = 1)
        public String directory;

        @Parameter(names = {"--type"}, description = "File type. CSV", required = false, arity = 1)
        public List<File.Type> types;

        @Parameter(names = {"--bioformat"}, description = "File bioformat. CSV", required = false, arity = 1)
        public List<File.Bioformat> bioformats;

        @Parameter(names = {"--status"}, description = "File status. CSV", required = false, arity = 1)
        public List<String> status;
    }


    @Parameters(commandNames = {"list"}, commandDescription = "List files in folder")
    public class ListCommandOptions extends BaseFileCommand {

        @Parameter(names = {"--level"}, description = "Descend only level directories deep.", arity = 1)
        public int level = 1;

        @Parameter(names = {"-R", "--recursive"}, description = "List subdirectories recursively", arity = 0)
        public boolean recursive;

        @Parameter(names = {"-U", "--show-uris"}, description = "Show uris from linked files and folders", arity = 0)
        public boolean uries;
    }


    @Parameters(commandNames = {"index"}, commandDescription = "Index file in the selected StorageEngine")
    public class IndexCommandOptions extends BaseFileCommand {

    //     @Parameter(description = " -- {opencga-storage internal parameter. Use your head}") //Wil contain args after "--"
    //    public List<String> dashDashParameters;

        @Parameter(names = {"-o", "--outdir-id"}, description = "Directory ID where to create the file", required = false, arity = 1)
        public String outdir = "";

    //     @Parameter(names = {"--enqueue"}, description = "Enqueue the job to be launched by the execution manager", arity = 0)
    //    public boolean enqueue;

    //    @Parameter(names = "--transform", description = "Run only the transform phase")
    //    public boolean transform = false;

    //    @Parameter(names = "--load", description = "Run only the load phase")
    //    public boolean load = false;

        @Parameter(names = "--calculate-stats", description = "Calculate stats for cohort ALL", arity = 0)
        public boolean calculateStats;

        @Parameter(names = "--annotate", description = "Annotate new variants", arity = 0)
        public boolean annotate;
    }


    @Parameters(commandNames = {"alignments"}, commandDescription = "Fetch alignments from a BAM file")
    public class AlignmentCommandOptions extends BaseFileCommand {
    }

    @Deprecated
    @Parameters(commandNames = {"fetch"}, commandDescription = "File fetch")
    public class FetchCommandOptions extends BaseFileCommand {

        @Parameter(names = {"--region"}, description = "Region", required = true, arity = 1)
        public String region;

        @Parameter(names = {"--view-as-pairs"}, description = "View_as_pairs", required = false, arity = 0)
        public boolean viewAsPairs;

        @Parameter(names = {"--include-coverage"}, description = "Include_coverage", required = false, arity = 0)
        public boolean includeCoverage;

        @Parameter(names = {"--process-differences"}, description = "Process_differences", required = false, arity = 0)
        public boolean processDifferences;

        @Parameter(names = {"--histogram"}, description = "Histogram", required = false, arity = 0)
        public boolean histogram;

        @Parameter(names = {"--group-by"}, description = "GroupBy: [ct, gene, ensemblGene]", required = false, arity = 1)
        public String groupBy;

        @Parameter(names = {"--variant-source"}, description = "Variant Source", required = false, arity = 0)
        public boolean variantSource;

        @Parameter(names = {"--interval"}, description = "Interval", required = false, arity = 1)
        public String interval;
    }

    @Parameters(commandNames = {"update"}, commandDescription = "Modify file")
    class UpdateCommandOptions extends BaseFileCommand {
    }


    @Parameters(commandNames = {"relink"}, commandDescription = "Change file location. Provided file must be either STAGED or an external" +
            " file")
    class RelinkCommandOptions extends BaseFileCommand {

        @Parameter(names = {"-i", "--input"}, description = "File location", required = true, arity = 1)
        public String inputFile;

        @Parameter(names = {"-ch", "--checksum"}, description = "Calculate checksum", required = false, arity = 0)
        public boolean calculateChecksum;
    }


    @Parameters(commandNames = {"delete"}, commandDescription = "Delete file")
    public class DeleteCommandOptions extends BaseFileCommand {

        @Parameter(names = {"--delete-external"}, description = "Boolean indicating whether to delete external files from disk as well"
                + " (only applicable for linked files/folders)", required = false, arity = 0)
        public boolean deleteExternal;

        @Parameter(names = {"--skip-trash"}, description = "Boolean indicating whether to skip sending the files to the trash first. "
                + "If set, it will not be possible to recover them!", required = false, arity = 0)
        public boolean skipTrash;

    }


    @Parameters(commandNames = {"refresh"}, commandDescription = "Refresh metadata from the selected file or folder. Print updated files.")
    public class RefreshCommandOptions extends BaseFileCommand {
    }


    @Parameters(commandNames = {"unlink"}, commandDescription = "Unlink an external file from catalog")
    public class UnlinkCommandOptions extends BaseFileCommand {
    }


    @Parameters(commandNames = {"link"}, commandDescription = "Link an external file or folder into catalog.")
    public class LinkCommandOptions {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

         @Parameter(names = {"-i", "--input"}, description = "File or folder location", required = true, arity = 1)
         public String input;
//        @Parameter(names = {"-uri"}, description = "File location", required = true, arity = 1)
//        public String uri;

        @Parameter(names = {"-s", "--study-id"}, description = "Study where the file or folder will be associated to", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"--path"}, description = "Virtual path within catalog where the file or folder will be linked (root folder if empty)", required = false, arity = 1)
        public String path;

        @Parameter(names = {"-d", "--description"}, description = "Brief description that will be attached to the files in catalog", required = false, arity = 1)
        public String description;

        @Parameter(names = {"-P", "--parents"}, description = "Create parent directories if needed", required = false)
        public boolean parents;

    }


    @Parameters(commandNames = {"upload"}, commandDescription = "Upload a physical local file to catalog.")
    public class UploadCommandOptions {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-s", "--study-id"}, description = "Study where the file will be associated to", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"-i","--input"}, description = "Input file", required = true, arity = 1)
        public String inputFile;

        @Parameter(names = {"--fileFormat"}, description = "Format of the file (VCF, BCF, GVCF, SAM, BAM, BAI...UNKNOWN)", required = true, arity = 1)
        public String fileFormat;

        @Parameter(names = {"--bioFormat"}, description = "Bioformat of the file (VARIANT, ALIGNMENT, SEQUENCE, PEDIGREE...NONE)", required = true, arity = 1)
        public String bioFormat;

        @Parameter(names = {"--catalogPath"}, description = "Path within catalog where the file will be located (Default: root folder)", required = false, arity = 1)
        public String catalogPath;

        @Parameter(names = {"--description"}, description = "Description of the file", required = false, arity = 1)
        public String description;

        @Parameter(names = {"--fileName"}, description = "Name of the file by which it will be stored in catalog", required = false, arity = 1)
        public String fileName;

        @Parameter(names = {"-P", "--parents"}, description = "Create parent directories if needed", required = false)
        public boolean parents;

        @Parameter(names = {"--replace"}, description = "[PENDING] Replace the existing attached file. ALERT: The existing file will be removed",
                required = false, arity = 0)
        public boolean replace;

        @Parameter(names = {"-ch", "--checksum"}, description = "[PENDING] Calculate checksum", required = false, arity = 0)
        public boolean calculateChecksum;

    }

    @Parameters(commandNames = {"group-by"}, commandDescription = "GroupBy cohort")
    public class GroupByCommandOptions {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--fields"}, description = "Comma separated list of fields by which to group by.", required = true, arity = 1)
        public String fields;

        @Parameter(names = {"--study-id"}, description = "Study id", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"--id"}, description = "Comma separated list of ids.", required = false, arity = 1)
        public String id;

        @Parameter(names = {"--name"}, description = "Comma separated list of names.", required = false, arity = 1)
        public String name;

        @Parameter(names = {"--path"}, description = "Path.", required = false, arity = 1)
        public String path;

        @Parameter(names = {"--type"}, description = "Comma separated Type values.", required = false, arity = 1)
        public String type;

        @Parameter(names = {"--format"}, description = "Comma separated Format values.", required = false, arity = 1)
        public String format;

        @Parameter(names = {"--status"}, description = "Status.", required = false, arity = 1)
        public String status;

        @Parameter(names = {"--directory"}, description = "Directory.", required = false, arity = 1)
        public String directory;

        @Parameter(names = {"--owner-id"}, description = "Owner id.", required = false, arity = 1)
        public String ownerId;

        @Parameter(names = {"--creation-date"}, description = "Creation date.", required = false, arity = 1)
        public String creationDate;

        @Parameter(names = {"--modification-date"}, description = "Modification date.", required = false, arity = 1)
        public String modificationDate;

        @Parameter(names = {"-d", "--description"}, description = "Description", required = false, arity = 1)
        public String description;

        @Parameter(names = {"--disk-usage"}, description = "diskUsage.", required = false, arity = 1)
        public Integer diskUsage;

        @Parameter(names = {"--sample-ids"}, description = "Sample ids", required = false, arity = 1)
        public String sampleIds;

        @Parameter(names = {"--job-id"}, description = "Job id", required = false, arity = 1)
        public String jobId;

        @Parameter(names = {"--attributes"}, description = "Attributes", required = false, arity = 1)
        public String attributes;

        @Parameter(names = {"--nattributes"}, description = "numerical attributes", required = false, arity = 1)
        public String nattributes;
    }
}
