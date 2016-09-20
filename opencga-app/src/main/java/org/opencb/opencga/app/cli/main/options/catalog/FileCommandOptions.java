package org.opencb.opencga.app.cli.main.options.catalog;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser.OpencgaCommonCommandOptions;
import org.opencb.opencga.app.cli.main.options.commons.AclCommandOptions;
import org.opencb.opencga.catalog.models.File;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by sgallego on 6/14/16.
 */
@Parameters(commandNames = {"files"}, commandDescription = "Files commands")
public class FileCommandOptions {

    public CopyCommandOptions copyCommandOptions;
    public CreateFolderCommandOptions createFolderCommandOptions;
    public InfoCommandOptions infoCommandOptions;
    public DownloadCommandOptions downloadCommandOptions;
    public GrepCommandOptions grepCommandOptions;
    public SearchCommandOptions searchCommandOptions;
    public ListCommandOptions listCommandOptions;
    public IndexCommandOptions indexCommandOptions;
    public TreeViewCommandOptions treeViewCommandOptions;
    public AlignmentCommandOptions alignmentCommandOptions;
    public ContentCommandOptions contentCommandOptions;
    public FetchCommandOptions fetchCommandOptions;
    public UpdateCommandOptions updateCommandOptions;
    public UploadCommandOptions uploadCommandOptions;
    public DeleteCommandOptions deleteCommandOptions;
    public LinkCommandOptions linkCommandOptions;
    public RelinkCommandOptions relinkCommandOptions;
    public UnlinkCommandOptions unlinkCommandOptions;
    public RefreshCommandOptions refreshCommandOptions;
    public GroupByCommandOptions groupByCommandOptions;
    public VariantsCommandOptions variantsCommandOptions;

    public AclCommandOptions.AclsCommandOptions aclsCommandOptions;
    public AclCommandOptions.AclsCreateCommandOptions aclsCreateCommandOptions;
    public AclCommandOptions.AclsMemberDeleteCommandOptions aclsMemberDeleteCommandOptions;
    public AclCommandOptions.AclsMemberInfoCommandOptions aclsMemberInfoCommandOptions;
    public AclCommandOptions.AclsMemberUpdateCommandOptions aclsMemberUpdateCommandOptions;

    public JCommander jCommander;
    public OpencgaCommonCommandOptions commonCommandOptions;

    private AclCommandOptions aclCommandOptions;

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
        this.treeViewCommandOptions = new TreeViewCommandOptions();
        this.alignmentCommandOptions = new AlignmentCommandOptions();
        this.contentCommandOptions = new ContentCommandOptions();
        this.fetchCommandOptions = new FetchCommandOptions();
        this.updateCommandOptions = new UpdateCommandOptions();
        this.relinkCommandOptions = new RelinkCommandOptions();
        this.deleteCommandOptions = new DeleteCommandOptions();
        this.refreshCommandOptions = new RefreshCommandOptions();
        this.unlinkCommandOptions = new UnlinkCommandOptions();
        this.linkCommandOptions = new LinkCommandOptions();
        this.uploadCommandOptions = new UploadCommandOptions();
        this.groupByCommandOptions = new GroupByCommandOptions();
        this.variantsCommandOptions = new VariantsCommandOptions();

        aclCommandOptions = new AclCommandOptions(commonCommandOptions);
        this.aclsCommandOptions = aclCommandOptions.getAclsCommandOptions();
        this.aclsCreateCommandOptions = aclCommandOptions.getAclsCreateCommandOptions();
        this.aclsMemberDeleteCommandOptions = aclCommandOptions.getAclsMemberDeleteCommandOptions();
        this.aclsMemberInfoCommandOptions = aclCommandOptions.getAclsMemberInfoCommandOptions();
        this.aclsMemberUpdateCommandOptions = aclCommandOptions.getAclsMemberUpdateCommandOptions();
    }

    public class BaseFileCommand {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--id"}, description = "File id", required = true, arity = 1)
        public String id;
    }

    @Deprecated
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

        @Parameter(names = {"-s", "--study-id"}, description = "Study Id", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"--folder"}, description = "Path where the folder is to be created", required = true, arity = 1)
        public String folder = "";

        @Parameter(names = {"-P", "--parents"}, description = "Create the parent directories if they do not exist. Default: false",
                required = false, arity = 0)
        public boolean parents = false;
    }

    @Parameters(commandNames = {"info"}, commandDescription = "Get file information")
    public class InfoCommandOptions {

        @Parameter(names = {"--file-ids"}, description = "Comma separated list of file ids", required = true, arity = 1)
        public String fileIds;

        @Parameter(names = {"--include"}, description = "Comma separated list of fields to be included in the response", arity = 1)
        public String include;

        @Parameter(names = {"--exclude"}, description = "Comma separated list of fields to be excluded from the response", arity = 1)
        public String exclude;
    }

    @Parameters(commandNames = {"download"}, commandDescription = "Download file")
    public class DownloadCommandOptions extends BaseFileCommand {

    }

    @Parameters(commandNames = {"grep"}, commandDescription = "Get file information")
    public class GrepCommandOptions extends BaseFileCommand {

        @Parameter(names = {"--pattern"}, description = "Pattern", required = false, arity = 1)
        public String pattern = ".*";

        @Parameter(names = {"--ignore-case"}, description = "Do a case insensitive search", required = false, arity = 0)
        public boolean ignoreCase;

        @Parameter(names = {"-m", "--multi"}, description = "Return multiple matches. Default = true", required = false, arity = 0)
        public boolean multi = true;
    }

    @Parameters(commandNames = {"search"}, commandDescription = "Search files")
    public class SearchCommandOptions {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--id"}, description = "Comma separated list of ids", arity = 1)
        public String id;

        @Parameter(names = {"-s", "--study-id"}, description = "Study id", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"--name"}, description = "Comma separated list of file names", required = false, arity = 1)
        public String name;

        @Parameter(names = {"--path"}, description = "Comma separated list of paths", required = false, arity = 1)
        public String path;

        @Parameter(names = {"--type"}, description = "File type. CSV", required = false, arity = 1)
        public List<File.Type> type;

        @Parameter(names = {"--bioformat"}, description = "File bioformat. CSV", required = false, arity = 1)
        public List<File.Bioformat> bioformat;

        @Parameter(names = {"--format"}, description = "Comma separated Format values.", required = false, arity = 1)
        public String format;

        @Parameter(names = {"--status"}, description = "Status.", required = false, arity = 1)
        public String status;

        @Parameter(names = {"--directory"}, description = "Directory under which we want to look for files or folders.", required = false,
                arity = 1)
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
        public String diskUsage;

        @Parameter(names = {"--sample-ids"}, description = "Sample ids", required = false, arity = 1)
        public String sampleIds;

        @Parameter(names = {"--job-id"}, description = "Job id", required = false, arity = 1)
        public String jobId;

        @Parameter(names = {"--attributes"}, description = "Attributes", required = false, arity = 1)
        public String attributes;

        @Parameter(names = {"--nattributes"}, description = "numerical attributes", required = false, arity = 1)
        public String nattributes;

        @Parameter(names = {"--include"}, description = "Comma separated list of fields to be included in the response", arity = 1)
        public String include;

        @Parameter(names = {"--exclude"}, description = "Comma separated list of fields to be excluded from the response", arity = 1)
        public String exclude;

        @Parameter(names = {"--skip"}, description = "Number of results to skip", arity = 1)
        public String skip;

        @Parameter(names = {"--limit"}, description = "Maximum number of results to be returned", arity = 1)
        public String limit;

        @Parameter(names = {"--count"}, description = "Total number of results.", arity = 0)
        public boolean count;
    }

    @Parameters(commandNames = {"list"}, commandDescription = "List files in folder")
    public class ListCommandOptions {

        @Parameter(names = {"--folder-id"}, description = "Folder Id", arity = 1, required = true)
        public String folderId;

        @Parameter(names = {"--include"}, description = "Comma separated list of fields to be included in the response", arity = 1)
        public String include;

        @Parameter(names = {"--exclude"}, description = "Comma separated list of fields to be excluded from the response", arity = 1)
        public String exclude;

        @Parameter(names = {"--skip"}, description = "Number of results to skip", arity = 1)
        public String skip;

        @Parameter(names = {"--limit"}, description = "Maximum number of results to be returned", arity = 1)
        public String limit;

        @Parameter(names = {"--count"}, description = "Total number of results.", arity = 0)
        public boolean count;
    }

    @Parameters(commandNames = {"index"}, commandDescription = "Index file in the selected StorageEngine")
    public class IndexCommandOptions {

        @ParametersDelegate
        public OpencgaCliOptionsParser.OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--id"}, description = "Comma separated list of file ids (files or directories)", required = true, arity = 1)
        public String fileIds;

        @Parameter(names = {"-s", "--study-id"}, description = "The study id or alias where you want to load the data", arity = 1)
        public String studyId;

        @Parameter(names = {"--transform"}, description = "If present it only runs the transform stage, no load is executed")
        public boolean transform;

        @Parameter(names = {"--load"}, description = "If present only the load stage is executed, transformation is skipped")
        public boolean load;

        @Parameter(names = {"-o", "--outdir"}, description = "Catalog directory where transformed index files will be stored", arity = 1)
        public String outdir;

        @Parameter(names = {"--include-extra-fields"}, description = "Index including other genotype fields [CSV]")
        public String extraFields;

        @Parameter(names = {"--aggregated"}, description = "Select the type of aggregated VCF file: none, basic, EVS or ExAC", arity = 1)
        public String aggregated = "NONE";

        @Parameter(names = {"--aggregation-mapping-file"}, description = "File containing population names mapping in an aggregated VCF file", arity = 1)
        public String aggregationMappingFile;
//
//        @Parameter(names = {"--gvcf"}, description = "The input file is in gvcf format")
//        public boolean gvcf;
//
//        @Parameter(names = {"--bgzip"}, description = "[PENDING] The input file is in bgzip format")
//        public boolean bgzip;

        @Parameter(names = {"--calculate-stats"}, description = "Calculate indexed variants statistics after the load step")
        public boolean calculateStats;

        @Parameter(names = {"--annotate"}, description = "Annotate indexed variants after the load step")
        public boolean annotate;
//
//        @Parameter(names = {"--annotator"}, description = "Annotation source {cellbase_rest, cellbase_db_adaptor}")
//        public org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager.AnnotationSource annotator = null;

        @Parameter(names = {"--overwrite-annotations"}, description = "Overwrite annotations already present in variants")
        public boolean overwriteAnnotations;
//
//    //     @Parameter(description = " -- {opencga-storage internal parameter. Use your head}") //Wil contain args after "--"
//    //    public List<String> dashDashParameters;
//
//        @Parameter(names = {"-o", "--outdir-id"}, description = "Directory ID where to create the file", required = false, arity = 1)
//        public String outdir = "";
//
//    //     @Parameter(names = {"--enqueue"}, description = "Enqueue the job to be launched by the execution manager", arity = 0)
//    //    public boolean enqueue;
//
//    //    @Parameter(names = "--transform", description = "Run only the transform phase")
//    //    public boolean transform = false;
//
//    //    @Parameter(names = "--load", description = "Run only the load phase")
//    //    public boolean load = false;
//
//        @Parameter(names = "--calculate-stats", description = "Calculate stats for cohort ALL", arity = 0)
//        public boolean calculateStats;
//
//        @Parameter(names = "--annotate", description = "Annotate new variants", arity = 0)
//        public boolean annotate;
    }

    @Parameters(commandNames = {"alignments"}, commandDescription = "Fetch alignments from a BAM file")
    public class AlignmentCommandOptions extends BaseFileCommand {

        @Parameter(names = {"--include"}, description = "Comma separated list of fields to be included in the response", arity = 1)
        public String include;

        @Parameter(names = {"--exclude"}, description = "Comma separated list of fields to be excluded from the response", arity = 1)
        public String exclude;

        @Parameter(names = {"--skip"}, description = "Number of results to skip", arity = 1)
        public String skip;

        @Parameter(names = {"--limit"}, description = "Maximum number of results to be returned", arity = 1)
        public String limit;

        @Parameter(names = {"--count"}, description = "Total number of results.", arity = 0)
        public boolean count;
    }

    @Parameters(commandNames = {"content"}, commandDescription = "Show the content of a file (up to a limit)")
    public class ContentCommandOptions extends BaseFileCommand{
        @Parameter(names = {"--start"}, description = "Start", arity = 1)
        public Integer start = -1;

        @Parameter(names = {"--limit"}, description = "Limit", arity = 1)
        public Integer limit = -1;
    }

    @Deprecated
    @Parameters(commandNames = {"fetch"}, commandDescription = "File fetch")
    public class FetchCommandOptions extends BaseFileCommand {

        @Parameter(names = {"--region"}, description = "Region", required = true, arity = 1)
        public String region;

        @Parameter(names = {"--view-as-pairs"}, description = "View_as_pairs", required = false, arity = 0)
        public boolean viewAsPairs;

        @Parameter(names = {"--include-coverage"}, description = "Include_coverage. Default: true", required = false, arity = 0)
        public boolean includeCoverage = true;

        @Parameter(names = {"--process-differences"}, description = "Process_differences Default: true", required = false, arity = 0)
        public boolean processDifferences = true;

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
    public class UpdateCommandOptions extends BaseFileCommand {

        @Parameter(names = {"--name"}, description = "Name", required = false, arity = 1)
        public String name;

        @Parameter(names = {"--format"}, description = "Format of the file (VCF, BCF, GVCF, SAM, BAM, BAI...UNKNOWN)",
                required = false, arity = 1)
        public String format;

        @Parameter(names = {"--bioformat"}, description = "Bioformat of the file (VARIANT, ALIGNMENT, SEQUENCE, PEDIGREE...NONE)",
                required = false, arity = 1)
        public String bioformat;

        @Parameter(names = {"--description"}, description = "Description of the file", required = false, arity = 1)
        public String description;

        @Parameter(names = {"--attributes"}, description = "Attributes", required = false, arity = 1)
        public String attributes;

        @Parameter(names = {"--stats"}, description = "Stats", required = false, arity = 1)
        public String stats;

        @Parameter(names = {"--sample-ids"}, description = "sampleIds", required = false, arity = 1)
        public String sampleIds;

        @Parameter(names = {"--job-id"}, description = "Job id", required = false, arity = 1)
        public String jobId;

        @Parameter(names = {"--path"}, description = "Path", required = false, arity = 1)
        public String path;



    }

    @Parameters(commandNames = {"relink"}, commandDescription = "Change location of linked or STAGED file.")
    public class RelinkCommandOptions extends BaseFileCommand {

        @Parameter(names = {"--uri"}, description = "New URI", required = true, arity = 1)
        public String uri;

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

    @Parameters(commandNames = {"tree-view"}, commandDescription = "Obtain a tree view of the files and folders within a folder")
    public class TreeViewCommandOptions {

        @Parameter(names = {"--folder-id"}, description = "Folder id", required = true, arity = 1)
        public String folderId;

        @Parameter(names = {"--max-Depth"}, description = "Maximum depth to get files from. Default: 5", required = false, arity = 1)
        public Integer maxDepth = 5;

        @Parameter(names = {"--include"}, description = "Fields included in the response, whole JSON path must be provided",
                required = false, arity = 0)
        public String include;

        @Parameter(names = {"--exclude"}, description = "Fields excluded in the response, whole JSON path must be provided", arity = 1)
        public String exclude;

        /*@Parameter(names = {"--skip"}, description = "Number of results to skip", arity = 1)
        public String skip;*/

        @Parameter(names = {"--limit"}, description = "[TO BE IMPLEMENTED] Number of results to be returned in the queries", arity = 1)
        public String limit;

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

         @Parameter(names = {"-i", "--input"}, description = "File or folder location", required = true, variableArity = true)
         public List<String> inputs;
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
    //---- POST ---//
    @Parameters(commandNames = {"upload"}, commandDescription = "Upload a physical local file to catalog.")
    public class UploadCommandOptions {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-s", "--study-id"}, description = "Study where the file will be associated to", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"-i","--input"}, description = "Input file", required = true, arity = 1)
        public String inputFile;

        @Parameter(names = {"--file-format"}, description = "Format of the file (VCF, BCF, GVCF, SAM, BAM, BAI...UNKNOWN)", required = true, arity = 1)
        public String fileFormat;

        @Parameter(names = {"--bioformat"}, description = "Bioformat of the file (VARIANT, ALIGNMENT, SEQUENCE, PEDIGREE...NONE)", required = true, arity = 1)
        public String bioformat;

        @Parameter(names = {"--catalog-path"}, description = "Path within catalog where the file will be located (Default: root folder)", required = false, arity = 1)
        public String catalogPath;

        @Parameter(names = {"--description"}, description = "Description of the file", required = false, arity = 1)
        public String description;

        @Parameter(names = {"--file-name"}, description = "Name of the file by which it will be stored in catalog", required = false, arity = 1)
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

        @Parameter(names = {"--bioformat"}, description = "Comma separated Bioformat values.", required = false, arity = 1)
        public String bioformat;

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
        public String diskUsage;

        @Parameter(names = {"--sample-ids"}, description = "Sample ids", required = false, arity = 1)
        public String sampleIds;

        @Parameter(names = {"--job-id"}, description = "Job id", required = false, arity = 1)
        public String jobId;

        @Parameter(names = {"--attributes"}, description = "Attributes", required = false, arity = 1)
        public String attributes;

        @Parameter(names = {"--nattributes"}, description = "numerical attributes", required = false, arity = 1)
        public String nattributes;
    }

    @Parameters(commandNames = {"variants"}, commandDescription = "Fetch variants from a VCF/gVCF file")
    public class VariantsCommandOptions extends BaseFileCommand {

        @Parameter(names = {"--variant-ids"}, description = "List of variant ids", arity = 1)
        public String ids;

        @Parameter(names = {"--region"}, description = "List of regions: {chr}:{start}-{end}", arity = 1)
        public String region;

        @Parameter(names = {"--chromosome"}, description = "List of chromosomes", arity = 1)
        public String chromosome;

        @Parameter(names = {"--gene"}, description = "List of genes", arity = 1)
        public String gene;

        @Parameter(names = {"--type"}, description = "Variant type: [SNV, MNV, INDEL, SV, CNV]", arity = 1)
        public String type;

        @Parameter(names = {"--reference"}, description = "Reference allele", arity = 1)
        public String reference;

        @Parameter(names = {"--alternate"}, description = "Main alternate allele", arity = 1)
        public String alternate;

        @Parameter(names = {"--returned-studies"}, description = "List of studies to be returned", arity = 1)
        public String returnedStudies;

        @Parameter(names = {"--returned-samples"}, description = "List of samples to be returned", arity = 1)
        public String returnedSamples;

        @Parameter(names = {"--returned-files"}, description = "List of files to be returned.", arity = 1)
        public String returnedFiles;

        @Parameter(names = {"--files"}, description = "Variants in specific files", arity = 1)
        public String files;

        @Parameter(names = {"--maf"}, description = "Minor Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}",
                arity = 1)
        public String maf;

        @Parameter(names = {"--mgf"}, description = "Minor Genotype Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}",
                arity = 1)
        public String mgf;

        @Parameter(names = {"--missing-alleles"}, description = "Number of missing alleles: [{study:}]{cohort}[<|>|<=|>=]{number}",
                arity = 1)
        public String missingAlleles;

        @Parameter(names = {"--missing-genotypes"}, description = "Number of missing genotypes: [{study:}]{cohort}[<|>|<=|>=]{number}",
                arity = 1)
        public String missingGenotypes;

        @Parameter(names = {"--annotation-exists"}, description = "Specify if the variant annotation must exists.",
                arity = 0)
        public boolean annotationExists;

        @Parameter(names = {"--genotype"}, description = "Samples with a specific genotype: {samp_1}:{gt_1}(,{gt_n})*(;{samp_n}:{gt_1}"
                + "(,{gt_n})*)* e.g. HG0097:0/0;HG0098:0/1,1/1", arity = 1)
        public String genotype;

        @Parameter(names = {"--annot-ct"}, description = "Consequence type SO term list. e.g. SO:0000045,SO:0000046",
                arity = 1)
        public String annot_ct;

        @Parameter(names = {"--annot-xref"}, description = "XRef", arity = 1)
        public String annot_xref;

        @Parameter(names = {"--annot-biotype"}, description = "Biotype", arity = 1)
        public String annot_biotype;

        @Parameter(names = {"--polyphen"}, description = "Polyphen, protein substitution score. [<|>|<=|>=]{number} or [~=|=|]{description}"
                + " e.g. <=0.9 , =benign", arity = 1)
        public String polyphen;

        @Parameter(names = {"--sift"}, description = "Sift, protein substitution score. [<|>|<=|>=]{number} or [~=|=|]{description} "
                + "e.g. >0.1 , ~=tolerant", arity = 1)
        public String sift;

        @Parameter(names = {"--conservation"}, description = "VConservation score: {conservation_score}[<|>|<=|>=]{number} "
                + "e.g. phastCons>0.5,phylop<0.1,gerp>0.1", arity = 1)
        public String conservation;

        @Parameter(names = {"--annot-population-maf"}, description = "Population minor allele frequency: "
                + "{study}:{population}[<|>|<=|>=]{number}", arity = 1)
        public String annotPopulationMaf;

        @Parameter(names = {"--alternate-frequency"}, description = "Alternate Population Frequency: "
                + "{study}:{population}[<|>|<=|>=]{number}", arity = 1)
        public String alternate_frequency;

        @Parameter(names = {"--reference-frequency"}, description = "Reference Population Frequency:"
                + " {study}:{population}[<|>|<=|>=]{number}", arity = 1)
        public String reference_frequency;

        @Parameter(names = {"--annot-transcription-flags"}, description = "List of transcript annotation flags. "
                + "e.g. CCDS, basic, cds_end_NF, mRNA_end_NF, cds_start_NF, mRNA_start_NF, seleno", arity = 1)
        public String transcriptionFlags;

        @Parameter(names = {"--annot-gene-trait-id"}, description = "List of gene trait association id. e.g. \"umls:C0007222\" , "
                + "\"OMIM:269600\"", arity = 1)
        public String geneTraitId;

        @Parameter(names = {"--annot-gene-trait-name"}, description = "List of gene trait association names. "
                + "e.g. \"Cardiovascular Diseases\"", arity = 1)
        public String geneTraitName;

        @Parameter(names = {"--annot-hpo"}, description = "List of HPO terms. e.g. \"HP:0000545\"", arity = 1)
        public String hpo;

        @Parameter(names = {"--annot-go"}, description = "List of GO (Genome Ontology) terms. e.g. \"GO:0002020\"", arity = 1)
        public String go;

        @Parameter(names = {"--annot-expression"}, description = "List of tissues of interest. e.g. \"tongue\"", arity = 1)
        public String expression;

        @Parameter(names = {"--annot-protein-keywords"}, description = "List of protein variant annotation keywords",
                arity = 1)
        public String proteinKeyword;

        @Parameter(names = {"--annot-drug"}, description = "List of drug names", arity = 1)
        public String drug;

        @Parameter(names = {"--annot-functional-score"}, description = "Functional score: {functional_score}[<|>|<=|>=]{number} "
                + "e.g. cadd_scaled>5.2 , cadd_raw<=0.3", arity = 1)
        public String functionalScore;

        @Parameter(names = {"--unknown-genotype"}, description = "Returned genotype for unknown genotypes. Common values: [0/0, 0|0, ./.]",
                arity = 1)
        public String unknownGenotype;

        @Parameter(names = {"--samples-metadata"}, description = "Returns the samples metadata group by studyId, instead of the variants",
                arity = 0)
        public boolean samplesMetadata;

        @Parameter(names = {"--sort"}, description = "Sort the results", arity = 0)
        public boolean sort;

        @Parameter(names = {"--group-by"}, description = "Group variants by: [ct, gene, ensemblGene]", arity = 1)
        public String groupBy;

        @Parameter(names = {"--histogram"}, description = "Calculate histogram. Requires one region.", arity = 1)
        public boolean histogram;

        @Parameter(names = {"--interval"}, description = "Histogram interval size. Default:2000", arity = 1)
        public String interval;

        @Parameter(names = {"--merge"}, description = "Merge results", arity = 1)
        public String merge;

        @Parameter(names = {"--include"}, description = "Comma separated list of fields to be included in the response", arity = 1)
        public String include;

        @Parameter(names = {"--exclude"}, description = "Comma separated list of fields to be excluded from the response", arity = 1)
        public String exclude;

        @Parameter(names = {"--skip"}, description = "Number of results to skip", arity = 1)
        public String skip;

        @Parameter(names = {"--limit"}, description = "Maximum number of results to be returned", arity = 1)
        public String limit;

        @Parameter(names = {"--count"}, description = "Total number of results.", arity = 0)
        public boolean count;

    }



}
