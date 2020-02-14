/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.app.cli.main.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.GeneralCliOptions.CommonCommandOptions;
import org.opencb.opencga.app.cli.GeneralCliOptions.DataModelOptions;
import org.opencb.opencga.app.cli.GeneralCliOptions.NumericOptions;
import org.opencb.opencga.app.cli.GeneralCliOptions.StudyOption;
import org.opencb.opencga.app.cli.main.options.commons.AclCommandOptions;
import org.opencb.opencga.app.cli.main.options.commons.AnnotationCommandOptions;
import org.opencb.opencga.core.models.file.File;

import java.util.List;

/**
 * Created by sgallego on 6/14/16.
 */
@Parameters(commandNames = {"files"}, commandDescription = "File commands")
public class FileCommandOptions {

//    public CopyCommandOptions copyCommandOptions;
    public CreateFolderCommandOptions createFolderCommandOptions;
    public InfoCommandOptions infoCommandOptions;
    public DownloadCommandOptions downloadCommandOptions;
    public GrepCommandOptions grepCommandOptions;
    public SearchCommandOptions searchCommandOptions;
    public ListCommandOptions listCommandOptions;
//    public IndexCommandOptions indexCommandOptions;
    public TreeCommandOptions treeCommandOptions;
    public ContentCommandOptions contentCommandOptions;
//    public FetchCommandOptions fetchCommandOptions;
    public UpdateCommandOptions updateCommandOptions;
    public UploadCommandOptions uploadCommandOptions;
    public DeleteCommandOptions deleteCommandOptions;
    public LinkCommandOptions linkCommandOptions;
//    public RelinkCommandOptions relinkCommandOptions;
    public UnlinkCommandOptions unlinkCommandOptions;
    public RefreshCommandOptions refreshCommandOptions;
//    public GroupByCommandOptions groupByCommandOptions;
    public StatsCommandOptions statsCommandOptions;
//    public VariantsCommandOptions variantsCommandOptions;

    public FileAclCommandOptions.AclsCommandOptions aclsCommandOptions;
    public FileAclCommandOptions.AclsUpdateCommandOptions aclsUpdateCommandOptions;

    public AnnotationCommandOptions.AnnotationSetsUpdateCommandOptions annotationUpdateCommandOptions;

    public JCommander jCommander;
    public CommonCommandOptions commonCommandOptions;
    public DataModelOptions commonDataModelOptions;
    public NumericOptions commonNumericOptions;

    public FileCommandOptions(CommonCommandOptions commonCommandOptions, DataModelOptions dataModelOptions, NumericOptions numericOptions,
                              JCommander jCommander) {

        this.commonCommandOptions = commonCommandOptions;
        this.commonDataModelOptions = dataModelOptions;
        this.commonNumericOptions = numericOptions;
        this.jCommander = jCommander;

//        this.copyCommandOptions = new CopyCommandOptions();
        this.createFolderCommandOptions = new CreateFolderCommandOptions();
        this.infoCommandOptions = new InfoCommandOptions();
        this.downloadCommandOptions = new DownloadCommandOptions();
        this.grepCommandOptions = new GrepCommandOptions();
        this.searchCommandOptions = new SearchCommandOptions();
        this.listCommandOptions = new ListCommandOptions();
//        this.indexCommandOptions = new IndexCommandOptions();
        this.treeCommandOptions = new TreeCommandOptions();
        this.contentCommandOptions = new ContentCommandOptions();
//        this.fetchCommandOptions = new FetchCommandOptions();
        this.updateCommandOptions = new UpdateCommandOptions();
        this.deleteCommandOptions = new DeleteCommandOptions();
        this.refreshCommandOptions = new RefreshCommandOptions();
        this.unlinkCommandOptions = new UnlinkCommandOptions();
        this.linkCommandOptions = new LinkCommandOptions();
        this.uploadCommandOptions = new UploadCommandOptions();
        this.statsCommandOptions = new StatsCommandOptions();
//        this.variantsCommandOptions = new VariantsCommandOptions();

        AnnotationCommandOptions annotationCommandOptions = new AnnotationCommandOptions(commonCommandOptions);
        this.annotationUpdateCommandOptions = annotationCommandOptions.getUpdateCommandOptions();

        FileAclCommandOptions aclCommandOptions = new FileAclCommandOptions(commonCommandOptions);
        this.aclsCommandOptions = aclCommandOptions.getAclsCommandOptions();
        this.aclsUpdateCommandOptions = aclCommandOptions.getAclsUpdateCommandOptions();
    }

    public class BaseFileCommand extends StudyOption {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--file"}, description = "File id, name or path", required = true, arity = 1)
        public String file;

    }

    @Deprecated
    @Parameters(commandNames = {"copy"}, commandDescription = "(DEPRECATED) Copy a file or folder")
    public class CopyCommandOptions extends StudyOption {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--path"}, description = "Directory where to create the file", required = false, arity = 1)
        public String path;

        @Parameter(names = {"-i", "--input"}, description = "Input file", required = true, arity = 1)
        public String inputFile;

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
    public class CreateFolderCommandOptions extends StudyOption {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--path"}, description = "Path where the folder will be created", required = true, arity = 1)
        public String folder = "";

        @Parameter(names = {"-P", "--parents"}, description = "Create the parent directories if they do not exist. Default: false",
                arity = 0)
        public boolean parents;
    }

    @Parameters(commandNames = {"info"}, commandDescription = "Get file information")
    public class InfoCommandOptions extends StudyOption {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public DataModelOptions dataModelOptions = commonDataModelOptions;

        @Parameter(names = {"--file"}, description = "Comma separated list of file ids", required = true, arity = 1)
        public String files;

        @Parameter(names = {"--no-lazy"}, description = "Obtain the entire related job and experiment objects", arity = 0)
        public boolean noLazy;

        @Parameter(names = {"--flatten-annotations"}, description = "Flag indicating whether nested annotations should be returned flattened",
                arity = 0)
        public boolean flattenAnnotations;
    }

    @Parameters(commandNames = {"download"}, commandDescription = "Download file")
    public class DownloadCommandOptions extends BaseFileCommand {
        @Parameter(names = {"--to"}, description = "Path where the file will be downloaded", arity = 1, required = true)
        public String fileDestiny;
    }

    @Parameters(commandNames = {"grep"}, commandDescription = "Get file information")
    public class GrepCommandOptions extends BaseFileCommand {

        @Parameter(names = {"--pattern"}, description = "Pattern", required = false, arity = 1)
        public String pattern = ".*";

        @Parameter(names = {"-i", "--ignore-case"}, description = "Do a case insensitive search", required = false, arity = 0)
        public boolean ignoreCase;

        @Parameter(names = {"-m", "--multi"}, description = "Return multiple matches. Default = true", required = false, arity = 0)
        public boolean multi = true;

    }

    @Parameters(commandNames = {"search"}, commandDescription = "Search files")
    public class SearchCommandOptions extends StudyOption {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public DataModelOptions dataModelOptions = commonDataModelOptions;

        @ParametersDelegate
        public NumericOptions numericOptions = commonNumericOptions;

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

        @Parameter(names = {"--folder"}, description = "Directory under which we want to look for files or folders.", arity = 1)
        public String folder;

        @Parameter(names = {"--owner-id"}, description = "Owner id.", required = false, arity = 1)
        public String ownerId;

        @Parameter(names = {"--creation-date"}, description = "Creation date.", required = false, arity = 1)
        public String creationDate;

        @Parameter(names = {"--modification-date"}, description = "Modification date.", required = false, arity = 1)
        public String modificationDate;

        @Parameter(names = {"-d", "--description"}, description = "Description", required = false, arity = 1)
        public String description;

        @Parameter(names = {"--size"}, description = "Size.", required = false, arity = 1)
        public String size;

        @Parameter(names = {"--samples"}, description = "Comma separated list of sample ids or names", required = false, arity = 1)
        public String samples;

        @Parameter(names = {"--job-id"}, description = "Job id", required = false, arity = 1)
        public String jobId;

        @Parameter(names = {"--attributes"}, description = "Attributes", required = false, arity = 1)
        public String attributes;

        @Parameter(names = {"--nattributes"}, description = "numerical attributes", required = false, arity = 1)
        public String nattributes;

        @Parameter(names = {"--no-lazy"}, description = "Obtain the entire related job and experiment objects", arity = 0)
        public boolean noLazy;

        @Parameter(names = {"--annotation"}, description = "Annotation filters. Example: age>30;gender=FEMALE. For more information, " +
                "please visit http://docs.opencb.org/display/opencga/AnnotationSets+1.4.0", arity = 1)
        public String annotation;

        @Parameter(names = {"--flatten-annotations"}, description = "Flag indicating whether nested annotations should be returned flattened",
                arity = 0)
        public boolean flattenAnnotations;

    }

    @Parameters(commandNames = {"list"}, commandDescription = "List files in folder")
    public class ListCommandOptions extends StudyOption {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public DataModelOptions dataModelOptions = commonDataModelOptions;

        @ParametersDelegate
        public NumericOptions numericOptions = commonNumericOptions;

        @Parameter(names = {"--folder"}, description = "Folder id, name or path.", arity = 1, required = false)
        public String folderId;

    }

    @Deprecated
    @Parameters(commandNames = {"index"}, commandDescription = "Index file in the selected StorageEngine")
    public class IndexCommandOptions extends BaseFileCommand {

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

        @Parameter(names = {"--aggregation-mapping-file"}, description = "File containing population names mapping in an aggregated VCF "
                + "file", arity = 1)
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

        @Parameter(names = {"--resume"}, description = "Resume a previously failed indexation", arity = 0)
        public boolean resume;
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

    @Parameters(commandNames = {"content"}, commandDescription = "Show the content of a file (up to a limit)")
    public class ContentCommandOptions extends BaseFileCommand{

        @Parameter(names = {"--start"}, description = "Start", arity = 1)
        public int start = -1;

        @Parameter(names = {"--limit"}, description = "Limit", arity = 1)
        public int limit = -1;

    }

    @Parameters(commandNames = {"update"}, commandDescription = "Modify file")
    public class UpdateCommandOptions extends BaseFileCommand {

        @Parameter(names = {"--name"}, description = "Name", required = false, arity = 1)
        public String name;

        @Parameter(names = {"--format"}, description = "Format of the file (VCF, BCF, GVCF, SAM, BAM, BAI...UNKNOWN)",
                required = false, arity = 1)
        public File.Format format;

        @Parameter(names = {"--bioformat"}, description = "Bioformat of the file (VARIANT, ALIGNMENT, SEQUENCE, PEDIGREE...NONE)",
                required = false, arity = 1)
        public File.Bioformat bioformat;

        @Parameter(names = {"--description"}, description = "Description of the file", required = false, arity = 1)
        public String description;

        @Parameter(names = {"--attributes"}, description = "Attributes", required = false, arity = 1)
        public String attributes;

        @Parameter(names = {"--stats"}, description = "Stats", required = false, arity = 1)
        public String stats;

        @Parameter(names = {"--samples"}, description = "Comma separated list of sample names or ids", required = false, arity = 1)
        public List<String> sampleIds;
    }

//    @Parameters(commandNames = {"relink"}, commandDescription = "Change location of linked or STAGED file.")
//    public class RelinkCommandOptions extends BaseFileCommand {
//
//        @Parameter(names = {"--uri"}, description = "New URI", required = true, arity = 1)
//        public String uri;
//
//        @Parameter(names = {"-ch", "--checksum"}, description = "Calculate checksum", required = false, arity = 0)
//        public boolean calculateChecksum;
//    }

    @Parameters(commandNames = {"delete"}, commandDescription = "Delete file")
    public class DeleteCommandOptions extends BaseFileCommand {

        @Parameter(names = {"--delete-external"}, description = "Boolean indicating whether to delete external files from disk as well"
                + " (only applicable for linked files/folders)", required = false, arity = 0)
        public boolean deleteExternal;

        @Parameter(names = {"--skip-trash"}, description = "Boolean indicating whether to skip sending the files to the trash first. "
                + "If set, it will not be possible to recover them!", required = false, arity = 0)
        public boolean skipTrash;

    }

    @Parameters(commandNames = {"tree"}, commandDescription = "Obtain a tree view of the files and folders within a folder")
    public class TreeCommandOptions extends StudyOption{

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public DataModelOptions dataModelOptions = commonDataModelOptions;

        @Parameter(names = {"--folder"}, description = "Folder id, name or path.", required = false, arity = 1)
        public String folderId = ".";

        @Parameter(names = {"-L", "--level"}, description = "Descend only 'level' directories deep. Default: 5", arity = 1)
        public Integer maxDepth = 5;

        @Parameter(names = {"--max-Depth"}, hidden = true)
        public void setMaxDepth(int maxDepth) {
            this.maxDepth = maxDepth;
        }

        @Parameter(names = {"--limit"}, description = "[TO BE IMPLEMENTED] Number of results to be returned in the queries", arity = 1)
        public String limit;

    }

    @Parameters(commandNames = {"refresh"}, commandDescription = "Refresh metadata from the selected file or folder. Print updated files.")
    public class RefreshCommandOptions extends BaseFileCommand {

    }

    @Parameters(commandNames = {"unlink"}, commandDescription = "Unlink an external file from catalog")
    public class UnlinkCommandOptions extends BaseFileCommand {

    }

    @Parameters(commandNames = {"link"}, commandDescription = "Link an external file or folder into catalog. Only works with direct " +
            "access to the database installation.")
    public class LinkCommandOptions extends StudyOption {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--input"}, description = "File or folder location", required = true, variableArity = true)
        public List<String> inputs;

//        @Parameter(names = {"-uri"}, description = "File location", required = true, arity = 1)
//        public String uri;

        @Parameter(names = {"--path"}, description = "Virtual path within catalog where the file or folder will be linked (root folder if empty)", arity = 1)
        public String path;

        @Parameter(names = {"-d", "--description"}, description = "Brief description that will be attached to the files in catalog", arity = 1)
        public String description;

        @Parameter(names = {"-P", "--parents"}, description = "Create parent directories if needed", required = false)
        public boolean parents;
    }
    //---- POST ---//
    @Parameters(commandNames = {"upload"}, commandDescription = "Upload a physical local file to catalog.")
    public class UploadCommandOptions extends StudyOption {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i","--input"}, description = "Input file", required = true, arity = 1)
        public String inputFile;

        @Parameter(names = {"--file-format"}, description = "Format of the file (VCF, BCF, GVCF, SAM, BAM, BAI...UNKNOWN)", required = true,
                arity = 1)
        public String fileFormat;

        @Parameter(names = {"--bioformat"}, description = "Bioformat of the file (VARIANT, ALIGNMENT, SEQUENCE, PEDIGREE...NONE)",
                required = true, arity = 1)
        public String bioformat;

        @Parameter(names = {"--catalog-path"}, description = "Path within catalog where the file will be located (Default: root folder)",
                required = false, arity = 1)
        public String catalogPath;

        @Parameter(names = {"--description"}, description = "Description of the file", required = false, arity = 1)
        public String description;

        @Parameter(names = {"--file-name"}, description = "Name of the file by which it will be stored in catalog", required = false,
                arity = 1)
        public String fileName;

        @Parameter(names = {"-P", "--parents"}, description = "Create parent directories if needed", required = false)
        public boolean parents;

        @Parameter(names = {"--replace"}, description = "[PENDING] Replace the existing attached file. ALERT: The existing file will be "
                + "removed", required = false, arity = 0)
        public boolean replace;

        @Parameter(names = {"-ch", "--checksum"}, description = "[PENDING] Calculate checksum", required = false, arity = 0)
        public boolean calculateChecksum;
    }

    public class FileAclCommandOptions extends AclCommandOptions {

        private AclsUpdateCommandOptions aclsUpdateCommandOptions;

        public FileAclCommandOptions(CommonCommandOptions commonCommandOptions) {
            super(commonCommandOptions);
        }

        @Parameters(commandNames = {"acl-update"}, commandDescription = "Update the permissions set for a member")
        public class AclsUpdateCommandOptions extends AclCommandOptions.AclsUpdateCommandOptions {

//            @Parameter(names = {"--file"}, description = "Comma separated list of file ids, names or paths", arity = 1)
//            public String file;

            @Parameter(names = {"--sample"}, description = "Comma separated list of sample ids or file containing the list of ids "
                    + "(one per line)", arity = 1)
            public String sample;
        }

        @Override
        public AclsUpdateCommandOptions getAclsUpdateCommandOptions() {
            if (this.aclsUpdateCommandOptions == null) {
                this.aclsUpdateCommandOptions = new AclsUpdateCommandOptions();
            }
            return aclsUpdateCommandOptions;
        }
    }

    @Parameters(commandNames = {"stats"}, commandDescription = "File stats")
    public class StatsCommandOptions extends StudyOption {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--default"}, description = "Flag to calculate default stats", arity = 0)
        public boolean defaultStats;

        @Parameter(names = {"--creation-year"}, description = "Creation year.", arity = 1)
        public String creationYear;

        @Parameter(names = {"--creation-month"}, description = "Creation month (JANUARY, FEBRUARY...).", arity = 1)
        public String creationMonth;

        @Parameter(names = {"--creation-day"}, description = "Creation day.", arity = 1)
        public String creationDay;

        @Parameter(names = {"--creation-day-of-week"}, description = "Creation day of week (MONDAY, TUESDAY...).", arity = 1)
        public String creationDayOfWeek;

        @Parameter(names = {"--name"}, description = "Name.", arity = 1)
        public String name;

        @Parameter(names = {"--type"}, description = "Type.", arity = 1)
        public String type;

        @Parameter(names = {"--format"}, description = "Format.", arity = 1)
        public String format;

        @Parameter(names = {"--bioformat"}, description = "Bioformat.", arity = 1)
        public String bioformat;

        @Parameter(names = {"--status"}, description = "Status.", arity = 1)
        public String status;

        @Parameter(names = {"--num-samples"}, description = "Number of samples", arity = 1)
        public String numSamples;

        @Parameter(names = {"--num-related-files"}, description = "Number of related files.", arity = 1)
        public String numRelatedFiles;

        @Parameter(names = {"--release"}, description = "Release.", arity = 1)
        public String release;

        @Parameter(names = {"--external"}, description = "External.", arity = 1)
        public Boolean external;

        @Parameter(names = {"--size"}, description = "Size in bytes.", arity = 1)
        public String size;

        @Parameter(names = {"--software"}, description = "Software.", arity = 1)
        public String software;

        @Parameter(names = {"--experiment"}, description = "Experiment.", arity = 1)
        public String experiment;

        @Parameter(names = {"--annotation"}, description = "Annotation. See documentation to see the options.", arity = 1)
        public String annotation;

        @Parameter(names = {"--field"}, description = "List of fields separated by semicolons, e.g.: studies;type. For nested "
                + "fields use >>, e.g.: studies>>biotype;type;numSamples[0..10]:1.", arity = 1)
        public String field;
    }

}
