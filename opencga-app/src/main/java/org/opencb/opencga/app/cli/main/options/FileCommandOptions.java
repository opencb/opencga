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

import com.beust.jcommander.*;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.GeneralCliOptions.CommonCommandOptions;
import org.opencb.opencga.app.cli.GeneralCliOptions.DataModelOptions;
import org.opencb.opencga.app.cli.GeneralCliOptions.NumericOptions;
import org.opencb.opencga.app.cli.GeneralCliOptions.StudyOption;
import org.opencb.opencga.app.cli.main.options.commons.AclCommandOptions;
import org.opencb.opencga.app.cli.main.options.commons.AnnotationCommandOptions;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.file.File;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by sgallego on 6/14/16.
 */
@Parameters(commandNames = {"files"}, commandDescription = "File commands")
public class FileCommandOptions {

    public CreateCommandOptions createCommandOptions;
    public InfoCommandOptions infoCommandOptions;
    public DownloadCommandOptions downloadCommandOptions;
    public GrepCommandOptions grepCommandOptions;
    public SearchCommandOptions searchCommandOptions;
    public ListCommandOptions listCommandOptions;
//    public IndexCommandOptions indexCommandOptions;
    public TreeCommandOptions treeCommandOptions;
    public HeadCommandOptions headCommandOptions;
    public TailCommandOptions tailCommandOptions;
//    public FetchCommandOptions fetchCommandOptions;
    public UpdateCommandOptions updateCommandOptions;
    public UploadCommandOptions uploadCommandOptions;
    public DeleteCommandOptions deleteCommandOptions;
    public LinkCommandOptions linkCommandOptions;
    public LinkRunCommandOptions linkRunCommandOptions;
    public PostLinkRunCommandOptions postLinkRunCommandOptions;
//    public RelinkCommandOptions relinkCommandOptions;
    public UnlinkCommandOptions unlinkCommandOptions;
    public RefreshCommandOptions refreshCommandOptions;
//    public GroupByCommandOptions groupByCommandOptions;
    public StatsCommandOptions statsCommandOptions;
    public final FetchCommandOptions fetchCommandOptions;
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

        this.createCommandOptions = new CreateCommandOptions();
        this.infoCommandOptions = new InfoCommandOptions();
        this.downloadCommandOptions = new DownloadCommandOptions();
        this.grepCommandOptions = new GrepCommandOptions();
        this.searchCommandOptions = new SearchCommandOptions();
        this.listCommandOptions = new ListCommandOptions();
//        this.indexCommandOptions = new IndexCommandOptions();
        this.treeCommandOptions = new TreeCommandOptions();
        this.headCommandOptions = new HeadCommandOptions();
        this.tailCommandOptions = new TailCommandOptions();
//        this.fetchCommandOptions = new FetchCommandOptions();
        this.updateCommandOptions = new UpdateCommandOptions();
        this.deleteCommandOptions = new DeleteCommandOptions();
        this.refreshCommandOptions = new RefreshCommandOptions();
        this.unlinkCommandOptions = new UnlinkCommandOptions();
        this.linkCommandOptions = new LinkCommandOptions();
        this.linkRunCommandOptions = new LinkRunCommandOptions();
        this.postLinkRunCommandOptions = new PostLinkRunCommandOptions();
        this.uploadCommandOptions = new UploadCommandOptions();
        this.statsCommandOptions = new StatsCommandOptions();
        this.fetchCommandOptions = new FetchCommandOptions();
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

    @Parameters(commandNames = {"create"}, commandDescription = "Create file or folder")
    public class CreateCommandOptions extends StudyOption {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--path"}, description = "Path where the file or folder will be created", required = true, arity = 1)
        public String folder = "";

        @Parameter(names = {"--description"}, description = "File or folder description", arity = 1)
        public String description = "";

        @Parameter(names = {"--content"}, description = "File content. If not provided, a folder will be created.", arity = 1)
        public String content = "";

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

        @Parameter(names = {"--deleted"}, description = "Flag to retrieve only deleted files", arity = 0)
        public boolean deleted;

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

        @Parameter(names = {"--pattern"}, description = "String pattern", arity = 1)
        public String pattern = "";

        @Parameter(names = {"-i", "--ignore-case"}, description = "Flag to perform a case insensitive search", arity = 0)
        public boolean ignoreCase;

        @Parameter(names = {"-m", "--max-count"}, description = "Stop reading a file after 'n' matching lines. 0 means no limit.", arity = 0)
        public Integer maxCount;
    }

    @Parameters(commandNames = {"search"}, commandDescription = "Search files")
    public class SearchCommandOptions extends StudyOption {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public DataModelOptions dataModelOptions = commonDataModelOptions;

        @ParametersDelegate
        public NumericOptions numericOptions = commonNumericOptions;

        @Parameter(names = {"--name"}, description = ParamConstants.FILE_NAMES_DESCRIPTION, arity = 1)
        public String name;

        @Parameter(names = {"--path"}, description = ParamConstants.FILE_PATHS_DESCRIPTION, arity = 1)
        public String path;

        @Parameter(names = {"--type"}, description = ParamConstants.FILE_TYPE_DESCRIPTION, arity = 1)
        public String type;

        @Parameter(names = {"--bioformat"}, description = ParamConstants.FILE_BIOFORMAT_DESCRIPTION, arity = 1)
        public List<File.Bioformat> bioformat;

        @Parameter(names = {"--format"}, description = ParamConstants.FILE_FORMAT_DESCRIPTION, arity = 1)
        public List<File.Format> format;

        @Parameter(names = {"--status"}, description = ParamConstants.FILE_STATUS_DESCRIPTION, arity = 1)
        public String status;

        @Parameter(names = {"--directory"}, description = ParamConstants.FILE_DIRECTORY_DESCRIPTION, arity = 1)
        public String folder;

        @Parameter(names = {"--creation-date"}, description = ParamConstants.CREATION_DATE_DESCRIPTION, arity = 1)
        public String creationDate;

        @Parameter(names = {"--modification-date"}, description = ParamConstants.MODIFICATION_DATE_DESCRIPTION, arity = 1)
        public String modificationDate;

        @Parameter(names = {"-d", "--description"}, description = ParamConstants.FILE_DESCRIPTION_DESCRIPTION, arity = 1)
        public String description;

        @Parameter(names = {"--tags"}, description = ParamConstants.FILE_TAGS_DESCRIPTION, arity = 1)
        public String tags;

        @Parameter(names = {"--size"}, description = ParamConstants.FILE_SIZE_DESCRIPTION, arity = 1)
        public String size;

        @Parameter(names = {"--index-status", "--internal-index-status"}, description = ParamConstants.INTERNAL_INDEX_STATUS_DESCRIPTION, arity = 1)
        public String internalIndexStatus;

        @Parameter(names = {"--samples"}, description = ParamConstants.SAMPLES_DESCRIPTION, arity = 1)
        public String samples;

        @Parameter(names = {"--job-id"}, description = ParamConstants.FILE_JOB_ID_DESCRIPTION, arity = 1)
        public String jobId;

        @Parameter(names = {"--attributes"}, description = ParamConstants.ATTRIBUTES_DESCRIPTION, arity = 1)
        public String attributes;

        @Parameter(names = {"--annotation"}, description = ParamConstants.ANNOTATION_DESCRIPTION, arity = 1)
        public String annotation;

        @Parameter(names = {"--acl"}, description = ParamConstants.ACL_DESCRIPTION, arity = 1)
        public String acl;

        @Parameter(names = {"--deleted"}, description = ParamConstants.DELETED_DESCRIPTION, arity = 1)
        public boolean deleted;

        @Parameter(names = {"--release"}, description = ParamConstants.RELEASE_DESCRIPTION, arity = 1)
        public Integer release;

        @Parameter(names = {"--flatten-annotations"}, description = ParamConstants.FLATTEN_ANNOTATION_DESCRIPTION, arity = 0)
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

        @Parameter(names = {"--" + ParamConstants.FILE_FOLDER}, description = ParamConstants.FILE_FOLDER_DESCRIPTION, arity = 1, required = true)
        public String folderId;

    }

    @Parameters(commandNames = {"head"}, commandDescription = "Show the first lines of a file (up to a limit)")
    public class HeadCommandOptions extends BaseFileCommand{

        @Parameter(names = {"--offset"}, description = "Starting byte from which the file will be read", arity = 1)
        public long offset;

        @Parameter(names = {"--lines"}, description = "Maximum number of lines to be returned", arity = 1)
        public Integer lines;
    }

    @Parameters(commandNames = {"tail"}, commandDescription = "Show the last lines of a file (up to a limit)")
    public class TailCommandOptions extends BaseFileCommand{

        @Parameter(names = {"--lines"}, description = "Maximum number of lines to be returned", arity = 1)
        public Integer lines;
    }

    @Parameters(commandNames = {"update"}, commandDescription = "Modify file")
    public class UpdateCommandOptions extends BaseFileCommand {

        @Parameter(names = {"--name"}, description = "New file name", arity = 1)
        public String name;

        @Parameter(names = {"--format"}, description = "New format (VCF, BCF, GVCF, SAM, BAM, BAI...UNKNOWN)",
                arity = 1)
        public File.Format format;

        @Parameter(names = {"--bioformat"}, description = "New bioformat (VARIANT, ALIGNMENT, SEQUENCE, PEDIGREE...NONE)",
                arity = 1)
        public File.Bioformat bioformat;

        @Parameter(names = {"--description"}, description = "New description", arity = 1)
        public String description;

        @Parameter(names = {"--samples-action"}, description = "Action to be performed if the array of samples is being updated (ADD, SET or REMOVE).",
                arity = 1)
        public String samplesAction;

        @Parameter(names = {"--samples"}, description = "Comma separated list of sample names or ids", arity = 1)
        public List<String> sampleIds;

        @Parameter(names = {"--tags-action"}, description = "Action to be performed if the array of tags is being updated (ADD, SET or REMOVE).",
                arity = 1)
        public String tagsAction;

        @Parameter(names = {"--tags"}, description = "Comma separated list of sample names or ids", arity = 1)
        public List<String> tags;

        @Parameter(names = {"--software-name"}, description = "Software name", arity = 1)
        public String softwareName;

        @Parameter(names = {"--software-version"}, description = "Software version", arity = 1)
        public String softwareVersion;

        @Parameter(names = {"--software-repository"}, description = "Software repository", arity = 1)
        public String softwareRepository;

        @Parameter(names = {"--software-commit"}, description = "Software commit", arity = 1)
        public String softwareCommit;

        @Parameter(names = {"--software-website"}, description = "Software website", arity = 1)
        public String softwareWebsite;

        @DynamicParameter(names = {"--software-params"}, description = "Software params")
        public Map<String, String> softwareParams = new HashMap<>();

    }

//    @Parameters(commandNames = {"relink"}, commandDescription = "Change location of linked or STAGED file.")
//    public class RelinkCommandOptions extends BaseFileCommand {
//
//        @Parameter(names = {"--uri"}, description = "New URI", required = true, arity = 1)
//        public String uri;
//
//        @Parameter(names = {"-ch", "--checksum"}, description = "Calculate checksum", arity = 0)
//        public boolean calculateChecksum;
//    }

    @Parameters(commandNames = {"delete"}, commandDescription = "Delete file")
    public class DeleteCommandOptions extends BaseFileCommand {

        @Parameter(names = {"--skip-trash"}, description = "Boolean indicating whether to skip sending the files to the trash first. "
                + "If set, it will not be possible to recover them!", arity = 0)
        public boolean skipTrash;
    }

    @Parameters(commandNames = {"tree"}, commandDescription = "Obtain a tree view of the files and folders within a folder")
    public class TreeCommandOptions extends StudyOption{

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public DataModelOptions dataModelOptions = commonDataModelOptions;

        @Parameter(names = {"--folder"}, description = "Folder id, name or path.", arity = 1)
        public String folderId = ".";

        @Parameter(names = {"-n", "--max-depth"}, description = "Descend only 'level' directories deep. Default: 5", arity = 1)
        public Integer maxDepth;
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


    @Parameters(commandNames = {"link-run"}, commandDescription = "Link an external file or folder into catalog. Creates an asynchronous job.")
    public class LinkRunCommandOptions extends StudyOption {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public GeneralCliOptions.JobOptions jobOptions = new GeneralCliOptions.JobOptions();

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

    @Parameters(commandNames = {"post-link-run"}, commandDescription = "Post link operation. Associate non-registered samples for files with high volumes of samples.")
    public class PostLinkRunCommandOptions extends StudyOption {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public GeneralCliOptions.JobOptions jobOptions = new GeneralCliOptions.JobOptions();

        @Parameter(names = {"--files"}, description = "Files that need to be processed. Use \"" + ParamConstants.ALL + "\" to process all files from the study", required = true, variableArity = true)
        public List<String> files;

        @Parameter(names = {"--batch-size"}, description = "Samples update batch size")
        public Integer batchSize;
    }

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
                arity = 1)
        public String catalogPath;

        @Parameter(names = {"--description"}, description = "Description of the file", arity = 1)
        public String description;

        @Parameter(names = {"--file-name"}, description = "Name of the file by which it will be stored in catalog", required = false,
                arity = 1)
        public String fileName;

        @Parameter(names = {"-P", "--parents"}, description = "Create parent directories if needed", required = false)
        public boolean parents;

        @Parameter(names = {"--replace"}, description = "[PENDING] Replace the existing attached file. ALERT: The existing file will be "
                + "removed", arity = 0)
        public boolean replace;

        @Parameter(names = {"-ch", "--checksum"}, description = "[PENDING] Calculate checksum", arity = 0)
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

    @Parameters
    public class FetchCommandOptions extends StudyOption {

        @Parameter(names = {"--" + ParamConstants.FILE_PATH_PARAM},
                description = "Folder path where the downloaded file will be registered", required = true, arity = 1)
        public String path;

        @Parameter(names = {"--url"}, description = "External url where the file to be registered can be downloaded from", required = true,
                arity = 1)
        public String url;
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
