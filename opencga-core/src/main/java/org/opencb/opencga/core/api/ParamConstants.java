/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.core.api;

public class ParamConstants {
    private static final String UP_TO_100 = " up to a maximum of 100";

    public static final String NONE = "none";
    public static final String ALL = "all";

    public static final String ID = "id";

    public static final String TOKEN = "token";

    public static final String INCLUDE_DESCRIPTION = "Fields included in the response, whole JSON path must be provided";
    public static final String EXCLUDE_DESCRIPTION = "Fields excluded in the response, whole JSON path must be provided";
    public static final String LIMIT_DESCRIPTION = "Number of results to be returned";
    public static final String SKIP_DESCRIPTION = "Number of results to skip";
    public static final String COUNT_DESCRIPTION = "Get the total number of results matching the query. Deactivated by default.";

    public static final String CREATION_DATE_DESCRIPTION = "Creation date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805";
    public static final String CREATION_DATE_PARAM = "creationDate";
    public static final String MODIFICATION_DATE_DESCRIPTION = "Modification date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805";
    public static final String MODIFICATION_DATE_PARAM = "modificationDate";
    public static final String RELEASE_PARAM = "release";
    public static final String RELEASE_DESCRIPTION = "Release when it was created";

    public static final String INTERNAL_STATUS_PARAM = "internalStatus";
    public static final String INTERNAL_STATUS_DESCRIPTION = "Filter by internal status";

    public static final String STATUS_PARAM = "status";
    public static final String STATUS_DESCRIPTION = "Filter by status";

    public static final String ACL_PARAM = "acl";
    public static final String ACL_FORMAT = "Format: acl={user}:{permissions}. Example: acl=john:WRITE,WRITE_ANNOTATIONS will return "
            + "all entries for which user john has both WRITE and WRITE_ANNOTATIONS permissions. Only study owners or administrators "
            + "can query by this field. ";
    public static final String ACL_DESCRIPTION = "Filter entries for which a user has the provided permissions. " + ACL_FORMAT;
    public static final String ACL_ACTION_PARAM = "action";
    public static final String ACL_ACTION_DESCRIPTION = "Action to be performed [ADD, SET, REMOVE or RESET].";

    public static final String TSV_ANNOTATION_DESCRIPTION = "JSON containing the 'content' of the TSV file if this has not yet been "
            + "registered into OpenCGA";

    public static final String DELETED_DESCRIPTION = "Boolean to retrieve deleted entries";
    public static final String DELETED_PARAM = "deleted";

    public static final String ATTRIBUTES_PARAM = "attributes";
    public static final String ATTRIBUTES_DESCRIPTION = "Text attributes (Format: sex=male,age>20 ...)";

    public static final String INCREMENT_VERSION_PARAM = "incVersion";
    public static final String INCREMENT_VERSION_DESCRIPTION = "Increment version of entry with the update";

    public static final String SNAPSHOT_PARAM = "snapshot";
    public static final String SNAPSHOT_DESCRIPTION = "Snapshot value (Latest version of the entry in the specified release)";

    public static final String PHENOTYPES_PARAM = "phenotypes";
    public static final String PHENOTYPES_DESCRIPTION = "Comma separated list of phenotype ids or names";
    public static final String DISORDERS_PARAM = "disorders";
    public static final String DISORDERS_DESCRIPTION = "Comma separated list of disorder ids or names";

    // ---------------------------------------------

    public static final String USER_DESCRIPTION = "User ID";

    // ---------------------------------------------

    public static final String PROJECT_PARAM = "project";
    public static final String PROJECT_DESCRIPTION = "Project [user@]project where project can be either the ID or the alias";
    public static final String PROJECTS_DESCRIPTION = "Comma separated list of projects [user@]project" + UP_TO_100;

    // ---------------------------------------------

    public static final String STUDY_PARAM = "study";
    public static final String STUDY_DESCRIPTION = "Study [[user@]project:]study where study and project can be either the ID or UUID";
    public static final String OTHER_STUDIES_FLAG = "otherStudies";
    public static final String OTHER_STUDIES_FLAG_DESCRIPTION = "Flag indicating the entries being queried can belong to any related study, not just the primary one.";
    public static final String STUDIES_PARAM = "studies";
    public static final String STUDIES_DESCRIPTION = "Comma separated list of Studies [[user@]project:]study where study and project can be either the ID or UUID" + UP_TO_100;

    public static final String STUDY_NAME_DESCRIPTION = "Study name";
    public static final String STUDY_ID_DESCRIPTION = "Study ID";
    public static final String STUDY_ALIAS_DESCRIPTION = "Study alias";
    public static final String STUDY_FQN_DESCRIPTION = "Study full qualified name";
    public static final String SILENT_DESCRIPTION = "Boolean to retrieve all possible entries that are queried for, false to raise an "
            + "exception whenever one of the entries looked for cannot be shown for whichever reason";

    // ---------------------------------------------

    public static final String FILES_DESCRIPTION = "Comma separated list of file IDs or names" + UP_TO_100;
    public static final String FILE_ID_DESCRIPTION = "File ID";
    public static final String FILE_NAME_DESCRIPTION = "File name";
    public static final String FILE_NAMES_DESCRIPTION = "Comma separated list of file names";
    public static final String FILE_PATH_PARAM = "path";
    public static final String FILE_PATH_DESCRIPTION = "File path";
    public static final String FILE_PATHS_DESCRIPTION = "Comma separated list of paths";
    public static final String FILE_TYPE_DESCRIPTION = "File type, either FILE or DIRECTORY";
    public static final String FILE_FORMAT_DESCRIPTION = "Comma separated Format values. For existing Formats see files/formats";
    public static final String FILE_BIOFORMAT_DESCRIPTION = "Comma separated Bioformat values. For existing Bioformats see files/bioformats";
    public static final String FILE_STATUS_DESCRIPTION = "File status";
    public static final String FILE_DESCRIPTION_DESCRIPTION = "Description";
    public static final String FILE_TAGS_DESCRIPTION = "Tags";
    public static final String FILE_JOB_ID_DESCRIPTION = "Job ID that created the file(s) or folder(s)";
    public static final String FILE_DIRECTORY_DESCRIPTION = "Directory under which we want to look for files or folders";
    public static final String FILE_CREATION_DATA_DESCRIPTION = "Creation date of the file";
    public static final String FILE_MODIFICATION_DATE_DESCRIPTION = "Last modification date of the file";
    public static final String FILE_SIZE_DESCRIPTION = "File size";
    public static final String FILE_FOLDER = "folder";
    public static final String FILE_FOLDER_DESCRIPTION = "Folder ID, name or path";

    // ---------------------------------------------

    public static final String SAMPLE_DESCRIPTION = "Sample ID or UUID";
    public static final String SAMPLES_DESCRIPTION = "Comma separated list sample IDs or UUIDs" + UP_TO_100;
    public static final String SAMPLE_INDIVIDUAL_ID_PARAM = "individualId";
    public static final String SAMPLE_INDIVIDUAL_ID_DESCRIPTION = "Individual ID";
    public static final String SAMPLE_FILE_IDS_PARAM = "fileIds";
    public static final String SAMPLE_FILE_IDS_DESCRIPTION = "Comma separated list of file IDs";

    public static final String SAMPLE_ID_PARAM = "id";
    public static final String SAMPLE_PARAM = "sample";
    public static final String SAMPLE_ID_DESCRIPTION = "Sample ID";
    public static final String SAMPLE_NAME_DESCRIPTION = "Sample name";
    public static final String SAMPLE_SOMATIC_PARAM = "somatic";
    public static final String SAMPLE_SOMATIC_DESCRIPTION = "Somatic sample";
    public static final String SAMPLE_VERSION_PARAM = "version";
    public static final String SAMPLE_VERSION_DESCRIPTION = "Sample version";

    public static final String SAMPLE_INCLUDE_INDIVIDUAL_PARAM = "includeIndividual";
    public static final String SAMPLE_INCLUDE_INDIVIDUAL_DESCRIPTION = "Include Individual object as an attribute";

    public static final String SAMPLE_EMPTY_FILES_ACTION_PARAM = "emptyFilesAction";
    public static final String SAMPLE_EMPTY_FILES_ACTION_DESCRIPTION = "Action to be performed over files that were associated only to"
            + " the sample to be deleted. Possible actions are NONE, TRASH, DELETE";
    public static final String SAMPLE_DELETE_EMPTY_COHORTS_PARAM = "deleteEmptyCohorts";
    public static final String SAMPLE_DELETE_EMPTY_COHORTS_DESCRIPTION = "Boolean indicating if the cohorts associated only to the "
            + "sample to be deleted should be also deleted.";
    public static final String SAMPLE_FORCE_DELETE_DESCRIPTION = "Force the deletion of samples even if they are associated to files, "
            + "individuals or cohorts.";

    // ---------------------------------------------

    public static final String INDIVIDUALS_DESCRIPTION = "Comma separated list of individual names or IDs" + UP_TO_100;
    public static final String INDIVIDUAL_DESCRIPTION = "Individual ID or UUID";

    // ---------------------------------------------

    public static final String FAMILIES_DESCRIPTION = "Comma separated list of family IDs or names" + UP_TO_100;
    public static final String FAMILY_UPDATE_ROLES_PARAM = "updateRoles";
    public static final String FAMILY_UPDATE_ROLES_DESCRIPTION = "Update the member roles within the family";

    // ---------------------------------------------

    public static final String COHORT_DESCRIPTION = "Cohort ID or name";
    public static final String COHORTS_DESCRIPTION = "Comma separated list of cohort names or IDs" + UP_TO_100;

    public static final String COHORT_ID_DESCRIPTION = "Cohort ID";
    public static final String COHORT_NAME_DESCRIPTION = "Cohort name";
    public static final String COHORT_TYPE_DESCRIPTION = "Cohort type";
    public static final String COHORT_STATUS_DESCRIPTION = "Cohort status";

    // ---------------------------------------------

    public static final String CLINICAL_ANALYSES_DESCRIPTION = "Comma separated list of clinical analysis IDs or names" + UP_TO_100;
    public static final String CLINICAL_ANALYSES_PARAM = "clinicalAnalyses";
    public static final String CLINICAL_ANALYSIS_CREATE_DEFAULT_PARAM = "createDefaultInterpretation";
    public static final String CLINICAL_ANALYSIS_CREATE_DEFAULT_DESCRIPTION = "Flag to create and initialise a default primary "
            + "interpretation (Id will be '{clinicalAnalysisId}.1')";

    // ---------------------------------------------

    public static final String PANEL_SOURCE = "source";
    public static final String PANEL_SOURCE_DESCRIPTION = "Comma separated list of sources to import panels from. Current supported "
            + "sources are 'panelapp' and 'genecensus'";
    public static final String PANEL_SOURCE_ID = "id";
    public static final String PANEL_SOURCE_ID_DESCRIPTION = "Comma separated list of panel IDs to be imported from the defined source."
            + "If 'source' is provided and 'id' is empty, it will import all the panels from the source. When 'id' is provided, only one "
            + "'source' will be allowed.";

    public static final String PANELS_DESCRIPTION = "Comma separated list of panel IDs" + UP_TO_100;

    // ---------------------------------------------

    public static final String JOBS_DESCRIPTION = "Comma separated list of job IDs or UUIDs" + UP_TO_100;

    public static final String JOB_ID_DESCRIPTION = "Job ID or UUID";
    public static final String JOB_ID_CREATION_DESCRIPTION = "Job ID. It must be a unique string within the study. An ID will be "
            + "autogenerated automatically if not provided.";
    public static final String JOB_ID = "jobId";
    public static final String JOB_ID_PARAM = ID;
    public static final String JOB_DESCRIPTION = "jobDescription";
    public static final String JOB_DESCRIPTION_DESCRIPTION = "Job description";
    public static final String JOB_DEPENDS_ON = "jobDependsOn";
    public static final String JOB_DEPENDS_ON_PARAM = "dependsOn";
    public static final String JOB_DEPENDS_ON_DESCRIPTION = "Comma separated list of existing job IDs the job will depend on.";
    public static final String JOB_TOOL_ID_PARAM = "toolId";
    public static final String JOB_TOOL_ID_DESCRIPTION = "Tool ID executed by the job";
    public static final String JOB_USER_PARAM = "userId";
    public static final String JOB_USER_DESCRIPTION = "User that created the job";
    public static final String JOB_PRIORITY_PARAM = "priority";
    public static final String JOB_PRIORITY_DESCRIPTION = "Priority of the job";
    public static final String JOB_STATUS_DESCRIPTION = "Job internal status";
    public static final String JOB_VISITED_PARAM = "visited";
    public static final String JOB_VISITED_DESCRIPTION = "Visited status of job";
    public static final String JOB_TAGS = "jobTags";
    public static final String JOB_TAGS_PARAM = "tags";
    public static final String JOB_TAGS_DESCRIPTION = "Job tags";
    public static final String JOB_INPUT_FILES_PARAM = "input";
    public static final String JOB_INPUT_FILES_DESCRIPTION = "Comma separated list of file IDs used as input.";
    public static final String JOB_OUTPUT_FILES_PARAM = "output";
    public static final String JOB_OUTPUT_FILES_DESCRIPTION = "Comma separated list of file IDs used as output.";

    // ---------------------------------------------

    public static final String ANNOTATION_DOC_URL = "http://docs.opencb.org/display/opencga/AnnotationSets+1.4.0";

    public static final String VARIABLE_SET_DESCRIPTION = "Variable set ID or name";
    public static final String ANNOTATION_DESCRIPTION = "Annotation filters. Example: age>30;gender=FEMALE. For more information, " +
            "please visit " + ANNOTATION_DOC_URL;
    public static final String ANNOTATION_AS_MAP_DESCRIPTION = "Indicates whether to show the annotations as key-value";
    public static final String ANNOTATION_SET_ID = "AnnotationSet ID to be updated.";
    public static final String ANNOTATION_SET_NAME = "Annotation set name. If provided, only chosen annotation set will be shown";
    public static final String ANNOTATION_SET_UPDATE_ACTION_DESCRIPTION = "Action to be performed: ADD to add new annotations; REPLACE to replace the value of an already existing "
            + "annotation; SET to set the new list of annotations removing any possible old annotations; REMOVE to remove some "
            + "annotations; RESET to set some annotations to the default value configured in the corresponding variables of the "
            + "VariableSet if any.";
    public static final String ANNOTATION_SET_UPDATE_PARAMS_DESCRIPTION = "Json containing the map of annotations when the action is ADD, SET or REPLACE, a json with only the key "
            + "'remove' containing the comma separated variables to be removed as a value when the action is REMOVE or a json "
            + "with only the key 'reset' containing the comma separated variables that will be set to the default value"
            + " when the action is RESET";
    /**
     * Boolean indicating if the annotations have to be returned flattened or not. Default: false
     */
    public static final String FLATTEN_ANNOTATIONS = "flattenAnnotations";
    public static final String FLATTEN_ANNOTATION_DESCRIPTION = "Boolean indicating to flatten the annotations.";

    // ---------------------------------------------

    public static final String VARIANTS_QUERY_DESCRIPTION = "Filter and fetch variants from indexed VCF files in the variant storage";

    // ---------------------------------------------

    public static final String OUTPUT_DIRECTORY_DESCRIPTION = "Output directory";

    public static final String REGION_DESCRIPTION = "Comma separated list of regions 'chr:start-end, e.g.: 2,3:63500-65000";
    public static final String REGION_PARAM = "region";

    public static final String GENE_DESCRIPTION = "Comma separated list of genes, e.g.: BCRA2,TP53";
    public static final String GENE_PARAM = "gene";

    // ---------------------------------------------
    // alignment

    public static final String ALIGNMENT_INDEX_DESCRIPTION = "Index alignment file";
    public static final String ALIGNMENT_QUERY_DESCRIPTION = "Search over indexed alignments";
    public static final String ALIGNMENT_COVERAGE_DESCRIPTION = "Compute coverage for a given alignemnt file";
    public static final String ALIGNMENT_COVERAGE_QUERY_DESCRIPTION = "Query the coverage of an alignment file for regions or genes";
    public static final String ALIGNMENT_COVERAGE_RATIO_DESCRIPTION = "Compute coverage ratio from file #1 vs file #2, (e.g. somatic vs germline)";
    public static final String ALIGNMENT_STATS_DESCRIPTION = "Compute stats for a given alignment file";
    public static final String ALIGNMENT_STATS_INFO_DESCRIPTION = "Show the stats for a given alignment file";
    public static final String ALIGNMENT_STATS_QUERY_DESCRIPTION = "Fetch alignment files according to their stats";

    // ---------------------------------------------
    // alignment query

    public static final String MINIMUM_MAPPING_QUALITY_DESCRIPTION = "Minimum mapping quality";
    public static final String MINIMUM_MAPPING_QUALITY_PARAM = "minMappingQuality";
    public static final String MAXIMUM_NUMBER_MISMATCHES_DESCRIPTION = "Maximum number of mismatches";
    public static final String MAXIMUM_NUMBER_MISMATCHES_PARAM = "maxNumMismatches";
    public static final String MAXIMUM_NUMBER_HITS_DESCRIPTION = "Maximum number of hits";
    public static final String MAXIMUM_NUMBER_HITS_PARAM = "maxNumHits";
    public static final String PROPERLY_PAIRED_DESCRIPTION = "Return only properly paired alignments";
    public static final String PROPERLY_PAIRED_PARAM = "properlyPaired";
    public static final String MAXIMUM_INSERT_SIZE_DESCRIPTION = "Maximum insert size";
    public static final String MAXIMUM_INSERT_SIZE_PARAM = "maxInsertSize";
    public static final String SKIP_UNMAPPED_DESCRIPTION = "Skip unmapped alignments";
    public static final String SKIP_UNMAPPED_PARAM = "skipUnmapped";
    public static final String SKIP_DUPLICATED_DESCRIPTION = "Skip duplicated alignments";
    public static final String SKIP_DUPLICATED_PARAM = "skipDuplicated";
    public static final String REGION_CONTAINED_DESCRIPTION = "Return alignments contained within boundaries of region";
    public static final String REGION_CONTAINED_PARAM = "regionContained";
    public static final String FORCE_MD_FIELD_DESCRIPTION = "Force SAM MD optional field to be set with the alignments";
    public static final String FORCE_MD_FIELD_PARAM = "forceMDField";
    public static final String BIN_QUALITIES_DESCRIPTION = "Compress the nucleotide qualities by using 8 quality levels";
    public static final String BIN_QUALITIES_PARAM = "binQualities";
    public static final String SPLIT_RESULTS_INTO_REGIONS_DESCRIPTION = "Split results into regions (or gene/exon regions)";
    public static final String SPLIT_RESULTS_INTO_REGIONS_PARAM = "splitResults";

    // ---------------------------------------------
    // alignment coverage

    public static final String OFFSET_DESCRIPTION = "Offset to extend the region, gene or exon at up and downstream";
    public static final String OFFSET_PARAM = "offset";
    public static final String OFFSET_DEFAULT = "200";
    public static final String ONLY_EXONS_DESCRIPTION = "Only exons are taking into account when genes are specified";
    public static final String ONLY_EXONS_PARAM = "onlyExons";
    public static final String COVERAGE_RANGE_DESCRIPTION = "Range of coverage values to be reported. Minimum and maximum values are separated by '-', e.g.: 20-40 (for coverage values greater or equal to 20 and less or equal to 40). A single value means to report coverage values less or equal to that value";
    public static final String COVERAGE_RANGE_PARAM = "range";
    public static final String COVERAGE_WINDOW_SIZE_DESCRIPTION = "Window size for the region coverage (if a coverage range is provided, window size must be 1)";
    public static final String COVERAGE_WINDOW_SIZE_PARAM = "windowSize";
    public static final String COVERAGE_WINDOW_SIZE_DEFAULT = "1";

    public static final String ALIGNMENT_COVERAGE_STATS_DESCRIPTION = "Compute coverage stats per transcript for a list of genes.";
    public static final String LOW_COVERAGE_REGION_THRESHOLD_DESCRIPTION = "Only regions whose coverage depth is under this threshold will be reported.";
    public static final String LOW_COVERAGE_REGION_THRESHOLD_PARAM = "threshold";
    public static final String LOW_COVERAGE_REGION_THRESHOLD_DEFAULT = "30";

    public static final String FILE_ID_PARAM = "file";
    public static final String FILE_ID_1_DESCRIPTION = "Input file #1 (e.g. somatic file)";
    public static final String FILE_ID_1_PARAM = "file1";
    public static final String FILE_ID_2_DESCRIPTION = "Input file #2 (e.g. germline file)";
    public static final String FILE_ID_2_PARAM = "file2";
    public static final String SKIP_LOG2_DESCRIPTION = "Do not apply Log2 to normalise the coverage ratio";
    public static final String SKIP_LOG2_PARAM = "skipLog2";

    // ---------------------------------------------
    // alignment stats query

    public static final String RAW_TOTAL_SEQUENCES = "rawTotalSequences";
    public static final String RAW_TOTAL_SEQUENCES_DESCRIPTION = "Raw total sequences: [<|>|<=|>=]{number}, e.g. >=1000";
    public static final String FILTERED_SEQUENCES = "filteredSequences";
    public static final String FILTERED_SEQUENCES_DESCRIPTION = "Filtered sequences: [<|>|<=|>=]{number}, e.g. <=500";
    public static final String READS_MAPPED = "readsMapped";
    public static final String READS_MAPPED_DESCRIPTION = "Reads mapped: [<|>|<=|>=]{number}, e.g. >3000";
    public static final String READS_MAPPED_AND_PAIRED = "readsMappedAndPaired";
    public static final String READS_MAPPED_AND_PAIRED_DESCRIPTION = "Reads mapped and paired: paired-end technology bit set + both mates"
            + " mapped: [<|>|<=|>=]{number}, e.g. >=1000";
    public static final String READS_UNMAPPED = "readsUnmapped";
    public static final String READS_UNMAPPED_DESCRIPTION = "Reads unmapped: [<|>|<=|>=]{number}, e.g. >=1000";
    public static final String READS_PROPERLY_PAIRED = "readsProperlyPaired";
    public static final String READS_PROPERLY_PAIRED_DESCRIPTION = "Reads properly paired (proper-pair bit set: [<|>|<=|>=]{number}, e.g. >=1000";
    public static final String READS_PAIRED = "readsPaired";
    public static final String READS_PAIRED_DESCRIPTION = "Reads paired: paired-end technology bit set: [<|>|<=|>=]{number}, e.g. >=1000";
    public static final String READS_DUPLICATED = "readsDuplicated";
    public static final String READS_DUPLICATED_DESCRIPTION = "Reads duplicated: PCR or optical duplicate bit set: [<|>|<=|>=]{number}, e.g. >=1000";
    public static final String READS_MQ0 = "readsMQ0";
    public static final String READS_MQ0_DESCRIPTION = "Reads mapped and MQ = 0: [<|>|<=|>=]{number}, e.g. >=1000";
    public static final String READS_QC_FAILED = "readsQCFailed";
    public static final String READS_QC_FAILED_DESCRIPTION = "Reads QC failed: [<|>|<=|>=]{number}, e.g. >=1000";
    public static final String NON_PRIMARY_ALIGNMENTS = "nonPrimaryAlignments";
    public static final String NON_PRIMARY_ALIGNMENTS_DESCRIPTION = "Non-primary alignments: [<|>|<=|>=]{number}, e.g. <=100";
    public static final String MISMATCHES = "mismatches";
    public static final String MISMATCHES_DESCRIPTION = "Mismatches from NM fields: [<|>|<=|>=]{number}, e.g. <=100";
    public static final String ERROR_RATE = "errorRate";
    public static final String ERROR_RATE_DESCRIPTION = "Error rate: mismatches / bases mapped (cigar): [<|>|<=|>=]{number}, e.g. <=0.002";
    public static final String AVERAGE_LENGTH = "averageLength";
    public static final String AVERAGE_LENGTH_DESCRIPTION = "Average_length: [<|>|<=|>=]{number}, e.g. >=90.0";
    public static final String AVERAGE_FIRST_FRAGMENT_LENGTH = "averageFirstFragmentLength";
    public static final String AVERAGE_FIRST_FRAGMENT_LENGTH_DESCRIPTION = "Average first fragment length: [<|>|<=|>=]{number}, e.g. >=90.0";
    public static final String AVERAGE_LAST_FRAGMENT_LENGTH = "averageLastFragmentLength";
    public static final String AVERAGE_LAST_FRAGMENT_LENGTH_DESCRIPTION = "Average_last_fragment_length: [<|>|<=|>=]{number}, e.g. >=90.0";
    public static final String AVERAGE_QUALITY = "averageQuality";
    public static final String AVERAGE_QUALITY_DESCRIPTION = "Average quality: [<|>|<=|>=]{number}, e.g. >=35.5";
    public static final String INSERT_SIZE_AVERAGE = "insertSizeAverage";
    public static final String INSERT_SIZE_AVERAGE_DESCRIPTION = "Insert size average: [<|>|<=|>=]{number}, e.g. >=100.0";
    public static final String INSERT_SIZE_STANDARD_DEVIATION = "insertSizeStandardDeviation";
    public static final String INSERT_SIZE_STANDARD_DEVIATION_DESCRIPTION = "Insert size standard deviation: [<|>|<=|>=]{number}, e.g. <=1.5";
    public static final String PAIRS_WITH_OTHER_ORIENTATION = "pairsWithOtherOrientation";
    public static final String PAIRS_WITH_OTHER_ORIENTATION_DESCRIPTION = "Pairs with other orientation: [<|>|<=|>=]{number}, e.g. >=1000";
    public static final String PAIRS_ON_DIFFERENT_CHROMOSOMES = "pairsOnDifferentChromosomes";
    public static final String PAIRS_ON_DIFFERENT_CHROMOSOMES_DESCRIPTION = "Pairs on different chromosomes: [<|>|<=|>=]{number}, e.g. >=1000";
    public static final String PERCENTAGE_OF_PROPERLY_PAIRED_READS = "percentageOfProperlyPairedReads";
    public static final String PERCENTAGE_OF_PROPERLY_PAIRED_READS_DESCRIPTION = "Percentage of properly paired reads: [<|>|<=|>=]{number}, e.g. >=96.5";

    // ---------------------------------------------

    public static final String SAMTOOLS_COMMANDS = "'sort', 'index' , 'view', 'stats', 'dict', 'faidx', 'depth'";
    public static final String SAMTOOLS_COMMAND_DESCRIPTION = "Samtools command: " + SAMTOOLS_COMMANDS;

    public static final String INPUT_FILE_DESCRIPTION = "Input file (full path).";
    public static final String INPUT_FILE_PARAM = "inputFile";
    public static final String OUTPUT_FILENAME_DESCRIPTION = "Output file name.";
    public static final String OUTPUT_FILENAME_PARAM = "outputFilename";
    public static final String REFERENCE_FILE_DESCRIPTION = "Reference sequence FASTA file";
    public static final String REFERENCE_FILE_PARAM = "referenceFile";
    public static final String READ_GROUP_FILE_DESCRIPTION = "Only include reads with read group listed in this file";
    public static final String READ_GROUP_FILE_PARAM = "readGroupFile";
    public static final String BED_FILE_DESCRIPTION = "File containing a list of positions or regions";
    public static final String BED_FILE_PARAM = "bedFile";
    public static final String REF_SEQ_FILE_DESCRIPTION = "Reference sequence (required for GC-depth and mismatches-per-cycle calculation)";
    public static final String REF_SEQ_FILE_PARAM = "refSeqFile";
    public static final String REFERENCE_NAMES_DESCRIPTION = "File listing reference names and lengths";
    public static final String REFERENCE_NAMES_PARAM = "referenceNamesFile";
    public static final String TARGET_REGION_DESCRIPTION = "Do stats in these regions only. Tab-delimited file chr,from,to, 1-based, inclusive";
    public static final String TARGET_REGION_PARAM = "targetRegionFile";
    public static final String READS_NOT_SELECTED_FILENAME_DESCRIPTION = "Output reads not selected by filters will be written into this file";
    public static final String READS_NOT_SELECTED_FILENAME_PARAM = "readsNotSelectedFilename";

    // ---------------------------------------------

    public static final String PICARD_TOOL_NAME_PARAMETER = "command";
    public static final String PICARD_TOOL_NAME_DESCRIPTION = "Picard tool name to execute. Supported tools: CollectHsMetrics, CollectWgsMetrics, BedToInvervalList";
    // ---------------------------------------------

    public static final String RPC_METHOD_DESCRIPTION = "RPC method used: {auto, gRPC, REST}. When auto, it will first try with gRPC and "
            + "if that does not work, it will try with REST";

    // ---------------------------------------------

    public static final String COMMAND_PARAMETER = "command";
    public static final String COMMAND_PARAMETER_DESCRIPTION = "Command name to execute in this tool.";

}
