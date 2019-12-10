package org.opencb.opencga.core.api;

public class ParamConstants {
    private static final String UP_TO_100 = " up to a maximum of 100";

    public static final String INCLUDE_DESCRIPTION = "Fields included in the response, whole JSON path must be provided";
    public static final String EXCLUDE_DESCRIPTION = "Fields excluded in the response, whole JSON path must be provided";
    public static final String LIMIT_DESCRIPTION = "Number of results to be returned";
    public static final String SKIP_DESCRIPTION = "Number of results to skip";
    public static final String COUNT_DESCRIPTION = "Get a count of the number of results obtained. Deactivated by default.";

    public static final String CREATION_DATE_DESCRIPTION = "Creation date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805";
    public static final String MODIFICATION_DATE_DESCRIPTION = "Modification date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805";

    // ---------------------------------------------

    public static final String USER_DESCRIPTION = "User id";

    // ---------------------------------------------

    public static final String PROJECT_PARAM = "project";
    public static final String PROJECT_DESCRIPTION = "Project [user@]project where project can be either the ID or the alias";
    public static final String PROJECTS_DESCRIPTION = "Comma separated list of projects [user@]project" + UP_TO_100;

    // ---------------------------------------------

    public static final String STUDY_PARAM = "study";
    public static final String STUDY_DESCRIPTION = "Study [[user@]project:]study where study and project can be either the id or alias";
    public static final String STUDIES_PARAM = "studies";
    public static final String STUDIES_DESCRIPTION = "Comma separated list of Studies [[user@]project:]study where study and project can be either the id or alias" + UP_TO_100;

    public static final String STUDY_NAME_DESCRIPTION = "Study name";
    public static final String STUDY_ID_DESCRIPTION = "Study id";
    public static final String STUDY_ALIAS_DESCRIPTION = "Study alias";
    public static final String STUDY_FQN_DESCRIPTION = "Study full qualified name";
    public static final String SILENT_DESCRIPTION = "Boolean to retrieve all possible entries that are queried for, false to raise an "
            + "exception whenever one of the entries looked for cannot be shown for whichever reason";

    // ---------------------------------------------

    public static final String FILES_DESCRIPTION = "Comma separated list of file ids or names" + UP_TO_100;
    public static final String FILE_ID_DESCRIPTION = "File ID";
    public static final String FILE_NAME_DESCRIPTION = "File name";
    public static final String FILE_PATH_DESCRIPTION = "File path";
    public static final String FILE_TYPE_DESCRIPTION = "File type, either FILE or DIRECTORY";
    public static final String FILE_FORMAT_DESCRIPTION = "Comma separated Format values. For existing Formats see files/formats";
    public static final String FILE_BIOFORMAT_DESCRIPTION = "Comma separated Bioformat values. For existing Bioformats see files/bioformats";
    public static final String FILE_STATUS_DESCRIPTION = "File status";
    public static final String FILE_DESCRIPTION_DESCRIPTION = "Description";
    public static final String FILE_TAGS_DESCRIPTION = "Tags";
    public static final String FILE_DIRECTORY_DESCRIPTION = "Directory under which we want to look for files or folders";
    public static final String FILE_CREATION_DATA_DESCRIPTION = "Creation date of the file";
    public static final String FILE_MODIFICATION_DATE_DESCRIPTION = "Last modification date of the file";
    public static final String FILE_SIZE_DESCRIPTION = "File size";

    // ---------------------------------------------

    public static final String SAMPLE_DESCRIPTION = "Sample id or name";
    public static final String SAMPLES_DESCRIPTION = "Comma separated list sample IDs or names" + UP_TO_100;

    public static final String SAMPLE_ID_DESCRIPTION = "Sample id";
    public static final String SAMPLE_NAME_DESCRIPTION = "Sample name";

    // ---------------------------------------------

    public static final String INDIVIDUALS_DESCRIPTION = "Comma separated list of individual names or ids" + UP_TO_100;
    public static final String INDIVIDUAL_DESCRIPTION = "Individual ID or name";

    // ---------------------------------------------

    public static final String FAMILIES_DESCRIPTION = "Comma separated list of family IDs or names" + UP_TO_100;

    // ---------------------------------------------

    public static final String COHORT_DESCRIPTION = "Cohort id or name";
    public static final String COHORTS_DESCRIPTION = "Comma separated list of cohort names or ids" + UP_TO_100;

    public static final String COHORT_ID_DESCRIPTION = "Cohort id";
    public static final String COHORT_NAME_DESCRIPTION = "Cohort name";
    public static final String COHORT_TYPE_DESCRIPTION = "Cohort type";
    public static final String COHORT_STATUS_DESCRIPTION = "Cohort status";

    // ---------------------------------------------

    public static final String CLINICAL_ANALYSES_DESCRIPTION = "Comma separated list of clinical analysis IDs or names" + UP_TO_100;

    // ---------------------------------------------

    public static final String PANELS_DESCRIPTION = "Comma separated list of panel ids" + UP_TO_100;

    // ---------------------------------------------

    public static final String JOBS_DESCRIPTION = "Comma separated list of job ids or names" + UP_TO_100;
    public static final String JOB_NAME = "jobName";
    public static final String JOB_NAME_DESCRIPTION = "Job Name";
    public static final String JOB_DESCRIPTION = "jobDescription";
    public static final String JOB_DESCRIPTION_DESCRIPTION = "Job Description";
    public static final String JOB_TAGS = "jobTags";
    public static final String JOB_TAGS_DESCRIPTION = "Job Tags";

    // ---------------------------------------------

    public static final String VARIANBLE_SET_DESCRIPTION = "Variable set id or name";
    public static final String ANNOTATION_DESCRIPTION = "Annotation, e.g: key1=value(,key2=value)";
    public static final String ANNOTATION_AS_MAP_DESCRIPTION = "Indicates whether to show the annotations as key-value";
    public static final String ANNOTATION_SET_ID = "AnnotationSet id to be updated.";
    public static final String ANNOTATION_SET_NAME = "Annotation set name. If provided, only chosen annotation set will be shown";
    public static final String ANNOTATION_SET_UPDATE_ACTION_DESCRIPTION = "Action to be performed: ADD to add new annotations; REPLACE to replace the value of an already existing "
            + "annotation; SET to set the new list of annotations removing any possible old annotations; REMOVE to remove some "
            + "annotations; RESET to set some annotations to the default value configured in the corresponding variables of the "
            + "VariableSet if any.";
    public static final String ANNOTATION_SET_UPDATE_PARAMS_DESCRIPTION = "Json containing the map of annotations when the action is ADD, SET or REPLACE, a json with only the key "
            + "'remove' containing the comma separated variables to be removed as a value when the action is REMOVE or a json "
            + "with only the key 'reset' containing the comma separated variables that will be set to the default value"
            + " when the action is RESET";

    // ---------------------------------------------

    public static final String VARIANTS_EXPORT_DESCRIPTION = "Filter and export variants from the variant storage to a file";
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
    public static final String ALIGNMENT_COVERAGE_LOG_2_RATIO_DESCRIPTION = "Compute Log2 coverage ratio from file #1 (somatic) and file #2 (germline)";
    public static final String ALIGNMENT_STATS_DESCRIPTION = "Compute stats for a given alignment file";
    public static final String ALIGNMENT_STATS_INFO_DESCRIPTION = "Show the stats for a given alignment file";
    public static final String ALIGNMENT_STATS_QUERY_DESCRIPTION = "Fetch alignment files according to their stats";

    // ---------------------------------------------
    // alignment coverage

    public static final String GENE_OFFSET_DESCRIPTION = "Gene offset to extend the gene region at up and downstream";
    public static final String GENE_OFFSET_PARAM = "geneOffset";
    public static final int GENE_OFFSET_DEFAULT = 500;
    public static final String ONLY_EXONS_DESCRIPTION = "Take only exons regions when taking into account genes";
    public static final String ONLY_EXONS_PARAM = "onlyExons";
    public static final String EXON_OFFSET_DESCRIPTION = "Exon offset to extend the gene region at up and downstream";
    public static final String EXON_OFFSET_PARAM = "exonOffset";
    public static final int EXON_OFFSET_DEFAULT = 50;
    public static final String COVERAGE_RANGE_DESCRIPTION = "Range of coverage values to be reported. Minimum and maximum values are separated by '-', e.g.: 20-40 (for coverage values greater or equal to 20 and less or equal to 40). A single value means to report coverage values less or equal to that value";
    public static final String COVERAGE_RANGE_PARAM = "range";
    public static final String COVERAGE_WINDOW_SIZE_DESCRIPTION = "Window size for the region coverage (if a coverage range is provided, window size must be 1)";
    public static final String COVERAGE_WINDOW_SIZE_PARAM = "windowSize";
    public static final int COVERAGE_WINDOW_SIZE_DEFAULT = 1;
    public static final String FILE_ID_PARAM = "inputFile";
    public static final String FILE_ID_1_DESCRIPTION = "Input file #1";
    public static final String FILE_ID_1_PARAM = "inputFile1";
    public static final String FILE_ID_2_DESCRIPTION = "Input file #2";
    public static final String FILE_ID_2_PARAM = "inputFile2";

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
}
