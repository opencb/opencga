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
    public static final String FILE_ID_DESCRIPTION = "File id";
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


}
