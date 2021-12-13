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
    public static final String INCLUDE_RESULT_PARAM = "includeResult";
    public static final String INCLUDE_RESULT_DESCRIPTION = "Flag indicating to include the created or updated document result in the response";
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
    public static final String INTERNAL_INDEX_STATUS_PARAM = "internalIndexStatus";
    public static final String INTERNAL_INDEX_STATUS_DESCRIPTION = "Filter by internal index status";
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
    public static final String DISTINCT_FIELD_PARAM = "field";
    public static final String DISTINCT_FIELD_DESCRIPTION = "Field for which to obtain the distinct values";
    public static final String PHENOTYPES_PARAM = "phenotypes";
    public static final String PHENOTYPES_DESCRIPTION = "Comma separated list of phenotype ids or names";
    public static final String DISORDERS_PARAM = "disorders";
    public static final String DISORDERS_DESCRIPTION = "Comma separated list of disorder ids or names";
    public static final String BODY_PARAM = "body";
    public static final String OVERWRITE = "overwrite";
    public static final String FORCE = "force";

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

    public static final String USER_PROJECT_SEPARATOR = "@";
    // ---------------------------------------------
    public static final String PROJECT_STUDY_SEPARATOR = ":";
    public static final String OPENCGA_USER_ID = "opencga";
    public static final String ADMIN_PROJECT = "admin";
    public static final String ADMIN_STUDY = "admin";
    public static final String ADMIN_STUDY_FQN = OPENCGA_USER_ID + USER_PROJECT_SEPARATOR + ADMIN_PROJECT + PROJECT_STUDY_SEPARATOR + ADMIN_STUDY;
    public static final String ANONYMOUS_USER_ID = "*";         // Any user, authenticated or not
    public static final String REGISTERED_USERS = "REGISTERED"; // Any authenticated user
    public static final String MEMBERS_GROUP = "@members";
    public static final String ADMINS_GROUP = "@admins";
    // -------------------- AUDIT -------------------------
    public static final String OPERATION_ID = "operationId";
    public static final String OPERATION_ID_DESCRIPTION = "Audit operation UUID";
    public static final String USER_ID = "userId";
    public static final String ACTION = "action";
    public static final String ACTION_DESCRIPTION = "Action performed by the user";
    public static final String RESOURCE = "resource";
    public static final String RESOURCE_DESCRIPTION = "Resource involved";
    public static final String RESOURCE_ID = "resourceId";
    public static final String RESOURCE_ID_DESCRIPTION = "Resource ID";
    public static final String RESOURCE_UUID = "resourceUuid";
    public static final String RESOURCE_UUID_DESCRIPTION = "resource UUID";
    public static final String STATUS = "status";
    public static final String DATE = "date";
    public static final String DATE_DESCRIPTION = "Date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805";
    public static final String USER = "user";

    // ---------------------------------------------
    public static final String USER_DESCRIPTION = "User ID";
    public static final String USERS_DESCRIPTION = "Comma separated list of user IDs";
    public static final String USER_ACCOUNT_TYPE = "account";
    public static final String USER_ACCOUNT_TYPE_DESCRIPTION = "Account type [GUEST, FULL, ADMINISTRATOR]";
    public static final String USER_AUTHENTICATION_ORIGIN = "authenticationId";
    public static final String USER_AUTHENTICATION_ORIGIN_DESCRIPTION = "Authentication origin ID";
    public static final String USER_CREATION_DATE = "creationDate";
    public static final String USER_CREATION_DATE_DESCRIPTION = CREATION_DATE_DESCRIPTION;
    public static final String PROJECT_PARAM = "project";

    // ---------------------------------------------
    public static final String PROJECT_DESCRIPTION = "Project [user@]project where project can be either the ID or the alias";
    public static final String STUDY_PARAM = "study";
    public static final String STUDY_DESCRIPTION = "Study [[user@]project:]study where study and project can be either the ID or UUID";

    // ---------------------------------------------
    public static final String OTHER_STUDIES_FLAG = "otherStudies";
    public static final String OTHER_STUDIES_FLAG_DESCRIPTION = "Flag indicating the entries being queried can belong to any related study, not just the primary one.";
    public static final String STUDIES_PARAM = "studies";
    public static final String STUDY_NAME_DESCRIPTION = "Study name";
    public static final String STUDY_ID_DESCRIPTION = "Study ID";
    public static final String STUDY_ALIAS_DESCRIPTION = "Study alias";
    public static final String STUDY_FQN_DESCRIPTION = "Study full qualified name";
    public static final String SILENT_DESCRIPTION = "Boolean to retrieve all possible entries that are queried for, false to raise an "
            + "exception whenever one of the entries looked for cannot be shown for whichever reason";

    // ---------------------------------------------

    public static final String FILES_DESCRIPTION = "Comma separated list of file IDs or names" + UP_TO_100;
    public static final String FILES_ID_DESCRIPTION = "Comma separated list of file IDs" + UP_TO_100;
    public static final String FILES_UUID_DESCRIPTION = "Comma separated list file UUIDs" + UP_TO_100;
    public static final String FILE_ID_DESCRIPTION = "File ID";
    public static final String FILE_NAME_DESCRIPTION = "File name";
    public static final String FILE_NAMES_DESCRIPTION = "Comma separated list of file names";

    // ---------------------------------------------
    public static final String FILE_PATH_PARAM = "path";
    public static final String FILE_PATH_DESCRIPTION = "File path";
    public static final String FILE_PATHS_DESCRIPTION = "Comma separated list of paths";
    public static final String FILE_URIS_DESCRIPTION = "Comma separated list of uris";
    public static final String FILE_TYPE_DESCRIPTION = "File type, either FILE or DIRECTORY";
    public static final String FILE_FORMAT_DESCRIPTION = "Comma separated Format values. For existing Formats see files/formats";
    public static final String FILE_EXTERNAL_DESCRIPTION = "Boolean field indicating whether to filter by external or non external files";
    public static final String FILE_BIOFORMAT_DESCRIPTION = "Comma separated Bioformat values. For existing Bioformats see files/bioformats";
    public static final String FILE_STATUS_DESCRIPTION = "File status";
    public static final String FILE_DESCRIPTION_DESCRIPTION = "Description";
    public static final String FILE_TAGS_DESCRIPTION = "Tags";
    public static final String FILE_SOFTWARE_NAME_PARAM = "softwareName";
    public static final String FILE_SOFTWARE_NAME_DESCRIPTION = "Software name";
    public static final String FILE_JOB_ID_DESCRIPTION = "Job ID that created the file(s) or folder(s)";
    public static final String FILE_DIRECTORY_DESCRIPTION = "Directory under which we want to look for files or folders";
    public static final String FILE_CREATION_DATA_DESCRIPTION = "Creation date of the file";
    public static final String FILE_MODIFICATION_DATE_DESCRIPTION = "Last modification date of the file";
    public static final String FILE_SIZE_DESCRIPTION = "File size";
    public static final String FILE_FOLDER = "folder";
    public static final String FILE_FOLDER_DESCRIPTION = "Folder ID, name or path";
    public static final String FILE_PARENTS_PARAM = "parents";
    public static final String FILE_PARENTS_DESCRIPTION = "Create the parent directories if they do not exist";


    public static final String FILE_ALREADY_LINKED = "File already linked. Nothing to do";
    public static final int MAXIMUM_LINES_CONTENT = 1000;
    public static final String MAXIMUM_LINES_CONTENT_DESCRIPTION =
            "Maximum number of lines to be returned up to a maximum of " + MAXIMUM_LINES_CONTENT;
    public static final String PHENOTYPES_ACTION_PARAM = "phenotypesAction";
    public static final String PHENOTYPES_ACTION_DESCRIPTION = "Action to be performed if the array of phenotypes is being updated "
            + "[SET, ADD, REMOVE]";
    public static final String SAMPLE_DESCRIPTION = "Sample ID or UUID";
    public static final String SAMPLES_DESCRIPTION = "Comma separated list sample IDs or UUIDs" + UP_TO_100;
    public static final String SAMPLES_ID_DESCRIPTION = "Comma separated list sample IDs" + UP_TO_100;
    public static final String SAMPLES_UUID_DESCRIPTION = "Comma separated list sample UUIDs" + UP_TO_100;
    public static final String SAMPLE_INDIVIDUAL_ID_PARAM = "individualId";
    public static final String SAMPLE_INDIVIDUAL_ID_DESCRIPTION = "Individual ID or UUID";
    public static final String SAMPLE_FILE_IDS_PARAM = "fileIds";

    // ---------------------------------------------
    public static final String SAMPLE_FILE_IDS_DESCRIPTION = "Comma separated list of file IDs, paths or UUIDs";
    public static final String SAMPLE_PHENOTYPES_ACTION_PARAM = PHENOTYPES_ACTION_PARAM;
    public static final String SAMPLE_PHENOTYPES_ACTION_DESCRIPTION = PHENOTYPES_ACTION_DESCRIPTION;
    public static final String SAMPLE_ID_PARAM = "id";
    public static final String SAMPLE_UUID_PARAM = "uuid";
    public static final String SAMPLE_PARAM = "sample";
    public static final String SAMPLE_ID_DESCRIPTION = "Sample ID";
    public static final String SAMPLE_NAME_DESCRIPTION = "Sample name";
    public static final String SAMPLE_SOMATIC_PARAM = "somatic";
    public static final String SAMPLE_SOMATIC_DESCRIPTION = "Somatic sample";
    public static final String SAMPLE_RGA_STATUS_PARAM = "internalRgaStatus";
    public static final String SAMPLE_RGA_STATUS_DESCRIPTION = "Index status of the sample for the Recessive Gene Analysis";
    public static final String SAMPLE_PROCESSING_PRODUCT_PARAM = "processingProduct";
    public static final String SAMPLE_PROCESSING_PREPARATION_METHOD_PARAM = "processingPreparationMethod";
    public static final String SAMPLE_PROCESSING_EXTRACTION_METHOD_PARAM = "processingExtractionMethod";
    public static final String SAMPLE_PROCESSING_LAB_SAMPLE_ID_PARAM = "processingLabSampleId";
    public static final String SAMPLE_COLLECTION_FROM_PARAM = "collectionFrom";
    public static final String SAMPLE_COLLECTION_METHOD_PARAM = "collectionMethod";
    public static final String SAMPLE_COLLECTION_TYPE_PARAM = "collectionType";
    public static final String SAMPLE_PROCESSING_PRODUCT_DESCRIPTION = "Processing product";
    public static final String SAMPLE_PROCESSING_PREPARATION_METHOD_DESCRIPTION = "Processing preparation method";
    public static final String SAMPLE_PROCESSING_EXTRACTION_METHOD_DESCRIPTION = "Processing extraction method";
    public static final String SAMPLE_PROCESSING_LAB_SAMPLE_ID_DESCRIPTION = "Processing lab sample id";
    public static final String SAMPLE_COLLECTION_FROM_DESCRIPTION = "Collection from";
    public static final String SAMPLE_COLLECTION_TYPE_DESCRIPTION = "Collection type";
    public static final String SAMPLE_COLLECTION_METHOD_DESCRIPTION = "Collection method";
    public static final String SAMPLE_VERSION_PARAM = "version";
    public static final String SAMPLE_VERSION_DESCRIPTION = "Comma separated list of sample versions. 'all' to get all the sample versions."
            + " Not supported if multiple sample ids are provided";

    public static final String SAMPLE_VARIANT_STATS_ID_PARAM = "statsId";
    public static final String SAMPLE_VARIANT_STATS_COUNT_PARAM = "statsVariantCount";
    public static final String SAMPLE_VARIANT_STATS_CHROMOSOME_COUNT_PARAM = "statsChromosomeCount";
    public static final String SAMPLE_VARIANT_STATS_TYPE_COUNT_PARAM = "statsTypeCount";
    public static final String SAMPLE_VARIANT_STATS_GENOTYPE_COUNT_PARAM = "statsGenotypeCount";
    public static final String SAMPLE_VARIANT_STATS_TI_TV_RATIO_PARAM = "statsTiTvRatio";
    public static final String SAMPLE_VARIANT_STATS_QUALITY_AVG_PARAM = "statsQualityAvg";
    public static final String SAMPLE_VARIANT_STATS_QUALITY_STD_DEV_PARAM = "statsQualityStdDev";
    public static final String SAMPLE_VARIANT_STATS_HETEROZYGOSITY_RATE_PARAM = "statsHeterozygosityRate";
    public static final String SAMPLE_VARIANT_STATS_DEPTH_COUNT_PARAM = "statsDepthCount";
    public static final String SAMPLE_VARIANT_STATS_BIOTYPE_COUNT_PARAM = "statsBiotypeCount";
    public static final String SAMPLE_VARIANT_STATS_CLINICAL_SIGNIFICANCE_COUNT_PARAM = "statsClinicalSignificanceCount";
    public static final String SAMPLE_VARIANT_STATS_CONSEQUENCE_TYPE_COUNT_PARAM = "statsConsequenceTypeCount";
    public static final String SAMPLE_VARIANT_STATS_ID_DESCRIPTION = "Sample variant stats Id. If this field is not provided and the user "
            + "filters by other stats fields, it will automatically be set to ALL";
    public static final String SAMPLE_VARIANT_STATS_COUNT_DESCRIPTION = "Sample variant stats VariantCount";
    public static final String SAMPLE_VARIANT_STATS_CHROMOSOME_COUNT_DESCRIPTION = "Sample variant stats ChromosomeCount";
    public static final String SAMPLE_VARIANT_STATS_TYPE_COUNT_DESCRIPTION = "Sample variant stats TypeCount";
    public static final String SAMPLE_VARIANT_STATS_GENOTYPE_COUNT_DESCRIPTION = "Sample variant stats GenotypeCount";
    public static final String SAMPLE_VARIANT_STATS_TI_TV_RATIO_DESCRIPTION = "Sample variant stats TiTvRatio";
    public static final String SAMPLE_VARIANT_STATS_QUALITY_AVG_DESCRIPTION = "Sample variant stats QualityAvg";
    public static final String SAMPLE_VARIANT_STATS_QUALITY_STD_DEV_DESCRIPTION = "Sample variant stats QualityStdDev";
    public static final String SAMPLE_VARIANT_STATS_HETEROZYGOSITY_RATE_DESCRIPTION = "Sample variant stats HeterozygosityRate";
    public static final String SAMPLE_VARIANT_STATS_DEPTH_COUNT_DESCRIPTION = "Sample variant stats DepthCount";
    public static final String SAMPLE_VARIANT_STATS_BIOTYPE_COUNT_DESCRIPTION = "Sample variant stats BiotypeCount";
    public static final String SAMPLE_VARIANT_STATS_CLINICAL_SIGNIFICANCE_COUNT_DESCRIPTION = "Sample variant stats ClinicalSignificanceCount";
    public static final String SAMPLE_VARIANT_STATS_CONSEQUENCE_TYPE_COUNT_DESCRIPTION = "Sample variant stats ConsequenceTypeCount";
    public static final String SAMPLE_COHORT_IDS_PARAM = "cohortIds";
    public static final String SAMPLE_COHORT_IDS_DESCRIPTION = "Comma separated list of cohort IDs";

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
    public static final String SAMPLES_ACTION_PARAM = "samplesAction";
    public static final String SAMPLES_ACTION_DESCRIPTION = "Action to be performed if the array of samples is being updated.";

    // ---------------------------------------------

    public static final String INDIVIDUAL_DESCRIPTION = "Individual ID, name or UUID";
    public static final String INDIVIDUALS_DESCRIPTION = "Comma separated list of individual IDs, names or UUIDs" + UP_TO_100;
    public static final String INDIVIDUALS_ID_DESCRIPTION = "Comma separated list individual IDs" + UP_TO_100;
    public static final String INDIVIDUAL_NAME_DESCRIPTION = "Comma separated list individual names" + UP_TO_100;
    public static final String INDIVIDUAL_UUID_DESCRIPTION = "Comma separated list individual UUIDs" + UP_TO_100;
    public static final String INDIVIDUAL_VERSION_PARAM = "version";
    public static final String INDIVIDUAL_VERSION_DESCRIPTION = "Comma separated list of individual versions. 'all' to get all the "
            + "individual versions. Not supported if multiple individual ids are provided";
    public static final String INDIVIDUAL_FAMILY_IDS_PARAM = "familyIds";
    public static final String INDIVIDUAL_FAMILY_IDS_DESCRIPTION = "Comma separated list of family ids the individuals may belong to.";

    // ---------------------------------------------
    public static final String INDIVIDUAL_PHENOTYPES_ACTION_PARAM = PHENOTYPES_ACTION_PARAM;
    public static final String INDIVIDUAL_PHENOTYPES_ACTION_DESCRIPTION = PHENOTYPES_ACTION_DESCRIPTION;
    public static final String INDIVIDUAL_DISORDERS_ACTION_PARAM = "disordersAction";
    public static final String INDIVIDUAL_DISORDERS_ACTION_DESCRIPTION = "Action to be performed if the array of disorders is being"
            + " updated [SET, ADD, REMOVE]";
    public static final String INDIVIDUAL_ID_PARAM = "id";
    public static final String INDIVIDUAL_NAME_PARAM = "name";
    public static final String INDIVIDUAL_UUID_PARAM = "uuid";
    public static final String INDIVIDUAL_FATHER_PARAM = "father";
    public static final String INDIVIDUAL_MOTHER_PARAM = "mother";
    public static final String INDIVIDUAL_SAMPLES_PARAM = "samples";
    public static final String INDIVIDUAL_SEX_PARAM = "sex";
    public static final String INDIVIDUAL_ETHNICITY_PARAM = "ethnicity";
    public static final String INDIVIDUAL_DATE_OF_BIRTH_PARAM = "dateOfBirth";
    public static final String INDIVIDUAL_DISORDERS_PARAM = DISORDERS_PARAM;
    public static final String INDIVIDUAL_PHENOTYPES_PARAM = PHENOTYPES_PARAM;
    public static final String INDIVIDUAL_POPULATION_NAME_PARAM = "populationName";
    public static final String INDIVIDUAL_POPULATION_SUBPOPULATION_PARAM = "populationSubpopulation";
    public static final String INDIVIDUAL_KARYOTYPIC_SEX_PARAM = "karyotypicSex";
    public static final String INDIVIDUAL_LIFE_STATUS_PARAM = "lifeStatus";
    public static final String INDIVIDUAL_DELETED_PARAM = DELETED_PARAM;
    public static final String INDIVIDUAL_CREATION_DATE_PARAM = CREATION_DATE_PARAM;
    public static final String INDIVIDUAL_MODIFICATION_DATE_PARAM = MODIFICATION_DATE_PARAM;
    public static final String INDIVIDUAL_RELEASE_PARAM = RELEASE_PARAM;
    public static final String INDIVIDUAL_SNAPSHOT_PARAM = SNAPSHOT_PARAM;
    public static final String INDIVIDUAL_FATHER_DESCRIPTION = "Father ID, name or UUID";
    public static final String INDIVIDUAL_MOTHER_DESCRIPTION = "Mother ID, name or UUID";
    public static final String INDIVIDUAL_SAMPLES_DESCRIPTION = "Sample ID, name or UUID";
    public static final String INDIVIDUAL_SEX_DESCRIPTION = "Individual sex";
    public static final String INDIVIDUAL_ETHNICITY_DESCRIPTION = "Individual ethnicity";
    public static final String INDIVIDUAL_DATE_OF_BIRTH_DESCRIPTION = "Individual date of birth";
    public static final String INDIVIDUAL_DISORDERS_DESCRIPTION = DISORDERS_DESCRIPTION;
    public static final String INDIVIDUAL_PHENOTYPES_DESCRIPTION = PHENOTYPES_DESCRIPTION;
    public static final String INDIVIDUAL_POPULATION_NAME_DESCRIPTION = "Population name";
    public static final String INDIVIDUAL_POPULATION_SUBPOPULATION_DESCRIPTION = "Subpopulation name";
    public static final String INDIVIDUAL_KARYOTYPIC_SEX_DESCRIPTION = "Individual karyotypic sex";
    public static final String INDIVIDUAL_LIFE_STATUS_DESCRIPTION = "Individual life status";
    public static final String INDIVIDUAL_DELETED_DESCRIPTION = DELETED_DESCRIPTION;
    public static final String INDIVIDUAL_CREATION_DATE_DESCRIPTION = CREATION_DATE_DESCRIPTION;
    public static final String INDIVIDUAL_MODIFICATION_DATE_DESCRIPTION = MODIFICATION_DATE_DESCRIPTION;
    public static final String INDIVIDUAL_RELEASE_DESCRIPTION = RELEASE_DESCRIPTION;
    public static final String INDIVIDUAL_SNAPSHOT_DESCRIPTION = SNAPSHOT_DESCRIPTION;

    // ---------------------------------------------

    public static final String FAMILIES_DESCRIPTION = "Comma separated list of family IDs or names" + UP_TO_100;
    public static final String FAMILY_UPDATE_ROLES_PARAM = "updateRoles";
    public static final String FAMILY_UPDATE_ROLES_DESCRIPTION = "Update the member roles within the family";
    public static final String FAMILY_VERSION_PARAM = "version";
    public static final String FAMILY_VERSION_DESCRIPTION = "Comma separated list of family versions. 'all' to get all the "
            + "family versions. Not supported if multiple family ids are provided";
    public static final String FAMILY_ID_PARAM = "id";
    public static final String FAMILY_UUID_PARAM = "uuid";
    public static final String FAMILY_NAME_PARAM = "name";
    public static final String FAMILY_MEMBERS_PARAM = "members";
    public static final String FAMILY_SAMPLES_PARAM = "samples";
    public static final String FAMILY_EXPECTED_SIZE_PARAM = "expectedSize";
    public static final String FAMILY_PHENOTYPES_PARAM = PHENOTYPES_PARAM;
    public static final String FAMILY_DISORDERS_PARAM = DISORDERS_PARAM;
    public static final String FAMILY_CREATION_DATE_PARAM = CREATION_DATE_PARAM;
    public static final String FAMILY_MODIFICATION_DATE_PARAM = MODIFICATION_DATE_PARAM;
    public static final String FAMILY_DELETED_PARAM = DELETED_PARAM;
    public static final String FAMILY_STATUS_PARAM = STATUS_PARAM;
    public static final String FAMILY_INTERNAL_STATUS_PARAM = INTERNAL_STATUS_PARAM;
    public static final String FAMILY_ANNOTATION_PARAM = "annotation";
    public static final String FAMILY_ACL_PARAM = ACL_PARAM;
    public static final String FAMILY_RELEASE_PARAM = RELEASE_PARAM;
    public static final String FAMILY_SNAPSHOT_PARAM = SNAPSHOT_PARAM;
    public static final String FAMILY_ID_DESCRIPTION = "Comma separated list family IDs" + UP_TO_100;
    public static final String FAMILY_NAME_DESCRIPTION = "Comma separated list family names" + UP_TO_100;
    public static final String FAMILY_UUID_DESCRIPTION = "Comma separated list family UUIDs" + UP_TO_100;
    public static final String FAMILY_MEMBERS_DESCRIPTION = "Comma separated list of family members";
    public static final String FAMILY_SAMPLES_DESCRIPTION = "Comma separated list of member's samples";
    public static final String FAMILY_EXPECTED_SIZE_DESCRIPTION = "Expected size of the family (number of members)";
    public static final String FAMILY_PHENOTYPES_DESCRIPTION = PHENOTYPES_DESCRIPTION;
    public static final String FAMILY_DISORDERS_DESCRIPTION = DISORDERS_DESCRIPTION;
    public static final String FAMILY_CREATION_DATE_DESCRIPTION = CREATION_DATE_DESCRIPTION;
    public static final String FAMILY_MODIFICATION_DATE_DESCRIPTION = MODIFICATION_DATE_DESCRIPTION;
    public static final String FAMILY_DELETED_DESCRIPTION = DELETED_DESCRIPTION;
    public static final String FAMILY_STATUS_DESCRIPTION = STATUS_DESCRIPTION;
    public static final String FAMILY_INTERNAL_STATUS_DESCRIPTION = INTERNAL_STATUS_DESCRIPTION;
    public static final String FAMILY_ANNOTATION_DESCRIPTION = ANNOTATION_DESCRIPTION;
    public static final String FAMILY_ACL_DESCRIPTION = ACL_DESCRIPTION;
    public static final String FAMILY_RELEASE_DESCRIPTION = RELEASE_DESCRIPTION;
    public static final String FAMILY_SNAPSHOT_DESCRIPTION = SNAPSHOT_DESCRIPTION;

    // ---------------------------------------------

    public static final String COHORT_DESCRIPTION = "Cohort ID or UUID";
    public static final String COHORTS_DESCRIPTION = "Comma separated list of cohort IDs or UUIDs" + UP_TO_100;
    public static final String COHORT_IDS_DESCRIPTION = "Comma separated list of cohort IDs" + UP_TO_100;
    public static final String COHORT_UUIDS_DESCRIPTION = "Comma separated list of cohort IDs" + UP_TO_100;

    public static final String COHORT_ID_PARAM = "id";
    public static final String COHORT_UUID_PARAM = "uuid";
    public static final String COHORT_TYPE_PARAM = "type";
    public static final String COHORT_CREATION_DATE_PARAM = CREATION_DATE_PARAM;
    public static final String COHORT_MODIFICATION_DATE_PARAM = MODIFICATION_DATE_PARAM;
    public static final String COHORT_DELETED_PARAM = DELETED_PARAM;
    public static final String COHORT_STATUS_PARAM = STATUS_PARAM;
    public static final String COHORT_INTERNAL_STATUS_PARAM = INTERNAL_STATUS_PARAM;
    public static final String COHORT_ANNOTATION_PARAM = "annotation";
    public static final String COHORT_ACL_PARAM = ACL_PARAM;
    public static final String COHORT_SAMPLES_PARAM = "samples";
    public static final String COHORT_NUMBER_OF_SAMPLES_PARAM = "numSamples";
    public static final String COHORT_RELEASE_PARAM = RELEASE_PARAM;

    public static final String COHORT_ID_DESCRIPTION = "Cohort ID";
    public static final String COHORT_UUID_DESCRIPTION = "Cohort UUID";
    public static final String COHORT_TYPE_DESCRIPTION = "Cohort type";
    public static final String COHORT_CREATION_DATE_DESCRIPTION = CREATION_DATE_PARAM;
    public static final String COHORT_MODIFICATION_DATE_DESCRIPTION = MODIFICATION_DATE_PARAM;
    public static final String COHORT_DELETED_DESCRIPTION = DELETED_PARAM;
    public static final String COHORT_STATUS_DESCRIPTION = STATUS_PARAM;
    public static final String COHORT_INTERNAL_STATUS_DESCRIPTION = INTERNAL_STATUS_PARAM;
    public static final String COHORT_ANNOTATION_DESCRIPTION = "Cohort annotation";
    public static final String COHORT_ACL_DESCRIPTION = ACL_PARAM;
    public static final String COHORT_SAMPLES_DESCRIPTION = "Cohort sample IDs";
    public static final String COHORT_NUMBER_OF_SAMPLES_DESCRIPTION = "Number of samples";
    public static final String COHORT_RELEASE_DESCRIPTION = RELEASE_PARAM;

    // ---------------------------------------------

    public static final String CLINICAL_ID_PARAM = "id";
    public static final String CLINICAL_UUID_PARAM = "uuid";
    public static final String CLINICAL_TYPE_PARAM = "type";
    public static final String CLINICAL_DISORDER_PARAM = "disorder";
    public static final String CLINICAL_FILES_PARAM = "files";
    public static final String CLINICAL_SAMPLE_PARAM = "sample";
    public static final String CLINICAL_INDIVIDUAL_PARAM = "individual";
    public static final String CLINICAL_PROBAND_PARAM = "proband";
    public static final String CLINICAL_PROBAND_SAMPLES_PARAM = "probandSamples";
    public static final String CLINICAL_FAMILY_PARAM = "family";
    public static final String CLINICAL_FAMILY_MEMBERS_PARAM = "familyMembers";
    public static final String CLINICAL_FAMILY_MEMBERS_SAMPLES_PARAM = "familyMemberSamples";
    public static final String CLINICAL_PANELS_PARAM = "panels";
    public static final String CLINICAL_LOCKED_PARAM = "locked";
    public static final String CLINICAL_ANALYST_ID_PARAM = "analystId";
    public static final String CLINICAL_PRIORITY_PARAM = "priority";
    public static final String CLINICAL_FLAGS_PARAM = "flags";
    public static final String CLINICAL_CREATION_DATE_PARAM = CREATION_DATE_PARAM;
    public static final String CLINICAL_MODIFICATION_DATE_PARAM = MODIFICATION_DATE_PARAM;
    public static final String CLINICAL_QUALITY_CONTROL_SUMMARY_PARAM = "qualityControlSummary";
    public static final String CLINICAL_RELEASE_PARAM = RELEASE_PARAM;
    public static final String CLINICAL_STATUS_PARAM = STATUS_PARAM;
    public static final String CLINICAL_INTERNAL_STATUS_PARAM = INTERNAL_STATUS_PARAM;

    public static final String CLINICAL_ID_DESCRIPTION = "Comma separated list of Clinical Analysis IDs" + UP_TO_100;
    public static final String CLINICAL_UUID_DESCRIPTION = "Comma separated list of Clinical Analysis UUIDs" + UP_TO_100;
    public static final String CLINICAL_TYPE_DESCRIPTION = "Clinical Analysis type";
    public static final String CLINICAL_DISORDER_DESCRIPTION = "Clinical Analysis disorder";
    public static final String CLINICAL_FILES_DESCRIPTION = "Clinical Analysis files";
    public static final String CLINICAL_SAMPLE_DESCRIPTION = "Sample associated to the proband or any member of a family";
    public static final String CLINICAL_INDIVIDUAL_DESCRIPTION = "Proband or any member of a family";
    public static final String CLINICAL_PROBAND_DESCRIPTION = "Clinical Analysis proband";
    public static final String CLINICAL_PROBAND_SAMPLES_DESCRIPTION = "Clinical Analysis proband samples";
    public static final String CLINICAL_FAMILY_DESCRIPTION = "Clinical Analysis family";
    public static final String CLINICAL_FAMILY_MEMBERS_DESCRIPTION = "Clinical Analysis family members";
    public static final String CLINICAL_FAMILY_MEMBERS_SAMPLES_DESCRIPTION = "Clinical Analysis family members samples";
    public static final String CLINICAL_PANELS_DESCRIPTION = "Clinical Analysis panels";
    public static final String CLINICAL_LOCKED_DESCRIPTION = "Locked Clinical Analyses";
    public static final String CLINICAL_ANALYST_ID_DESCRIPTION = "Clinical Analysis analyst id";
    public static final String CLINICAL_PRIORITY_DESCRIPTION = "Clinical Analysis priority";
    public static final String CLINICAL_FLAGS_DESCRIPTION = "Clinical Analysis flags";
    public static final String CLINICAL_CREATION_DATE_DESCRIPTION = "Clinical Analysis " + CREATION_DATE_DESCRIPTION;
    public static final String CLINICAL_MODIFICATION_DATE_DESCRIPTION = "Clinical Analysis " + MODIFICATION_DATE_DESCRIPTION;
    public static final String CLINICAL_QUALITY_CONTROL_SUMMARY_DESCRIPTION = "Clinical Analysis quality control summary";
    public static final String CLINICAL_RELEASE_DESCRIPTION = RELEASE_DESCRIPTION;
    public static final String CLINICAL_STATUS_DESCRIPTION = STATUS_DESCRIPTION;
    public static final String CLINICAL_INTERNAL_STATUS_DESCRIPTION = INTERNAL_STATUS_DESCRIPTION;

    public static final String CLINICAL_ANALYSES_DESCRIPTION = "Comma separated list of clinical analysis IDs or names" + UP_TO_100;
    public static final String CLINICAL_ANALYSES_PARAM = "clinicalAnalyses";
    public static final String CLINICAL_ANALYSIS_SKIP_CREATE_DEFAULT_INTERPRETATION_PARAM = "skipCreateDefaultInterpretation";
    public static final String CLINICAL_ANALYSIS_SKIP_CREATE_DEFAULT_INTERPRETATION_DESCRIPTION = "Flag to skip creating and initialise "
            + "an empty default primary interpretation (Id will be '{clinicalAnalysisId}.1'). This flag is only considered if no "
            + "Interpretation object is passed.";

    // ---------------------------------------------

    public static final String INTERPRETATION_ID_PARAM = "id";
    public static final String INTERPRETATION_UUID_PARAM = "uuid";
    public static final String INTERPRETATION_CLINICAL_ANALYSIS_ID_PARAM = "clinicalAnalysisId";
    public static final String INTERPRETATION_ANALYST_ID_PARAM = "analystId";
    public static final String INTERPRETATION_METHOD_NAME_PARAM = "methodName";
    public static final String INTERPRETATION_PANELS_PARAM = "panels";
    public static final String INTERPRETATION_PRIMARY_FINDINGS_IDS_PARAM = "primaryFindings";
    public static final String INTERPRETATION_SECONDARY_FINDINGS_IDS_PARAM = "secondaryFindings";
    public static final String INTERPRETATION_CREATION_DATE_PARAM = CREATION_DATE_PARAM;
    public static final String INTERPRETATION_MODIFICATION_DATE_PARAM = MODIFICATION_DATE_PARAM;
    public static final String INTERPRETATION_STATUS_PARAM = STATUS_PARAM;
    public static final String INTERPRETATION_INTERNAL_STATUS_PARAM = INTERNAL_STATUS_PARAM;
    //    public static final String INTERPRETATION_VERSION_PARAM = "version";
    public static final String INTERPRETATION_RELEASE_PARAM = RELEASE_PARAM;

    public static final String INTERPRETATION_ID_DESCRIPTION = "Comma separated list of Interpretation IDs" + UP_TO_100;
    public static final String INTERPRETATION_UUID_DESCRIPTION = "Comma separated list of Interpretation UUIDs" + UP_TO_100;
    public static final String INTERPRETATION_CLINICAL_ANALYSIS_ID_DESCRIPTION = "Clinical Analysis id";
    public static final String INTERPRETATION_ANALYST_ID_DESCRIPTION = "Analyst ID";
    public static final String INTERPRETATION_METHOD_NAME_DESCRIPTION = "Interpretation method name";
    public static final String INTERPRETATION_PANELS_DESCRIPTION = "Interpretation panels";
    public static final String INTERPRETATION_PRIMARY_FINDINGS_IDS_DESCRIPTION = "Interpretation primary findings";
    public static final String INTERPRETATION_SECONDARY_FINDINGS_IDS_DESCRIPTION = "Interpretation secondary findings";
    public static final String INTERPRETATION_CREATION_DATE_DESCRIPTION = "Interpretation " + CREATION_DATE_DESCRIPTION;
    public static final String INTERPRETATION_MODIFICATION_DATE_DESCRIPTION = "Interpretation " + MODIFICATION_DATE_DESCRIPTION;
    public static final String INTERPRETATION_STATUS_DESCRIPTION = STATUS_DESCRIPTION;
    public static final String INTERPRETATION_INTERNAL_STATUS_DESCRIPTION = INTERNAL_STATUS_DESCRIPTION;
    //    public static final String INTERPRETATION_VERSION_DESCRIPTION = VERSION_DESCRIPTION;
    public static final String INTERPRETATION_RELEASE_DESCRIPTION = RELEASE_DESCRIPTION;

    public static final String INTERPRETATION_DESCRIPTION = "Comma separated list of clinical interpretation IDs " + UP_TO_100;

    // ---------------------------------------------

    public static final String PANEL_ID_PARAM = ID;
    public static final String PANEL_UUID_PARAM = "uuid";
    public static final String PANEL_NAME_PARAM = "name";
    public static final String PANEL_DISORDERS_PARAM = DISORDERS_PARAM;
    public static final String PANEL_VARIANTS_PARAM = "variants";
    public static final String PANEL_GENES_PARAM = "genes";
    public static final String PANEL_REGIONS_PARAM = "regions";
    public static final String PANEL_CATEGORIES_PARAM = "categories";
    public static final String PANEL_TAGS_PARAM = "tags";
    public static final String PANEL_DELETED_PARAM = DELETED_PARAM;
    public static final String PANEL_STATUS_PARAM = STATUS_PARAM;
    public static final String PANEL_CREATION_DATE_PARAM = CREATION_DATE_PARAM;
    public static final String PANEL_MODIFICATION_DATE_PARAM = MODIFICATION_DATE_PARAM;
    public static final String PANEL_ACL_PARAM = ACL_PARAM;
    public static final String PANEL_RELEASE_PARAM = RELEASE_PARAM;
    public static final String PANEL_SNAPSHOT_PARAM = SNAPSHOT_PARAM;

    public static final String PANEL_ID_DESCRIPTION = "Comma separated list of panel IDs " + UP_TO_100;
    public static final String PANEL_UUID_DESCRIPTION = "Comma separated list of panel UUIDs " + UP_TO_100;
    public static final String PANEL_NAME_DESCRIPTION = "Comma separated list of panel names " + UP_TO_100;
    public static final String PANEL_DISORDERS_DESCRIPTION = DISORDERS_DESCRIPTION;
    public static final String PANEL_VARIANTS_DESCRIPTION = "Comma separated list of variant ids";
    public static final String PANEL_GENES_DESCRIPTION = "Comma separated list of gene ids";
    public static final String PANEL_REGIONS_DESCRIPTION = "Comma separated list of regions";
    public static final String PANEL_CATEGORIES_DESCRIPTION = "Comma separated list of category names";
    public static final String PANEL_TAGS_DESCRIPTION = "Panel tags";
    public static final String PANEL_DELETED_DESCRIPTION = DELETED_DESCRIPTION;
    public static final String PANEL_STATUS_DESCRIPTION = STATUS_DESCRIPTION;
    public static final String PANEL_CREATION_DATE_DESCRIPTION = CREATION_DATE_DESCRIPTION;
    public static final String PANEL_MODIFICATION_DATE_DESCRIPTION = MODIFICATION_DATE_DESCRIPTION;
    public static final String PANEL_ACL_DESCRIPTION = ACL_DESCRIPTION;
    public static final String PANEL_RELEASE_DESCRIPTION = RELEASE_DESCRIPTION;
    public static final String PANEL_SNAPSHOT_DESCRIPTION = SNAPSHOT_DESCRIPTION;
    public static final String PANEL_VERSION_DESCRIPTION = "Comma separated list of panel versions. 'all' to get all the "
            + "panel versions. Not supported if multiple panel ids are provided";

    public static final String PANEL_SOURCE = "source";
    public static final String PANEL_SOURCE_DESCRIPTION = "Comma separated list of sources to import panels from. Current supported "
            + "sources are 'panelapp' and 'cancer-gene-census'";
    public static final String PANEL_SOURCE_ID = "id";
    public static final String PANEL_SOURCE_ID_DESCRIPTION = "Comma separated list of panel IDs to be imported from the defined source."
            + "If 'source' is provided and 'id' is empty, it will import all the panels from the source. When 'id' is provided, only one "
            + "'source' will be allowed.";
    public static final String PANEL_VERSION_PARAM = "version";

    // ---------------------------------------------

    public static final String JOBS_DESCRIPTION = "Comma separated list of job IDs or UUIDs" + UP_TO_100;
    public static final String JOB_IDS_DESCRIPTION = "Comma separated list of job IDs" + UP_TO_100;
    public static final String JOB_UUIDS_DESCRIPTION = "Comma separated list of job UUIDs" + UP_TO_100;
    public static final String JOB_ID_DESCRIPTION = "Job ID or UUID";
    public static final String JOB_ID_CREATION_DESCRIPTION = "Job ID. It must be a unique string within the study. An ID will be "
            + "autogenerated automatically if not provided.";
    public static final String JOB_PARAM = "job";

    // ---------------------------------------------
    public static final String JOB_ID = "jobId";

    // ---------------------------------------------
    public static final String JOB_ID_PARAM = ID;
    public static final String JOB_UUID_PARAM = "uuid";
    public static final String JOB_DESCRIPTION = "jobDescription";
    public static final String JOB_DESCRIPTION_DESCRIPTION = "Job description";
    public static final String JOB_DEPENDS_ON = "jobDependsOn";
    public static final String JOB_DEPENDS_ON_PARAM = "dependsOn";
    public static final String JOB_DEPENDS_ON_DESCRIPTION = "Comma separated list of existing job IDs the job will depend on.";
    public static final String JOB_TOOL_ID_PARAM = "toolId";
    public static final String JOB_TOOL_TYPE_PARAM = "toolType";
    public static final String JOB_TOOL_ID_DESCRIPTION = "Tool ID executed by the job";
    public static final String JOB_TOOL_TYPE_DESCRIPTION = "Tool type executed by the job [OPERATION, ANALYSIS]";
    public static final String JOB_USER_PARAM = "userId";
    public static final String JOB_USER_DESCRIPTION = "User that created the job";
    public static final String JOB_PRIORITY_PARAM = "priority";
    public static final String JOB_PRIORITY_DESCRIPTION = "Priority of the job";
    public static final String JOB_INTERNAL_STATUS_PARAM = INTERNAL_STATUS_PARAM;
    public static final String JOB_INTERNAL_STATUS_DESCRIPTION = INTERNAL_STATUS_DESCRIPTION;
    public static final String JOB_STATUS_PARAM = STATUS_PARAM;
    public static final String JOB_STATUS_DESCRIPTION = STATUS_DESCRIPTION;
    public static final String JOB_VISITED_PARAM = "visited";
    public static final String JOB_VISITED_DESCRIPTION = "Visited status of job";
    public static final String JOB_TAGS = "jobTags";
    public static final String JOB_TAGS_PARAM = "tags";
    public static final String JOB_TAGS_DESCRIPTION = "Job tags";
    public static final String JOB_INPUT_FILES_PARAM = "input";
    public static final String JOB_INPUT_FILES_DESCRIPTION = "Comma separated list of file IDs used as input.";
    public static final String JOB_OUTPUT_FILES_PARAM = "output";
    public static final String JOB_OUTPUT_FILES_DESCRIPTION = "Comma separated list of file IDs used as output.";
    public static final String JOB_EXECUTION_START_PARAM = "execution.start";
    public static final String JOB_EXECUTION_START_DESCRIPTION = "Execution start date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805";
    public static final String JOB_EXECUTION_END_PARAM = "execution.end";
    public static final String JOB_EXECUTION_END_DESCRIPTION = "Execution end date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805";

    // Parameter used by the daemon to provide the directory where the just must be executed
    public static final String JOB_OUTDIR_PARAM = "outdir";

    public static final String VARIANTS_QUERY_DESCRIPTION = "Filter and fetch variants from indexed VCF files in the variant storage";
    public static final String OUTPUT_DIRECTORY_DESCRIPTION = "Output directory";

    // ---------------------------------------------
    public static final String REGION_DESCRIPTION = "Comma separated list of regions 'chr:start-end, e.g.: 2,3:63500-65000";
    public static final String REGION_PARAM = "region";
    public static final String GENE_DESCRIPTION = "Comma separated list of genes, e.g.: BCRA2,TP53";
    public static final String GENE_PARAM = "gene";
    // ---------------------------------------------
    // RGA
    public static final String INDEX_AUXILIAR_COLLECTION_DESCRIPTION = "Index auxiliar collection to improve performance assuming RGA is completely indexed.";
    public static final String INDEX_AUXILIAR_COLLECTION = "auxiliarIndex";
    public static final String ALIGNMENT_INDEX_DESCRIPTION = "Index alignment file";
    public static final String ALIGNMENT_QUERY_DESCRIPTION = "Search over indexed alignments";
    public static final String ALIGNMENT_COVERAGE_DESCRIPTION = "Compute coverage for a given alignemnt file";
    public static final String ALIGNMENT_COVERAGE_QUERY_DESCRIPTION = "Query the coverage of an alignment file for regions or genes";

    // ---------------------------------------------
    public static final String ALIGNMENT_COVERAGE_RATIO_DESCRIPTION = "Compute coverage ratio from file #1 vs file #2, (e.g. somatic vs germline)";

    // ---------------------------------------------
    public static final String ALIGNMENT_QC_DESCRIPTION = "Compute quality control (QC) metrics for a given alignment file (including samtools stats, samtools flag stats, FastQC and HS metrics)";
    public static final String ALIGNMENT_STATS_DESCRIPTION = "Compute stats (based on samtools/stats command) for a given alignment file";
    public static final String ALIGNMENT_FLAG_STATS_DESCRIPTION = "Compute flag stats (based on samtools/flagstat command) for a given alignment file";
    public static final String ALIGNMENT_FASTQC_METRICS_DESCRIPTION = "Compute sequence stats (based on FastQC tool) for a given alignment file";
    public static final String ALIGNMENT_HS_METRICS_DESCRIPTION = "Compute hybrid-selection (HS) metrics (based on Picard tool) for a given alignment file";
    public static final String ALIGNMENT_GENE_COVERAGE_STATS_DESCRIPTION = "Compute gene coverage stats for a given alignment file and a list of genes";
    public static final String ALIGNMENT_QC_SAMTOOLS_STATS_DESCRIPTION = "Compute samtools stats";

    // ---------------------------------------------
    // alignment
    public static final String ALIGNMENT_QC_SAMTOOLS_FLAG_STATS_DESCRIPTION = "Compute samtools flag stats";
    public static final String ALIGNMENT_QC_FASTQC_DESCRIPTION = "Compute FastQC";
    public static final String ALIGNMENT_QC_HS_METRICS_DESCRIPTION = "Compute hybrid-selection (HS) metrics based on the Picard/HsMetrics command";
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

    // ---------------------------------------------
    // alignment query
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

    // ---------------------------------------------
    // alignment coverage
    public static final String LOW_COVERAGE_REGION_THRESHOLD_PARAM = "threshold";
    public static final String LOW_COVERAGE_REGION_THRESHOLD_DEFAULT = "30";
    public static final String FILE_ID_PARAM = "file";
    public static final String FILE_ID_1_DESCRIPTION = "Input file #1 (e.g. somatic file)";
    public static final String FILE_ID_1_PARAM = "file1";
    public static final String FILE_ID_2_DESCRIPTION = "Input file #2 (e.g. germline file)";
    public static final String FILE_ID_2_PARAM = "file2";
    public static final String SKIP_LOG2_DESCRIPTION = "Do not apply Log2 to normalise the coverage ratio";
    public static final String SKIP_LOG2_PARAM = "skipLog2";
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

    // ---------------------------------------------
    // alignment stats query
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
    public static final String INPUT_FILE_DESCRIPTION = FILE_ID_DESCRIPTION + " (input file)";
    public static final String SAMTOOLS_COMMANDS_SUPPORTED = "sort, index, view, stats, flagstat, dict, faidx, depth, plot-bamstats";
    public static final String SAMTOOLS_COMMAND_DESCRIPTION = "Supported Samtools commands: " + SAMTOOLS_COMMANDS_SUPPORTED;
    public static final String DEEPTOOLS_COMMANDS_SUPPORTED = "bamCoverage, bamCompare";
    public static final String DEEPTOOLS_COMMAND_DESCRIPTION = "Supported Deeptools commands: " + DEEPTOOLS_COMMANDS_SUPPORTED;
    public static final String PICARD_COMMANDS_SUPPORTED = "CollectHsMetrics, CollectWgsMetrics, BedToIntervalList";
    public static final String PICARD_COMMAND_DESCRIPTION = "Supported Picard commands: " + PICARD_COMMANDS_SUPPORTED;
    public static final String BWA_COMMANDS_SUPPORTED = "index, mem";
    public static final String BWA_COMMAND_DESCRIPTION = "Supported BWA commands: " + BWA_COMMANDS_SUPPORTED;
    public static final String GATK_COMMANDS_SUPPORTED = "HaplotypeCaller";
    public static final String GATK_COMMAND_DESCRIPTION = "Supported Gatk commands: " + GATK_COMMANDS_SUPPORTED;
    public static final String RVTESTS_COMMANDS_SUPPORTED = "rvtest, vcf2kinship";

    // ---------------------------------------------
    public static final String RVTESTS_COMMAND_DESCRIPTION = "Supported RvTests commands: " + RVTESTS_COMMANDS_SUPPORTED;

    // ---------------------------------------------
    public static final String RPC_METHOD_DESCRIPTION = "RPC method used: {auto, gRPC, REST}. When auto, it will first try with gRPC and "
            + "if that does not work, it will try with REST";
    public static final String COMMAND_PARAMETER = "command";

    // ---------------------------------------------
    public static final String COMMAND_PARAMETER_DESCRIPTION = "Command name to execute in this tool.";
    public static final String FIELD_PARAM = "field";

    // ---------------------------------------------
    public static final String PROJECTS_DESCRIPTION = "Comma separated list of projects [user@]project" + UP_TO_100;

    // ---------------------------------------------
    public static final String STUDIES_DESCRIPTION = "Comma separated list of Studies [[user@]project:]study where study and project can be either the ID or UUID" + UP_TO_100;

    public static final String PANELS_DESCRIPTION = "Comma separated list of panel IDs" + UP_TO_100;
}