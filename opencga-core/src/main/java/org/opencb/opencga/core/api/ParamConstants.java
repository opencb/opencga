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
    public static final String MODIFICATION_DATE_DESCRIPTION = "Modification date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, " +
            "<201805";
    public static final String MODIFICATION_DATE_PARAM = "modificationDate";
    public static final String RELEASE_PARAM = "release";
    public static final String RELEASE_DESCRIPTION = "Release when it was created";
    public static final String INTERNAL_STATUS_PARAM = "internalStatus";
    public static final String INTERNAL_STATUS_DESCRIPTION = "Filter by internal status";
    @Deprecated // Use INTERNAL_VARIANT_INDEX_STATUS_PARAM
    public static final String INTERNAL_INDEX_STATUS_PARAM = "internalIndexStatus";
    public static final String INTERNAL_VARIANT_INDEX_STATUS_PARAM = "internalVariantIndexStatus";
    public static final String INTERNAL_VARIANT_INDEX_STATUS_DESCRIPTION = "Filter by internal variant index status";
    public static final String STATUS_PARAM = "status";
    public static final String STATUS_DESCRIPTION = "Filter by status";
    public static final String ACL_PARAM = "acl";
    public static final String ACL_FORMAT = "Format: acl={user}:{permissions}. Example: acl=john:WRITE,WRITE_ANNOTATIONS will return "
            + "all entries for which user john has both WRITE and WRITE_ANNOTATIONS permissions. Only study owners or administrators "
            + "can query by this field. ";
    public static final String ACL_DESCRIPTION = "Filter entries for which a user has the provided permissions. " + ACL_FORMAT;
    public static final String FAMILY_ACL_DESCRIPTION = ACL_DESCRIPTION;
    public static final String PANEL_ACL_DESCRIPTION = ACL_DESCRIPTION;
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

    public static final String POP_FREQ_1000G_CB_V4 = "1kG_phase3";
    public static final String POP_FREQ_1000G_CB_V5 = "1000G";
    public static final String POP_FREQ_1000G = POP_FREQ_1000G_CB_V5;
    public static final String POP_FREQ_GNOMAD_GENOMES = "GNOMAD_GENOMES";

    // ---------------------------------------------
    public static final String FORCE = "force";
    public static final String ANNOTATION_DOC_URL = "http://docs.opencb.org/display/opencga/AnnotationSets+1.4.0";
    public static final String VARIABLE_SET_DESCRIPTION = "Variable set ID or name";
    public static final String ANNOTATION_DESCRIPTION = "Annotation filters. Example: age>30;gender=FEMALE. For more information, " +
            "please visit " + ANNOTATION_DOC_URL;
    public static final String FAMILY_ANNOTATION_DESCRIPTION = ANNOTATION_DESCRIPTION;
    public static final String ANNOTATION_AS_MAP_DESCRIPTION = "Indicates whether to show the annotations as key-value";
    public static final String ANNOTATION_SET_ID = "AnnotationSet ID to be updated.";
    public static final String ANNOTATION_SET_NAME = "Annotation set name. If provided, only chosen annotation set will be shown";
    public static final String ANNOTATION_SET_UPDATE_ACTION_DESCRIPTION = "Action to be performed: ADD to add new annotations; REPLACE to" +
            " replace the value of an already existing "
            + "annotation; SET to set the new list of annotations removing any possible old annotations; REMOVE to remove some "
            + "annotations; RESET to set some annotations to the default value configured in the corresponding variables of the "
            + "VariableSet if any.";
    public static final String ANNOTATION_SET_UPDATE_PARAMS_DESCRIPTION = "Json containing the map of annotations when the action is ADD," +
            " SET or REPLACE, a json with only the key "
            + "'remove' containing the comma separated variables to be removed as a value when the action is REMOVE or a json "
            + "with only the key 'reset' containing the comma separated variables that will be set to the default value"
            + " when the action is RESET";

    // ---------------------------------------------
    /**
     * Boolean indicating if the annotations have to be returned flattened or not. Default: false
     */
    public static final String FLATTEN_ANNOTATIONS = "flattenAnnotations";
    public static final String FLATTEN_ANNOTATION_DESCRIPTION = "Boolean indicating to flatten the annotations.";
    public static final String USER_PROJECT_SEPARATOR = "@";
    // ---------------------------------------------
    public static final String PROJECT_STUDY_SEPARATOR = ":";
    public static final String OPENCGA_USER_ID = "opencga";
    public static final String ADMIN_PROJECT = "admin";
    public static final String ADMIN_STUDY = "admin";
    public static final String ADMIN_STUDY_FQN =
            OPENCGA_USER_ID + USER_PROJECT_SEPARATOR + ADMIN_PROJECT + PROJECT_STUDY_SEPARATOR + ADMIN_STUDY;
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
    public static final String OTHER_STUDIES_FLAG_DESCRIPTION = "Flag indicating the entries being queried can belong to any related " +
            "study, not just the primary one.";
    public static final String STUDIES_PARAM = "studies";
    public static final String STUDY_NAME_DESCRIPTION = "Study name";
    public static final String STUDY_ID_DESCRIPTION = "Study ID";
    public static final String STUDY_ALIAS_DESCRIPTION = "Study alias";

    // ---------------------------------------------
    public static final String STUDY_FQN_DESCRIPTION = "Study full qualified name";
    public static final String SILENT_DESCRIPTION = "Boolean to retrieve all possible entries that are queried for, false to raise an "
            + "exception whenever one of the entries looked for cannot be shown for whichever reason";
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
    public static final String FILE_BIOFORMAT_DESCRIPTION = "Comma separated Bioformat values. For existing Bioformats see " +
            "files/bioformats";
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
    public static final String SAMPLE_VARIANT_STATS_CLINICAL_SIGNIFICANCE_COUNT_DESCRIPTION = "Sample variant stats " +
            "ClinicalSignificanceCount";
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
    public static final String INDIVIDUAL_DESCRIPTION = "Individual ID, name or UUID";
    public static final String INDIVIDUAL_VERSION_PARAM = "version";
    public static final String INDIVIDUAL_VERSION_DESCRIPTION = "Comma separated list of individual versions. 'all' to get all the "
            + "individual versions. Not supported if multiple individual ids are provided";
    public static final String INDIVIDUAL_FAMILY_IDS_PARAM = "familyIds";

    // ---------------------------------------------
    public static final String INDIVIDUAL_FAMILY_IDS_DESCRIPTION = "Comma separated list of family ids the individuals may belong to.";
    public static final String INDIVIDUAL_PHENOTYPES_ACTION_PARAM = PHENOTYPES_ACTION_PARAM;
    public static final String INDIVIDUAL_PHENOTYPES_ACTION_DESCRIPTION = PHENOTYPES_ACTION_DESCRIPTION;
    public static final String INDIVIDUAL_DISORDERS_ACTION_PARAM = "disordersAction";
    public static final String INDIVIDUAL_DISORDERS_ACTION_DESCRIPTION = "Action to be performed if the array of disorders is being"
            + " updated [SET, ADD, REMOVE]";
    public static final String INDIVIDUAL_ID_PARAM = "id";
    public static final String INDIVIDUAL_NAME_PARAM = "name";
    public static final String INDIVIDUAL_UUID_PARAM = "uuid";
    public static final String INDIVIDUAL_FATHER_PARAM = "father";

    // ---------------------------------------------
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
    public static final String FAMILY_UPDATE_ROLES_PARAM = "updateRoles";
    public static final String FAMILY_UPDATE_ROLES_DESCRIPTION = "Update the member roles within the family";
    public static final String FAMILY_VERSION_PARAM = "version";
    public static final String FAMILY_VERSION_DESCRIPTION = "Comma separated list of family versions. 'all' to get all the "
            + "family versions. Not supported if multiple family ids are provided";
    public static final String FAMILY_ID_PARAM = "id";
    public static final String FAMILY_UUID_PARAM = "uuid";
    public static final String FAMILY_NAME_PARAM = "name";
    public static final String FAMILY_MEMBERS_PARAM = "members";

    // ---------------------------------------------
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
    public static final String FAMILY_RELEASE_DESCRIPTION = RELEASE_DESCRIPTION;
    public static final String FAMILY_SNAPSHOT_DESCRIPTION = SNAPSHOT_DESCRIPTION;
    public static final String COHORT_DESCRIPTION = "Cohort ID or UUID";
    public static final String COHORT_ID_PARAM = "id";
    public static final String COHORT_NAME_PARAM = "name";
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

    // ---------------------------------------------
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
    public static final String CLINICAL_DUE_DATE_PARAM = "dueDate";
    public static final String CLINICAL_QUALITY_CONTROL_SUMMARY_PARAM = "qualityControlSummary";
    public static final String CLINICAL_RELEASE_PARAM = RELEASE_PARAM;
    public static final String CLINICAL_STATUS_PARAM = STATUS_PARAM;
    public static final String CLINICAL_INTERNAL_STATUS_PARAM = INTERNAL_STATUS_PARAM;
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
    public static final String CLINICAL_DUE_DATE_DESCRIPTION = "Clinical Analysis due date. Format: yyyyMMddHHmmss. Examples: >2018, "
            + "2017-2018, <201805";
    public static final String CLINICAL_QUALITY_CONTROL_SUMMARY_DESCRIPTION = "Clinical Analysis quality control summary";
    public static final String CLINICAL_RELEASE_DESCRIPTION = RELEASE_DESCRIPTION;
    public static final String CLINICAL_STATUS_DESCRIPTION = STATUS_DESCRIPTION;
    public static final String CLINICAL_INTERNAL_STATUS_DESCRIPTION = INTERNAL_STATUS_DESCRIPTION;
    public static final String CLINICAL_ANALYSES_PARAM = "clinicalAnalyses";
    public static final String CLINICAL_ANALYSIS_SKIP_CREATE_DEFAULT_INTERPRETATION_PARAM = "skipCreateDefaultInterpretation";
    public static final String CLINICAL_ANALYSIS_SKIP_CREATE_DEFAULT_INTERPRETATION_DESCRIPTION = "Flag to skip creating and initialise "
            + "an empty default primary interpretation (Id will be '{clinicalAnalysisId}.1'). This flag is only considered if no "
            + "Interpretation object is passed.";
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
    public static final String INTERPRETATION_LOCKED_PARAM = "locked";
    public static final String INTERPRETATION_CLINICAL_ANALYSIS_ID_DESCRIPTION = "Clinical Analysis id";
    public static final String INTERPRETATION_ANALYST_ID_DESCRIPTION = "Analyst ID";
    public static final String INTERPRETATION_METHOD_NAME_DESCRIPTION = "Interpretation method name";
    public static final String INTERPRETATION_PANELS_DESCRIPTION = "Interpretation panels";
    public static final String INTERPRETATION_PRIMARY_FINDINGS_IDS_DESCRIPTION = "Interpretation primary findings";
    public static final String INTERPRETATION_SECONDARY_FINDINGS_IDS_DESCRIPTION = "Interpretation secondary findings";
    public static final String INTERPRETATION_CREATION_DATE_DESCRIPTION = "Interpretation " + CREATION_DATE_DESCRIPTION;
    public static final String INCLUDE_INTERPRETATION = "includeInterpretation";
    public static final String INCLUDE_INTERPRETATION_DESCRIPTION = "Interpretation ID to include the fields related to this"
        + " interpretation";
    // ---------------------------------------------
    public static final String INTERPRETATION_MODIFICATION_DATE_DESCRIPTION = "Interpretation " + MODIFICATION_DATE_DESCRIPTION;
    public static final String INTERPRETATION_STATUS_DESCRIPTION = STATUS_DESCRIPTION;
    public static final String INTERPRETATION_INTERNAL_STATUS_DESCRIPTION = INTERNAL_STATUS_DESCRIPTION;
    //    public static final String INTERPRETATION_VERSION_DESCRIPTION = VERSION_DESCRIPTION;
    public static final String INTERPRETATION_RELEASE_DESCRIPTION = RELEASE_DESCRIPTION;
    public static final String INTERPRETATION_LOCKED_DESCRIPTION = "Field indicating whether the Interpretation is locked or not "
            + "(can be altered or not)";
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
    public static final String PANEL_DISORDERS_DESCRIPTION = DISORDERS_DESCRIPTION;
    public static final String PANEL_VARIANTS_DESCRIPTION = "Comma separated list of variant ids";
    public static final String PANEL_GENES_DESCRIPTION = "Comma separated list of gene ids";
    public static final String PANEL_REGIONS_DESCRIPTION = "Comma separated list of regions";
    public static final String PANEL_CATEGORIES_DESCRIPTION = "Comma separated list of category names";
    public static final String PANEL_TAGS_DESCRIPTION = "Panel tags";
    public static final String PANEL_DELETED_DESCRIPTION = DELETED_DESCRIPTION;
    public static final String PANEL_STATUS_DESCRIPTION = STATUS_DESCRIPTION;

    // ---------------------------------------------
    public static final String PANEL_CREATION_DATE_DESCRIPTION = CREATION_DATE_DESCRIPTION;
    public static final String PANEL_MODIFICATION_DATE_DESCRIPTION = MODIFICATION_DATE_DESCRIPTION;
    public static final String PANEL_RELEASE_DESCRIPTION = RELEASE_DESCRIPTION;
    public static final String PANEL_SNAPSHOT_DESCRIPTION = SNAPSHOT_DESCRIPTION;
    public static final String PANEL_VERSION_DESCRIPTION = "Comma separated list of panel versions. 'all' to get all the "
            + "panel versions. Not supported if multiple panel ids are provided";
    public static final String PANEL_SOURCE_PARAM = "source";
    public static final String PANEL_SOURCE_DESCRIPTION = "Comma separated list of source ids or names.";
    public static final String PANEL_IMPORT_SOURCE_DESCRIPTION = "Comma separated list of sources to import panels from. Current supported "
            + "sources are 'panelapp' and 'cancer-gene-census'";
    public static final String PANEL_SOURCE_ID = "id";
    public static final String PANEL_SOURCE_ID_DESCRIPTION = "Comma separated list of panel IDs to be imported from the defined source."
            + "If 'source' is provided and 'id' is empty, it will import all the panels from the source. When 'id' is provided, only one "
            + "'source' will be allowed.";
    public static final String PANEL_VERSION_PARAM = "version";
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

    // ---------------------------------------------
    public static final String JOB_INPUT_FILES_PARAM = "input";
    public static final String JOB_INPUT_FILES_DESCRIPTION = "Comma separated list of file IDs used as input.";
    public static final String JOB_OUTPUT_FILES_PARAM = "output";
    public static final String JOB_OUTPUT_FILES_DESCRIPTION = "Comma separated list of file IDs used as output.";
    public static final String JOB_EXECUTION_START_PARAM = "execution.start";
    public static final String JOB_EXECUTION_START_DESCRIPTION = "Execution start date. Format: yyyyMMddHHmmss. Examples: >2018, " +
            "2017-2018, <201805";
    public static final String JOB_EXECUTION_END_PARAM = "execution.end";
    public static final String JOB_EXECUTION_END_DESCRIPTION = "Execution end date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, " +
            "<201805";
    public static final String VARIANTS_QUERY_DESCRIPTION = "Filter and fetch variants from indexed VCF files in the variant storage";
    public static final String OUTPUT_DIRECTORY_DESCRIPTION = "Output directory";
    // ---------------------------------------------
    public static final String REGION_DESCRIPTION = "Comma separated list of regions 'chr:start-end, e.g.: 2,3:63500-65000";
    public static final String REGION_PARAM = "region";
    public static final String GENE_DESCRIPTION = "Comma separated list of genes, e.g.: BCRA2,TP53";
    public static final String GENE_PARAM = "gene";
    // ---------------------------------------------
    // RGA
    public static final String INDEX_AUXILIAR_COLLECTION_DESCRIPTION = "Index auxiliar collection to improve performance assuming RGA is " +
            "completely indexed.";
    public static final String INDEX_AUXILIAR_COLLECTION = "auxiliarIndex";
    public static final String ALIGNMENT_INDEX_DESCRIPTION = "Index alignment file";
    public static final String ALIGNMENT_QUERY_DESCRIPTION = "Search over indexed alignments";
    public static final String ALIGNMENT_COVERAGE_DESCRIPTION = "Compute coverage for a given alignemnt file";
    public static final String ALIGNMENT_COVERAGE_QUERY_DESCRIPTION = "Query the coverage of an alignment file for regions or genes";
    public static final String ALIGNMENT_COVERAGE_RATIO_DESCRIPTION = "Compute coverage ratio from file #1 vs file #2, (e.g. somatic vs " +
            "germline)";
    // ---------------------------------------------
    public static final String ALIGNMENT_QC_DESCRIPTION = "Compute quality control (QC) metrics for a given alignment file (including " +
            "samtools stats, samtools flag stats, FastQC and HS metrics)";
    public static final String ALIGNMENT_STATS_DESCRIPTION = "Compute stats (based on samtools/stats command) for a given alignment file";
    public static final String ALIGNMENT_FLAG_STATS_DESCRIPTION = "Compute flag stats (based on samtools/flagstat command) for a given " +
            "alignment file";
    public static final String ALIGNMENT_FASTQC_METRICS_DESCRIPTION = "Compute sequence stats (based on FastQC tool) for a given " +
            "alignment file";
    public static final String ALIGNMENT_HS_METRICS_DESCRIPTION = "Compute hybrid-selection (HS) metrics (based on Picard tool) for a " +
            "given alignment file";
    public static final String ALIGNMENT_GENE_COVERAGE_STATS_DESCRIPTION = "Compute gene coverage stats for a given alignment file and a " +
            "list of genes";
    public static final String ALIGNMENT_QC_SAMTOOLS_STATS_DESCRIPTION = "Compute samtools stats";
    // ---------------------------------------------
    // alignment
    public static final String ALIGNMENT_QC_SAMTOOLS_FLAG_STATS_DESCRIPTION = "Compute samtools flag stats";
    public static final String ALIGNMENT_QC_FASTQC_DESCRIPTION = "Compute FastQC";
    public static final String ALIGNMENT_QC_HS_METRICS_DESCRIPTION = "Compute hybrid-selection (HS) metrics based on the Picard/HsMetrics" +
            " command";
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

    // ---------------------------------------------
    public static final String SPLIT_RESULTS_INTO_REGIONS_DESCRIPTION = "Split results into regions (or gene/exon regions)";
    public static final String SPLIT_RESULTS_INTO_REGIONS_PARAM = "splitResults";
    public static final String OFFSET_DESCRIPTION = "Offset to extend the region, gene or exon at up and downstream";
    public static final String OFFSET_PARAM = "offset";
    public static final String OFFSET_DEFAULT = "200";
    public static final String ONLY_EXONS_DESCRIPTION = "Only exons are taking into account when genes are specified";
    public static final String ONLY_EXONS_PARAM = "onlyExons";
    public static final String COVERAGE_RANGE_DESCRIPTION = "Range of coverage values to be reported. Minimum and maximum values are " +
            "separated by '-', e.g.: 20-40 (for coverage values greater or equal to 20 and less or equal to 40). A single value means to " +
            "report coverage values less or equal to that value";
    public static final String COVERAGE_RANGE_PARAM = "range";
    public static final String COVERAGE_WINDOW_SIZE_DESCRIPTION = "Window size for the region coverage (if a coverage range is provided, " +
            "window size must be 1)";
    public static final String COVERAGE_WINDOW_SIZE_PARAM = "windowSize";
    public static final String COVERAGE_WINDOW_SIZE_DEFAULT = "50";
    public static final String ALIGNMENT_COVERAGE_STATS_DESCRIPTION = "Compute coverage stats per transcript for a list of genes.";
    public static final String LOW_COVERAGE_REGION_THRESHOLD_DESCRIPTION = "Only regions whose coverage depth is under this threshold " +
            "will be reported.";
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
    public static final String READS_PROPERLY_PAIRED_DESCRIPTION = "Reads properly paired (proper-pair bit set: [<|>|<=|>=]{number}, e.g." +
            " >=1000";
    // ---------------------------------------------
    // alignment stats query
    public static final String READS_PAIRED = "readsPaired";
    public static final String READS_PAIRED_DESCRIPTION = "Reads paired: paired-end technology bit set: [<|>|<=|>=]{number}, e.g. >=1000";
    public static final String READS_DUPLICATED = "readsDuplicated";
    public static final String READS_DUPLICATED_DESCRIPTION = "Reads duplicated: PCR or optical duplicate bit set: [<|>|<=|>=]{number}, e" +
            ".g. >=1000";
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
    public static final String AVERAGE_FIRST_FRAGMENT_LENGTH_DESCRIPTION = "Average first fragment length: [<|>|<=|>=]{number}, e.g. >=90" +
            ".0";
    public static final String AVERAGE_LAST_FRAGMENT_LENGTH = "averageLastFragmentLength";
    public static final String AVERAGE_LAST_FRAGMENT_LENGTH_DESCRIPTION = "Average_last_fragment_length: [<|>|<=|>=]{number}, e.g. >=90.0";
    public static final String AVERAGE_QUALITY = "averageQuality";
    public static final String AVERAGE_QUALITY_DESCRIPTION = "Average quality: [<|>|<=|>=]{number}, e.g. >=35.5";
    public static final String INSERT_SIZE_AVERAGE = "insertSizeAverage";
    public static final String INSERT_SIZE_AVERAGE_DESCRIPTION = "Insert size average: [<|>|<=|>=]{number}, e.g. >=100.0";
    public static final String INSERT_SIZE_STANDARD_DEVIATION = "insertSizeStandardDeviation";
    public static final String INSERT_SIZE_STANDARD_DEVIATION_DESCRIPTION = "Insert size standard deviation: [<|>|<=|>=]{number}, e.g. " +
            "<=1.5";
    public static final String PAIRS_WITH_OTHER_ORIENTATION = "pairsWithOtherOrientation";
    public static final String PAIRS_WITH_OTHER_ORIENTATION_DESCRIPTION = "Pairs with other orientation: [<|>|<=|>=]{number}, e.g. >=1000";
    public static final String PAIRS_ON_DIFFERENT_CHROMOSOMES = "pairsOnDifferentChromosomes";
    public static final String PAIRS_ON_DIFFERENT_CHROMOSOMES_DESCRIPTION = "Pairs on different chromosomes: [<|>|<=|>=]{number}, e.g. " +
            ">=1000";
    public static final String PERCENTAGE_OF_PROPERLY_PAIRED_READS = "percentageOfProperlyPairedReads";
    public static final String PERCENTAGE_OF_PROPERLY_PAIRED_READS_DESCRIPTION = "Percentage of properly paired reads: " +
            "[<|>|<=|>=]{number}, e.g. >=96.5";
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
    public static final String USERS_PASSWORD_USER = "The body web service user parameter";
    public static final String USERS_PASSWORD_PASSWORD = "The body web service password parameter";
    public static final String USERS_PASSWORD_NEWPASSWORD = "The body web service newPassword parameter";
    public static final String USERS_UPDATE_NAME = "The body web service name parameter";
    public static final String USERS_UPDATE_EMAIL = "The body web service email parameter";
    public static final String USERS_UPDATE_ORGANIZATION = "The body web service organization parameter";
    public static final String USERS_UPDATE_ATTRIBUTES = "The body web service attributes parameter";
    public static final String USERS_CONFIGS_UPDATE_ID = "The body web service id parameter";
    public static final String USERS_CONFIGS_UPDATE_CONFIGURATION = "The body web service configuration parameter";
    public static final String USERS_FILTERS_UPDATE_ID = "The body web service id parameter";
    public static final String USERS_FILTERS_UPDATE_DESCRIPTION = "The body web service description parameter";
    public static final String USERS_FILTERS_UPDATE_RESOURCE = "The body web service resource parameter";
    public static final String USERS_FILTERS_UPDATE_QUERY = "The body web service query parameter";
    public static final String USERS_FILTERS_UPDATE_OPTIONS = "The body web service options parameter";
    public static final String USERS_FILTERID_UPDATE_RESOURCE = "The body web service resource parameter";
    public static final String USERS_FILTERID_UPDATE_DESCRIPTION = "The body web service description parameter";
    public static final String USERS_FILTERID_UPDATE_QUERY = "The body web service query parameter";
    public static final String USERS_FILTERID_UPDATE_OPTIONS = "The body web service options parameter";
    public static final String USERS_CREATE_ID = "The body web service id parameter";
    public static final String USERS_CREATE_NAME = "The body web service name parameter";
    public static final String USERS_CREATE_EMAIL = "The body web service email parameter";
    public static final String USERS_CREATE_PASSWORD = "The body web service password parameter";
    public static final String USERS_CREATE_ORGANIZATION = "The body web service organization parameter";
    public static final String USERS_LOGIN_USER = "The body web service user parameter";
    public static final String USERS_LOGIN_PASSWORD = "The body web service password parameter";
    public static final String USERS_LOGIN_REFRESHTOKEN = "The body web service refreshToken parameter";
    public static final String PROJECTS_UPDATE_NAME = "The body web service name parameter";
    public static final String PROJECTS_UPDATE_DESCRIPTION = "The body web service description parameter";
    public static final String PROJECTS_UPDATE_CREATIONDATE = "The body web service creationDate parameter";
    public static final String PROJECTS_UPDATE_MODIFICATIONDATE = "The body web service modificationDate parameter";
    public static final String PROJECTS_UPDATE_ORGANISM = "The body web service organism parameter";
    public static final String PROJECTS_UPDATE_ATTRIBUTES = "The body web service attributes parameter";
    public static final String PROJECTS_CREATE_ID = "The body web service id parameter";
    public static final String PROJECTS_CREATE_NAME = "The body web service name parameter";
    public static final String PROJECTS_CREATE_DESCRIPTION = "The body web service description parameter";
    public static final String PROJECTS_CREATE_CREATIONDATE = "The body web service creationDate parameter";
    public static final String PROJECTS_CREATE_MODIFICATIONDATE = "The body web service modificationDate parameter";
    public static final String PROJECTS_CREATE_ORGANISM = "The body web service organism parameter";
    public static final String PROJECTS_CREATE_ATTRIBUTES = "The body web service attributes parameter";
    public static final String STUDIES_UPDATE_NAME = "The body web service name parameter";
    public static final String STUDIES_UPDATE_ALIAS = "The body web service alias parameter";
    public static final String STUDIES_UPDATE_DESCRIPTION = "The body web service description parameter";
    public static final String STUDIES_UPDATE_CREATIONDATE = "The body web service creationDate parameter";
    public static final String STUDIES_UPDATE_MODIFICATIONDATE = "The body web service modificationDate parameter";
    public static final String STUDIES_UPDATE_NOTIFICATION = "The body web service notification parameter";
    public static final String STUDIES_UPDATE_ATTRIBUTES = "The body web service attributes parameter";
    public static final String STUDIES_UPDATE_STATUS = "The body web service status parameter";
    public static final String STUDIES_TEMPLATES_RUN_ID = "The body web service id parameter";
    public static final String STUDIES_TEMPLATES_RUN_OVERWRITE = "The body web service overwrite parameter";
    public static final String STUDIES_TEMPLATES_RUN_RESUME = "The body web service resume parameter";
    public static final String STUDIES_CREATE_ID = "The body web service id parameter";
    public static final String STUDIES_CREATE_NAME = "The body web service name parameter";
    public static final String STUDIES_CREATE_ALIAS = "The body web service alias parameter";
    public static final String STUDIES_CREATE_DESCRIPTION = "The body web service description parameter";
    public static final String STUDIES_CREATE_CREATIONDATE = "The body web service creationDate parameter";
    public static final String STUDIES_CREATE_MODIFICATIONDATE = "The body web service modificationDate parameter";
    public static final String STUDIES_CREATE_NOTIFICATION = "The body web service notification parameter";
    public static final String STUDIES_CREATE_STATUS = "The body web service status parameter";
    public static final String STUDIES_CREATE_ATTRIBUTES = "The body web service attributes parameter";
    public static final String STUDIES_GROUPS_UPDATE_ID = "The body web service id parameter";
    public static final String STUDIES_GROUPS_UPDATE_USERS = "The body web service users parameter";
    public static final String STUDIES_GROUP_USERS_UPDATE_USERS = "The body web service users parameter";
    public static final String STUDIES_PERMISSIONRULES_UPDATE_ID = "The body web service id parameter";
    public static final String STUDIES_PERMISSIONRULES_UPDATE_QUERY = "The body web service query parameter";
    public static final String STUDIES_PERMISSIONRULES_UPDATE_MEMBERS = "The body web service members parameter";
    public static final String STUDIES_PERMISSIONRULES_UPDATE_PERMISSIONS = "The body web service permissions parameter";
    public static final String STUDIES_UPDATE_STUDY = "The body web service study parameter";
    public static final String STUDIES_UPDATE_TEMPLATE = "The body web service template parameter";
    public static final String STUDIES_VARIABLESETS_UPDATE_ID = "The body web service id parameter";
    public static final String STUDIES_VARIABLESETS_UPDATE_NAME = "The body web service name parameter";
    public static final String STUDIES_VARIABLESETS_UPDATE_UNIQUE = "The body web service unique parameter";
    public static final String STUDIES_VARIABLESETS_UPDATE_CONFIDENTIAL = "The body web service confidential parameter";
    public static final String STUDIES_VARIABLESETS_UPDATE_DESCRIPTION = "The body web service description parameter";
    public static final String STUDIES_VARIABLESETS_UPDATE_ENTITIES = "The body web service entities parameter";
    public static final String STUDIES_VARIABLESETS_UPDATE_VARIABLES = "The body web service variables parameter";
    public static final String STUDIES_VARIABLESET_VARIABLES_UPDATE_ID = "The body web service id parameter";
    public static final String STUDIES_VARIABLESET_VARIABLES_UPDATE_NAME = "The body web service name parameter";
    public static final String STUDIES_VARIABLESET_VARIABLES_UPDATE_CATEGORY = "The body web service category parameter";
//    public static final String STUDIES_VARIABLESET_VARIABLES_UPDATE_TYPE = "The body web service type parameter";
    public static final String STUDIES_VARIABLESET_VARIABLES_UPDATE_DEFAULTVALUE = "The body web service defaultValue parameter";
    public static final String STUDIES_VARIABLESET_VARIABLES_UPDATE_REQUIRED = "The body web service required parameter";
    public static final String STUDIES_VARIABLESET_VARIABLES_UPDATE_MULTIVALUE = "The body web service multiValue parameter";
    public static final String STUDIES_VARIABLESET_VARIABLES_UPDATE_ALLOWEDVALUES = "The body web service allowedValues parameter";
    public static final String STUDIES_VARIABLESET_VARIABLES_UPDATE_ALLOWEDKEYS = "The body web service allowedKeys parameter";
    public static final String STUDIES_VARIABLESET_VARIABLES_UPDATE_RANK = "The body web service rank parameter";
    public static final String STUDIES_VARIABLESET_VARIABLES_UPDATE_DEPENDSON = "The body web service dependsOn parameter";
    public static final String STUDIES_VARIABLESET_VARIABLES_UPDATE_DESCRIPTION = "The body web service description parameter";
    public static final String STUDIES_VARIABLESET_VARIABLES_UPDATE_VARIABLESET = "The body web service variableSet parameter";
    public static final String STUDIES_VARIABLESET_VARIABLES_UPDATE_ATTRIBUTES = "The body web service attributes parameter";
    public static final String FILES_UPLOAD_VALUE = "The body web service value parameter";
    public static final String FILES_UPLOAD_HASH = "The body web service hash parameter";
    public static final String FILES_CREATE_PATH = "The body web service path parameter";
    public static final String FILES_CREATE_CONTENT = "The body web service content parameter";
    public static final String FILES_CREATE_DESCRIPTION = "The body web service description parameter";
    public static final String FILES_CREATE_PARENTS = "The body web service parents parameter";
    public static final String FILES_CREATE_DIRECTORY = "The body web service directory parameter";
    public static final String FILES_FETCH_URL = "The body web service url parameter";
    public static final String FILES_FETCH_PATH = "The body web service path parameter";
    public static final String FILES_UPDATE_NAME = "The body web service name parameter";
    public static final String FILES_UPDATE_DESCRIPTION = "The body web service description parameter";
    public static final String FILES_UPDATE_CREATIONDATE = "The body web service creationDate parameter";
    public static final String FILES_UPDATE_MODIFICATIONDATE = "The body web service modificationDate parameter";
    public static final String FILES_UPDATE_SAMPLEIDS = "The body web service sampleIds parameter";
    public static final String FILES_UPDATE_CHECKSUM = "The body web service checksum parameter";
//    public static final String FILES_UPDATE_FORMAT = "The body web service format parameter";
//    public static final String FILES_UPDATE_BIOFORMAT = "The body web service bioformat parameter";
    public static final String FILES_UPDATE_SOFTWARE = "The body web service software parameter";
    public static final String FILES_UPDATE_EXPERIMENT = "The body web service experiment parameter";
    public static final String FILES_UPDATE_TAGS = "The body web service tags parameter";
    public static final String FILES_UPDATE_INTERNAL = "The body web service internal parameter";
    public static final String FILES_UPDATE_RELATEDFILES = "The body web service relatedFiles parameter";
    public static final String FILES_UPDATE_SIZE = "The body web service size parameter";
    public static final String FILES_UPDATE_STATUS = "The body web service status parameter";
    public static final String FILES_UPDATE_ANNOTATIONSETS = "The body web service annotationSets parameter";
    public static final String FILES_UPDATE_QUALITYCONTROL = "The body web service qualityControl parameter";
    public static final String FILES_UPDATE_STATS = "The body web service stats parameter";
    public static final String FILES_UPDATE_ATTRIBUTES = "The body web service attributes parameter";
    public static final String FILES_ANNOTATIONSETS_LOAD_CONTENT = "The body web service content parameter";
    public static final String FILES_LINK_URI = "The body web service uri parameter";
    public static final String FILES_LINK_PATH = "The body web service path parameter";
    public static final String FILES_LINK_DESCRIPTION = "The body web service description parameter";
    public static final String FILES_LINK_CREATIONDATE = "The body web service creationDate parameter";
    public static final String FILES_LINK_MODIFICATIONDATE = "The body web service modificationDate parameter";
    public static final String FILES_LINK_RELATEDFILES = "The body web service relatedFiles parameter";
    public static final String FILES_LINK_STATUS = "The body web service status parameter";
    public static final String FILES_LINK_INTERNAL = "The body web service internal parameter";
    public static final String FILES_POSTLINK_RUN_FILES = "The body web service files parameter";
    public static final String FILES_POSTLINK_RUN_BATCHSIZE = "The body web service batchSize parameter";
    public static final String FILES_LINK_RUN_URI = "The body web service uri parameter";
    public static final String FILES_LINK_RUN_PATH = "The body web service path parameter";
    public static final String FILES_LINK_RUN_DESCRIPTION = "The body web service description parameter";
    public static final String FILES_LINK_RUN_PARENTS = "The body web service parents parameter";
    public static final String FILES_UPDATE_FILE = "The body web service file parameter";
    public static final String FILES_UPDATE_SAMPLE = "The body web service sample parameter";
    public static final String JOBS_UPDATE_DESCRIPTION = "The body web service description parameter";
    public static final String JOBS_UPDATE_TAGS = "The body web service tags parameter";
    public static final String JOBS_UPDATE_VISITED = "The body web service visited parameter";
    public static final String JOBS_UPDATE_ATTRIBUTES = "The body web service attributes parameter";
    public static final String JOBS_UPDATE_JOB = "The body web service job parameter";
    public static final String JOBS_CREATE_ID = "The body web service id parameter";
    public static final String JOBS_CREATE_DESCRIPTION = "The body web service description parameter";
    public static final String JOBS_CREATE_TOOL = "The body web service tool parameter";
    public static final String JOBS_CREATE_PRIORITY = "The body web service priority parameter";
    public static final String JOBS_CREATE_COMMANDLINE = "The body web service commandLine parameter";
    public static final String JOBS_CREATE_PARAMS = "The body web service params parameter";
    public static final String JOBS_CREATE_CREATIONDATE = "The body web service creationDate parameter";
    public static final String JOBS_CREATE_MODIFICATIONDATE = "The body web service modificationDate parameter";
    public static final String JOBS_CREATE_INTERNAL = "The body web service internal parameter";
    public static final String JOBS_CREATE_OUTDIR = "The body web service outDir parameter";
    public static final String JOBS_CREATE_INPUT = "The body web service input parameter";
    public static final String JOBS_CREATE_OUTPUT = "The body web service output parameter";
    public static final String JOBS_CREATE_TAGS = "The body web service tags parameter";
    public static final String JOBS_CREATE_RESULT = "The body web service result parameter";
    public static final String JOBS_CREATE_STDOUT = "The body web service stdout parameter";
    public static final String JOBS_CREATE_STDERR = "The body web service stderr parameter";
    public static final String JOBS_CREATE_ATTRIBUTES = "The body web service attributes parameter";
    public static final String JOBS_RETRY_JOB = "The body web service job parameter";
    public static final String SAMPLES_UPDATE_ID = "The body web service id parameter";
    public static final String SAMPLES_UPDATE_DESCRIPTION = "The body web service description parameter";
    public static final String SAMPLES_UPDATE_CREATIONDATE = "The body web service creationDate parameter";
    public static final String SAMPLES_UPDATE_MODIFICATIONDATE = "The body web service modificationDate parameter";
    public static final String SAMPLES_UPDATE_INDIVIDUALID = "The body web service individualId parameter";
    public static final String SAMPLES_UPDATE_PROCESSING = "The body web service processing parameter";
    public static final String SAMPLES_UPDATE_COLLECTION = "The body web service collection parameter";
    public static final String SAMPLES_UPDATE_QUALITYCONTROL = "The body web service qualityControl parameter";
    public static final String SAMPLES_UPDATE_SOMATIC = "The body web service somatic parameter";
    public static final String SAMPLES_UPDATE_PHENOTYPES = "The body web service phenotypes parameter";
    public static final String SAMPLES_UPDATE_ANNOTATIONSETS = "The body web service annotationSets parameter";
    public static final String SAMPLES_UPDATE_ATTRIBUTES = "The body web service attributes parameter";
    public static final String SAMPLES_UPDATE_STATUS = "The body web service status parameter";
    public static final String SAMPLES_ANNOTATIONSETS_LOAD_CONTENT = "The body web service content parameter";
    public static final String SAMPLES_UPDATE_SAMPLE = "The body web service sample parameter";
    public static final String SAMPLES_UPDATE_INDIVIDUAL = "The body web service individual parameter";
    public static final String SAMPLES_UPDATE_FAMILY = "The body web service family parameter";
    public static final String SAMPLES_UPDATE_FILE = "The body web service file parameter";
    public static final String SAMPLES_UPDATE_COHORT = "The body web service cohort parameter";
    public static final String SAMPLES_CREATE_ID = "The body web service id parameter";
    public static final String SAMPLES_CREATE_DESCRIPTION = "The body web service description parameter";
    public static final String SAMPLES_CREATE_CREATIONDATE = "The body web service creationDate parameter";
    public static final String SAMPLES_CREATE_MODIFICATIONDATE = "The body web service modificationDate parameter";
    public static final String SAMPLES_CREATE_INDIVIDUALID = "The body web service individualId parameter";
    public static final String SAMPLES_CREATE_PROCESSING = "The body web service processing parameter";
    public static final String SAMPLES_CREATE_COLLECTION = "The body web service collection parameter";
    public static final String SAMPLES_CREATE_SOMATIC = "The body web service somatic parameter";
    public static final String SAMPLES_CREATE_PHENOTYPES = "The body web service phenotypes parameter";
    public static final String SAMPLES_CREATE_STATUS = "The body web service status parameter";
    public static final String SAMPLES_CREATE_ANNOTATIONSETS = "The body web service annotationSets parameter";
    public static final String SAMPLES_CREATE_ATTRIBUTES = "The body web service attributes parameter";
    public static final String INDIVIDUALS_UPDATE_ID = "The body web service id parameter";
    public static final String INDIVIDUALS_UPDATE_NAME = "The body web service name parameter";
    public static final String INDIVIDUALS_UPDATE_FATHER = "The body web service father parameter";
    public static final String INDIVIDUALS_UPDATE_MOTHER = "The body web service mother parameter";
    public static final String INDIVIDUALS_UPDATE_CREATIONDATE = "The body web service creationDate parameter";
    public static final String INDIVIDUALS_UPDATE_MODIFICATIONDATE = "The body web service modificationDate parameter";
    public static final String INDIVIDUALS_UPDATE_PARENTALCONSANGUINITY = "The body web service parentalConsanguinity parameter";
    public static final String INDIVIDUALS_UPDATE_LOCATION = "The body web service location parameter";
    public static final String INDIVIDUALS_UPDATE_SEX = "The body web service sex parameter";
    public static final String INDIVIDUALS_UPDATE_ETHNICITY = "The body web service ethnicity parameter";
    public static final String INDIVIDUALS_UPDATE_POPULATION = "The body web service population parameter";
    public static final String INDIVIDUALS_UPDATE_DATEOFBIRTH = "The body web service dateOfBirth parameter";
    public static final String INDIVIDUALS_UPDATE_KARYOTYPICSEX = "The body web service karyotypicSex parameter";
    public static final String INDIVIDUALS_UPDATE_LIFESTATUS = "The body web service lifeStatus parameter";
    public static final String INDIVIDUALS_UPDATE_SAMPLES = "The body web service samples parameter";
    public static final String INDIVIDUALS_UPDATE_ANNOTATIONSETS = "The body web service annotationSets parameter";
    public static final String INDIVIDUALS_UPDATE_PHENOTYPES = "The body web service phenotypes parameter";
    public static final String INDIVIDUALS_UPDATE_DISORDERS = "The body web service disorders parameter";
    public static final String INDIVIDUALS_UPDATE_STATUS = "The body web service status parameter";
    public static final String INDIVIDUALS_UPDATE_QUALITYCONTROL = "The body web service qualityControl parameter";
    public static final String INDIVIDUALS_UPDATE_ATTRIBUTES = "The body web service attributes parameter";
    public static final String INDIVIDUALS_ANNOTATIONSETS_LOAD_CONTENT = "The body web service content parameter";
    public static final String INDIVIDUALS_UPDATE_INDIVIDUAL = "The body web service individual parameter";
    public static final String INDIVIDUALS_UPDATE_SAMPLE = "The body web service sample parameter";
    public static final String INDIVIDUALS_CREATE_ID = "The body web service id parameter";
    public static final String INDIVIDUALS_CREATE_NAME = "The body web service name parameter";
    public static final String INDIVIDUALS_CREATE_FATHER = "The body web service father parameter";
    public static final String INDIVIDUALS_CREATE_MOTHER = "The body web service mother parameter";
    public static final String INDIVIDUALS_CREATE_CREATIONDATE = "The body web service creationDate parameter";
    public static final String INDIVIDUALS_CREATE_MODIFICATIONDATE = "The body web service modificationDate parameter";
    public static final String INDIVIDUALS_CREATE_LOCATION = "The body web service location parameter";
    public static final String INDIVIDUALS_CREATE_SAMPLES = "The body web service samples parameter";
    public static final String INDIVIDUALS_CREATE_SEX = "The body web service sex parameter";
    public static final String INDIVIDUALS_CREATE_ETHNICITY = "The body web service ethnicity parameter";
    public static final String INDIVIDUALS_CREATE_PARENTALCONSANGUINITY = "The body web service parentalConsanguinity parameter";
    public static final String INDIVIDUALS_CREATE_POPULATION = "The body web service population parameter";
    public static final String INDIVIDUALS_CREATE_DATEOFBIRTH = "The body web service dateOfBirth parameter";
    public static final String INDIVIDUALS_CREATE_KARYOTYPICSEX = "The body web service karyotypicSex parameter";
    public static final String INDIVIDUALS_CREATE_LIFESTATUS = "The body web service lifeStatus parameter";
    public static final String INDIVIDUALS_CREATE_ANNOTATIONSETS = "The body web service annotationSets parameter";
    public static final String INDIVIDUALS_CREATE_PHENOTYPES = "The body web service phenotypes parameter";
    public static final String INDIVIDUALS_CREATE_DISORDERS = "The body web service disorders parameter";
    public static final String INDIVIDUALS_CREATE_STATUS = "The body web service status parameter";
    public static final String INDIVIDUALS_CREATE_ATTRIBUTES = "The body web service attributes parameter";
    public static final String FAMILIES_UPDATE_ID = "The body web service id parameter";
    public static final String FAMILIES_UPDATE_NAME = "The body web service name parameter";
    public static final String FAMILIES_UPDATE_DESCRIPTION = "The body web service description parameter";
    public static final String FAMILIES_UPDATE_CREATIONDATE = "The body web service creationDate parameter";
    public static final String FAMILIES_UPDATE_MODIFICATIONDATE = "The body web service modificationDate parameter";
    public static final String FAMILIES_UPDATE_MEMBERS = "The body web service members parameter";
    public static final String FAMILIES_UPDATE_EXPECTEDSIZE = "The body web service expectedSize parameter";
    public static final String FAMILIES_UPDATE_QUALITYCONTROL = "The body web service qualityControl parameter";
    public static final String FAMILIES_UPDATE_STATUS = "The body web service status parameter";
    public static final String FAMILIES_UPDATE_ANNOTATIONSETS = "The body web service annotationSets parameter";
    public static final String FAMILIES_UPDATE_ATTRIBUTES = "The body web service attributes parameter";
    public static final String FAMILIES_ANNOTATIONSETS_LOAD_CONTENT = "The body web service content parameter";
    public static final String FAMILIES_UPDATE_FAMILY = "The body web service family parameter";
    public static final String FAMILIES_UPDATE_INDIVIDUAL = "The body web service individual parameter";
    public static final String FAMILIES_UPDATE_SAMPLE = "The body web service sample parameter";
    public static final String FAMILIES_CREATE_ID = "The body web service id parameter";
    public static final String FAMILIES_CREATE_NAME = "The body web service name parameter";
    public static final String FAMILIES_CREATE_DESCRIPTION = "The body web service description parameter";
    public static final String FAMILIES_CREATE_CREATIONDATE = "The body web service creationDate parameter";
    public static final String FAMILIES_CREATE_MODIFICATIONDATE = "The body web service modificationDate parameter";
    public static final String FAMILIES_CREATE_MEMBERS = "The body web service members parameter";
    public static final String FAMILIES_CREATE_EXPECTEDSIZE = "The body web service expectedSize parameter";
    public static final String FAMILIES_CREATE_STATUS = "The body web service status parameter";
    public static final String FAMILIES_CREATE_ATTRIBUTES = "The body web service attributes parameter";
    public static final String FAMILIES_CREATE_ANNOTATIONSETS = "The body web service annotationSets parameter";
    public static final String COHORTS_UPDATE_ID = "The body web service id parameter";
//    public static final String COHORTS_UPDATE_TYPE = "The body web service type parameter";
    public static final String COHORTS_UPDATE_DESCRIPTION = "The body web service description parameter";
    public static final String COHORTS_UPDATE_CREATIONDATE = "The body web service creationDate parameter";
    public static final String COHORTS_UPDATE_MODIFICATIONDATE = "The body web service modificationDate parameter";
    public static final String COHORTS_UPDATE_SAMPLES = "The body web service samples parameter";
    public static final String COHORTS_UPDATE_ANNOTATIONSETS = "The body web service annotationSets parameter";
    public static final String COHORTS_UPDATE_ATTRIBUTES = "The body web service attributes parameter";
    public static final String COHORTS_UPDATE_STATUS = "The body web service status parameter";
    public static final String COHORTS_ANNOTATIONSETS_LOAD_CONTENT = "The body web service content parameter";
    public static final String COHORTS_UPDATE_COHORT = "The body web service cohort parameter";
    public static final String COHORTS_CREATE_ID = "The body web service id parameter";
//    public static final String COHORTS_CREATE_TYPE = "The body web service type parameter";
    public static final String COHORTS_CREATE_DESCRIPTION = "The body web service description parameter";
    public static final String COHORTS_CREATE_CREATIONDATE = "The body web service creationDate parameter";
    public static final String COHORTS_CREATE_MODIFICATIONDATE = "The body web service modificationDate parameter";
    public static final String COHORTS_CREATE_SAMPLES = "The body web service samples parameter";
    public static final String COHORTS_CREATE_ANNOTATIONSETS = "The body web service annotationSets parameter";
    public static final String COHORTS_CREATE_ATTRIBUTES = "The body web service attributes parameter";
    public static final String COHORTS_CREATE_STATUS = "The body web service status parameter";
    public static final String COHORTS_GENERATE_ID = "The body web service id parameter";
//    public static final String COHORTS_GENERATE_TYPE = "The body web service type parameter";
    public static final String COHORTS_GENERATE_DESCRIPTION = "The body web service description parameter";
    public static final String COHORTS_GENERATE_CREATIONDATE = "The body web service creationDate parameter";
    public static final String COHORTS_GENERATE_MODIFICATIONDATE = "The body web service modificationDate parameter";
    public static final String COHORTS_GENERATE_ANNOTATIONSETS = "The body web service annotationSets parameter";
    public static final String COHORTS_GENERATE_STATUS = "The body web service status parameter";
    public static final String COHORTS_GENERATE_ATTRIBUTES = "The body web service attributes parameter";
    public static final String DISEASE_PANELS_UPDATE_PANEL = "The body web service panel parameter";
    public static final String DISEASE_PANELS_CREATE_ID = "The body web service id parameter";
    public static final String DISEASE_PANELS_CREATE_NAME = "The body web service name parameter";
    public static final String DISEASE_PANELS_CREATE_DESCRIPTION = "The body web service description parameter";
    public static final String DISEASE_PANELS_CREATE_AUTHOR = "The body web service author parameter";
    public static final String DISEASE_PANELS_CREATE_SOURCE = "The body web service source parameter";
    public static final String DISEASE_PANELS_CREATE_CATEGORIES = "The body web service categories parameter";
    public static final String DISEASE_PANELS_CREATE_TAGS = "The body web service tags parameter";
    public static final String DISEASE_PANELS_CREATE_DISORDERS = "The body web service disorders parameter";
    public static final String DISEASE_PANELS_CREATE_VARIANTS = "The body web service variants parameter";
    public static final String DISEASE_PANELS_CREATE_GENES = "The body web service genes parameter";
    public static final String DISEASE_PANELS_CREATE_REGIONS = "The body web service regions parameter";
    public static final String DISEASE_PANELS_CREATE_STRS = "The body web service strs parameter";
    public static final String DISEASE_PANELS_CREATE_STATS = "The body web service stats parameter";
    public static final String DISEASE_PANELS_CREATE_ATTRIBUTES = "The body web service attributes parameter";
    public static final String DISEASE_PANELS_UPDATE_ID = "The body web service id parameter";
    public static final String DISEASE_PANELS_UPDATE_NAME = "The body web service name parameter";
    public static final String DISEASE_PANELS_UPDATE_DESCRIPTION = "The body web service description parameter";
    public static final String DISEASE_PANELS_UPDATE_AUTHOR = "The body web service author parameter";
    public static final String DISEASE_PANELS_UPDATE_SOURCE = "The body web service source parameter";
    public static final String DISEASE_PANELS_UPDATE_CATEGORIES = "The body web service categories parameter";
    public static final String DISEASE_PANELS_UPDATE_TAGS = "The body web service tags parameter";
    public static final String DISEASE_PANELS_UPDATE_DISORDERS = "The body web service disorders parameter";
    public static final String DISEASE_PANELS_UPDATE_VARIANTS = "The body web service variants parameter";
    public static final String DISEASE_PANELS_UPDATE_GENES = "The body web service genes parameter";
    public static final String DISEASE_PANELS_UPDATE_REGIONS = "The body web service regions parameter";
    public static final String DISEASE_PANELS_UPDATE_STRS = "The body web service strs parameter";
    public static final String DISEASE_PANELS_UPDATE_STATS = "The body web service stats parameter";
    public static final String DISEASE_PANELS_UPDATE_ATTRIBUTES = "The body web service attributes parameter";
    public static final String ANALYSIS_ALIGNMENT_COVERAGE_INDEX_RUN_FILE = "The body web service file parameter";
    public static final String ANALYSIS_ALIGNMENT_COVERAGE_INDEX_RUN_WINDOWSIZE = "The body web service windowSize parameter";
    public static final String ANALYSIS_ALIGNMENT_COVERAGE_INDEX_RUN_OVERWRITE = "The body web service overwrite parameter";
    public static final String ANALYSIS_ALIGNMENT_COVERAGE_QC_GENECOVERAGESTATS_RUN_BAMFILE = "The body web service bamFile parameter";
    public static final String ANALYSIS_ALIGNMENT_COVERAGE_QC_GENECOVERAGESTATS_RUN_GENES = "The body web service genes parameter";
    public static final String ANALYSIS_ALIGNMENT_COVERAGE_QC_GENECOVERAGESTATS_RUN_OUTDIR = "The body web service outdir parameter";
    public static final String ANALYSIS_ALIGNMENT_QC_RUN_BAMFILE = "The body web service bamFile parameter";
    public static final String ANALYSIS_ALIGNMENT_QC_RUN_BEDFILE = "The body web service bedFile parameter";
    public static final String ANALYSIS_ALIGNMENT_QC_RUN_DICTFILE = "The body web service dictFile parameter";
    public static final String ANALYSIS_ALIGNMENT_QC_RUN_SKIP = "The body web service skip parameter";
    public static final String ANALYSIS_ALIGNMENT_QC_RUN_OVERWRITE = "The body web service overwrite parameter";
    public static final String ANALYSIS_ALIGNMENT_QC_RUN_OUTDIR = "The body web service outdir parameter";
    public static final String ANALYSIS_ALIGNMENT_BWA_RUN_COMMAND = "The body web service command parameter";
    public static final String ANALYSIS_ALIGNMENT_BWA_RUN_FASTAFILE = "The body web service fastaFile parameter";
    public static final String ANALYSIS_ALIGNMENT_BWA_RUN_FASTQ1FILE = "The body web service fastq1File parameter";
    public static final String ANALYSIS_ALIGNMENT_BWA_RUN_FASTQ2FILE = "The body web service fastq2File parameter";
    public static final String ANALYSIS_ALIGNMENT_BWA_RUN_OUTDIR = "The body web service outdir parameter";
    public static final String ANALYSIS_ALIGNMENT_BWA_RUN_BWAPARAMS = "The body web service bwaParams parameter";
    public static final String ANALYSIS_ALIGNMENT_SAMTOOLS_RUN_COMMAND = "The body web service command parameter";
    public static final String ANALYSIS_ALIGNMENT_SAMTOOLS_RUN_INPUTFILE = "The body web service inputFile parameter";
    public static final String ANALYSIS_ALIGNMENT_SAMTOOLS_RUN_OUTDIR = "The body web service outdir parameter";
    public static final String ANALYSIS_ALIGNMENT_SAMTOOLS_RUN_SAMTOOLSPARAMS = "The body web service samtoolsParams parameter";
    public static final String ANALYSIS_ALIGNMENT_DEEPTOOLS_RUN_COMMAND = "The body web service command parameter";
    public static final String ANALYSIS_ALIGNMENT_DEEPTOOLS_RUN_OUTDIR = "The body web service outdir parameter";
    public static final String ANALYSIS_ALIGNMENT_DEEPTOOLS_RUN_DEEPTOOLSPARAMS = "The body web service deeptoolsParams parameter";
    public static final String ANALYSIS_ALIGNMENT_FASTQC_RUN_INPUTFILE = "The body web service inputFile parameter";
    public static final String ANALYSIS_ALIGNMENT_FASTQC_RUN_OUTDIR = "The body web service outdir parameter";
    public static final String ANALYSIS_ALIGNMENT_FASTQC_RUN_FASTQCPARAMS = "The body web service fastqcParams parameter";
    public static final String ANALYSIS_ALIGNMENT_PICARD_RUN_COMMAND = "The body web service command parameter";
    public static final String ANALYSIS_ALIGNMENT_PICARD_RUN_OUTDIR = "The body web service outdir parameter";
    public static final String ANALYSIS_ALIGNMENT_PICARD_RUN_PICARDPARAMS = "The body web service picardParams parameter";
    public static final String ANALYSIS_ALIGNMENT_INDEX_RUN_FILE = "The body web service file parameter";
    public static final String ANALYSIS_ALIGNMENT_INDEX_RUN_OVERWRITE = "The body web service overwrite parameter";
    public static final String ANALYSIS_VARIANT_EXPORT_RUN_OUTDIR = "The body web service outdir parameter";
    public static final String ANALYSIS_VARIANT_EXPORT_RUN_OUTPUTFILENAME = "The body web service outputFileName parameter";
    public static final String ANALYSIS_VARIANT_EXPORT_RUN_OUTPUTFORMAT = "The body web service outputFormat parameter";
    public static final String ANALYSIS_VARIANT_EXPORT_RUN_COMPRESS = "The body web service compress parameter";
    public static final String ANALYSIS_VARIANT_EXPORT_RUN_VARIANTSFILE = "The body web service variantsFile parameter";
    public static final String ANALYSIS_VARIANT_EXPORT_RUN_INCLUDE = "The body web service include parameter";
    public static final String ANALYSIS_VARIANT_EXPORT_RUN_EXCLUDE = "The body web service exclude parameter";
    public static final String ANALYSIS_VARIANT_EXPORT_RUN_LIMIT = "The body web service limit parameter";
    public static final String ANALYSIS_VARIANT_EXPORT_RUN_SKIP = "The body web service skip parameter";
    public static final String ANALYSIS_VARIANT_EXPORT_RUN_SUMMARY = "The body web service summary parameter";
    public static final String ANALYSIS_VARIANT_STATS_RUN_COHORT = "The body web service cohort parameter";
    public static final String ANALYSIS_VARIANT_STATS_RUN_SAMPLES = "The body web service samples parameter";
    public static final String ANALYSIS_VARIANT_STATS_RUN_REGION = "The body web service region parameter";
    public static final String ANALYSIS_VARIANT_STATS_RUN_GENE = "The body web service gene parameter";
    public static final String ANALYSIS_VARIANT_STATS_RUN_OUTDIR = "The body web service outdir parameter";
    public static final String ANALYSIS_VARIANT_STATS_RUN_OUTPUTFILENAME = "The body web service outputFileName parameter";
    public static final String ANALYSIS_VARIANT_STATS_RUN_AGGREGATED = "The body web service aggregated parameter";
    public static final String ANALYSIS_VARIANT_STATS_RUN_AGGREGATIONMAPPINGFILE = "The body web service aggregationMappingFile parameter";
    public static final String ANALYSIS_VARIANT_STATS_EXPORT_RUN_COHORTS = "The body web service cohorts parameter";
    public static final String ANALYSIS_VARIANT_STATS_EXPORT_RUN_OUTPUT = "The body web service output parameter";
    public static final String ANALYSIS_VARIANT_STATS_EXPORT_RUN_REGION = "The body web service region parameter";
    public static final String ANALYSIS_VARIANT_STATS_EXPORT_RUN_GENE = "The body web service gene parameter";
    public static final String ANALYSIS_VARIANT_STATS_EXPORT_RUN_OUTPUTFORMAT = "The body web service outputFormat parameter";
    public static final String ANALYSIS_VARIANT_SAMPLE_RUN_GENOTYPES = "The body web service genotypes parameter";
    public static final String ANALYSIS_VARIANT_SAMPLE_RUN_SAMPLE = "The body web service sample parameter";
    public static final String ANALYSIS_VARIANT_SAMPLE_RUN_SAMPLESINALLVARIANTS = "The body web service samplesInAllVariants parameter";
    public static final String ANALYSIS_VARIANT_SAMPLE_RUN_MAXVARIANTS = "The body web service maxVariants parameter";
    public static final String ANALYSIS_VARIANT_SAMPLE_ELIGIBILITY_RUN_QUERY = "The body web service query parameter";
    public static final String ANALYSIS_VARIANT_SAMPLE_ELIGIBILITY_RUN_INDEX = "The body web service index parameter";
    public static final String ANALYSIS_VARIANT_SAMPLE_ELIGIBILITY_RUN_COHORTID = "The body web service cohortId parameter";
    public static final String ANALYSIS_VARIANT_SAMPLE_STATS_RUN_SAMPLE = "The body web service sample parameter";
    public static final String ANALYSIS_VARIANT_SAMPLE_STATS_RUN_INDIVIDUAL = "The body web service individual parameter";
    public static final String ANALYSIS_VARIANT_SAMPLE_STATS_RUN_VARIANTQUERY = "The body web service variantQuery parameter";
    public static final String ANALYSIS_VARIANT_SAMPLE_STATS_RUN_OUTDIR = "The body web service outdir parameter";
    public static final String ANALYSIS_VARIANT_SAMPLE_STATS_RUN_INDEX = "The body web service index parameter";
    public static final String ANALYSIS_VARIANT_SAMPLE_STATS_RUN_INDEXOVERWRITE = "The body web service indexOverwrite parameter";
    public static final String ANALYSIS_VARIANT_SAMPLE_STATS_RUN_INDEXID = "The body web service indexId parameter";
    public static final String ANALYSIS_VARIANT_SAMPLE_STATS_RUN_INDEXDESCRIPTION = "The body web service indexDescription parameter";
    public static final String ANALYSIS_VARIANT_SAMPLE_STATS_RUN_BATCHSIZE = "The body web service batchSize parameter";
    public static final String ANALYSIS_VARIANT_INDEX_RUN_FILE = "The body web service file parameter";
    public static final String ANALYSIS_VARIANT_INDEX_RUN_RESUME = "The body web service resume parameter";
    public static final String ANALYSIS_VARIANT_INDEX_RUN_OUTDIR = "The body web service outdir parameter";
    public static final String ANALYSIS_VARIANT_INDEX_RUN_TRANSFORM = "The body web service transform parameter";
    public static final String ANALYSIS_VARIANT_INDEX_RUN_GVCF = "The body web service gvcf parameter";
    public static final String ANALYSIS_VARIANT_INDEX_RUN_NORMALIZATIONSKIP = "The body web service normalizationSkip parameter";
    public static final String ANALYSIS_VARIANT_INDEX_RUN_REFERENCEGENOME = "The body web service referenceGenome parameter";
    public static final String ANALYSIS_VARIANT_INDEX_RUN_FAMILY = "The body web service family parameter";
    public static final String ANALYSIS_VARIANT_INDEX_RUN_SOMATIC = "The body web service somatic parameter";
    public static final String ANALYSIS_VARIANT_INDEX_RUN_LOAD = "The body web service load parameter";
    public static final String ANALYSIS_VARIANT_INDEX_RUN_LOADSPLITDATA = "The body web service loadSplitData parameter";
    public static final String ANALYSIS_VARIANT_INDEX_RUN_LOADMULTIFILEDATA = "The body web service loadMultiFileData parameter";
    public static final String ANALYSIS_VARIANT_INDEX_RUN_LOADSAMPLEINDEX = "The body web service loadSampleIndex parameter";
    public static final String ANALYSIS_VARIANT_INDEX_RUN_LOADARCHIVE = "The body web service loadArchive parameter";
    public static final String ANALYSIS_VARIANT_INDEX_RUN_LOADHOMREF = "The body web service loadHomRef parameter";
    public static final String ANALYSIS_VARIANT_INDEX_RUN_POSTLOADCHECK = "The body web service postLoadCheck parameter";
    public static final String ANALYSIS_VARIANT_INDEX_RUN_EXCLUDEGENOTYPES = "The body web service excludeGenotypes parameter";
    public static final String ANALYSIS_VARIANT_INDEX_RUN_INCLUDESAMPLEDATA = "The body web service includeSampleData parameter";
    public static final String ANALYSIS_VARIANT_INDEX_RUN_MERGE = "The body web service merge parameter";
    public static final String ANALYSIS_VARIANT_INDEX_RUN_DEDUPLICATIONPOLICY = "The body web service deduplicationPolicy parameter";
    public static final String ANALYSIS_VARIANT_INDEX_RUN_CALCULATESTATS = "The body web service calculateStats parameter";
    public static final String ANALYSIS_VARIANT_INDEX_RUN_AGGREGATED = "The body web service aggregated parameter";
    public static final String ANALYSIS_VARIANT_INDEX_RUN_AGGREGATIONMAPPINGFILE = "The body web service aggregationMappingFile parameter";
    public static final String ANALYSIS_VARIANT_INDEX_RUN_ANNOTATE = "The body web service annotate parameter";
    public static final String ANALYSIS_VARIANT_INDEX_RUN_ANNOTATOR = "The body web service annotator parameter";
    public static final String ANALYSIS_VARIANT_INDEX_RUN_OVERWRITEANNOTATIONS = "The body web service overwriteAnnotations parameter";
    public static final String ANALYSIS_VARIANT_INDEX_RUN_INDEXSEARCH = "The body web service indexSearch parameter";
    public static final String ANALYSIS_VARIANT_INDEX_RUN_SKIPINDEXEDFILES = "The body web service skipIndexedFiles parameter";
    public static final String ANALYSIS_VARIANT_COHORT_STATS_RUN_COHORT = "The body web service cohort parameter";
    public static final String ANALYSIS_VARIANT_COHORT_STATS_RUN_SAMPLES = "The body web service samples parameter";
    public static final String ANALYSIS_VARIANT_COHORT_STATS_RUN_INDEX = "The body web service index parameter";
    public static final String ANALYSIS_VARIANT_COHORT_STATS_RUN_SAMPLEANNOTATION = "The body web service sampleAnnotation parameter";
    public static final String ANALYSIS_VARIANT_COHORT_STATS_RUN_OUTDIR = "The body web service outdir parameter";
    public static final String ANALYSIS_VARIANT_GWAS_RUN_PHENOTYPE = "The body web service phenotype parameter";
    public static final String ANALYSIS_VARIANT_GWAS_RUN_INDEX = "The body web service index parameter";
    public static final String ANALYSIS_VARIANT_GWAS_RUN_INDEXSCOREID = "The body web service indexScoreId parameter";
    public static final String ANALYSIS_VARIANT_GWAS_RUN_METHOD = "The body web service method parameter";
    public static final String ANALYSIS_VARIANT_GWAS_RUN_FISHERMODE = "The body web service fisherMode parameter";
    public static final String ANALYSIS_VARIANT_GWAS_RUN_CASECOHORT = "The body web service caseCohort parameter";
    public static final String ANALYSIS_VARIANT_GWAS_RUN_CASECOHORTSAMPLESANNOTATION = "The body web service caseCohortSamplesAnnotation " +
            "parameter";
    public static final String ANALYSIS_VARIANT_GWAS_RUN_CASECOHORTSAMPLES = "The body web service caseCohortSamples parameter";
    public static final String ANALYSIS_VARIANT_GWAS_RUN_CONTROLCOHORT = "The body web service controlCohort parameter";
    public static final String ANALYSIS_VARIANT_GWAS_RUN_CONTROLCOHORTSAMPLESANNOTATION = "The body web service " +
            "controlCohortSamplesAnnotation parameter";
    public static final String ANALYSIS_VARIANT_GWAS_RUN_CONTROLCOHORTSAMPLES = "The body web service controlCohortSamples parameter";
    public static final String ANALYSIS_VARIANT_GWAS_RUN_OUTDIR = "The body web service outdir parameter";
    public static final String ANALYSIS_VARIANT_MUTATIONALSIGNATURE_RUN_SAMPLE = "The body web service sample parameter";
    public static final String ANALYSIS_VARIANT_MUTATIONALSIGNATURE_RUN_ID = "The body web service id parameter";
    public static final String ANALYSIS_VARIANT_MUTATIONALSIGNATURE_RUN_DESCRIPTION = "The body web service description parameter";
    public static final String ANALYSIS_VARIANT_MUTATIONALSIGNATURE_RUN_QUERY = "The body web service query parameter";
    public static final String ANALYSIS_VARIANT_MUTATIONALSIGNATURE_RUN_RELEASE = "The body web service release parameter";
    public static final String ANALYSIS_VARIANT_MUTATIONALSIGNATURE_RUN_FITTING = "The body web service fitting parameter";
    public static final String ANALYSIS_VARIANT_MUTATIONALSIGNATURE_RUN_OUTDIR = "The body web service outdir parameter";
    public static final String ANALYSIS_VARIANT_MENDELIANERROR_RUN_FAMILY = "The body web service family parameter";
    public static final String ANALYSIS_VARIANT_MENDELIANERROR_RUN_INDIVIDUAL = "The body web service individual parameter";
    public static final String ANALYSIS_VARIANT_MENDELIANERROR_RUN_SAMPLE = "The body web service sample parameter";
    public static final String ANALYSIS_VARIANT_MENDELIANERROR_RUN_OUTDIR = "The body web service outdir parameter";
    public static final String ANALYSIS_VARIANT_INFERREDSEX_RUN_INDIVIDUAL = "The body web service individual parameter";
    public static final String ANALYSIS_VARIANT_INFERREDSEX_RUN_SAMPLE = "The body web service sample parameter";
    public static final String ANALYSIS_VARIANT_INFERREDSEX_RUN_OUTDIR = "The body web service outdir parameter";
    public static final String ANALYSIS_VARIANT_RELATEDNESS_RUN_INDIVIDUALS = "The body web service individuals parameter";
    public static final String ANALYSIS_VARIANT_RELATEDNESS_RUN_SAMPLES = "The body web service samples parameter";
    public static final String ANALYSIS_VARIANT_RELATEDNESS_RUN_MINORALLELEFREQ = "The body web service minorAlleleFreq parameter";
    public static final String ANALYSIS_VARIANT_RELATEDNESS_RUN_METHOD = "The body web service method parameter";
    public static final String ANALYSIS_VARIANT_RELATEDNESS_RUN_OUTDIR = "The body web service outdir parameter";
    public static final String ANALYSIS_VARIANT_FAMILY_QC_RUN_FAMILY = "The body web service family parameter";
    public static final String ANALYSIS_VARIANT_FAMILY_QC_RUN_RELATEDNESSMETHOD = "The body web service relatednessMethod parameter";
    public static final String ANALYSIS_VARIANT_FAMILY_QC_RUN_RELATEDNESSMAF = "The body web service relatednessMaf parameter";
    public static final String ANALYSIS_VARIANT_FAMILY_QC_RUN_OUTDIR = "The body web service outdir parameter";
    public static final String ANALYSIS_VARIANT_INDIVIDUAL_QC_RUN_INDIVIDUAL = "The body web service individual parameter";
    public static final String ANALYSIS_VARIANT_INDIVIDUAL_QC_RUN_SAMPLE = "The body web service sample parameter";
    public static final String ANALYSIS_VARIANT_INDIVIDUAL_QC_RUN_INFERREDSEXMETHOD = "The body web service inferredSexMethod parameter";
    public static final String ANALYSIS_VARIANT_INDIVIDUAL_QC_RUN_OUTDIR = "The body web service outdir parameter";
    public static final String ANALYSIS_VARIANT_SAMPLE_QC_RUN_SAMPLE = "The body web service sample parameter";
    public static final String ANALYSIS_VARIANT_SAMPLE_QC_RUN_VARIANTSTATSID = "The body web service variantStatsId parameter";
    public static final String ANALYSIS_VARIANT_SAMPLE_QC_RUN_VARIANTSTATSDESCRIPTION = "The body web service variantStatsDescription " +
            "parameter";
    public static final String ANALYSIS_VARIANT_SAMPLE_QC_RUN_VARIANTSTATSQUERY = "The body web service variantStatsQuery parameter";
    public static final String ANALYSIS_VARIANT_SAMPLE_QC_RUN_SIGNATUREID = "The body web service signatureId parameter";
    public static final String ANALYSIS_VARIANT_SAMPLE_QC_RUN_SIGNATUREDESCRIPTION = "The body web service signatureDescription parameter";
    public static final String ANALYSIS_VARIANT_SAMPLE_QC_RUN_SIGNATUREQUERY = "The body web service signatureQuery parameter";
    public static final String ANALYSIS_VARIANT_SAMPLE_QC_RUN_SIGNATURERELEASE = "The body web service signatureRelease parameter";
    public static final String ANALYSIS_VARIANT_SAMPLE_QC_RUN_GENOMEPLOTID = "The body web service genomePlotId parameter";
    public static final String ANALYSIS_VARIANT_SAMPLE_QC_RUN_GENOMEPLOTDESCRIPTION = "The body web service genomePlotDescription " +
            "parameter";
    public static final String ANALYSIS_VARIANT_SAMPLE_QC_RUN_GENOMEPLOTCONFIGFILE = "The body web service genomePlotConfigFile parameter";
    public static final String ANALYSIS_VARIANT_SAMPLE_QC_RUN_OUTDIR = "The body web service outdir parameter";
    public static final String ANALYSIS_VARIANT_PLINK_RUN_OUTDIR = "The body web service outdir parameter";
    public static final String ANALYSIS_VARIANT_PLINK_RUN_PLINKPARAMS = "The body web service plinkParams parameter";
    public static final String ANALYSIS_VARIANT_RVTESTS_RUN_COMMAND = "The body web service command parameter";
    public static final String ANALYSIS_VARIANT_RVTESTS_RUN_OUTDIR = "The body web service outdir parameter";
    public static final String ANALYSIS_VARIANT_RVTESTS_RUN_RVTESTSPARAMS = "The body web service rvtestsParams parameter";
    public static final String ANALYSIS_VARIANT_GATK_RUN_COMMAND = "The body web service command parameter";
    public static final String ANALYSIS_VARIANT_GATK_RUN_OUTDIR = "The body web service outdir parameter";
    public static final String ANALYSIS_VARIANT_GATK_RUN_GATKPARAMS = "The body web service gatkParams parameter";
    public static final String ANALYSIS_VARIANT_KNOCKOUT_RUN_SAMPLE = "The body web service sample parameter";
    public static final String ANALYSIS_VARIANT_KNOCKOUT_RUN_GENE = "The body web service gene parameter";
    public static final String ANALYSIS_VARIANT_KNOCKOUT_RUN_PANEL = "The body web service panel parameter";
    public static final String ANALYSIS_VARIANT_KNOCKOUT_RUN_BIOTYPE = "The body web service biotype parameter";
    public static final String ANALYSIS_VARIANT_KNOCKOUT_RUN_CONSEQUENCETYPE = "The body web service consequenceType parameter";
    public static final String ANALYSIS_VARIANT_KNOCKOUT_RUN_FILTER = "The body web service filter parameter";
    public static final String ANALYSIS_VARIANT_KNOCKOUT_RUN_QUAL = "The body web service qual parameter";
    public static final String ANALYSIS_VARIANT_KNOCKOUT_RUN_SKIPGENESFILE = "The body web service skipGenesFile parameter";
    public static final String ANALYSIS_VARIANT_KNOCKOUT_RUN_OUTDIR = "The body web service outdir parameter";
    public static final String ANALYSIS_VARIANT_KNOCKOUT_RUN_INDEX = "The body web service index parameter";
    public static final String ANALYSIS_VARIANT_GENOMEPLOT_RUN_SAMPLE = "The body web service sample parameter";
    public static final String ANALYSIS_VARIANT_GENOMEPLOT_RUN_ID = "The body web service id parameter";
    public static final String ANALYSIS_VARIANT_GENOMEPLOT_RUN_DESCRIPTION = "The body web service description parameter";
    public static final String ANALYSIS_VARIANT_GENOMEPLOT_RUN_CONFIGFILE = "The body web service configFile parameter";
    public static final String ANALYSIS_VARIANT_GENOMEPLOT_RUN_OUTDIR = "The body web service outdir parameter";
    public static final String ANALYSIS_VARIANT_CIRCOS_RUN_TITLE = "The body web service title parameter";
    public static final String ANALYSIS_VARIANT_CIRCOS_RUN_DENSITY = "The body web service density parameter";
    public static final String ANALYSIS_VARIANT_CIRCOS_RUN_QUERY = "The body web service query parameter";
    public static final String ANALYSIS_VARIANT_CIRCOS_RUN_TRACKS = "The body web service tracks parameter";
    public static final String ANALYSIS_VARIANT_CIRCOS_RUN_OUTDIR = "The body web service outdir parameter";
    public static final String ANALYSIS_CLINICAL_UPDATE_ID = "The body web service id parameter";
    public static final String ANALYSIS_CLINICAL_UPDATE_DESCRIPTION = "The body web service description parameter";
//    public static final String ANALYSIS_CLINICAL_UPDATE_TYPE = "The body web service type parameter";
    public static final String ANALYSIS_CLINICAL_UPDATE_DISORDER = "The body web service disorder parameter";
    public static final String ANALYSIS_CLINICAL_UPDATE_FILES = "The body web service files parameter";
    public static final String ANALYSIS_CLINICAL_UPDATE_PANELS = "The body web service panels parameter";
    public static final String ANALYSIS_CLINICAL_UPDATE_PANELLOCK = "The body web service panelLock parameter";
    public static final String ANALYSIS_CLINICAL_UPDATE_PROBAND = "The body web service proband parameter";
    public static final String ANALYSIS_CLINICAL_UPDATE_FAMILY = "The body web service family parameter";
    public static final String ANALYSIS_CLINICAL_UPDATE_LOCKED = "The body web service locked parameter";
    public static final String ANALYSIS_CLINICAL_UPDATE_ANALYST = "The body web service analyst parameter";
    public static final String ANALYSIS_CLINICAL_UPDATE_QUALITYCONTROL = "The body web service qualityControl parameter";
    public static final String ANALYSIS_CLINICAL_UPDATE_CONSENT = "The body web service consent parameter";
    public static final String ANALYSIS_CLINICAL_UPDATE_CREATIONDATE = "The body web service creationDate parameter";
    public static final String ANALYSIS_CLINICAL_UPDATE_MODIFICATIONDATE = "The body web service modificationDate parameter";
    public static final String ANALYSIS_CLINICAL_UPDATE_DUEDATE = "The body web service dueDate parameter";
    public static final String ANALYSIS_CLINICAL_UPDATE_COMMENTS = "The body web service comments parameter";
    public static final String ANALYSIS_CLINICAL_UPDATE_PRIORITY = "The body web service priority parameter";
    public static final String ANALYSIS_CLINICAL_UPDATE_FLAGS = "The body web service flags parameter";
    public static final String ANALYSIS_CLINICAL_UPDATE_ATTRIBUTES = "The body web service attributes parameter";
    public static final String ANALYSIS_CLINICAL_UPDATE_STATUS = "The body web service status parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETATION_CREATE_ID = "The body web service id parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETATION_CREATE_DESCRIPTION = "The body web service description parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETATION_CREATE_CLINICALANALYSISID = "The body web service clinicalAnalysisId " +
            "parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETATION_CREATE_CREATIONDATE = "The body web service creationDate parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETATION_CREATE_MODIFICATIONDATE = "The body web service modificationDate parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETATION_CREATE_ANALYST = "The body web service analyst parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETATION_CREATE_METHODS = "The body web service methods parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETATION_CREATE_PRIMARYFINDINGS = "The body web service primaryFindings parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETATION_CREATE_SECONDARYFINDINGS = "The body web service secondaryFindings " +
            "parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETATION_CREATE_PANELS = "The body web service panels parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETATION_CREATE_COMMENTS = "The body web service comments parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETATION_CREATE_ATTRIBUTES = "The body web service attributes parameter";
    public static final String ANALYSIS_CLINICAL_CREATE_ID = "The body web service id parameter";
    public static final String ANALYSIS_CLINICAL_CREATE_DESCRIPTION = "The body web service description parameter";
//    public static final String ANALYSIS_CLINICAL_CREATE_TYPE = "The body web service type parameter";
    public static final String ANALYSIS_CLINICAL_CREATE_DISORDER = "The body web service disorder parameter";
    public static final String ANALYSIS_CLINICAL_CREATE_FILES = "The body web service files parameter";
    public static final String ANALYSIS_CLINICAL_CREATE_PROBAND = "The body web service proband parameter";
    public static final String ANALYSIS_CLINICAL_CREATE_FAMILY = "The body web service family parameter";
    public static final String ANALYSIS_CLINICAL_CREATE_PANELS = "The body web service panels parameter";
    public static final String ANALYSIS_CLINICAL_CREATE_PANELLOCK = "The body web service panelLock parameter";
    public static final String ANALYSIS_CLINICAL_CREATE_ANALYST = "The body web service analyst parameter";
    public static final String ANALYSIS_CLINICAL_CREATE_INTERPRETATION = "The body web service interpretation parameter";
    public static final String ANALYSIS_CLINICAL_CREATE_QUALITYCONTROL = "The body web service qualityControl parameter";
    public static final String ANALYSIS_CLINICAL_CREATE_CONSENT = "The body web service consent parameter";
    public static final String ANALYSIS_CLINICAL_CREATE_CREATIONDATE = "The body web service creationDate parameter";
    public static final String ANALYSIS_CLINICAL_CREATE_MODIFICATIONDATE = "The body web service modificationDate parameter";
    public static final String ANALYSIS_CLINICAL_CREATE_DUEDATE = "The body web service dueDate parameter";
    public static final String ANALYSIS_CLINICAL_CREATE_COMMENTS = "The body web service comments parameter";
    public static final String ANALYSIS_CLINICAL_CREATE_PRIORITY = "The body web service priority parameter";
    public static final String ANALYSIS_CLINICAL_CREATE_FLAGS = "The body web service flags parameter";
    public static final String ANALYSIS_CLINICAL_CREATE_ATTRIBUTES = "The body web service attributes parameter";
    public static final String ANALYSIS_CLINICAL_CREATE_STATUS = "The body web service status parameter";
    public static final String ANALYSIS_CLINICAL_UPDATE_CLINICALANALYSIS = "The body web service clinicalAnalysis parameter";
    public static final String ANALYSIS_CLINICAL_CLINICAL_CONFIGURATION_UPDATE_STATUS = "The body web service status parameter";
    public static final String ANALYSIS_CLINICAL_CLINICAL_CONFIGURATION_UPDATE_INTERPRETATION = "The body web service interpretation " +
            "parameter";
    public static final String ANALYSIS_CLINICAL_CLINICAL_CONFIGURATION_UPDATE_PRIORITIES = "The body web service priorities parameter";
    public static final String ANALYSIS_CLINICAL_CLINICAL_CONFIGURATION_UPDATE_FLAGS = "The body web service flags parameter";
    public static final String ANALYSIS_CLINICAL_CLINICAL_CONFIGURATION_UPDATE_CONSENT = "The body web service consent parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETATION_UPDATE_ID = "The body web service id parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETATION_UPDATE_DESCRIPTION = "The body web service description parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETATION_UPDATE_ANALYST = "The body web service analyst parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETATION_UPDATE_METHODS = "The body web service methods parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETATION_UPDATE_CREATIONDATE = "The body web service creationDate parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETATION_UPDATE_MODIFICATIONDATE = "The body web service modificationDate parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETATION_UPDATE_PRIMARYFINDINGS = "The body web service primaryFindings parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETATION_UPDATE_SECONDARYFINDINGS = "The body web service secondaryFindings " +
            "parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETATION_UPDATE_PANELS = "The body web service panels parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETATION_UPDATE_COMMENTS = "The body web service comments parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETATION_UPDATE_ATTRIBUTES = "The body web service attributes parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETATION_UPDATE_STATUS = "The body web service status parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETATION_MERGE_METHODS = "The body web service methods parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETATION_MERGE_PRIMARYFINDINGS = "The body web service primaryFindings parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETATION_MERGE_SECONDARYFINDINGS = "The body web service secondaryFindings " +
            "parameter";
    public static final String ANALYSIS_CLINICAL_RGA_INDEX_RUN_FILE = "The body web service file parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_TIERING_RUN_CLINICALANALYSIS = "The body web service clinicalAnalysis " +
            "parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_TIERING_RUN_PANELS = "The body web service panels parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_TIERING_RUN_PENETRANCE = "The body web service penetrance parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_TIERING_RUN_PRIMARY = "The body web service primary parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_TEAM_RUN_CLINICALANALYSIS = "The body web service clinicalAnalysis parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_TEAM_RUN_PANELS = "The body web service panels parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_TEAM_RUN_FAMILYSEGREGATION = "The body web service familySegregation " +
            "parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_TEAM_RUN_PRIMARY = "The body web service primary parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_CLINICALANALYSIS = "The body web service clinicalAnalysis parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_ID = "The body web service id parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_REGION = "The body web service region parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_TYPE = "The body web service type parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_STUDY = "The body web service study parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_FILE = "The body web service file parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_FILTER = "The body web service filter parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_QUAL = "The body web service qual parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_FILEDATA = "The body web service fileData parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_SAMPLE = "The body web service sample parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_SAMPLEDATA = "The body web service sampleData parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_SAMPLEANNOTATION = "The body web service sampleAnnotation parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_SAMPLEMETADATA = "The body web service sampleMetadata parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_COHORT = "The body web service cohort parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_COHORTSTATSREF = "The body web service cohortStatsRef parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_COHORTSTATSALT = "The body web service cohortStatsAlt parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_COHORTSTATSMAF = "The body web service cohortStatsMaf parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_COHORTSTATSMGF = "The body web service cohortStatsMgf parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_COHORTSTATSPASS = "The body web service cohortStatsPass parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_SCORE = "The body web service score parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_FAMILY = "The body web service family parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_FAMILYDISORDER = "The body web service familyDisorder parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_FAMILYSEGREGATION = "The body web service familySegregation " +
            "parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_FAMILYMEMBERS = "The body web service familyMembers parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_FAMILYPROBAND = "The body web service familyProband parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_GENE = "The body web service gene parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_CT = "The body web service ct parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_XREF = "The body web service xref parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_BIOTYPE = "The body web service biotype parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_PROTEINSUBSTITUTION = "The body web service proteinSubstitution " +
            "parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_CONSERVATION = "The body web service conservation parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_POPULATIONFREQUENCYALT = "The body web service " +
            "populationFrequencyAlt parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_POPULATIONFREQUENCYREF = "The body web service " +
            "populationFrequencyRef parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_POPULATIONFREQUENCYMAF = "The body web service " +
            "populationFrequencyMaf parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_TRANSCRIPTFLAG = "The body web service transcriptFlag parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_GENETRAITID = "The body web service geneTraitId parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_GO = "The body web service go parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_EXPRESSION = "The body web service expression parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_PROTEINKEYWORD = "The body web service proteinKeyword parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_DRUG = "The body web service drug parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_FUNCTIONALSCORE = "The body web service functionalScore parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_CLINICAL = "The body web service clinical parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_CLINICALSIGNIFICANCE = "The body web service clinicalSignificance " +
            "parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_CLINICALCONFIRMEDSTATUS = "The body web service " +
            "clinicalConfirmedStatus parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_CUSTOMANNOTATION = "The body web service customAnnotation parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_PANEL = "The body web service panel parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_PANELMODEOFINHERITANCE = "The body web service " +
            "panelModeOfInheritance parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_PANELCONFIDENCE = "The body web service panelConfidence parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_PANELROLEINCANCER = "The body web service panelRoleInCancer " +
            "parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_TRAIT = "The body web service trait parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_ZETTA_RUN_PRIMARY = "The body web service primary parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_CANCERTIERING_RUN_CLINICALANALYSIS = "The body web service clinicalAnalysis " +
            "parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_CANCERTIERING_RUN_DISCARDEDVARIANTS = "The body web service " +
            "discardedVariants parameter";
    public static final String ANALYSIS_CLINICAL_INTERPRETER_CANCERTIERING_RUN_PRIMARY = "The body web service primary parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_ANNOTATION_INDEX_OUTDIR = "The body web service outdir parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_ANNOTATION_INDEX_OUTPUTFILENAME = "The body web service outputFileName " +
            "parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_ANNOTATION_INDEX_ANNOTATOR = "The body web service annotator parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_ANNOTATION_INDEX_OVERWRITEANNOTATIONS = "The body web service " +
            "overwriteAnnotations parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_ANNOTATION_INDEX_REGION = "The body web service region parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_ANNOTATION_INDEX_CREATE = "The body web service create parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_ANNOTATION_INDEX_LOAD = "The body web service load parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_ANNOTATION_INDEX_CUSTOMNAME = "The body web service customName parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_ANNOTATION_INDEX_SAMPLEINDEXANNOTATION = "The body web service " +
            "sampleIndexAnnotation parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_STATS_INDEX_COHORT = "The body web service cohort parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_STATS_INDEX_REGION = "The body web service region parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_STATS_INDEX_OVERWRITESTATS = "The body web service overwriteStats " +
            "parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_STATS_INDEX_RESUME = "The body web service resume parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_STATS_INDEX_AGGREGATED = "The body web service aggregated parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_STATS_INDEX_AGGREGATIONMAPPINGFILE = "The body web service " +
            "aggregationMappingFile parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_INDEX_LAUNCHER_NAME = "The body web service name parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_INDEX_LAUNCHER_DIRECTORY = "The body web service directory parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_INDEX_LAUNCHER_RESUMEFAILED = "The body web service resumeFailed " +
            "parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_INDEX_LAUNCHER_IGNOREFAILED = "The body web service ignoreFailed " +
            "parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_INDEX_LAUNCHER_MAXJOBS = "The body web service maxJobs parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_INDEX_LAUNCHER_INDEXPARAMS = "The body web service indexParams parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_METADATA_SYNCHRONIZE_STUDY = "The body web service study parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_METADATA_SYNCHRONIZE_FILES = "The body web service files parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_METADATA_REPAIR_STUDIES = "The body web service studies parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_METADATA_REPAIR_SAMPLESBATCHSIZE = "The body web service " +
            "samplesBatchSize parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_METADATA_REPAIR_WHAT = "The body web service what parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_SAMPLE_INDEX_CONFIGURE_FILEINDEXCONFIGURATION = "The body web service " +
            "fileIndexConfiguration parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_SAMPLE_INDEX_CONFIGURE_ANNOTATIONINDEXCONFIGURATION = "The body web " +
            "service annotationIndexConfiguration parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_FAMILY_AGGREGATE_SAMPLES = "The body web service samples parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_FAMILY_AGGREGATE_RESUME = "The body web service resume parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_AGGREGATE_OVERWRITE = "The body web service overwrite parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_AGGREGATE_RESUME = "The body web service resume parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_JULIE_RUN_COHORTS = "The body web service cohorts parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_JULIE_RUN_REGION = "The body web service region parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_JULIE_RUN_OVERWRITE = "The body web service overwrite parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_ANNOTATION_SAVE_ANNOTATIONID = "The body web service annotationId " +
            "parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_CELLBASE_CONFIGURE_URL = "The body web service url parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_CELLBASE_CONFIGURE_VERSION = "The body web service version parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_CONFIGURE_OBJECTMAP = "The body web service objectMap parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_SCORE_INDEX_SCORENAME = "The body web service scoreName parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_SCORE_INDEX_COHORT1 = "The body web service cohort1 parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_SCORE_INDEX_COHORT2 = "The body web service cohort2 parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_SCORE_INDEX_INPUT = "The body web service input parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_SCORE_INDEX_INPUTCOLUMNS = "The body web service inputColumns parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_SCORE_INDEX_RESUME = "The body web service resume parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_SECONDARYINDEX_REGION = "The body web service region parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_SECONDARYINDEX_SAMPLE = "The body web service sample parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_SECONDARYINDEX_OVERWRITE = "The body web service overwrite parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_FAMILY_INDEX_FAMILY = "The body web service family parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_FAMILY_INDEX_OVERWRITE = "The body web service overwrite parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_FAMILY_INDEX_SKIPINCOMPLETEFAMILIES = "The body web service " +
            "skipIncompleteFamilies parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_SAMPLE_INDEX_SAMPLE = "The body web service sample parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_SAMPLE_INDEX_BUILDINDEX = "The body web service buildIndex parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_SAMPLE_INDEX_ANNOTATE = "The body web service annotate parameter";
    public static final String OPERATIONS_VARIANT_STORAGE_VARIANT_SAMPLE_INDEX_OVERWRITE = "The body web service overwrite parameter";
//    public static final String ADMIN_USERS_CREATE_TYPE = "The body web service type parameter";
    public static final String ADMIN_USERS_SYNC_AUTHENTICATIONORIGINID = "The body web service authenticationOriginId parameter";
    public static final String ADMIN_USERS_SYNC_FROM = "The body web service from parameter";
    public static final String ADMIN_USERS_SYNC_TO = "The body web service to parameter";
    public static final String ADMIN_USERS_SYNC_STUDY = "The body web service study parameter";
    public static final String ADMIN_USERS_SYNC_SYNCALL = "The body web service syncAll parameter";
//    public static final String ADMIN_USERS_SYNC_TYPE = "The body web service type parameter";
    public static final String ADMIN_USERS_SYNC_FORCE = "The body web service force parameter";
    public static final String ADMIN_USERS_IMPORT_AUTHENTICATIONORIGINID = "The body web service authenticationOriginId parameter";
    public static final String ADMIN_USERS_IMPORT_ID = "The body web service id parameter";
    public static final String ADMIN_USERS_IMPORT_RESOURCETYPE = "The body web service resourceType parameter";
    public static final String ADMIN_USERS_IMPORT_STUDY = "The body web service study parameter";
    public static final String ADMIN_USERS_IMPORT_STUDYGROUP = "The body web service studyGroup parameter";
    public static final String ADMIN_CATALOG_INSTALL_SECRETKEY = "The body web service secretKey parameter";
    public static final String ADMIN_CATALOG_INSTALL_PASSWORD = "The body web service password parameter";
    public static final String ADMIN_CATALOG_INSTALL_EMAIL = "The body web service email parameter";
    public static final String ADMIN_CATALOG_INSTALL_ORGANIZATION = "The body web service organization parameter";
    public static final String ADMIN_CATALOG_JWT_SECRETKEY = "The body web service secretKey parameter";
    private static final String UP_TO_100 = " up to a maximum of 100";
    public static final String FILES_DESCRIPTION = "Comma separated list of file IDs or names" + UP_TO_100;
    public static final String FILES_ID_DESCRIPTION = "Comma separated list of file IDs" + UP_TO_100;
    public static final String FILES_UUID_DESCRIPTION = "Comma separated list file UUIDs" + UP_TO_100;
    public static final String SAMPLES_DESCRIPTION = "Comma separated list sample IDs or UUIDs" + UP_TO_100;
    public static final String SAMPLES_ID_DESCRIPTION = "Comma separated list sample IDs" + UP_TO_100;
    public static final String SAMPLES_UUID_DESCRIPTION = "Comma separated list sample UUIDs" + UP_TO_100;
    public static final String INDIVIDUALS_DESCRIPTION = "Comma separated list of individual IDs, names or UUIDs" + UP_TO_100;
    public static final String INDIVIDUALS_ID_DESCRIPTION = "Comma separated list individual IDs" + UP_TO_100;
    public static final String INDIVIDUAL_NAME_DESCRIPTION = "Comma separated list individual names" + UP_TO_100;
    public static final String INDIVIDUAL_UUID_DESCRIPTION = "Comma separated list individual UUIDs" + UP_TO_100;
    public static final String FAMILIES_DESCRIPTION = "Comma separated list of family IDs or names" + UP_TO_100;
    public static final String FAMILY_ID_DESCRIPTION = "Comma separated list family IDs" + UP_TO_100;
    public static final String FAMILY_NAME_DESCRIPTION = "Comma separated list family names" + UP_TO_100;
    public static final String FAMILY_UUID_DESCRIPTION = "Comma separated list family UUIDs" + UP_TO_100;
    public static final String COHORTS_DESCRIPTION = "Comma separated list of cohort IDs or UUIDs" + UP_TO_100;
    public static final String COHORT_IDS_DESCRIPTION = "Comma separated list of cohort IDs" + UP_TO_100;
    public static final String COHORT_NAMES_DESCRIPTION = "Comma separated list of cohort names" + UP_TO_100;
    public static final String COHORT_UUIDS_DESCRIPTION = "Comma separated list of cohort IDs" + UP_TO_100;
    public static final String CLINICAL_ID_DESCRIPTION = "Comma separated list of Clinical Analysis IDs" + UP_TO_100;
    public static final String CLINICAL_UUID_DESCRIPTION = "Comma separated list of Clinical Analysis UUIDs" + UP_TO_100;
    public static final String CLINICAL_ANALYSES_DESCRIPTION = "Comma separated list of clinical analysis IDs or names" + UP_TO_100;
    public static final String INTERPRETATION_ID_DESCRIPTION = "Comma separated list of Interpretation IDs" + UP_TO_100;
    public static final String INTERPRETATION_UUID_DESCRIPTION = "Comma separated list of Interpretation UUIDs" + UP_TO_100;
    public static final String INTERPRETATION_DESCRIPTION = "Comma separated list of clinical interpretation IDs " + UP_TO_100;
    public static final String PANEL_ID_DESCRIPTION = "Comma separated list of panel IDs " + UP_TO_100;
    public static final String PANEL_UUID_DESCRIPTION = "Comma separated list of panel UUIDs " + UP_TO_100;
    public static final String PANEL_NAME_DESCRIPTION = "Comma separated list of panel names " + UP_TO_100;
    public static final String JOBS_DESCRIPTION = "Comma separated list of job IDs or UUIDs" + UP_TO_100;
    public static final String JOB_IDS_DESCRIPTION = "Comma separated list of job IDs" + UP_TO_100;
    public static final String JOB_UUIDS_DESCRIPTION = "Comma separated list of job UUIDs" + UP_TO_100;
    // ---------------------------------------------
    public static final String PROJECTS_DESCRIPTION = "Comma separated list of projects [user@]project" + UP_TO_100;
    // ---------------------------------------------
    public static final String STUDIES_DESCRIPTION = "Comma separated list of Studies [[user@]project:]study where study and project can " +
            "be either the ID or UUID" + UP_TO_100;
    public static final String PANELS_DESCRIPTION = "Comma separated list of panel IDs" + UP_TO_100;

    public static final String RGA_ANALYSIS_PARAMS_FILE_DESCRIPTION = "The body file RgaAnalysisParams web service parameter";




    public static final String CLINICAL_ANALYSIS_VARIANT_QC_STATS_DESCRIPTION = "The body stats ClinicalAnalysisVariantQc web service parameter";
    public static final String CLINICAL_ANALYSIS_VARIANT_QC_FILES_DESCRIPTION = "The body files ClinicalAnalysisVariantQc web service parameter";


    public static final String CLINICAL_ANALYST_PARAM_ID_DESCRIPTION = "The body id ClinicalAnalystParam web service parameter";


    public static final String FLAG_VALUE_PARAM_ID_DESCRIPTION = "The body id FlagValueParam web service parameter";


    public static final String SAMPLE_PARAMS_ID_DESCRIPTION = "The body id SampleParams web service parameter";






    public static final String CLINICAL_ANALYSIS_ALIGNMENT_QC_STATS_DESCRIPTION = "The body stats ClinicalAnalysisAlignmentQc web service parameter";
    public static final String CLINICAL_ANALYSIS_ALIGNMENT_QC_GENE_COVERAGE_STATS_DESCRIPTION = "The body geneCoverageStats ClinicalAnalysisAlignmentQc web service parameter";
    public static final String CLINICAL_ANALYSIS_ALIGNMENT_QC_FILES_DESCRIPTION = "The body files ClinicalAnalysisAlignmentQc web service parameter";


    public static final String CANCER_TIERING_INTERPRETATION_ANALYSIS_PARAMS_CLINICAL_ANALYSIS_DESCRIPTION = "The body clinicalAnalysis CancerTieringInterpretationAnalysisParams web service parameter";
    public static final String CANCER_TIERING_INTERPRETATION_ANALYSIS_PARAMS_DISCARDED_VARIANTS_DESCRIPTION = "The body discardedVariants CancerTieringInterpretationAnalysisParams web service parameter";
    public static final String CANCER_TIERING_INTERPRETATION_ANALYSIS_PARAMS_PRIMARY_DESCRIPTION = "The body primary CancerTieringInterpretationAnalysisParams web service parameter";


    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_CLINICAL_ANALYSIS_DESCRIPTION = "The body clinicalAnalysis ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_ID_DESCRIPTION = "The body id ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_REGION_DESCRIPTION = "The body region ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_TYPE_DESCRIPTION = "The body type ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_STUDY_DESCRIPTION = "The body study ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_FILE_DESCRIPTION = "The body file ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_FILTER_DESCRIPTION = "The body filter ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_QUAL_DESCRIPTION = "The body qual ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_FILE_DATA_DESCRIPTION = "The body fileData ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_SAMPLE_DESCRIPTION = "The body sample ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_SAMPLE_DATA_DESCRIPTION = "The body sampleData ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_SAMPLE_ANNOTATION_DESCRIPTION = "The body sampleAnnotation ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_SAMPLE_METADATA_DESCRIPTION = "The body sampleMetadata ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_COHORT_DESCRIPTION = "The body cohort ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_COHORT_STATS_REF_DESCRIPTION = "The body cohortStatsRef ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_COHORT_STATS_ALT_DESCRIPTION = "The body cohortStatsAlt ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_COHORT_STATS_MAF_DESCRIPTION = "The body cohortStatsMaf ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_COHORT_STATS_MGF_DESCRIPTION = "The body cohortStatsMgf ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_COHORT_STATS_PASS_DESCRIPTION = "The body cohortStatsPass ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_SCORE_DESCRIPTION = "The body score ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_FAMILY_DESCRIPTION = "The body family ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_FAMILY_DISORDER_DESCRIPTION = "The body familyDisorder ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_FAMILY_SEGREGATION_DESCRIPTION = "The body familySegregation ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_FAMILY_MEMBERS_DESCRIPTION = "The body familyMembers ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_FAMILY_PROBAND_DESCRIPTION = "The body familyProband ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_GENE_DESCRIPTION = "The body gene ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_CT_DESCRIPTION = "The body ct ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_XREF_DESCRIPTION = "The body xref ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_BIOTYPE_DESCRIPTION = "The body biotype ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_PROTEIN_SUBSTITUTION_DESCRIPTION = "The body proteinSubstitution ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_CONSERVATION_DESCRIPTION = "The body conservation ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_POPULATION_FREQUENCY_ALT_DESCRIPTION = "The body populationFrequencyAlt ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_POPULATION_FREQUENCY_REF_DESCRIPTION = "The body populationFrequencyRef ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_POPULATION_FREQUENCY_MAF_DESCRIPTION = "The body populationFrequencyMaf ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_TRANSCRIPT_FLAG_DESCRIPTION = "The body transcriptFlag ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_GENE_TRAIT_ID_DESCRIPTION = "The body geneTraitId ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_GO_DESCRIPTION = "The body go ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_EXPRESSION_DESCRIPTION = "The body expression ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_PROTEIN_KEYWORD_DESCRIPTION = "The body proteinKeyword ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_DRUG_DESCRIPTION = "The body drug ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_FUNCTIONAL_SCORE_DESCRIPTION = "The body functionalScore ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_CLINICAL_DESCRIPTION = "The body clinical ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_CLINICAL_SIGNIFICANCE_DESCRIPTION = "The body clinicalSignificance ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_CLINICAL_CONFIRMED_STATUS_DESCRIPTION = "The body clinicalConfirmedStatus ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_CUSTOM_ANNOTATION_DESCRIPTION = "The body customAnnotation ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_PANEL_DESCRIPTION = "The body panel ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_PANEL_MODE_OF_INHERITANCE_DESCRIPTION = "The body panelModeOfInheritance ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_PANEL_CONFIDENCE_DESCRIPTION = "The body panelConfidence ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_PANEL_ROLE_IN_CANCER_DESCRIPTION = "The body panelRoleInCancer ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_TRAIT_DESCRIPTION = "The body trait ZettaInterpretationAnalysisParams web service parameter";
    public static final String ZETTA_INTERPRETATION_ANALYSIS_PARAMS_PRIMARY_DESCRIPTION = "The body primary ZettaInterpretationAnalysisParams web service parameter";


    public static final String FAMILY_PARAM_ID_DESCRIPTION = "The body id FamilyParam web service parameter";
    public static final String FAMILY_PARAM_MEMBERS_DESCRIPTION = "The body members FamilyParam web service parameter";


    public static final String CLINICAL_ANALYSIS_QUALITY_CONTROL_UPDATE_PARAM_SUMMARY_DESCRIPTION = "The body summary ClinicalAnalysisQualityControlUpdateParam web service parameter";
    public static final String CLINICAL_ANALYSIS_QUALITY_CONTROL_UPDATE_PARAM_COMMENTS_DESCRIPTION = "The body comments ClinicalAnalysisQualityControlUpdateParam web service parameter";
    public static final String CLINICAL_ANALYSIS_QUALITY_CONTROL_UPDATE_PARAM_FILES_DESCRIPTION = "The body files ClinicalAnalysisQualityControlUpdateParam web service parameter";


    public static final String GENERIC_DESCRIPTION_DESCRIPTION = "Field to store information of the item";
    public static final String INTERPRETATION_UPDATE_PARAMS_ANALYST_DESCRIPTION = "The body analyst InterpretationUpdateParams web service parameter";
    public static final String INTERPRETATION_UPDATE_PARAMS_METHOD_DESCRIPTION = "The body method InterpretationUpdateParams web service parameter";
    public static final String GENERIC_CREATION_DATE_DESCRIPTION = "The creation date of the item";
    public static final String GENERIC_MODIFICATION_DATE_DESCRIPTION = "The last modification date of the item";
    public static final String INTERPRETATION_UPDATE_PARAMS_PRIMARY_FINDINGS_DESCRIPTION = "The body primaryFindings InterpretationUpdateParams web service parameter";
    public static final String INTERPRETATION_UPDATE_PARAMS_SECONDARY_FINDINGS_DESCRIPTION = "The body secondaryFindings InterpretationUpdateParams web service parameter";
    public static final String INTERPRETATION_UPDATE_PARAMS_PANELS_DESCRIPTION = "The body panels InterpretationUpdateParams web service parameter";
    public static final String INTERPRETATION_UPDATE_PARAMS_COMMENTS_DESCRIPTION = "The body comments InterpretationUpdateParams web service parameter";
    public static final String GENERIC_STATUS_DESCRIPTION = "A map of customizable attributes";
    public static final String INTERPRETATION_UPDATE_PARAMS_LOCKED_DESCRIPTION = "The body locked InterpretationUpdateParams web service parameter";
    public static final String GENERIC_ATTRIBUTES_DESCRIPTION = "A map of customizable attributes";


    public static final String CLINICAL_ANALYSIS_UPDATE_PARAMS_ID_DESCRIPTION = "The body id ClinicalAnalysisUpdateParams web service parameter";
    public static final String CLINICAL_ANALYSIS_UPDATE_PARAMS_TYPE_DESCRIPTION = "The body type ClinicalAnalysisUpdateParams web service parameter";
    public static final String CLINICAL_ANALYSIS_UPDATE_PARAMS_DISORDER_DESCRIPTION = "The body disorder ClinicalAnalysisUpdateParams web service parameter";
    public static final String CLINICAL_ANALYSIS_UPDATE_PARAMS_FILES_DESCRIPTION = "The body files ClinicalAnalysisUpdateParams web service parameter";
    public static final String CLINICAL_ANALYSIS_UPDATE_PARAMS_PANELS_DESCRIPTION = "The body panels ClinicalAnalysisUpdateParams web service parameter";
    public static final String CLINICAL_ANALYSIS_UPDATE_PARAMS_PANEL_LOCK_DESCRIPTION = "The body panelLock ClinicalAnalysisUpdateParams web service parameter";
    public static final String CLINICAL_ANALYSIS_UPDATE_PARAMS_PROBAND_DESCRIPTION = "The body proband ClinicalAnalysisUpdateParams web service parameter";
    public static final String CLINICAL_ANALYSIS_UPDATE_PARAMS_FAMILY_DESCRIPTION = "The body family ClinicalAnalysisUpdateParams web service parameter";
    public static final String CLINICAL_ANALYSIS_UPDATE_PARAMS_LOCKED_DESCRIPTION = "The body locked ClinicalAnalysisUpdateParams web service parameter";
    public static final String CLINICAL_ANALYSIS_UPDATE_PARAMS_ANALYST_DESCRIPTION = "The body analyst ClinicalAnalysisUpdateParams web service parameter";
    public static final String CLINICAL_ANALYSIS_UPDATE_PARAMS_REPORT_DESCRIPTION = "The body report ClinicalAnalysisUpdateParams web service parameter";
    public static final String CLINICAL_ANALYSIS_UPDATE_PARAMS_QUALITY_CONTROL_DESCRIPTION = "The body qualityControl ClinicalAnalysisUpdateParams web service parameter";
    public static final String CLINICAL_ANALYSIS_UPDATE_PARAMS_CONSENT_DESCRIPTION = "The body consent ClinicalAnalysisUpdateParams web service parameter";
    public static final String CLINICAL_ANALYSIS_UPDATE_PARAMS_DUE_DATE_DESCRIPTION = "The body dueDate ClinicalAnalysisUpdateParams web service parameter";
    public static final String CLINICAL_ANALYSIS_UPDATE_PARAMS_COMMENTS_DESCRIPTION = "The body comments ClinicalAnalysisUpdateParams web service parameter";
    public static final String CLINICAL_ANALYSIS_UPDATE_PARAMS_PRIORITY_DESCRIPTION = "The body priority ClinicalAnalysisUpdateParams web service parameter";
    public static final String CLINICAL_ANALYSIS_UPDATE_PARAMS_FLAGS_DESCRIPTION = "The body flags ClinicalAnalysisUpdateParams web service parameter";






    public static final String PRIORITY_PARAM_ID_DESCRIPTION = "The body id PriorityParam web service parameter";




    public static final String TEAM_INTERPRETATION_ANALYSIS_PARAMS_CLINICAL_ANALYSIS_DESCRIPTION = "The body clinicalAnalysis TeamInterpretationAnalysisParams web service parameter";
    public static final String TEAM_INTERPRETATION_ANALYSIS_PARAMS_PANELS_DESCRIPTION = "The body panels TeamInterpretationAnalysisParams web service parameter";
    public static final String TEAM_INTERPRETATION_ANALYSIS_PARAMS_FAMILY_SEGREGATION_DESCRIPTION = "The body familySegregation TeamInterpretationAnalysisParams web service parameter";
    public static final String TEAM_INTERPRETATION_ANALYSIS_PARAMS_PRIMARY_DESCRIPTION = "The body primary TeamInterpretationAnalysisParams web service parameter";


    public static final String CLINICAL_CONSENT_PRIMARY_FINDINGS_DESCRIPTION = "The body primaryFindings ClinicalConsent web service parameter";
    public static final String CLINICAL_CONSENT_SECONDARY_FINDINGS_DESCRIPTION = "The body secondaryFindings ClinicalConsent web service parameter";
    public static final String CLINICAL_CONSENT_CARRIER_FINDINGS_DESCRIPTION = "The body carrierFindings ClinicalConsent web service parameter";
    public static final String CLINICAL_CONSENT_RESEARCH_FINDINGS_DESCRIPTION = "The body researchFindings ClinicalConsent web service parameter";


    public static final String ALERT_AUTHOR_DESCRIPTION = "The body author Alert web service parameter";
    public static final String ALERT_DATE_DESCRIPTION = "The body date Alert web service parameter";
    public static final String ALERT_MESSAGE_DESCRIPTION = "The body message Alert web service parameter";
    public static final String ALERT_RISK_DESCRIPTION = "The body risk Alert web service parameter";


    public static final String CLINICAL_ANALYSIS_CREATE_PARAMS_ID_DESCRIPTION = "The body id ClinicalAnalysisCreateParams web service parameter";
    public static final String CLINICAL_ANALYSIS_CREATE_PARAMS_TYPE_DESCRIPTION = "The body type ClinicalAnalysisCreateParams web service parameter";
    public static final String CLINICAL_ANALYSIS_CREATE_PARAMS_DISORDER_DESCRIPTION = "The body disorder ClinicalAnalysisCreateParams web service parameter";
    public static final String CLINICAL_ANALYSIS_CREATE_PARAMS_FILES_DESCRIPTION = "The body files ClinicalAnalysisCreateParams web service parameter";
    public static final String CLINICAL_ANALYSIS_CREATE_PARAMS_PROBAND_DESCRIPTION = "The body proband ClinicalAnalysisCreateParams web service parameter";
    public static final String CLINICAL_ANALYSIS_CREATE_PARAMS_FAMILY_DESCRIPTION = "The body family ClinicalAnalysisCreateParams web service parameter";
    public static final String CLINICAL_ANALYSIS_CREATE_PARAMS_PANELS_DESCRIPTION = "The body panels ClinicalAnalysisCreateParams web service parameter";
    public static final String CLINICAL_ANALYSIS_CREATE_PARAMS_PANEL_LOCK_DESCRIPTION = "The body panelLock ClinicalAnalysisCreateParams web service parameter";
    public static final String CLINICAL_ANALYSIS_CREATE_PARAMS_ANALYST_DESCRIPTION = "The body analyst ClinicalAnalysisCreateParams web service parameter";
    public static final String CLINICAL_ANALYSIS_CREATE_PARAMS_REPORT_DESCRIPTION = "The body report ClinicalAnalysisCreateParams web service parameter";
    public static final String CLINICAL_ANALYSIS_CREATE_PARAMS_INTERPRETATION_DESCRIPTION = "The body interpretation ClinicalAnalysisCreateParams web service parameter";
    public static final String CLINICAL_ANALYSIS_CREATE_PARAMS_QUALITY_CONTROL_DESCRIPTION = "The body qualityControl ClinicalAnalysisCreateParams web service parameter";
    public static final String CLINICAL_ANALYSIS_CREATE_PARAMS_CONSENT_DESCRIPTION = "The body consent ClinicalAnalysisCreateParams web service parameter";
    public static final String CLINICAL_ANALYSIS_CREATE_PARAMS_DUE_DATE_DESCRIPTION = "The body dueDate ClinicalAnalysisCreateParams web service parameter";
    public static final String CLINICAL_ANALYSIS_CREATE_PARAMS_COMMENTS_DESCRIPTION = "The body comments ClinicalAnalysisCreateParams web service parameter";
    public static final String CLINICAL_ANALYSIS_CREATE_PARAMS_PRIORITY_DESCRIPTION = "The body priority ClinicalAnalysisCreateParams web service parameter";
    public static final String CLINICAL_ANALYSIS_CREATE_PARAMS_FLAGS_DESCRIPTION = "The body flags ClinicalAnalysisCreateParams web service parameter";


    public static final String PROBAND_PARAM_ID_DESCRIPTION = "The body id ProbandParam web service parameter";
    public static final String PROBAND_PARAM_SAMPLES_DESCRIPTION = "The body samples ProbandParam web service parameter";


    public static final String CLINICAL_COMMENT_PARAM_MESSAGE_DESCRIPTION = "The body message ClinicalCommentParam web service parameter";
    public static final String CLINICAL_COMMENT_PARAM_TAGS_DESCRIPTION = "The body tags ClinicalCommentParam web service parameter";
    public static final String CLINICAL_COMMENT_PARAM_DATE_DESCRIPTION = "The body date ClinicalCommentParam web service parameter";




    public static final String DISORDER_REFERENCE_PARAM_ID_DESCRIPTION = "The body id DisorderReferenceParam web service parameter";


    public static final String CLINICAL_ANALYSIS_ACL_UPDATE_PARAMS_CLINICAL_ANALYSIS_DESCRIPTION = "The body clinicalAnalysis ClinicalAnalysisAclUpdateParams web service parameter";


    public static final String EXOMISER_WRAPPER_PARAMS_SAMPLE_DESCRIPTION = "The body sample ExomiserWrapperParams web service parameter";
    public static final String EXOMISER_WRAPPER_PARAMS_OUTDIR_DESCRIPTION = "The body outdir ExomiserWrapperParams web service parameter";






    public static final String INTERPRETATION_MERGE_PARAMS_METHOD_DESCRIPTION = "The body method InterpretationMergeParams web service parameter";
    public static final String INTERPRETATION_MERGE_PARAMS_PRIMARY_FINDINGS_DESCRIPTION = "The body primaryFindings InterpretationMergeParams web service parameter";
    public static final String INTERPRETATION_MERGE_PARAMS_SECONDARY_FINDINGS_DESCRIPTION = "The body secondaryFindings InterpretationMergeParams web service parameter";


    public static final String TIERING_INTERPRETATION_ANALYSIS_PARAMS_CLINICAL_ANALYSIS_DESCRIPTION = "The body clinicalAnalysis TieringInterpretationAnalysisParams web service parameter";
    public static final String TIERING_INTERPRETATION_ANALYSIS_PARAMS_PANELS_DESCRIPTION = "The body panels TieringInterpretationAnalysisParams web service parameter";
    public static final String TIERING_INTERPRETATION_ANALYSIS_PARAMS_PENETRANCE_DESCRIPTION = "The body penetrance TieringInterpretationAnalysisParams web service parameter";
    public static final String TIERING_INTERPRETATION_ANALYSIS_PARAMS_PRIMARY_DESCRIPTION = "The body primary TieringInterpretationAnalysisParams web service parameter";


    public static final String EXOMISER_INTERPRETATION_ANALYSIS_PARAMS_CLINICAL_ANALYSIS_DESCRIPTION = "The body clinicalAnalysis ExomiserInterpretationAnalysisParams web service parameter";


    public static final String INTERPRETATION_CREATE_PARAMS_CLINICAL_ANALYSIS_ID_DESCRIPTION = "The body clinicalAnalysisId InterpretationCreateParams web service parameter";
    public static final String INTERPRETATION_CREATE_PARAMS_ANALYST_DESCRIPTION = "The body analyst InterpretationCreateParams web service parameter";
    public static final String INTERPRETATION_CREATE_PARAMS_METHOD_DESCRIPTION = "The body method InterpretationCreateParams web service parameter";
    public static final String INTERPRETATION_CREATE_PARAMS_PRIMARY_FINDINGS_DESCRIPTION = "The body primaryFindings InterpretationCreateParams web service parameter";
    public static final String INTERPRETATION_CREATE_PARAMS_SECONDARY_FINDINGS_DESCRIPTION = "The body secondaryFindings InterpretationCreateParams web service parameter";
    public static final String INTERPRETATION_CREATE_PARAMS_PANELS_DESCRIPTION = "The body panels InterpretationCreateParams web service parameter";
    public static final String INTERPRETATION_CREATE_PARAMS_COMMENTS_DESCRIPTION = "The body comments InterpretationCreateParams web service parameter";
    public static final String INTERPRETATION_CREATE_PARAMS_LOCKED_DESCRIPTION = "The body locked InterpretationCreateParams web service parameter";


    public static final String CLINICAL_STATUS_VALUE_ID_DESCRIPTION = "The body id ClinicalStatusValue web service parameter";
    public static final String CLINICAL_STATUS_VALUE_TYPE_DESCRIPTION = "The body type ClinicalStatusValue web service parameter";




    public static final String DATASET_ID_DESCRIPTION = "The body id Dataset web service parameter";
    public static final String DATASET_NAME_DESCRIPTION = "The body name Dataset web service parameter";
    public static final String DATASET_FILES_DESCRIPTION = "The body files Dataset web service parameter";




    public static final String VARIANT_AGGREGATE_PARAMS_OVERWRITE_DESCRIPTION = "The body overwrite VariantAggregateParams web service parameter";
    public static final String VARIANT_AGGREGATE_PARAMS_RESUME_DESCRIPTION = "The body resume VariantAggregateParams web service parameter";


    public static final String VARIANT_STORAGE_METADATA_REPAIR_TOOL_PARAMS_STUDIES_DESCRIPTION = "The body studies VariantStorageMetadataRepairToolParams web service parameter";
    public static final String VARIANT_STORAGE_METADATA_REPAIR_TOOL_PARAMS_SAMPLES_BATCH_SIZE_DESCRIPTION = "The body samplesBatchSize VariantStorageMetadataRepairToolParams web service parameter";
    public static final String VARIANT_STORAGE_METADATA_REPAIR_TOOL_PARAMS_WHAT_DESCRIPTION = "The body what VariantStorageMetadataRepairToolParams web service parameter";


    public static final String VARIANT_ANNOTATION_SAVE_PARAMS_ANNOTATION_ID_DESCRIPTION = "The body annotationId VariantAnnotationSaveParams web service parameter";


    public static final String VARIANT_AGGREGATE_FAMILY_PARAMS_SAMPLES_DESCRIPTION = "The body samples VariantAggregateFamilyParams web service parameter";
    public static final String VARIANT_AGGREGATE_FAMILY_PARAMS_GAPS_GENOTYPE_DESCRIPTION = "The body gapsGenotype VariantAggregateFamilyParams web service parameter";
    public static final String VARIANT_AGGREGATE_FAMILY_PARAMS_RESUME_DESCRIPTION = "The body resume VariantAggregateFamilyParams web service parameter";


    public static final String VARIANT_STATS_EXPORT_PARAMS_COHORTS_DESCRIPTION = "The body cohorts VariantStatsExportParams web service parameter";
    public static final String VARIANT_STATS_EXPORT_PARAMS_OUTPUT_DESCRIPTION = "The body output VariantStatsExportParams web service parameter";
    public static final String VARIANT_STATS_EXPORT_PARAMS_REGION_DESCRIPTION = "The body region VariantStatsExportParams web service parameter";
    public static final String VARIANT_STATS_EXPORT_PARAMS_GENE_DESCRIPTION = "The body gene VariantStatsExportParams web service parameter";
    public static final String VARIANT_STATS_EXPORT_PARAMS_OUTPUT_FILE_FORMAT_DESCRIPTION = "The body outputFileFormat VariantStatsExportParams web service parameter";


    public static final String VARIANT_SCORE_INDEX_PARAMS_SCORE_NAME_DESCRIPTION = "The body scoreName VariantScoreIndexParams web service parameter";
    public static final String VARIANT_SCORE_INDEX_PARAMS_COHORT1_DESCRIPTION = "The body cohort1 VariantScoreIndexParams web service parameter";
    public static final String VARIANT_SCORE_INDEX_PARAMS_COHORT2_DESCRIPTION = "The body cohort2 VariantScoreIndexParams web service parameter";
    public static final String VARIANT_SCORE_INDEX_PARAMS_INPUT_DESCRIPTION = "The body input VariantScoreIndexParams web service parameter";
    public static final String VARIANT_SCORE_INDEX_PARAMS_INPUT_COLUMNS_DESCRIPTION = "The body inputColumns VariantScoreIndexParams web service parameter";
    public static final String VARIANT_SCORE_INDEX_PARAMS_RESUME_DESCRIPTION = "The body resume VariantScoreIndexParams web service parameter";


    public static final String VARIANT_SCORE_DELETE_PARAMS_SCORE_NAME_DESCRIPTION = "The body scoreName VariantScoreDeleteParams web service parameter";
    public static final String VARIANT_SCORE_DELETE_PARAMS_FORCE_DESCRIPTION = "The body force VariantScoreDeleteParams web service parameter";
    public static final String VARIANT_SCORE_DELETE_PARAMS_RESUME_DESCRIPTION = "The body resume VariantScoreDeleteParams web service parameter";


    public static final String JULIE_PARAMS_COHORTS_DESCRIPTION = "The body cohorts JulieParams web service parameter";
    public static final String JULIE_PARAMS_REGION_DESCRIPTION = "The body region JulieParams web service parameter";
    public static final String JULIE_PARAMS_OVERWRITE_DESCRIPTION = "The body overwrite JulieParams web service parameter";


    public static final String VARIANT_SECONDARY_INDEX_PARAMS_REGION_DESCRIPTION = "The body region VariantSecondaryIndexParams web service parameter";
    public static final String VARIANT_SECONDARY_INDEX_PARAMS_SAMPLE_DESCRIPTION = "The body sample VariantSecondaryIndexParams web service parameter";
    public static final String VARIANT_SECONDARY_INDEX_PARAMS_OVERWRITE_DESCRIPTION = "The body overwrite VariantSecondaryIndexParams web service parameter";


    public static final String VARIANT_ANNOTATION_DELETE_PARAMS_ANNOTATION_ID_DESCRIPTION = "The body annotationId VariantAnnotationDeleteParams web service parameter";


    public static final String VARIANT_ANNOTATION_INDEX_PARAMS_OUTDIR_DESCRIPTION = "The body outdir VariantAnnotationIndexParams web service parameter";
    public static final String VARIANT_ANNOTATION_INDEX_PARAMS_OUTPUT_FILE_NAME_DESCRIPTION = "The body outputFileName VariantAnnotationIndexParams web service parameter";
    public static final String VARIANT_ANNOTATION_INDEX_PARAMS_ANNOTATOR_DESCRIPTION = "The body annotator VariantAnnotationIndexParams web service parameter";
    public static final String VARIANT_ANNOTATION_INDEX_PARAMS_OVERWRITE_ANNOTATIONS_DESCRIPTION = "The body overwriteAnnotations VariantAnnotationIndexParams web service parameter";
    public static final String VARIANT_ANNOTATION_INDEX_PARAMS_REGION_DESCRIPTION = "The body region VariantAnnotationIndexParams web service parameter";
    public static final String VARIANT_ANNOTATION_INDEX_PARAMS_CREATE_DESCRIPTION = "The body create VariantAnnotationIndexParams web service parameter";
    public static final String VARIANT_ANNOTATION_INDEX_PARAMS_LOAD_DESCRIPTION = "The body load VariantAnnotationIndexParams web service parameter";
    public static final String VARIANT_ANNOTATION_INDEX_PARAMS_CUSTOM_NAME_DESCRIPTION = "The body customName VariantAnnotationIndexParams web service parameter";
    public static final String VARIANT_ANNOTATION_INDEX_PARAMS_SAMPLE_INDEX_ANNOTATION_DESCRIPTION = "The body sampleIndexAnnotation VariantAnnotationIndexParams web service parameter";


    public static final String VARIANT_SAMPLE_INDEX_PARAMS_SAMPLE_DESCRIPTION = "The body sample VariantSampleIndexParams web service parameter";
    public static final String VARIANT_SAMPLE_INDEX_PARAMS_BUILD_INDEX_DESCRIPTION = "The body buildIndex VariantSampleIndexParams web service parameter";
    public static final String VARIANT_SAMPLE_INDEX_PARAMS_ANNOTATE_DESCRIPTION = "The body annotate VariantSampleIndexParams web service parameter";
    public static final String VARIANT_SAMPLE_INDEX_PARAMS_FAMILY_INDEX_DESCRIPTION = "The body familyIndex VariantSampleIndexParams web service parameter";
    public static final String VARIANT_SAMPLE_INDEX_PARAMS_OVERWRITE_DESCRIPTION = "The body overwrite VariantSampleIndexParams web service parameter";


    public static final String VARIANT_STATS_DELETE_PARAMS_COHORT_DESCRIPTION = "The body cohort VariantStatsDeleteParams web service parameter";
    public static final String VARIANT_STATS_DELETE_PARAMS_FORCE_DESCRIPTION = "The body force VariantStatsDeleteParams web service parameter";


    public static final String VARIANT_FAMILY_INDEX_PARAMS_FAMILY_DESCRIPTION = "The body family VariantFamilyIndexParams web service parameter";
    public static final String VARIANT_FAMILY_INDEX_PARAMS_OVERWRITE_DESCRIPTION = "The body overwrite VariantFamilyIndexParams web service parameter";
    public static final String VARIANT_FAMILY_INDEX_PARAMS_UPDATE_INDEX_DESCRIPTION = "The body updateIndex VariantFamilyIndexParams web service parameter";
    public static final String VARIANT_FAMILY_INDEX_PARAMS_SKIP_INCOMPLETE_FAMILIES_DESCRIPTION = "The body skipIncompleteFamilies VariantFamilyIndexParams web service parameter";


    public static final String VARIANT_STATS_INDEX_PARAMS_COHORT_DESCRIPTION = "The body cohort VariantStatsIndexParams web service parameter";
    public static final String VARIANT_STATS_INDEX_PARAMS_REGION_DESCRIPTION = "The body region VariantStatsIndexParams web service parameter";
    public static final String VARIANT_STATS_INDEX_PARAMS_OVERWRITE_STATS_DESCRIPTION = "The body overwriteStats VariantStatsIndexParams web service parameter";
    public static final String VARIANT_STATS_INDEX_PARAMS_RESUME_DESCRIPTION = "The body resume VariantStatsIndexParams web service parameter";
    public static final String VARIANT_STATS_INDEX_PARAMS_AGGREGATED_DESCRIPTION = "The body aggregated VariantStatsIndexParams web service parameter";
    public static final String VARIANT_STATS_INDEX_PARAMS_AGGREGATION_MAPPING_FILE_DESCRIPTION = "The body aggregationMappingFile VariantStatsIndexParams web service parameter";






    public static final String PANEL_ACL_UPDATE_PARAMS_PANEL_DESCRIPTION = "The body panel PanelAclUpdateParams web service parameter";


    public static final String PANEL_REFERENCE_PARAM_ID_DESCRIPTION = "The body id PanelReferenceParam web service parameter";


    public static final String PANEL_UPDATE_PARAMS_ID_DESCRIPTION = "The body id PanelUpdateParams web service parameter";
    public static final String PANEL_UPDATE_PARAMS_NAME_DESCRIPTION = "The body name PanelUpdateParams web service parameter";
    public static final String PANEL_UPDATE_PARAMS_AUTHOR_DESCRIPTION = "The body author PanelUpdateParams web service parameter";
    public static final String PANEL_UPDATE_PARAMS_SOURCE_DESCRIPTION = "The body source PanelUpdateParams web service parameter";
    public static final String PANEL_UPDATE_PARAMS_CATEGORIES_DESCRIPTION = "The body categories PanelUpdateParams web service parameter";
    public static final String PANEL_UPDATE_PARAMS_TAGS_DESCRIPTION = "The body tags PanelUpdateParams web service parameter";
    public static final String PANEL_UPDATE_PARAMS_DISORDERS_DESCRIPTION = "The body disorders PanelUpdateParams web service parameter";
    public static final String PANEL_UPDATE_PARAMS_VARIANTS_DESCRIPTION = "The body variants PanelUpdateParams web service parameter";
    public static final String PANEL_UPDATE_PARAMS_GENES_DESCRIPTION = "The body genes PanelUpdateParams web service parameter";
    public static final String PANEL_UPDATE_PARAMS_REGIONS_DESCRIPTION = "The body regions PanelUpdateParams web service parameter";
    public static final String PANEL_UPDATE_PARAMS_STRS_DESCRIPTION = "The body strs PanelUpdateParams web service parameter";
    public static final String PANEL_UPDATE_PARAMS_STATS_DESCRIPTION = "The body stats PanelUpdateParams web service parameter";




    public static final String PANEL_CREATE_PARAMS_ID_DESCRIPTION = "The body id PanelCreateParams web service parameter";
    public static final String PANEL_CREATE_PARAMS_NAME_DESCRIPTION = "The body name PanelCreateParams web service parameter";
    public static final String PANEL_CREATE_PARAMS_AUTHOR_DESCRIPTION = "The body author PanelCreateParams web service parameter";
    public static final String PANEL_CREATE_PARAMS_SOURCE_DESCRIPTION = "The body source PanelCreateParams web service parameter";
    public static final String PANEL_CREATE_PARAMS_CATEGORIES_DESCRIPTION = "The body categories PanelCreateParams web service parameter";
    public static final String PANEL_CREATE_PARAMS_TAGS_DESCRIPTION = "The body tags PanelCreateParams web service parameter";
    public static final String PANEL_CREATE_PARAMS_DISORDERS_DESCRIPTION = "The body disorders PanelCreateParams web service parameter";
    public static final String PANEL_CREATE_PARAMS_VARIANTS_DESCRIPTION = "The body variants PanelCreateParams web service parameter";
    public static final String PANEL_CREATE_PARAMS_GENES_DESCRIPTION = "The body genes PanelCreateParams web service parameter";
    public static final String PANEL_CREATE_PARAMS_REGIONS_DESCRIPTION = "The body regions PanelCreateParams web service parameter";
    public static final String PANEL_CREATE_PARAMS_STRS_DESCRIPTION = "The body strs PanelCreateParams web service parameter";
    public static final String PANEL_CREATE_PARAMS_STATS_DESCRIPTION = "The body stats PanelCreateParams web service parameter";








    public static final String AUDIT_RECORD_ID_DESCRIPTION = "The body id AuditRecord web service parameter";
    public static final String AUDIT_RECORD_OPERATION_ID_DESCRIPTION = "The body operationId AuditRecord web service parameter";
    public static final String AUDIT_RECORD_USER_ID_DESCRIPTION = "The body userId AuditRecord web service parameter";
    public static final String AUDIT_RECORD_API_VERSION_DESCRIPTION = "The body apiVersion AuditRecord web service parameter";
    public static final String AUDIT_RECORD_ACTION_DESCRIPTION = "The body action AuditRecord web service parameter";
    public static final String AUDIT_RECORD_RESOURCE_DESCRIPTION = "The body resource AuditRecord web service parameter";
    public static final String AUDIT_RECORD_RESOURCE_ID_DESCRIPTION = "The body resourceId AuditRecord web service parameter";
    public static final String AUDIT_RECORD_RESOURCE_UUID_DESCRIPTION = "The body resourceUuid AuditRecord web service parameter";
    public static final String AUDIT_RECORD_STUDY_ID_DESCRIPTION = "The body studyId AuditRecord web service parameter";
    public static final String AUDIT_RECORD_STUDY_UUID_DESCRIPTION = "The body studyUuid AuditRecord web service parameter";
    public static final String AUDIT_RECORD_PARAMS_DESCRIPTION = "The body params AuditRecord web service parameter";
    public static final String AUDIT_RECORD_DATE_DESCRIPTION = "The body date AuditRecord web service parameter";


    public static final String VARIABLE_SET_SUMMARY_ID_DESCRIPTION = "The body id VariableSetSummary web service parameter";
    public static final String VARIABLE_SET_SUMMARY_NAME_DESCRIPTION = "The body name VariableSetSummary web service parameter";
    public static final String VARIABLE_SET_SUMMARY_SAMPLES_DESCRIPTION = "The body samples VariableSetSummary web service parameter";
    public static final String VARIABLE_SET_SUMMARY_INDIVIDUALS_DESCRIPTION = "The body individuals VariableSetSummary web service parameter";
    public static final String VARIABLE_SET_SUMMARY_COHORTS_DESCRIPTION = "The body cohorts VariableSetSummary web service parameter";
    public static final String VARIABLE_SET_SUMMARY_FAMILIES_DESCRIPTION = "The body families VariableSetSummary web service parameter";


    public static final String STUDY_SUMMARY_NAME_DESCRIPTION = "The body name StudySummary web service parameter";
    public static final String STUDY_SUMMARY_ALIAS_DESCRIPTION = "The body alias StudySummary web service parameter";
    public static final String STUDY_SUMMARY_CREATOR_ID_DESCRIPTION = "The body creatorId StudySummary web service parameter";
    public static final String STUDY_SUMMARY_INTERNAL_DESCRIPTION = "The body internal StudySummary web service parameter";
    public static final String STUDY_SUMMARY_SIZE_DESCRIPTION = "The body size StudySummary web service parameter";
    public static final String STUDY_SUMMARY_CIPHER_DESCRIPTION = "The body cipher StudySummary web service parameter";
    public static final String STUDY_SUMMARY_GROUPS_DESCRIPTION = "The body groups StudySummary web service parameter";
    public static final String STUDY_SUMMARY_EXPERIMENTS_DESCRIPTION = "The body experiments StudySummary web service parameter";
    public static final String STUDY_SUMMARY_FILES_DESCRIPTION = "The body files StudySummary web service parameter";
    public static final String STUDY_SUMMARY_JOBS_DESCRIPTION = "The body jobs StudySummary web service parameter";
    public static final String STUDY_SUMMARY_INDIVIDUALS_DESCRIPTION = "The body individuals StudySummary web service parameter";
    public static final String STUDY_SUMMARY_SAMPLES_DESCRIPTION = "The body samples StudySummary web service parameter";
    public static final String STUDY_SUMMARY_COHORTS_DESCRIPTION = "The body cohorts StudySummary web service parameter";
    public static final String STUDY_SUMMARY_VARIABLE_SETS_DESCRIPTION = "The body variableSets StudySummary web service parameter";


    public static final String VARIABLE_SUMMARY_NAME_DESCRIPTION = "The body name VariableSummary web service parameter";
    public static final String VARIABLE_SUMMARY_ANNOTATIONS_DESCRIPTION = "The body annotations VariableSummary web service parameter";


    public static final String FEATURE_COUNT_NAME_DESCRIPTION = "The body name FeatureCount web service parameter";
    public static final String FEATURE_COUNT_COUNT_DESCRIPTION = "The body count FeatureCount web service parameter";




    public static final String FILE_INTERNAL_VARIANT_INDEX_DESCRIPTION = "The body index FileInternalVariant web service parameter";
    public static final String FILE_INTERNAL_VARIANT_ANNOTATION_INDEX_DESCRIPTION = "The body annotationIndex FileInternalVariant web service parameter";
    public static final String FILE_INTERNAL_VARIANT_SECONDARY_INDEX_DESCRIPTION = "The body secondaryIndex FileInternalVariant web service parameter";


    public static final String FILE_LINK_INTERNAL_PARAMS_SAMPLE_MAP_DESCRIPTION = "The body sampleMap FileLinkInternalParams web service parameter";


    public static final String SMALL_FILE_INTERNAL_MISSING_SAMPLES_DESCRIPTION = "The body missingSamples SmallFileInternal web service parameter";


    public static final String FILE_REFERENCE_PARAM_ID_DESCRIPTION = "The body id FileReferenceParam web service parameter";


    public static final String FILE_INTERNAL_ALIGNMENT_INDEX_FILE_ID_DESCRIPTION = "The body fileId FileInternalAlignmentIndex web service parameter";
    public static final String FILE_INTERNAL_ALIGNMENT_INDEX_INDEXER_DESCRIPTION = "The body indexer FileInternalAlignmentIndex web service parameter";


    public static final String FILE_INTERNAL_VARIANT_INDEX_RELEASE_DESCRIPTION = "The body release FileInternalVariantIndex web service parameter";
    public static final String FILE_INTERNAL_VARIANT_INDEX_TRANSFORM_DESCRIPTION = "The body transform FileInternalVariantIndex web service parameter";




    public static final String FILE_RELATED_FILE_FILE_DESCRIPTION = "The body file FileRelatedFile web service parameter";
    public static final String FILE_RELATED_FILE_RELATION_DESCRIPTION = "The body relation FileRelatedFile web service parameter";


    public static final String POST_LINK_TOOL_PARAMS_FILES_DESCRIPTION = "The body files PostLinkToolParams web service parameter";
    public static final String POST_LINK_TOOL_PARAMS_BATCH_SIZE_DESCRIPTION = "The body batchSize PostLinkToolParams web service parameter";






    public static final String SMALL_RELATED_FILE_PARAMS_FILE_DESCRIPTION = "The body file SmallRelatedFileParams web service parameter";
    public static final String SMALL_RELATED_FILE_PARAMS_RELATION_DESCRIPTION = "The body relation SmallRelatedFileParams web service parameter";


    public static final String FILE_FETCH_URL_DESCRIPTION = "The body url FileFetch web service parameter";
    public static final String FILE_FETCH_PATH_DESCRIPTION = "The body path FileFetch web service parameter";


    public static final String FILE_INTERNAL_ALIGNMENT_INDEX_DESCRIPTION = "The body index FileInternalAlignment web service parameter";




    public static final String FILE_CREATE_PARAMS_CONTENT_DESCRIPTION = "The body content FileCreateParams web service parameter";
    public static final String FILE_CREATE_PARAMS_PATH_DESCRIPTION = "The body path FileCreateParams web service parameter";
    public static final String FILE_CREATE_PARAMS_TYPE_DESCRIPTION = "The body type FileCreateParams web service parameter";
    public static final String FILE_CREATE_PARAMS_FORMAT_DESCRIPTION = "The body format FileCreateParams web service parameter";
    public static final String FILE_CREATE_PARAMS_BIOFORMAT_DESCRIPTION = "The body bioformat FileCreateParams web service parameter";
    public static final String FILE_CREATE_PARAMS_SAMPLE_IDS_DESCRIPTION = "The body sampleIds FileCreateParams web service parameter";
    public static final String FILE_CREATE_PARAMS_SOFTWARE_DESCRIPTION = "The body software FileCreateParams web service parameter";
    public static final String FILE_CREATE_PARAMS_TAGS_DESCRIPTION = "The body tags FileCreateParams web service parameter";
    public static final String FILE_CREATE_PARAMS_JOB_ID_DESCRIPTION = "The body jobId FileCreateParams web service parameter";




    public static final String FILE_EXPERIMENT_TECHNOLOGY_DESCRIPTION = "The body technology FileExperiment web service parameter";
    public static final String FILE_EXPERIMENT_METHOD_DESCRIPTION = "The body method FileExperiment web service parameter";
    public static final String FILE_EXPERIMENT_NUCLEIC_ACID_TYPE_DESCRIPTION = "The body nucleicAcidType FileExperiment web service parameter";
    public static final String FILE_EXPERIMENT_MANUFACTURER_DESCRIPTION = "The body manufacturer FileExperiment web service parameter";
    public static final String FILE_EXPERIMENT_PLATFORM_DESCRIPTION = "The body platform FileExperiment web service parameter";
    public static final String FILE_EXPERIMENT_LIBRARY_DESCRIPTION = "The body library FileExperiment web service parameter";
    public static final String FILE_EXPERIMENT_DATE_DESCRIPTION = "The body date FileExperiment web service parameter";
    public static final String FILE_EXPERIMENT_CENTER_DESCRIPTION = "The body center FileExperiment web service parameter";
    public static final String FILE_EXPERIMENT_LAB_DESCRIPTION = "The body lab FileExperiment web service parameter";
    public static final String FILE_EXPERIMENT_RESPONSIBLE_DESCRIPTION = "The body responsible FileExperiment web service parameter";


    public static final String FILE_TREE_BUILDER_FILE_DESCRIPTION = "The body file FileTreeBuilder web service parameter";
    public static final String FILE_TREE_BUILDER_FILE_TREE_MAP_DESCRIPTION = "The body fileTreeMap FileTreeBuilder web service parameter";


    public static final String FILE_CONTENT_FILE_ID_DESCRIPTION = "The body fileId FileContent web service parameter";
    public static final String FILE_CONTENT_EOF_DESCRIPTION = "The body eof FileContent web service parameter";
    public static final String FILE_CONTENT_OFFSET_DESCRIPTION = "The body offset FileContent web service parameter";
    public static final String FILE_CONTENT_SIZE_DESCRIPTION = "The body size FileContent web service parameter";
    public static final String FILE_CONTENT_LINES_DESCRIPTION = "The body lines FileContent web service parameter";
    public static final String FILE_CONTENT_CONTENT_DESCRIPTION = "The body content FileContent web service parameter";




    public static final String FILE_UPDATE_PARAMS_NAME_DESCRIPTION = "The body name FileUpdateParams web service parameter";
    public static final String FILE_UPDATE_PARAMS_SAMPLE_IDS_DESCRIPTION = "The body sampleIds FileUpdateParams web service parameter";
    public static final String FILE_UPDATE_PARAMS_CHECKSUM_DESCRIPTION = "The body checksum FileUpdateParams web service parameter";
    public static final String FILE_UPDATE_PARAMS_FORMAT_DESCRIPTION = "The body format FileUpdateParams web service parameter";
    public static final String FILE_UPDATE_PARAMS_BIOFORMAT_DESCRIPTION = "The body bioformat FileUpdateParams web service parameter";
    public static final String FILE_UPDATE_PARAMS_SOFTWARE_DESCRIPTION = "The body software FileUpdateParams web service parameter";
    public static final String FILE_UPDATE_PARAMS_EXPERIMENT_DESCRIPTION = "The body experiment FileUpdateParams web service parameter";
    public static final String FILE_UPDATE_PARAMS_TAGS_DESCRIPTION = "The body tags FileUpdateParams web service parameter";
    public static final String FILE_UPDATE_PARAMS_INTERNAL_DESCRIPTION = "The body internal FileUpdateParams web service parameter";
    public static final String FILE_UPDATE_PARAMS_RELATED_FILES_DESCRIPTION = "The body relatedFiles FileUpdateParams web service parameter";
    public static final String FILE_UPDATE_PARAMS_SIZE_DESCRIPTION = "The body size FileUpdateParams web service parameter";
    public static final String FILE_UPDATE_PARAMS_ANNOTATION_SETS_DESCRIPTION = "The body annotationSets FileUpdateParams web service parameter";
    public static final String FILE_UPDATE_PARAMS_QUALITY_CONTROL_DESCRIPTION = "The body qualityControl FileUpdateParams web service parameter";
    public static final String FILE_UPDATE_PARAMS_STATS_DESCRIPTION = "The body stats FileUpdateParams web service parameter";


    public static final String FILE_ACL_UPDATE_PARAMS_FILE_DESCRIPTION = "The body file FileAclUpdateParams web service parameter";
    public static final String FILE_ACL_UPDATE_PARAMS_SAMPLE_DESCRIPTION = "The body sample FileAclUpdateParams web service parameter";




    public static final String FILE_LINK_PARAMS_URI_DESCRIPTION = "The body uri FileLinkParams web service parameter";
    public static final String FILE_LINK_PARAMS_PATH_DESCRIPTION = "The body path FileLinkParams web service parameter";
    public static final String FILE_LINK_PARAMS_RELATED_FILES_DESCRIPTION = "The body relatedFiles FileLinkParams web service parameter";
    public static final String FILE_LINK_PARAMS_INTERNAL_DESCRIPTION = "The body internal FileLinkParams web service parameter";


    public static final String FILE_QUALITY_CONTROL_VARIANT_DESCRIPTION = "The body variant FileQualityControl web service parameter";
    public static final String FILE_QUALITY_CONTROL_ALIGNMENT_DESCRIPTION = "The body alignment FileQualityControl web service parameter";
    public static final String FILE_QUALITY_CONTROL_COVERAGE_DESCRIPTION = "The body coverage FileQualityControl web service parameter";
    public static final String FILE_QUALITY_CONTROL_COMMENTS_DESCRIPTION = "The body comments FileQualityControl web service parameter";
    public static final String FILE_QUALITY_CONTROL_FILES_DESCRIPTION = "The body files FileQualityControl web service parameter";




    public static final String FILE_LINK_TOOL_PARAMS_URI_DESCRIPTION = "The body uri FileLinkToolParams web service parameter";
    public static final String FILE_LINK_TOOL_PARAMS_PATH_DESCRIPTION = "The body path FileLinkToolParams web service parameter";
    public static final String FILE_LINK_TOOL_PARAMS_PARENTS_DESCRIPTION = "The body parents FileLinkToolParams web service parameter";


    public static final String FILE_TREE_FILE_DESCRIPTION = "The body file FileTree web service parameter";
    public static final String FILE_TREE_CHILDREN_DESCRIPTION = "The body children FileTree web service parameter";


    public static final String FILE_ACL_PARAMS_SAMPLE_DESCRIPTION = "The body sample FileAclParams web service parameter";


    public static final String FILE_CREATE_PARAMS_OLD_PATH_DESCRIPTION = "The body path FileCreateParamsOld web service parameter";
    public static final String FILE_CREATE_PARAMS_OLD_CONTENT_DESCRIPTION = "The body content FileCreateParamsOld web service parameter";
    public static final String FILE_CREATE_PARAMS_OLD_PARENTS_DESCRIPTION = "The body parents FileCreateParamsOld web service parameter";
    public static final String FILE_CREATE_PARAMS_OLD_DIRECTORY_DESCRIPTION = "The body directory FileCreateParamsOld web service parameter";


    public static final String FILE_LIBRARY_EXPERIMENT_PREPARATION_KIT_DESCRIPTION = "The body preparationKit FileLibraryExperiment web service parameter";
    public static final String FILE_LIBRARY_EXPERIMENT_PREPARATION_KIT_MANUFACTURER_DESCRIPTION = "The body preparationKitManufacturer FileLibraryExperiment web service parameter";
    public static final String FILE_LIBRARY_EXPERIMENT_CAPTURE_MANUFACTURER_DESCRIPTION = "The body captureManufacturer FileLibraryExperiment web service parameter";
    public static final String FILE_LIBRARY_EXPERIMENT_CAPTURE_KIT_DESCRIPTION = "The body captureKit FileLibraryExperiment web service parameter";
    public static final String FILE_LIBRARY_EXPERIMENT_CAPTURE_VERSION_DESCRIPTION = "The body captureVersion FileLibraryExperiment web service parameter";
    public static final String FILE_LIBRARY_EXPERIMENT_TARGETED_REGION_DESCRIPTION = "The body targetedRegion FileLibraryExperiment web service parameter";




    public static final String STATUS_PARAMS_ID_DESCRIPTION = "The body id StatusParams web service parameter";
    public static final String STATUS_PARAMS_NAME_DESCRIPTION = "The body name StatusParams web service parameter";




    public static final String INTERNAL_STATUS_VERSION_DESCRIPTION = "The body version InternalStatus web service parameter";
    public static final String INTERNAL_STATUS_COMMIT_DESCRIPTION = "The body commit InternalStatus web service parameter";


    public static final String ANNOTABLE_ID_DESCRIPTION = "The body id Annotable web service parameter";




    public static final String ANNOTATION_SET_ID_DESCRIPTION = "The body id AnnotationSet web service parameter";
    public static final String ANNOTATION_SET_NAME_DESCRIPTION = "The body name AnnotationSet web service parameter";
    public static final String ANNOTATION_SET_VARIABLE_SET_ID_DESCRIPTION = "The body variableSetId AnnotationSet web service parameter";
    public static final String ANNOTATION_SET_ANNOTATIONS_DESCRIPTION = "The body annotations AnnotationSet web service parameter";
    public static final String ANNOTATION_SET_RELEASE_DESCRIPTION = "The body release AnnotationSet web service parameter";


    public static final String FLAG_VALUE_ID_DESCRIPTION = "The body id FlagValue web service parameter";








    public static final String TSV_ANNOTATION_PARAMS_CONTENT_DESCRIPTION = "The body content TsvAnnotationParams web service parameter";




    public static final String ENTRY_PARAM_ID_DESCRIPTION = "The body id EntryParam web service parameter";




    public static final String STATUS_PARAM_ID_DESCRIPTION = "The body id StatusParam web service parameter";




    public static final String CUSTOM_GROUP_ID_DESCRIPTION = "The body id CustomGroup web service parameter";
    public static final String CUSTOM_GROUP_USERS_DESCRIPTION = "The body users CustomGroup web service parameter";
    public static final String CUSTOM_GROUP_SYNCED_FROM_DESCRIPTION = "The body syncedFrom CustomGroup web service parameter";






    public static final String STUDY_VARIANT_ENGINE_CONFIGURATION_OPTIONS_DESCRIPTION = "The body options StudyVariantEngineConfiguration web service parameter";
    public static final String STUDY_VARIANT_ENGINE_CONFIGURATION_SAMPLE_INDEX_DESCRIPTION = "The body sampleIndex StudyVariantEngineConfiguration web service parameter";


    public static final String GROUP_ID_DESCRIPTION = "The body id Group web service parameter";
    public static final String GROUP_USER_IDS_DESCRIPTION = "The body userIds Group web service parameter";
    public static final String GROUP_SYNCED_FROM_DESCRIPTION = "The body syncedFrom Group web service parameter";


    public static final String TEMPLATE_PARAMS_ID_DESCRIPTION = "The body id TemplateParams web service parameter";
    public static final String TEMPLATE_PARAMS_OVERWRITE_DESCRIPTION = "The body overwrite TemplateParams web service parameter";
    public static final String TEMPLATE_PARAMS_RESUME_DESCRIPTION = "The body resume TemplateParams web service parameter";




    public static final String STUDY_UPDATE_PARAMS_NAME_DESCRIPTION = "The body name StudyUpdateParams web service parameter";
    public static final String STUDY_UPDATE_PARAMS_ALIAS_DESCRIPTION = "The body alias StudyUpdateParams web service parameter";
    public static final String STUDY_UPDATE_PARAMS_TYPE_DESCRIPTION = "The body type StudyUpdateParams web service parameter";
    public static final String STUDY_UPDATE_PARAMS_SOURCES_DESCRIPTION = "The body sources StudyUpdateParams web service parameter";
    public static final String STUDY_UPDATE_PARAMS_NOTIFICATION_DESCRIPTION = "The body notification StudyUpdateParams web service parameter";
    public static final String STUDY_UPDATE_PARAMS_ADDITIONAL_INFO_DESCRIPTION = "The body additionalInfo StudyUpdateParams web service parameter";


    public static final String STUDY_ACL_UPDATE_PARAMS_STUDY_DESCRIPTION = "The body study StudyAclUpdateParams web service parameter";
    public static final String STUDY_ACL_UPDATE_PARAMS_TEMPLATE_DESCRIPTION = "The body template StudyAclUpdateParams web service parameter";


    public static final String CLINICAL_CONSENT_ANNOTATION_PARAM_CONSENTS_DESCRIPTION = "The body consents ClinicalConsentAnnotationParam web service parameter";


    public static final String INTERPRETATION_VARIANT_CALLER_CONFIGURATION_ID_DESCRIPTION = "The body id InterpretationVariantCallerConfiguration web service parameter";
    public static final String INTERPRETATION_VARIANT_CALLER_CONFIGURATION_SOMATIC_DESCRIPTION = "The body somatic InterpretationVariantCallerConfiguration web service parameter";
    public static final String INTERPRETATION_VARIANT_CALLER_CONFIGURATION_TYPES_DESCRIPTION = "The body types InterpretationVariantCallerConfiguration web service parameter";
    public static final String INTERPRETATION_VARIANT_CALLER_CONFIGURATION_COLUMNS_DESCRIPTION = "The body columns InterpretationVariantCallerConfiguration web service parameter";
    public static final String INTERPRETATION_VARIANT_CALLER_CONFIGURATION_DATA_FILTERS_DESCRIPTION = "The body dataFilters InterpretationVariantCallerConfiguration web service parameter";






    public static final String CLINICAL_CONSENT_ID_DESCRIPTION = "The body id ClinicalConsent web service parameter";
    public static final String CLINICAL_CONSENT_NAME_DESCRIPTION = "The body name ClinicalConsent web service parameter";


    public static final String CLINICAL_PRIORITY_VALUE_ID_DESCRIPTION = "The body id ClinicalPriorityValue web service parameter";
    public static final String CLINICAL_PRIORITY_VALUE_RANK_DESCRIPTION = "The body rank ClinicalPriorityValue web service parameter";
    public static final String CLINICAL_PRIORITY_VALUE_DEFAULT_PRIORITY_DESCRIPTION = "The body defaultPriority ClinicalPriorityValue web service parameter";


    public static final String STUDY_CONFIGURATION_CLINICAL_DESCRIPTION = "The body clinical StudyConfiguration web service parameter";
    public static final String STUDY_CONFIGURATION_VARIANT_ENGINE_DESCRIPTION = "The body variantEngine StudyConfiguration web service parameter";


    public static final String INTERPRETATION_STUDY_CONFIGURATION_VARIANT_CALLERS_DESCRIPTION = "The body variantCallers InterpretationStudyConfiguration web service parameter";
    public static final String INTERPRETATION_STUDY_CONFIGURATION_DEFAULT_FILTER_DESCRIPTION = "The body defaultFilter InterpretationStudyConfiguration web service parameter";


    public static final String CLINICAL_ANALYSIS_STUDY_CONFIGURATION_INTERPRETATION_DESCRIPTION = "The body interpretation ClinicalAnalysisStudyConfiguration web service parameter";
    public static final String CLINICAL_ANALYSIS_STUDY_CONFIGURATION_PRIORITIES_DESCRIPTION = "The body priorities ClinicalAnalysisStudyConfiguration web service parameter";
    public static final String CLINICAL_ANALYSIS_STUDY_CONFIGURATION_FLAGS_DESCRIPTION = "The body flags ClinicalAnalysisStudyConfiguration web service parameter";
    public static final String CLINICAL_ANALYSIS_STUDY_CONFIGURATION_CONSENT_DESCRIPTION = "The body consent ClinicalAnalysisStudyConfiguration web service parameter";


    public static final String CLINICAL_CONSENT_CONFIGURATION_CONSENTS_DESCRIPTION = "The body consents ClinicalConsentConfiguration web service parameter";






    public static final String STUDY_CREATE_PARAMS_ID_DESCRIPTION = "The body id StudyCreateParams web service parameter";
    public static final String STUDY_CREATE_PARAMS_NAME_DESCRIPTION = "The body name StudyCreateParams web service parameter";
    public static final String STUDY_CREATE_PARAMS_ALIAS_DESCRIPTION = "The body alias StudyCreateParams web service parameter";
    public static final String STUDY_CREATE_PARAMS_TYPE_DESCRIPTION = "The body type StudyCreateParams web service parameter";
    public static final String STUDY_CREATE_PARAMS_SOURCES_DESCRIPTION = "The body sources StudyCreateParams web service parameter";
    public static final String STUDY_CREATE_PARAMS_NOTIFICATION_DESCRIPTION = "The body notification StudyCreateParams web service parameter";
    public static final String STUDY_CREATE_PARAMS_ADDITIONAL_INFO_DESCRIPTION = "The body additionalInfo StudyCreateParams web service parameter";




    public static final String VARIABLE_SET_CREATE_PARAMS_ID_DESCRIPTION = "The body id VariableSetCreateParams web service parameter";
    public static final String VARIABLE_SET_CREATE_PARAMS_NAME_DESCRIPTION = "The body name VariableSetCreateParams web service parameter";
    public static final String VARIABLE_SET_CREATE_PARAMS_UNIQUE_DESCRIPTION = "The body unique VariableSetCreateParams web service parameter";
    public static final String VARIABLE_SET_CREATE_PARAMS_CONFIDENTIAL_DESCRIPTION = "The body confidential VariableSetCreateParams web service parameter";
    public static final String VARIABLE_SET_CREATE_PARAMS_ENTITIES_DESCRIPTION = "The body entities VariableSetCreateParams web service parameter";
    public static final String VARIABLE_SET_CREATE_PARAMS_VARIABLES_DESCRIPTION = "The body variables VariableSetCreateParams web service parameter";


    public static final String STUDY_ACL_PARAMS_TEMPLATE_DESCRIPTION = "The body template StudyAclParams web service parameter";


    public static final String VARIABLE_SET_ID_DESCRIPTION = "The body id VariableSet web service parameter";
    public static final String VARIABLE_SET_NAME_DESCRIPTION = "The body name VariableSet web service parameter";
    public static final String VARIABLE_SET_UNIQUE_DESCRIPTION = "The body unique VariableSet web service parameter";
    public static final String VARIABLE_SET_CONFIDENTIAL_DESCRIPTION = "The body confidential VariableSet web service parameter";
    public static final String VARIABLE_SET_INTERNAL_DESCRIPTION = "The body internal VariableSet web service parameter";
    public static final String VARIABLE_SET_VARIABLES_DESCRIPTION = "The body variables VariableSet web service parameter";
    public static final String VARIABLE_SET_ENTITIES_DESCRIPTION = "The body entities VariableSet web service parameter";
    public static final String VARIABLE_SET_RELEASE_DESCRIPTION = "The body release VariableSet web service parameter";






    public static final String GROUP_CREATE_PARAMS_ID_DESCRIPTION = "The body id GroupCreateParams web service parameter";
    public static final String GROUP_CREATE_PARAMS_USERS_DESCRIPTION = "The body users GroupCreateParams web service parameter";




    public static final String GROUP_UPDATE_PARAMS_USERS_DESCRIPTION = "The body users GroupUpdateParams web service parameter";


    public static final String VARIABLE_ID_DESCRIPTION = "The body id Variable web service parameter";
    public static final String VARIABLE_NAME_DESCRIPTION = "The body name Variable web service parameter";
    public static final String VARIABLE_CATEGORY_DESCRIPTION = "The body category Variable web service parameter";
    public static final String VARIABLE_TYPE_DESCRIPTION = "The body type Variable web service parameter";
    public static final String VARIABLE_DEFAULT_VALUE_DESCRIPTION = "The body defaultValue Variable web service parameter";
    public static final String VARIABLE_REQUIRED_DESCRIPTION = "The body required Variable web service parameter";
    public static final String VARIABLE_MULTI_VALUE_DESCRIPTION = "The body multiValue Variable web service parameter";
    public static final String VARIABLE_ALLOWED_VALUES_DESCRIPTION = "The body allowedValues Variable web service parameter";
    public static final String VARIABLE_ALLOWED_KEYS_DESCRIPTION = "The body allowedKeys Variable web service parameter";
    public static final String VARIABLE_RANK_DESCRIPTION = "The body rank Variable web service parameter";
    public static final String VARIABLE_DEPENDS_ON_DESCRIPTION = "The body dependsOn Variable web service parameter";
    public static final String VARIABLE_VARIABLE_SET_DESCRIPTION = "The body variableSet Variable web service parameter";
    public static final String VARIABLE_VARIABLES_DESCRIPTION = "The body variables Variable web service parameter";


    public static final String STUDY_INDEX_RECESSIVE_GENE_DESCRIPTION = "The body recessiveGene StudyIndex web service parameter";




    public static final String PROJECT_UPDATE_PARAMS_NAME_DESCRIPTION = "The body name ProjectUpdateParams web service parameter";
    public static final String PROJECT_UPDATE_PARAMS_ORGANISM_DESCRIPTION = "The body organism ProjectUpdateParams web service parameter";




    public static final String DATASTORES_VARIANT_DESCRIPTION = "The body variant Datastores web service parameter";


    public static final String PROJECT_CREATE_PARAMS_ID_DESCRIPTION = "The body id ProjectCreateParams web service parameter";
    public static final String PROJECT_CREATE_PARAMS_NAME_DESCRIPTION = "The body name ProjectCreateParams web service parameter";
    public static final String PROJECT_CREATE_PARAMS_ORGANISM_DESCRIPTION = "The body organism ProjectCreateParams web service parameter";
    public static final String PROJECT_CREATE_PARAMS_CELLBASE_DESCRIPTION = "The body cellbase ProjectCreateParams web service parameter";


    public static final String DATA_STORE_STORAGE_ENGINE_DESCRIPTION = "The body storageEngine DataStore web service parameter";
    public static final String DATA_STORE_DB_NAME_DESCRIPTION = "The body dbName DataStore web service parameter";
    public static final String DATA_STORE_OPTIONS_DESCRIPTION = "The body options DataStore web service parameter";


    public static final String PROJECT_ORGANISM_SCIENTIFIC_NAME_DESCRIPTION = "The body scientificName ProjectOrganism web service parameter";
    public static final String PROJECT_ORGANISM_COMMON_NAME_DESCRIPTION = "The body commonName ProjectOrganism web service parameter";
    public static final String PROJECT_ORGANISM_ASSEMBLY_DESCRIPTION = "The body assembly ProjectOrganism web service parameter";


    public static final String SAMPLE_INTERNAL_VARIANT_INDEX_DESCRIPTION = "The body index SampleInternalVariant web service parameter";
    public static final String SAMPLE_INTERNAL_VARIANT_SAMPLE_GENOTYPE_INDEX_DESCRIPTION = "The body sampleGenotypeIndex SampleInternalVariant web service parameter";
    public static final String SAMPLE_INTERNAL_VARIANT_ANNOTATION_INDEX_DESCRIPTION = "The body annotationIndex SampleInternalVariant web service parameter";
    public static final String SAMPLE_INTERNAL_VARIANT_SECONDARY_INDEX_DESCRIPTION = "The body secondaryIndex SampleInternalVariant web service parameter";




    public static final String SAMPLE_INTERNAL_VARIANT_INDEX_NUM_FILES_DESCRIPTION = "The body numFiles SampleInternalVariantIndex web service parameter";
    public static final String SAMPLE_INTERNAL_VARIANT_INDEX_MULTI_FILE_DESCRIPTION = "The body multiFile SampleInternalVariantIndex web service parameter";


    public static final String SAMPLE_ALIGNMENT_QUALITY_CONTROL_METRICS_BAM_FILE_ID_DESCRIPTION = "The body bamFileId SampleAlignmentQualityControlMetrics web service parameter";
    public static final String SAMPLE_ALIGNMENT_QUALITY_CONTROL_METRICS_FAST_QC_METRICS_DESCRIPTION = "The body fastQcMetrics SampleAlignmentQualityControlMetrics web service parameter";
    public static final String SAMPLE_ALIGNMENT_QUALITY_CONTROL_METRICS_SAMTOOLS_FLAGSTATS_DESCRIPTION = "The body samtoolsFlagstats SampleAlignmentQualityControlMetrics web service parameter";
    public static final String SAMPLE_ALIGNMENT_QUALITY_CONTROL_METRICS_HS_METRICS_DESCRIPTION = "The body hsMetrics SampleAlignmentQualityControlMetrics web service parameter";
    public static final String SAMPLE_ALIGNMENT_QUALITY_CONTROL_METRICS_GENE_COVERAGE_STATS_DESCRIPTION = "The body geneCoverageStats SampleAlignmentQualityControlMetrics web service parameter";


    public static final String SAMPLE_CREATE_PARAMS_ID_DESCRIPTION = "The body id SampleCreateParams web service parameter";
    public static final String SAMPLE_CREATE_PARAMS_INDIVIDUAL_ID_DESCRIPTION = "The body individualId SampleCreateParams web service parameter";
    public static final String SAMPLE_CREATE_PARAMS_SOURCE_DESCRIPTION = "The body source SampleCreateParams web service parameter";
    public static final String SAMPLE_CREATE_PARAMS_PROCESSING_DESCRIPTION = "The body processing SampleCreateParams web service parameter";
    public static final String SAMPLE_CREATE_PARAMS_COLLECTION_DESCRIPTION = "The body collection SampleCreateParams web service parameter";
    public static final String SAMPLE_CREATE_PARAMS_SOMATIC_DESCRIPTION = "The body somatic SampleCreateParams web service parameter";
    public static final String SAMPLE_CREATE_PARAMS_PHENOTYPES_DESCRIPTION = "The body phenotypes SampleCreateParams web service parameter";
    public static final String SAMPLE_CREATE_PARAMS_ANNOTATION_SETS_DESCRIPTION = "The body annotationSets SampleCreateParams web service parameter";








    public static final String SAMPLE_REFERENCE_PARAM_ID_DESCRIPTION = "The body id SampleReferenceParam web service parameter";
    public static final String SAMPLE_REFERENCE_PARAM_UUID_DESCRIPTION = "The body uuid SampleReferenceParam web service parameter";




    public static final String SAMPLE_ACL_PARAMS_INDIVIDUAL_DESCRIPTION = "The body individual SampleAclParams web service parameter";
    public static final String SAMPLE_ACL_PARAMS_FAMILY_DESCRIPTION = "The body family SampleAclParams web service parameter";
    public static final String SAMPLE_ACL_PARAMS_FILE_DESCRIPTION = "The body file SampleAclParams web service parameter";
    public static final String SAMPLE_ACL_PARAMS_COHORT_DESCRIPTION = "The body cohort SampleAclParams web service parameter";






    public static final String SAMPLE_UPDATE_PARAMS_ID_DESCRIPTION = "The body id SampleUpdateParams web service parameter";
    public static final String SAMPLE_UPDATE_PARAMS_INDIVIDUAL_ID_DESCRIPTION = "The body individualId SampleUpdateParams web service parameter";
    public static final String SAMPLE_UPDATE_PARAMS_SOURCE_DESCRIPTION = "The body source SampleUpdateParams web service parameter";
    public static final String SAMPLE_UPDATE_PARAMS_PROCESSING_DESCRIPTION = "The body processing SampleUpdateParams web service parameter";
    public static final String SAMPLE_UPDATE_PARAMS_COLLECTION_DESCRIPTION = "The body collection SampleUpdateParams web service parameter";
    public static final String SAMPLE_UPDATE_PARAMS_QUALITY_CONTROL_DESCRIPTION = "The body qualityControl SampleUpdateParams web service parameter";
    public static final String SAMPLE_UPDATE_PARAMS_SOMATIC_DESCRIPTION = "The body somatic SampleUpdateParams web service parameter";
    public static final String SAMPLE_UPDATE_PARAMS_PHENOTYPES_DESCRIPTION = "The body phenotypes SampleUpdateParams web service parameter";
    public static final String SAMPLE_UPDATE_PARAMS_ANNOTATION_SETS_DESCRIPTION = "The body annotationSets SampleUpdateParams web service parameter";


    public static final String SAMPLE_INTERNAL_VARIANT_DESCRIPTION = "The body variant SampleInternal web service parameter";






    public static final String SAMPLE_ACL_UPDATE_PARAMS_SAMPLE_DESCRIPTION = "The body sample SampleAclUpdateParams web service parameter";
    public static final String SAMPLE_ACL_UPDATE_PARAMS_INDIVIDUAL_DESCRIPTION = "The body individual SampleAclUpdateParams web service parameter";
    public static final String SAMPLE_ACL_UPDATE_PARAMS_FAMILY_DESCRIPTION = "The body family SampleAclUpdateParams web service parameter";
    public static final String SAMPLE_ACL_UPDATE_PARAMS_FILE_DESCRIPTION = "The body file SampleAclUpdateParams web service parameter";
    public static final String SAMPLE_ACL_UPDATE_PARAMS_COHORT_DESCRIPTION = "The body cohort SampleAclUpdateParams web service parameter";




    public static final String FAMILY_ACL_PARAMS_FAMILY_DESCRIPTION = "The body family FamilyAclParams web service parameter";
    public static final String FAMILY_ACL_PARAMS_INDIVIDUAL_DESCRIPTION = "The body individual FamilyAclParams web service parameter";
    public static final String FAMILY_ACL_PARAMS_SAMPLE_DESCRIPTION = "The body sample FamilyAclParams web service parameter";
    public static final String FAMILY_ACL_PARAMS_PROPAGATE_DESCRIPTION = "The body propagate FamilyAclParams web service parameter";






    public static final String FAMILY_ACL_UPDATE_PARAMS_FAMILY_DESCRIPTION = "The body family FamilyAclUpdateParams web service parameter";
    public static final String FAMILY_ACL_UPDATE_PARAMS_INDIVIDUAL_DESCRIPTION = "The body individual FamilyAclUpdateParams web service parameter";
    public static final String FAMILY_ACL_UPDATE_PARAMS_SAMPLE_DESCRIPTION = "The body sample FamilyAclUpdateParams web service parameter";




    public static final String FAMILY_UPDATE_PARAMS_ID_DESCRIPTION = "The body id FamilyUpdateParams web service parameter";
    public static final String FAMILY_UPDATE_PARAMS_NAME_DESCRIPTION = "The body name FamilyUpdateParams web service parameter";
    public static final String FAMILY_UPDATE_PARAMS_MEMBERS_DESCRIPTION = "The body members FamilyUpdateParams web service parameter";
    public static final String FAMILY_UPDATE_PARAMS_EXPECTED_SIZE_DESCRIPTION = "The body expectedSize FamilyUpdateParams web service parameter";
    public static final String FAMILY_UPDATE_PARAMS_QUALITY_CONTROL_DESCRIPTION = "The body qualityControl FamilyUpdateParams web service parameter";
    public static final String FAMILY_UPDATE_PARAMS_ANNOTATION_SETS_DESCRIPTION = "The body annotationSets FamilyUpdateParams web service parameter";




    public static final String INDIVIDUAL_CREATE_PARAMS_ID_DESCRIPTION = "The body id IndividualCreateParams web service parameter";
    public static final String INDIVIDUAL_CREATE_PARAMS_NAME_DESCRIPTION = "The body name IndividualCreateParams web service parameter";
    public static final String INDIVIDUAL_CREATE_PARAMS_FATHER_DESCRIPTION = "The body father IndividualCreateParams web service parameter";
    public static final String INDIVIDUAL_CREATE_PARAMS_MOTHER_DESCRIPTION = "The body mother IndividualCreateParams web service parameter";
    public static final String INDIVIDUAL_CREATE_PARAMS_LOCATION_DESCRIPTION = "The body location IndividualCreateParams web service parameter";
    public static final String INDIVIDUAL_CREATE_PARAMS_SEX_DESCRIPTION = "The body sex IndividualCreateParams web service parameter";
    public static final String INDIVIDUAL_CREATE_PARAMS_ETHNICITY_DESCRIPTION = "The body ethnicity IndividualCreateParams web service parameter";
    public static final String INDIVIDUAL_CREATE_PARAMS_PARENTAL_CONSANGUINITY_DESCRIPTION = "The body parentalConsanguinity IndividualCreateParams web service parameter";
    public static final String INDIVIDUAL_CREATE_PARAMS_POPULATION_DESCRIPTION = "The body population IndividualCreateParams web service parameter";
    public static final String INDIVIDUAL_CREATE_PARAMS_DATE_OF_BIRTH_DESCRIPTION = "The body dateOfBirth IndividualCreateParams web service parameter";
    public static final String INDIVIDUAL_CREATE_PARAMS_KARYOTYPIC_SEX_DESCRIPTION = "The body karyotypicSex IndividualCreateParams web service parameter";
    public static final String INDIVIDUAL_CREATE_PARAMS_LIFE_STATUS_DESCRIPTION = "The body lifeStatus IndividualCreateParams web service parameter";
    public static final String INDIVIDUAL_CREATE_PARAMS_ANNOTATION_SETS_DESCRIPTION = "The body annotationSets IndividualCreateParams web service parameter";
    public static final String INDIVIDUAL_CREATE_PARAMS_PHENOTYPES_DESCRIPTION = "The body phenotypes IndividualCreateParams web service parameter";
    public static final String INDIVIDUAL_CREATE_PARAMS_DISORDERS_DESCRIPTION = "The body disorders IndividualCreateParams web service parameter";




    public static final String FAMILY_CREATE_PARAMS_ID_DESCRIPTION = "The body id FamilyCreateParams web service parameter";
    public static final String FAMILY_CREATE_PARAMS_NAME_DESCRIPTION = "The body name FamilyCreateParams web service parameter";
    public static final String FAMILY_CREATE_PARAMS_MEMBERS_DESCRIPTION = "The body members FamilyCreateParams web service parameter";
    public static final String FAMILY_CREATE_PARAMS_EXPECTED_SIZE_DESCRIPTION = "The body expectedSize FamilyCreateParams web service parameter";
    public static final String FAMILY_CREATE_PARAMS_ANNOTATION_SETS_DESCRIPTION = "The body annotationSets FamilyCreateParams web service parameter";




    public static final String GATK_WRAPPER_PARAMS_COMMAND_DESCRIPTION = "The body command GatkWrapperParams web service parameter";
    public static final String GATK_WRAPPER_PARAMS_OUTDIR_DESCRIPTION = "The body outdir GatkWrapperParams web service parameter";
    public static final String GATK_WRAPPER_PARAMS_GATK_PARAMS_DESCRIPTION = "The body gatkParams GatkWrapperParams web service parameter";




    public static final String FAMILY_QC_ANALYSIS_PARAMS_FAMILY_DESCRIPTION = "The body family FamilyQcAnalysisParams web service parameter";
    public static final String FAMILY_QC_ANALYSIS_PARAMS_RELATEDNESS_METHOD_DESCRIPTION = "The body relatednessMethod FamilyQcAnalysisParams web service parameter";
    public static final String FAMILY_QC_ANALYSIS_PARAMS_RELATEDNESS_MAF_DESCRIPTION = "The body relatednessMaf FamilyQcAnalysisParams web service parameter";
    public static final String FAMILY_QC_ANALYSIS_PARAMS_OUTDIR_DESCRIPTION = "The body outdir FamilyQcAnalysisParams web service parameter";


    public static final String VARIANT_FILE_QUALITY_CONTROL_VARIANT_SET_METRICS_DESCRIPTION = "The body variantSetMetrics VariantFileQualityControl web service parameter";
    public static final String VARIANT_FILE_QUALITY_CONTROL_ASCAT_METRICS_DESCRIPTION = "The body ascatMetrics VariantFileQualityControl web service parameter";


    public static final String GENOME_PLOT_ANALYSIS_PARAMS_SAMPLE_DESCRIPTION = "The body sample GenomePlotAnalysisParams web service parameter";
    public static final String GENOME_PLOT_ANALYSIS_PARAMS_ID_DESCRIPTION = "The body id GenomePlotAnalysisParams web service parameter";
    public static final String GENOME_PLOT_ANALYSIS_PARAMS_CONFIG_FILE_DESCRIPTION = "The body configFile GenomePlotAnalysisParams web service parameter";
    public static final String GENOME_PLOT_ANALYSIS_PARAMS_OUTDIR_DESCRIPTION = "The body outdir GenomePlotAnalysisParams web service parameter";


    public static final String VARIANT_STUDY_DELETE_PARAMS_RESUME_DESCRIPTION = "The body resume VariantStudyDeleteParams web service parameter";


    public static final String VARIANT_INDEX_PARAMS_FILE_DESCRIPTION = "The body file VariantIndexParams web service parameter";
    public static final String VARIANT_INDEX_PARAMS_RESUME_DESCRIPTION = "The body resume VariantIndexParams web service parameter";
    public static final String VARIANT_INDEX_PARAMS_OUTDIR_DESCRIPTION = "The body outdir VariantIndexParams web service parameter";
    public static final String VARIANT_INDEX_PARAMS_TRANSFORM_DESCRIPTION = "The body transform VariantIndexParams web service parameter";
    public static final String VARIANT_INDEX_PARAMS_GVCF_DESCRIPTION = "The body gvcf VariantIndexParams web service parameter";
    public static final String VARIANT_INDEX_PARAMS_NORMALIZATION_SKIP_DESCRIPTION = "The body normalizationSkip VariantIndexParams web service parameter";
    public static final String VARIANT_INDEX_PARAMS_REFERENCE_GENOME_DESCRIPTION = "The body referenceGenome VariantIndexParams web service parameter";
    public static final String VARIANT_INDEX_PARAMS_FAIL_ON_MALFORMED_LINES_DESCRIPTION = "The body failOnMalformedLines VariantIndexParams web service parameter";
    public static final String VARIANT_INDEX_PARAMS_FAMILY_DESCRIPTION = "The body family VariantIndexParams web service parameter";
    public static final String VARIANT_INDEX_PARAMS_SOMATIC_DESCRIPTION = "The body somatic VariantIndexParams web service parameter";
    public static final String VARIANT_INDEX_PARAMS_LOAD_DESCRIPTION = "The body load VariantIndexParams web service parameter";
    public static final String VARIANT_INDEX_PARAMS_LOAD_SPLIT_DATA_DESCRIPTION = "The body loadSplitData VariantIndexParams web service parameter";
    public static final String VARIANT_INDEX_PARAMS_LOAD_MULTI_FILE_DATA_DESCRIPTION = "The body loadMultiFileData VariantIndexParams web service parameter";
    public static final String VARIANT_INDEX_PARAMS_LOAD_SAMPLE_INDEX_DESCRIPTION = "The body loadSampleIndex VariantIndexParams web service parameter";
    public static final String VARIANT_INDEX_PARAMS_LOAD_ARCHIVE_DESCRIPTION = "The body loadArchive VariantIndexParams web service parameter";
    public static final String VARIANT_INDEX_PARAMS_LOAD_HOM_REF_DESCRIPTION = "The body loadHomRef VariantIndexParams web service parameter";
    public static final String VARIANT_INDEX_PARAMS_POST_LOAD_CHECK_DESCRIPTION = "The body postLoadCheck VariantIndexParams web service parameter";
    public static final String VARIANT_INDEX_PARAMS_INCLUDE_GENOTYPES_DESCRIPTION = "The body includeGenotypes VariantIndexParams web service parameter";
    public static final String VARIANT_INDEX_PARAMS_INCLUDE_SAMPLE_DATA_DESCRIPTION = "The body includeSampleData VariantIndexParams web service parameter";
    public static final String VARIANT_INDEX_PARAMS_MERGE_DESCRIPTION = "The body merge VariantIndexParams web service parameter";
    public static final String VARIANT_INDEX_PARAMS_DEDUPLICATION_POLICY_DESCRIPTION = "The body deduplicationPolicy VariantIndexParams web service parameter";
    public static final String VARIANT_INDEX_PARAMS_CALCULATE_STATS_DESCRIPTION = "The body calculateStats VariantIndexParams web service parameter";
    public static final String VARIANT_INDEX_PARAMS_AGGREGATED_DESCRIPTION = "The body aggregated VariantIndexParams web service parameter";
    public static final String VARIANT_INDEX_PARAMS_AGGREGATION_MAPPING_FILE_DESCRIPTION = "The body aggregationMappingFile VariantIndexParams web service parameter";
    public static final String VARIANT_INDEX_PARAMS_ANNOTATE_DESCRIPTION = "The body annotate VariantIndexParams web service parameter";
    public static final String VARIANT_INDEX_PARAMS_ANNOTATOR_DESCRIPTION = "The body annotator VariantIndexParams web service parameter";
    public static final String VARIANT_INDEX_PARAMS_OVERWRITE_ANNOTATIONS_DESCRIPTION = "The body overwriteAnnotations VariantIndexParams web service parameter";
    public static final String VARIANT_INDEX_PARAMS_INDEX_SEARCH_DESCRIPTION = "The body indexSearch VariantIndexParams web service parameter";
    public static final String VARIANT_INDEX_PARAMS_SKIP_INDEXED_FILES_DESCRIPTION = "The body skipIndexedFiles VariantIndexParams web service parameter";


    public static final String VARIANT_PRUNE_PARAMS_PROJECT_DESCRIPTION = "The body project VariantPruneParams web service parameter";
    public static final String VARIANT_PRUNE_PARAMS_DRY_RUN_DESCRIPTION = "The body dryRun VariantPruneParams web service parameter";
    public static final String VARIANT_PRUNE_PARAMS_RESUME_DESCRIPTION = "The body resume VariantPruneParams web service parameter";


    public static final String KNOCKOUT_ANALYSIS_PARAMS_SAMPLE_DESCRIPTION = "The body sample KnockoutAnalysisParams web service parameter";
    public static final String KNOCKOUT_ANALYSIS_PARAMS_GENE_DESCRIPTION = "The body gene KnockoutAnalysisParams web service parameter";
    public static final String KNOCKOUT_ANALYSIS_PARAMS_PANEL_DESCRIPTION = "The body panel KnockoutAnalysisParams web service parameter";
    public static final String KNOCKOUT_ANALYSIS_PARAMS_BIOTYPE_DESCRIPTION = "The body biotype KnockoutAnalysisParams web service parameter";
    public static final String KNOCKOUT_ANALYSIS_PARAMS_CONSEQUENCE_TYPE_DESCRIPTION = "The body consequenceType KnockoutAnalysisParams web service parameter";
    public static final String KNOCKOUT_ANALYSIS_PARAMS_FILTER_DESCRIPTION = "The body filter KnockoutAnalysisParams web service parameter";
    public static final String KNOCKOUT_ANALYSIS_PARAMS_QUAL_DESCRIPTION = "The body qual KnockoutAnalysisParams web service parameter";
    public static final String KNOCKOUT_ANALYSIS_PARAMS_SKIP_GENES_FILE_DESCRIPTION = "The body skipGenesFile KnockoutAnalysisParams web service parameter";
    public static final String KNOCKOUT_ANALYSIS_PARAMS_OUTDIR_DESCRIPTION = "The body outdir KnockoutAnalysisParams web service parameter";
    public static final String KNOCKOUT_ANALYSIS_PARAMS_INDEX_DESCRIPTION = "The body index KnockoutAnalysisParams web service parameter";


    public static final String VARIANT_SAMPLE_DELETE_PARAMS_SAMPLE_DESCRIPTION = "The body sample VariantSampleDeleteParams web service parameter";
    public static final String VARIANT_SAMPLE_DELETE_PARAMS_FORCE_DESCRIPTION = "The body force VariantSampleDeleteParams web service parameter";
    public static final String VARIANT_SAMPLE_DELETE_PARAMS_RESUME_DESCRIPTION = "The body resume VariantSampleDeleteParams web service parameter";


    public static final String SAMPLE_QC_ANALYSIS_PARAMS_SAMPLE_DESCRIPTION = "The body sample SampleQcAnalysisParams web service parameter";
    public static final String SAMPLE_QC_ANALYSIS_PARAMS_VARIANT_STATS_ID_DESCRIPTION = "The body variantStatsId SampleQcAnalysisParams web service parameter";
    public static final String SAMPLE_QC_ANALYSIS_PARAMS_VARIANT_STATS_DESCRIPTION_DESCRIPTION = "The body variantStatsDescription SampleQcAnalysisParams web service parameter";
    public static final String SAMPLE_QC_ANALYSIS_PARAMS_VARIANT_STATS_QUERY_DESCRIPTION = "The body variantStatsQuery SampleQcAnalysisParams web service parameter";
    public static final String SAMPLE_QC_ANALYSIS_PARAMS_SIGNATURE_ID_DESCRIPTION = "The body signatureId SampleQcAnalysisParams web service parameter";
    public static final String SAMPLE_QC_ANALYSIS_PARAMS_SIGNATURE_DESCRIPTION_DESCRIPTION = "The body signatureDescription SampleQcAnalysisParams web service parameter";
    public static final String SAMPLE_QC_ANALYSIS_PARAMS_SIGNATURE_QUERY_DESCRIPTION = "The body signatureQuery SampleQcAnalysisParams web service parameter";
    public static final String SAMPLE_QC_ANALYSIS_PARAMS_SIGNATURE_RELEASE_DESCRIPTION = "The body signatureRelease SampleQcAnalysisParams web service parameter";
    public static final String SAMPLE_QC_ANALYSIS_PARAMS_GENOME_PLOT_ID_DESCRIPTION = "The body genomePlotId SampleQcAnalysisParams web service parameter";
    public static final String SAMPLE_QC_ANALYSIS_PARAMS_GENOME_PLOT_DESCRIPTION_DESCRIPTION = "The body genomePlotDescription SampleQcAnalysisParams web service parameter";
    public static final String SAMPLE_QC_ANALYSIS_PARAMS_GENOME_PLOT_CONFIG_FILE_DESCRIPTION = "The body genomePlotConfigFile SampleQcAnalysisParams web service parameter";
    public static final String SAMPLE_QC_ANALYSIS_PARAMS_OUTDIR_DESCRIPTION = "The body outdir SampleQcAnalysisParams web service parameter";


    public static final String MENDELIAN_ERROR_ANALYSIS_PARAMS_FAMILY_DESCRIPTION = "The body family MendelianErrorAnalysisParams web service parameter";
    public static final String MENDELIAN_ERROR_ANALYSIS_PARAMS_INDIVIDUAL_DESCRIPTION = "The body individual MendelianErrorAnalysisParams web service parameter";
    public static final String MENDELIAN_ERROR_ANALYSIS_PARAMS_SAMPLE_DESCRIPTION = "The body sample MendelianErrorAnalysisParams web service parameter";
    public static final String MENDELIAN_ERROR_ANALYSIS_PARAMS_OUTDIR_DESCRIPTION = "The body outdir MendelianErrorAnalysisParams web service parameter";


    public static final String INFERRED_SEX_ANALYSIS_PARAMS_INDIVIDUAL_DESCRIPTION = "The body individual InferredSexAnalysisParams web service parameter";
    public static final String INFERRED_SEX_ANALYSIS_PARAMS_SAMPLE_DESCRIPTION = "The body sample InferredSexAnalysisParams web service parameter";
    public static final String INFERRED_SEX_ANALYSIS_PARAMS_OUTDIR_DESCRIPTION = "The body outdir InferredSexAnalysisParams web service parameter";


    public static final String VARIANT_QUERY_PARAMS_SAVED_FILTER_DESCRIPTION = "The body savedFilter VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_CHROMOSOME_DESCRIPTION = "The body chromosome VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_REFERENCE_DESCRIPTION = "The body reference VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_ALTERNATE_DESCRIPTION = "The body alternate VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_RELEASE_DESCRIPTION = "The body release VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_INCLUDE_STUDY_DESCRIPTION = "The body includeStudy VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_INCLUDE_SAMPLE_DESCRIPTION = "The body includeSample VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_INCLUDE_FILE_DESCRIPTION = "The body includeFile VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_INCLUDE_SAMPLE_DATA_DESCRIPTION = "The body includeSampleData VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_INCLUDE_SAMPLE_ID_DESCRIPTION = "The body includeSampleId VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_INCLUDE_GENOTYPE_DESCRIPTION = "The body includeGenotype VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_FILE_DESCRIPTION = "The body file VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_QUAL_DESCRIPTION = "The body qual VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_FILTER_DESCRIPTION = "The body filter VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_FILE_DATA_DESCRIPTION = "The body fileData VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_GENOTYPE_DESCRIPTION = "The body genotype VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_SAMPLE_DESCRIPTION = "The body sample VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_SAMPLE_LIMIT_DESCRIPTION = "The body sampleLimit VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_SAMPLE_SKIP_DESCRIPTION = "The body sampleSkip VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_SAMPLE_DATA_DESCRIPTION = "The body sampleData VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_SAMPLE_ANNOTATION_DESCRIPTION = "The body sampleAnnotation VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_FAMILY_DESCRIPTION = "The body family VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_FAMILY_MEMBERS_DESCRIPTION = "The body familyMembers VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_FAMILY_DISORDER_DESCRIPTION = "The body familyDisorder VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_FAMILY_PROBAND_DESCRIPTION = "The body familyProband VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_FAMILY_SEGREGATION_DESCRIPTION = "The body familySegregation VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_COHORT_DESCRIPTION = "The body cohort VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_COHORT_STATS_PASS_DESCRIPTION = "The body cohortStatsPass VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_COHORT_STATS_MGF_DESCRIPTION = "The body cohortStatsMgf VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_MISSING_ALLELES_DESCRIPTION = "The body missingAlleles VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_MISSING_GENOTYPES_DESCRIPTION = "The body missingGenotypes VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_ANNOTATION_EXISTS_DESCRIPTION = "The body annotationExists VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_SCORE_DESCRIPTION = "The body score VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_POLYPHEN_DESCRIPTION = "The body polyphen VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_SIFT_DESCRIPTION = "The body sift VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_GENE_TRAIT_ID_DESCRIPTION = "The body geneTraitId VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_GENE_TRAIT_NAME_DESCRIPTION = "The body geneTraitName VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_TRAIT_DESCRIPTION = "The body trait VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_COSMIC_DESCRIPTION = "The body cosmic VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_CLINVAR_DESCRIPTION = "The body clinvar VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_HPO_DESCRIPTION = "The body hpo VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_GO_DESCRIPTION = "The body go VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_EXPRESSION_DESCRIPTION = "The body expression VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_PROTEIN_KEYWORD_DESCRIPTION = "The body proteinKeyword VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_DRUG_DESCRIPTION = "The body drug VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_CUSTOM_ANNOTATION_DESCRIPTION = "The body customAnnotation VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_UNKNOWN_GENOTYPE_DESCRIPTION = "The body unknownGenotype VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_SAMPLE_METADATA_DESCRIPTION = "The body sampleMetadata VariantQueryParams web service parameter";
    public static final String VARIANT_QUERY_PARAMS_SORT_DESCRIPTION = "The body sort VariantQueryParams web service parameter";


    public static final String SAMPLE_VARIANT_STATS_ANALYSIS_PARAMS_SAMPLE_DESCRIPTION = "The body sample SampleVariantStatsAnalysisParams web service parameter";
    public static final String SAMPLE_VARIANT_STATS_ANALYSIS_PARAMS_INDIVIDUAL_DESCRIPTION = "The body individual SampleVariantStatsAnalysisParams web service parameter";
    public static final String SAMPLE_VARIANT_STATS_ANALYSIS_PARAMS_VARIANT_QUERY_DESCRIPTION = "The body variantQuery SampleVariantStatsAnalysisParams web service parameter";
    public static final String SAMPLE_VARIANT_STATS_ANALYSIS_PARAMS_OUTDIR_DESCRIPTION = "The body outdir SampleVariantStatsAnalysisParams web service parameter";
    public static final String SAMPLE_VARIANT_STATS_ANALYSIS_PARAMS_INDEX_DESCRIPTION = "The body index SampleVariantStatsAnalysisParams web service parameter";
    public static final String SAMPLE_VARIANT_STATS_ANALYSIS_PARAMS_INDEX_OVERWRITE_DESCRIPTION = "The body indexOverwrite SampleVariantStatsAnalysisParams web service parameter";
    public static final String SAMPLE_VARIANT_STATS_ANALYSIS_PARAMS_INDEX_ID_DESCRIPTION = "The body indexId SampleVariantStatsAnalysisParams web service parameter";
    public static final String SAMPLE_VARIANT_STATS_ANALYSIS_PARAMS_INDEX_DESCRIPTION_DESCRIPTION = "The body indexDescription SampleVariantStatsAnalysisParams web service parameter";
    public static final String SAMPLE_VARIANT_STATS_ANALYSIS_PARAMS_BATCH_SIZE_DESCRIPTION = "The body batchSize SampleVariantStatsAnalysisParams web service parameter";


    public static final String COHORT_VARIANT_STATS_ANALYSIS_PARAMS_COHORT_DESCRIPTION = "The body cohort CohortVariantStatsAnalysisParams web service parameter";
    public static final String COHORT_VARIANT_STATS_ANALYSIS_PARAMS_SAMPLES_DESCRIPTION = "The body samples CohortVariantStatsAnalysisParams web service parameter";
    public static final String COHORT_VARIANT_STATS_ANALYSIS_PARAMS_INDEX_DESCRIPTION = "The body index CohortVariantStatsAnalysisParams web service parameter";
    public static final String COHORT_VARIANT_STATS_ANALYSIS_PARAMS_SAMPLE_ANNOTATION_DESCRIPTION = "The body sampleAnnotation CohortVariantStatsAnalysisParams web service parameter";
    public static final String COHORT_VARIANT_STATS_ANALYSIS_PARAMS_OUTDIR_DESCRIPTION = "The body outdir CohortVariantStatsAnalysisParams web service parameter";


    public static final String MUTATIONAL_SIGNATURE_ANALYSIS_PARAMS_SAMPLE_DESCRIPTION = "The body sample MutationalSignatureAnalysisParams web service parameter";
    public static final String MUTATIONAL_SIGNATURE_ANALYSIS_PARAMS_ID_DESCRIPTION = "The body id MutationalSignatureAnalysisParams web service parameter";
    public static final String MUTATIONAL_SIGNATURE_ANALYSIS_PARAMS_QUERY_DESCRIPTION = "The body query MutationalSignatureAnalysisParams web service parameter";
    public static final String MUTATIONAL_SIGNATURE_ANALYSIS_PARAMS_RELEASE_DESCRIPTION = "The body release MutationalSignatureAnalysisParams web service parameter";
    public static final String MUTATIONAL_SIGNATURE_ANALYSIS_PARAMS_FITTING_DESCRIPTION = "The body fitting MutationalSignatureAnalysisParams web service parameter";
    public static final String MUTATIONAL_SIGNATURE_ANALYSIS_PARAMS_OUTDIR_DESCRIPTION = "The body outdir MutationalSignatureAnalysisParams web service parameter";


    public static final String BASIC_VARIANT_QUERY_PARAMS_PROJECT_DESCRIPTION = "The body project BasicVariantQueryParams web service parameter";
    public static final String BASIC_VARIANT_QUERY_PARAMS_STUDY_DESCRIPTION = "The body study BasicVariantQueryParams web service parameter";


    public static final String GWAS_ANALYSIS_PARAMS_PHENOTYPE_DESCRIPTION = "The body phenotype GwasAnalysisParams web service parameter";
    public static final String GWAS_ANALYSIS_PARAMS_INDEX_DESCRIPTION = "The body index GwasAnalysisParams web service parameter";
    public static final String GWAS_ANALYSIS_PARAMS_INDEX_SCORE_ID_DESCRIPTION = "The body indexScoreId GwasAnalysisParams web service parameter";
    public static final String GWAS_ANALYSIS_PARAMS_METHOD_DESCRIPTION = "The body method GwasAnalysisParams web service parameter";
    public static final String GWAS_ANALYSIS_PARAMS_FISHER_MODE_DESCRIPTION = "The body fisherMode GwasAnalysisParams web service parameter";
    public static final String GWAS_ANALYSIS_PARAMS_CASE_COHORT_DESCRIPTION = "The body caseCohort GwasAnalysisParams web service parameter";
    public static final String GWAS_ANALYSIS_PARAMS_CASE_COHORT_SAMPLES_ANNOTATION_DESCRIPTION = "The body caseCohortSamplesAnnotation GwasAnalysisParams web service parameter";
    public static final String GWAS_ANALYSIS_PARAMS_CASE_COHORT_SAMPLES_DESCRIPTION = "The body caseCohortSamples GwasAnalysisParams web service parameter";
    public static final String GWAS_ANALYSIS_PARAMS_CONTROL_COHORT_DESCRIPTION = "The body controlCohort GwasAnalysisParams web service parameter";
    public static final String GWAS_ANALYSIS_PARAMS_CONTROL_COHORT_SAMPLES_ANNOTATION_DESCRIPTION = "The body controlCohortSamplesAnnotation GwasAnalysisParams web service parameter";
    public static final String GWAS_ANALYSIS_PARAMS_CONTROL_COHORT_SAMPLES_DESCRIPTION = "The body controlCohortSamples GwasAnalysisParams web service parameter";
    public static final String GWAS_ANALYSIS_PARAMS_OUTDIR_DESCRIPTION = "The body outdir GwasAnalysisParams web service parameter";


    public static final String SAMPLE_QC_SIGNATURE_QUERY_PARAMS_SAMPLE_DESCRIPTION = "The body sample SampleQcSignatureQueryParams web service parameter";
    public static final String SAMPLE_QC_SIGNATURE_QUERY_PARAMS_CT_DESCRIPTION = "The body ct SampleQcSignatureQueryParams web service parameter";
    public static final String SAMPLE_QC_SIGNATURE_QUERY_PARAMS_BIOTYPE_DESCRIPTION = "The body biotype SampleQcSignatureQueryParams web service parameter";
    public static final String SAMPLE_QC_SIGNATURE_QUERY_PARAMS_FILTER_DESCRIPTION = "The body filter SampleQcSignatureQueryParams web service parameter";
    public static final String SAMPLE_QC_SIGNATURE_QUERY_PARAMS_QUAL_DESCRIPTION = "The body qual SampleQcSignatureQueryParams web service parameter";
    public static final String SAMPLE_QC_SIGNATURE_QUERY_PARAMS_REGION_DESCRIPTION = "The body region SampleQcSignatureQueryParams web service parameter";
    public static final String SAMPLE_QC_SIGNATURE_QUERY_PARAMS_GENE_DESCRIPTION = "The body gene SampleQcSignatureQueryParams web service parameter";
    public static final String SAMPLE_QC_SIGNATURE_QUERY_PARAMS_PANEL_DESCRIPTION = "The body panel SampleQcSignatureQueryParams web service parameter";


    public static final String VARIANT_FILE_DELETE_PARAMS_FILE_DESCRIPTION = "The body file VariantFileDeleteParams web service parameter";
    public static final String VARIANT_FILE_DELETE_PARAMS_RESUME_DESCRIPTION = "The body resume VariantFileDeleteParams web service parameter";


    public static final String SAMPLE_VARIANT_FILTER_PARAMS_GENOTYPES_DESCRIPTION = "The body genotypes SampleVariantFilterParams web service parameter";
    public static final String SAMPLE_VARIANT_FILTER_PARAMS_SAMPLE_DESCRIPTION = "The body sample SampleVariantFilterParams web service parameter";
    public static final String SAMPLE_VARIANT_FILTER_PARAMS_SAMPLES_IN_ALL_VARIANTS_DESCRIPTION = "The body samplesInAllVariants SampleVariantFilterParams web service parameter";
    public static final String SAMPLE_VARIANT_FILTER_PARAMS_MAX_VARIANTS_DESCRIPTION = "The body maxVariants SampleVariantFilterParams web service parameter";


    public static final String PLINK_WRAPPER_PARAMS_OUTDIR_DESCRIPTION = "The body outdir PlinkWrapperParams web service parameter";
    public static final String PLINK_WRAPPER_PARAMS_PLINK_PARAMS_DESCRIPTION = "The body plinkParams PlinkWrapperParams web service parameter";


    public static final String RELATEDNESS_ANALYSIS_PARAMS_INDIVIDUALS_DESCRIPTION = "The body individuals RelatednessAnalysisParams web service parameter";
    public static final String RELATEDNESS_ANALYSIS_PARAMS_SAMPLES_DESCRIPTION = "The body samples RelatednessAnalysisParams web service parameter";
    public static final String RELATEDNESS_ANALYSIS_PARAMS_MINOR_ALLELE_FREQ_DESCRIPTION = "The body minorAlleleFreq RelatednessAnalysisParams web service parameter";
    public static final String RELATEDNESS_ANALYSIS_PARAMS_METHOD_DESCRIPTION = "The body method RelatednessAnalysisParams web service parameter";
    public static final String RELATEDNESS_ANALYSIS_PARAMS_OUTDIR_DESCRIPTION = "The body outdir RelatednessAnalysisParams web service parameter";


    public static final String CIRCOS_ANALYSIS_PARAMS_TITLE_DESCRIPTION = "The body title CircosAnalysisParams web service parameter";
    public static final String CIRCOS_ANALYSIS_PARAMS_DENSITY_DESCRIPTION = "The body density CircosAnalysisParams web service parameter";
    public static final String CIRCOS_ANALYSIS_PARAMS_QUERY_DESCRIPTION = "The body query CircosAnalysisParams web service parameter";
    public static final String CIRCOS_ANALYSIS_PARAMS_TRACKS_DESCRIPTION = "The body tracks CircosAnalysisParams web service parameter";
    public static final String CIRCOS_ANALYSIS_PARAMS_OUTDIR_DESCRIPTION = "The body outdir CircosAnalysisParams web service parameter";




    public static final String VARIANT_FILE_INDEX_JOB_LAUNCHER_PARAMS_NAME_DESCRIPTION = "The body name VariantFileIndexJobLauncherParams web service parameter";
    public static final String VARIANT_FILE_INDEX_JOB_LAUNCHER_PARAMS_DIRECTORY_DESCRIPTION = "The body directory VariantFileIndexJobLauncherParams web service parameter";
    public static final String VARIANT_FILE_INDEX_JOB_LAUNCHER_PARAMS_RESUME_FAILED_DESCRIPTION = "The body resumeFailed VariantFileIndexJobLauncherParams web service parameter";
    public static final String VARIANT_FILE_INDEX_JOB_LAUNCHER_PARAMS_IGNORE_FAILED_DESCRIPTION = "The body ignoreFailed VariantFileIndexJobLauncherParams web service parameter";
    public static final String VARIANT_FILE_INDEX_JOB_LAUNCHER_PARAMS_MAX_JOBS_DESCRIPTION = "The body maxJobs VariantFileIndexJobLauncherParams web service parameter";
    public static final String VARIANT_FILE_INDEX_JOB_LAUNCHER_PARAMS_INDEX_PARAMS_DESCRIPTION = "The body indexParams VariantFileIndexJobLauncherParams web service parameter";


    public static final String RVTESTS_WRAPPER_PARAMS_COMMAND_DESCRIPTION = "The body command RvtestsWrapperParams web service parameter";
    public static final String RVTESTS_WRAPPER_PARAMS_OUTDIR_DESCRIPTION = "The body outdir RvtestsWrapperParams web service parameter";
    public static final String RVTESTS_WRAPPER_PARAMS_RVTESTS_PARAMS_DESCRIPTION = "The body rvtestsParams RvtestsWrapperParams web service parameter";


    public static final String VARIANT_CONFIGURE_PARAMS_CONFIGURATION_DESCRIPTION = "The body configuration VariantConfigureParams web service parameter";


    public static final String VARIANT_EXPORT_PARAMS_OUTDIR_DESCRIPTION = "The body outdir VariantExportParams web service parameter";
    public static final String VARIANT_EXPORT_PARAMS_OUTPUT_FILE_NAME_DESCRIPTION = "The body outputFileName VariantExportParams web service parameter";
    public static final String VARIANT_EXPORT_PARAMS_OUTPUT_FILE_FORMAT_DESCRIPTION = "The body outputFileFormat VariantExportParams web service parameter";
    public static final String VARIANT_EXPORT_PARAMS_VARIANTS_FILE_DESCRIPTION = "The body variantsFile VariantExportParams web service parameter";
    public static final String VARIANT_EXPORT_PARAMS_INCLUDE_DESCRIPTION = "The body include VariantExportParams web service parameter";
    public static final String VARIANT_EXPORT_PARAMS_EXCLUDE_DESCRIPTION = "The body exclude VariantExportParams web service parameter";
    public static final String VARIANT_EXPORT_PARAMS_COMPRESS_DESCRIPTION = "The body compress VariantExportParams web service parameter";
    public static final String VARIANT_EXPORT_PARAMS_LIMIT_DESCRIPTION = "The body limit VariantExportParams web service parameter";
    public static final String VARIANT_EXPORT_PARAMS_SKIP_DESCRIPTION = "The body skip VariantExportParams web service parameter";
    public static final String VARIANT_EXPORT_PARAMS_SUMMARY_DESCRIPTION = "The body summary VariantExportParams web service parameter";


    public static final String SAMPLE_ELIGIBILITY_ANALYSIS_PARAMS_QUERY_DESCRIPTION = "The body query SampleEligibilityAnalysisParams web service parameter";
    public static final String SAMPLE_ELIGIBILITY_ANALYSIS_PARAMS_INDEX_DESCRIPTION = "The body index SampleEligibilityAnalysisParams web service parameter";
    public static final String SAMPLE_ELIGIBILITY_ANALYSIS_PARAMS_COHORT_ID_DESCRIPTION = "The body cohortId SampleEligibilityAnalysisParams web service parameter";


    public static final String ANNOTATION_VARIANT_QUERY_PARAMS_ID_DESCRIPTION = "The body id AnnotationVariantQueryParams web service parameter";
    public static final String ANNOTATION_VARIANT_QUERY_PARAMS_REGION_DESCRIPTION = "The body region AnnotationVariantQueryParams web service parameter";
    public static final String ANNOTATION_VARIANT_QUERY_PARAMS_GENE_DESCRIPTION = "The body gene AnnotationVariantQueryParams web service parameter";
    public static final String ANNOTATION_VARIANT_QUERY_PARAMS_TYPE_DESCRIPTION = "The body type AnnotationVariantQueryParams web service parameter";
    public static final String ANNOTATION_VARIANT_QUERY_PARAMS_PANEL_DESCRIPTION = "The body panel AnnotationVariantQueryParams web service parameter";
    public static final String ANNOTATION_VARIANT_QUERY_PARAMS_PANEL_MODE_OF_INHERITANCE_DESCRIPTION = "The body panelModeOfInheritance AnnotationVariantQueryParams web service parameter";
    public static final String ANNOTATION_VARIANT_QUERY_PARAMS_PANEL_CONFIDENCE_DESCRIPTION = "The body panelConfidence AnnotationVariantQueryParams web service parameter";
    public static final String ANNOTATION_VARIANT_QUERY_PARAMS_PANEL_ROLE_IN_CANCER_DESCRIPTION = "The body panelRoleInCancer AnnotationVariantQueryParams web service parameter";
    public static final String ANNOTATION_VARIANT_QUERY_PARAMS_PANEL_INTERSECTION_DESCRIPTION = "The body panelIntersection AnnotationVariantQueryParams web service parameter";
    public static final String ANNOTATION_VARIANT_QUERY_PARAMS_COHORT_STATS_REF_DESCRIPTION = "The body cohortStatsRef AnnotationVariantQueryParams web service parameter";
    public static final String ANNOTATION_VARIANT_QUERY_PARAMS_COHORT_STATS_ALT_DESCRIPTION = "The body cohortStatsAlt AnnotationVariantQueryParams web service parameter";
    public static final String ANNOTATION_VARIANT_QUERY_PARAMS_COHORT_STATS_MAF_DESCRIPTION = "The body cohortStatsMaf AnnotationVariantQueryParams web service parameter";
    public static final String ANNOTATION_VARIANT_QUERY_PARAMS_CT_DESCRIPTION = "The body ct AnnotationVariantQueryParams web service parameter";
    public static final String ANNOTATION_VARIANT_QUERY_PARAMS_XREF_DESCRIPTION = "The body xref AnnotationVariantQueryParams web service parameter";
    public static final String ANNOTATION_VARIANT_QUERY_PARAMS_BIOTYPE_DESCRIPTION = "The body biotype AnnotationVariantQueryParams web service parameter";
    public static final String ANNOTATION_VARIANT_QUERY_PARAMS_PROTEIN_SUBSTITUTION_DESCRIPTION = "The body proteinSubstitution AnnotationVariantQueryParams web service parameter";
    public static final String ANNOTATION_VARIANT_QUERY_PARAMS_CONSERVATION_DESCRIPTION = "The body conservation AnnotationVariantQueryParams web service parameter";
    public static final String ANNOTATION_VARIANT_QUERY_PARAMS_POPULATION_FREQUENCY_MAF_DESCRIPTION = "The body populationFrequencyMaf AnnotationVariantQueryParams web service parameter";
    public static final String ANNOTATION_VARIANT_QUERY_PARAMS_POPULATION_FREQUENCY_ALT_DESCRIPTION = "The body populationFrequencyAlt AnnotationVariantQueryParams web service parameter";
    public static final String ANNOTATION_VARIANT_QUERY_PARAMS_POPULATION_FREQUENCY_REF_DESCRIPTION = "The body populationFrequencyRef AnnotationVariantQueryParams web service parameter";
    public static final String ANNOTATION_VARIANT_QUERY_PARAMS_TRANSCRIPT_FLAG_DESCRIPTION = "The body transcriptFlag AnnotationVariantQueryParams web service parameter";
    public static final String ANNOTATION_VARIANT_QUERY_PARAMS_FUNCTIONAL_SCORE_DESCRIPTION = "The body functionalScore AnnotationVariantQueryParams web service parameter";
    public static final String ANNOTATION_VARIANT_QUERY_PARAMS_CLINICAL_DESCRIPTION = "The body clinical AnnotationVariantQueryParams web service parameter";
    public static final String ANNOTATION_VARIANT_QUERY_PARAMS_CLINICAL_SIGNIFICANCE_DESCRIPTION = "The body clinicalSignificance AnnotationVariantQueryParams web service parameter";
    public static final String ANNOTATION_VARIANT_QUERY_PARAMS_CLINICAL_CONFIRMED_STATUS_DESCRIPTION = "The body clinicalConfirmedStatus AnnotationVariantQueryParams web service parameter";


    public static final String CIRCOS_TRACK_ID_DESCRIPTION = "The body id CircosTrack web service parameter";
    public static final String CIRCOS_TRACK_TYPE_DESCRIPTION = "The body type CircosTrack web service parameter";
    public static final String CIRCOS_TRACK_QUERY_DESCRIPTION = "The body query CircosTrack web service parameter";


    public static final String VARIANT_STORAGE_METADATA_SYNCHRONIZE_PARAMS_STUDY_DESCRIPTION = "The body study VariantStorageMetadataSynchronizeParams web service parameter";
    public static final String VARIANT_STORAGE_METADATA_SYNCHRONIZE_PARAMS_FILES_DESCRIPTION = "The body files VariantStorageMetadataSynchronizeParams web service parameter";


    public static final String VARIANT_STATS_ANALYSIS_PARAMS_COHORT_DESCRIPTION = "The body cohort VariantStatsAnalysisParams web service parameter";
    public static final String VARIANT_STATS_ANALYSIS_PARAMS_SAMPLES_DESCRIPTION = "The body samples VariantStatsAnalysisParams web service parameter";
    public static final String VARIANT_STATS_ANALYSIS_PARAMS_REGION_DESCRIPTION = "The body region VariantStatsAnalysisParams web service parameter";
    public static final String VARIANT_STATS_ANALYSIS_PARAMS_GENE_DESCRIPTION = "The body gene VariantStatsAnalysisParams web service parameter";
    public static final String VARIANT_STATS_ANALYSIS_PARAMS_OUTDIR_DESCRIPTION = "The body outdir VariantStatsAnalysisParams web service parameter";
    public static final String VARIANT_STATS_ANALYSIS_PARAMS_OUTPUT_FILE_NAME_DESCRIPTION = "The body outputFileName VariantStatsAnalysisParams web service parameter";
    public static final String VARIANT_STATS_ANALYSIS_PARAMS_AGGREGATED_DESCRIPTION = "The body aggregated VariantStatsAnalysisParams web service parameter";
    public static final String VARIANT_STATS_ANALYSIS_PARAMS_AGGREGATION_MAPPING_FILE_DESCRIPTION = "The body aggregationMappingFile VariantStatsAnalysisParams web service parameter";


    public static final String INDIVIDUAL_QC_ANALYSIS_PARAMS_INDIVIDUAL_DESCRIPTION = "The body individual IndividualQcAnalysisParams web service parameter";
    public static final String INDIVIDUAL_QC_ANALYSIS_PARAMS_SAMPLE_DESCRIPTION = "The body sample IndividualQcAnalysisParams web service parameter";
    public static final String INDIVIDUAL_QC_ANALYSIS_PARAMS_INFERRED_SEX_METHOD_DESCRIPTION = "The body inferredSexMethod IndividualQcAnalysisParams web service parameter";
    public static final String INDIVIDUAL_QC_ANALYSIS_PARAMS_OUTDIR_DESCRIPTION = "The body outdir IndividualQcAnalysisParams web service parameter";


    public static final String TOOL_ID_DESCRIPTION = "The body id Tool web service parameter";
    public static final String TOOL_ALIAS_DESCRIPTION = "The body alias Tool web service parameter";
    public static final String TOOL_NAME_DESCRIPTION = "The body name Tool web service parameter";
    public static final String TOOL_MANIFEST_DESCRIPTION = "The body manifest Tool web service parameter";
    public static final String TOOL_RESULT_DESCRIPTION = "The body result Tool web service parameter";
    public static final String TOOL_PATH_DESCRIPTION = "The body path Tool web service parameter";




    public static final String JOB_TOP_DATE_DESCRIPTION = "The body date JobTop web service parameter";
    public static final String JOB_TOP_STATS_DESCRIPTION = "The body stats JobTop web service parameter";
    public static final String JOB_TOP_JOBS_DESCRIPTION = "The body jobs JobTop web service parameter";


    public static final String JOB_RETRY_PARAMS_JOB_DESCRIPTION = "The body job JobRetryParams web service parameter";
    public static final String JOB_RETRY_PARAMS_FORCE_DESCRIPTION = "The body force JobRetryParams web service parameter";
    public static final String JOB_RETRY_PARAMS_PARAMS_DESCRIPTION = "The body params JobRetryParams web service parameter";


    public static final String JOB_CREATE_PARAMS_ID_DESCRIPTION = "The body id JobCreateParams web service parameter";
    public static final String JOB_CREATE_PARAMS_TOOL_DESCRIPTION = "The body tool JobCreateParams web service parameter";
    public static final String JOB_CREATE_PARAMS_PRIORITY_DESCRIPTION = "The body priority JobCreateParams web service parameter";
    public static final String JOB_CREATE_PARAMS_COMMAND_LINE_DESCRIPTION = "The body commandLine JobCreateParams web service parameter";
    public static final String JOB_CREATE_PARAMS_PARAMS_DESCRIPTION = "The body params JobCreateParams web service parameter";
    public static final String JOB_CREATE_PARAMS_INTERNAL_DESCRIPTION = "The body internal JobCreateParams web service parameter";
    public static final String JOB_CREATE_PARAMS_OUT_DIR_DESCRIPTION = "The body outDir JobCreateParams web service parameter";
    public static final String JOB_CREATE_PARAMS_INPUT_DESCRIPTION = "The body input JobCreateParams web service parameter";
    public static final String JOB_CREATE_PARAMS_OUTPUT_DESCRIPTION = "The body output JobCreateParams web service parameter";
    public static final String JOB_CREATE_PARAMS_TAGS_DESCRIPTION = "The body tags JobCreateParams web service parameter";
    public static final String JOB_CREATE_PARAMS_RESULT_DESCRIPTION = "The body result JobCreateParams web service parameter";
    public static final String JOB_CREATE_PARAMS_STDOUT_DESCRIPTION = "The body stdout JobCreateParams web service parameter";
    public static final String JOB_CREATE_PARAMS_STDERR_DESCRIPTION = "The body stderr JobCreateParams web service parameter";




    public static final String JOB_REFERENCE_PARAM_STUDY_ID_DESCRIPTION = "The body studyId JobReferenceParam web service parameter";
    public static final String JOB_REFERENCE_PARAM_ID_DESCRIPTION = "The body id JobReferenceParam web service parameter";
    public static final String JOB_REFERENCE_PARAM_UUID_DESCRIPTION = "The body uuid JobReferenceParam web service parameter";




    public static final String JOB_TOP_STATS_RUNNING_DESCRIPTION = "The body running JobTopStats web service parameter";
    public static final String JOB_TOP_STATS_QUEUED_DESCRIPTION = "The body queued JobTopStats web service parameter";
    public static final String JOB_TOP_STATS_PENDING_DESCRIPTION = "The body pending JobTopStats web service parameter";
    public static final String JOB_TOP_STATS_DONE_DESCRIPTION = "The body done JobTopStats web service parameter";
    public static final String JOB_TOP_STATS_ABORTED_DESCRIPTION = "The body aborted JobTopStats web service parameter";
    public static final String JOB_TOP_STATS_ERROR_DESCRIPTION = "The body error JobTopStats web service parameter";








    public static final String JOB_ACL_UPDATE_PARAMS_JOB_DESCRIPTION = "The body job JobAclUpdateParams web service parameter";


    public static final String JOB_UPDATE_PARAMS_TAGS_DESCRIPTION = "The body tags JobUpdateParams web service parameter";
    public static final String JOB_UPDATE_PARAMS_VISITED_DESCRIPTION = "The body visited JobUpdateParams web service parameter";


    public static final String USER_IMPORT_PARAMS_AUTHENTICATION_ORIGIN_ID_DESCRIPTION = "The body authenticationOriginId UserImportParams web service parameter";
    public static final String USER_IMPORT_PARAMS_ID_DESCRIPTION = "The body id UserImportParams web service parameter";
    public static final String USER_IMPORT_PARAMS_RESOURCE_TYPE_DESCRIPTION = "The body resourceType UserImportParams web service parameter";
    public static final String USER_IMPORT_PARAMS_STUDY_DESCRIPTION = "The body study UserImportParams web service parameter";
    public static final String USER_IMPORT_PARAMS_STUDY_GROUP_DESCRIPTION = "The body studyGroup UserImportParams web service parameter";


    public static final String GROUP_SYNC_PARAMS_AUTHENTICATION_ORIGIN_ID_DESCRIPTION = "The body authenticationOriginId GroupSyncParams web service parameter";
    public static final String GROUP_SYNC_PARAMS_FROM_DESCRIPTION = "The body from GroupSyncParams web service parameter";
    public static final String GROUP_SYNC_PARAMS_TO_DESCRIPTION = "The body to GroupSyncParams web service parameter";
    public static final String GROUP_SYNC_PARAMS_STUDY_DESCRIPTION = "The body study GroupSyncParams web service parameter";
    public static final String GROUP_SYNC_PARAMS_SYNC_ALL_DESCRIPTION = "The body syncAll GroupSyncParams web service parameter";
    public static final String GROUP_SYNC_PARAMS_TYPE_DESCRIPTION = "The body type GroupSyncParams web service parameter";
    public static final String GROUP_SYNC_PARAMS_FORCE_DESCRIPTION = "The body force GroupSyncParams web service parameter";


    public static final String JWT_PARAMS_SECRET_KEY_DESCRIPTION = "The body secretKey JWTParams web service parameter";


    public static final String INSTALLATION_PARAMS_SECRET_KEY_DESCRIPTION = "The body secretKey InstallationParams web service parameter";
    public static final String INSTALLATION_PARAMS_PASSWORD_DESCRIPTION = "The body password InstallationParams web service parameter";
    public static final String INSTALLATION_PARAMS_EMAIL_DESCRIPTION = "The body email InstallationParams web service parameter";
    public static final String INSTALLATION_PARAMS_ORGANIZATION_DESCRIPTION = "The body organization InstallationParams web service parameter";


    public static final String USER_CREATE_PARAMS_TYPE_DESCRIPTION = "The body type UserCreateParams web service parameter";


    public static final String PRIVATE_STUDY_UID_ID_DESCRIPTION = "The body id PrivateStudyUid web service parameter";
    public static final String PRIVATE_STUDY_UID_STUDY_UID_DESCRIPTION = "The body studyUid PrivateStudyUid web service parameter";




    public static final String ANNOTATION_NAME_DESCRIPTION = "The body name Annotation web service parameter";
    public static final String ANNOTATION_VALUE_DESCRIPTION = "The body value Annotation web service parameter";




    public static final String METADATA_VERSION_DESCRIPTION = "The body version Metadata web service parameter";
    public static final String METADATA_ID_COUNTER_DESCRIPTION = "The body idCounter Metadata web service parameter";


    public static final String USER_UPDATE_PARAMS_NAME_DESCRIPTION = "The body name UserUpdateParams web service parameter";
    public static final String USER_UPDATE_PARAMS_EMAIL_DESCRIPTION = "The body email UserUpdateParams web service parameter";
    public static final String USER_UPDATE_PARAMS_ORGANIZATION_DESCRIPTION = "The body organization UserUpdateParams web service parameter";


    public static final String AUTHENTICATION_RESPONSE_TOKEN_DESCRIPTION = "The body token AuthenticationResponse web service parameter";
    public static final String AUTHENTICATION_RESPONSE_REFRESH_TOKEN_DESCRIPTION = "The body refreshToken AuthenticationResponse web service parameter";




    public static final String USER_QUOTA_DISK_USAGE_DESCRIPTION = "The body diskUsage UserQuota web service parameter";
    public static final String USER_QUOTA_CPU_USAGE_DESCRIPTION = "The body cpuUsage UserQuota web service parameter";
    public static final String USER_QUOTA_MAX_DISK_DESCRIPTION = "The body maxDisk UserQuota web service parameter";
    public static final String USER_QUOTA_MAX_CPU_DESCRIPTION = "The body maxCpu UserQuota web service parameter";






    public static final String CONFIG_UPDATE_PARAMS_CONFIGURATION_DESCRIPTION = "The body configuration ConfigUpdateParams web service parameter";


    public static final String LOGIN_PARAMS_USER_DESCRIPTION = "The body user LoginParams web service parameter";
    public static final String LOGIN_PARAMS_PASSWORD_DESCRIPTION = "The body password LoginParams web service parameter";
    public static final String LOGIN_PARAMS_REFRESH_TOKEN_DESCRIPTION = "The body refreshToken LoginParams web service parameter";


    public static final String FILTER_UPDATE_PARAMS_RESOURCE_DESCRIPTION = "The body resource FilterUpdateParams web service parameter";
    public static final String FILTER_UPDATE_PARAMS_QUERY_DESCRIPTION = "The body query FilterUpdateParams web service parameter";
    public static final String FILTER_UPDATE_PARAMS_OPTIONS_DESCRIPTION = "The body options FilterUpdateParams web service parameter";


    public static final String PASSWORD_CHANGE_PARAMS_USER_DESCRIPTION = "The body user PasswordChangeParams web service parameter";
    public static final String PASSWORD_CHANGE_PARAMS_PASSWORD_DESCRIPTION = "The body password PasswordChangeParams web service parameter";
    public static final String PASSWORD_CHANGE_PARAMS_NEW_PASSWORD_DESCRIPTION = "The body newPassword PasswordChangeParams web service parameter";
    public static final String PASSWORD_CHANGE_PARAMS_RESET_DESCRIPTION = "The body reset PasswordChangeParams web service parameter";




    public static final String USER_CONFIGURATION_OBJECT_MAPPER_DESCRIPTION = "The body objectMapper UserConfiguration web service parameter";
    public static final String USER_CONFIGURATION_OBJECT_READER_DESCRIPTION = "The body objectReader UserConfiguration web service parameter";


    public static final String USER_CREATE_PARAMS_ID_DESCRIPTION = "The body id UserCreateParams web service parameter";
    public static final String USER_CREATE_PARAMS_NAME_DESCRIPTION = "The body name UserCreateParams web service parameter";
    public static final String USER_CREATE_PARAMS_EMAIL_DESCRIPTION = "The body email UserCreateParams web service parameter";
    public static final String USER_CREATE_PARAMS_PASSWORD_DESCRIPTION = "The body password UserCreateParams web service parameter";
    public static final String USER_CREATE_PARAMS_ORGANIZATION_DESCRIPTION = "The body organization UserCreateParams web service parameter";




    public static final String KNOCKOUT_VARIANT_ID_DESCRIPTION = "The body id KnockoutVariant web service parameter";
    public static final String KNOCKOUT_VARIANT_DB_SNP_DESCRIPTION = "The body dbSnp KnockoutVariant web service parameter";
    public static final String KNOCKOUT_VARIANT_CHROMOSOME_DESCRIPTION = "The body chromosome KnockoutVariant web service parameter";
    public static final String KNOCKOUT_VARIANT_START_DESCRIPTION = "The body start KnockoutVariant web service parameter";
    public static final String KNOCKOUT_VARIANT_END_DESCRIPTION = "The body end KnockoutVariant web service parameter";
    public static final String KNOCKOUT_VARIANT_LENGTH_DESCRIPTION = "The body length KnockoutVariant web service parameter";
    public static final String KNOCKOUT_VARIANT_REFERENCE_DESCRIPTION = "The body reference KnockoutVariant web service parameter";
    public static final String KNOCKOUT_VARIANT_ALTERNATE_DESCRIPTION = "The body alternate KnockoutVariant web service parameter";
    public static final String KNOCKOUT_VARIANT_TYPE_DESCRIPTION = "The body type KnockoutVariant web service parameter";
    public static final String KNOCKOUT_VARIANT_GENOTYPE_DESCRIPTION = "The body genotype KnockoutVariant web service parameter";
    public static final String KNOCKOUT_VARIANT_DEPTH_DESCRIPTION = "The body depth KnockoutVariant web service parameter";
    public static final String KNOCKOUT_VARIANT_FILTER_DESCRIPTION = "The body filter KnockoutVariant web service parameter";
    public static final String KNOCKOUT_VARIANT_QUAL_DESCRIPTION = "The body qual KnockoutVariant web service parameter";
    public static final String KNOCKOUT_VARIANT_STATS_DESCRIPTION = "The body stats KnockoutVariant web service parameter";
    public static final String KNOCKOUT_VARIANT_KNOCKOUT_TYPE_DESCRIPTION = "The body knockoutType KnockoutVariant web service parameter";
    public static final String KNOCKOUT_VARIANT_PARENTAL_ORIGIN_DESCRIPTION = "The body parentalOrigin KnockoutVariant web service parameter";
    public static final String KNOCKOUT_VARIANT_POPULATION_FREQUENCIES_DESCRIPTION = "The body populationFrequencies KnockoutVariant web service parameter";
    public static final String KNOCKOUT_VARIANT_SEQUENCE_ONTOLOGY_TERMS_DESCRIPTION = "The body sequenceOntologyTerms KnockoutVariant web service parameter";
    public static final String KNOCKOUT_VARIANT_CLINICAL_SIGNIFICANCE_DESCRIPTION = "The body clinicalSignificance KnockoutVariant web service parameter";


    public static final String KNOCKOUT_BY_GENE_ID_DESCRIPTION = "The body id KnockoutByGene web service parameter";
    public static final String KNOCKOUT_BY_GENE_NAME_DESCRIPTION = "The body name KnockoutByGene web service parameter";
    public static final String KNOCKOUT_BY_GENE_CHROMOSOME_DESCRIPTION = "The body chromosome KnockoutByGene web service parameter";
    public static final String KNOCKOUT_BY_GENE_START_DESCRIPTION = "The body start KnockoutByGene web service parameter";
    public static final String KNOCKOUT_BY_GENE_END_DESCRIPTION = "The body end KnockoutByGene web service parameter";
    public static final String KNOCKOUT_BY_GENE_STRAND_DESCRIPTION = "The body strand KnockoutByGene web service parameter";
    public static final String KNOCKOUT_BY_GENE_BIOTYPE_DESCRIPTION = "The body biotype KnockoutByGene web service parameter";
    public static final String KNOCKOUT_BY_GENE_ANNOTATION_DESCRIPTION = "The body annotation KnockoutByGene web service parameter";
    public static final String KNOCKOUT_BY_GENE_INDIVIDUALS_DESCRIPTION = "The body individuals KnockoutByGene web service parameter";


    public static final String KNOCKOUT_STATS_COUNT_DESCRIPTION = "The body count KnockoutStats web service parameter";
    public static final String KNOCKOUT_STATS_NUM_HOM_ALT_DESCRIPTION = "The body numHomAlt KnockoutStats web service parameter";
    public static final String KNOCKOUT_STATS_NUM_COMP_HET_DESCRIPTION = "The body numCompHet KnockoutStats web service parameter";
    public static final String KNOCKOUT_STATS_NUM_HET_ALT_DESCRIPTION = "The body numHetAlt KnockoutStats web service parameter";
    public static final String KNOCKOUT_STATS_NUM_DEL_OVERLAP_DESCRIPTION = "The body numDelOverlap KnockoutStats web service parameter";


    public static final String KNOCKOUT_BY_VARIANT_ID_DESCRIPTION = "The body id KnockoutByVariant web service parameter";
    public static final String KNOCKOUT_BY_VARIANT_DB_SNP_DESCRIPTION = "The body dbSnp KnockoutByVariant web service parameter";
    public static final String KNOCKOUT_BY_VARIANT_CHROMOSOME_DESCRIPTION = "The body chromosome KnockoutByVariant web service parameter";
    public static final String KNOCKOUT_BY_VARIANT_START_DESCRIPTION = "The body start KnockoutByVariant web service parameter";
    public static final String KNOCKOUT_BY_VARIANT_END_DESCRIPTION = "The body end KnockoutByVariant web service parameter";
    public static final String KNOCKOUT_BY_VARIANT_LENGTH_DESCRIPTION = "The body length KnockoutByVariant web service parameter";
    public static final String KNOCKOUT_BY_VARIANT_REFERENCE_DESCRIPTION = "The body reference KnockoutByVariant web service parameter";
    public static final String KNOCKOUT_BY_VARIANT_ALTERNATE_DESCRIPTION = "The body alternate KnockoutByVariant web service parameter";
    public static final String KNOCKOUT_BY_VARIANT_TYPE_DESCRIPTION = "The body type KnockoutByVariant web service parameter";
    public static final String KNOCKOUT_BY_VARIANT_POPULATION_FREQUENCIES_DESCRIPTION = "The body populationFrequencies KnockoutByVariant web service parameter";
    public static final String KNOCKOUT_BY_VARIANT_CLINICAL_SIGNIFICANCE_DESCRIPTION = "The body clinicalSignificance KnockoutByVariant web service parameter";
    public static final String KNOCKOUT_BY_VARIANT_NUM_INDIVIDUALS_DESCRIPTION = "The body numIndividuals KnockoutByVariant web service parameter";
    public static final String KNOCKOUT_BY_VARIANT_HAS_NEXT_INDIVIDUAL_DESCRIPTION = "The body hasNextIndividual KnockoutByVariant web service parameter";
    public static final String KNOCKOUT_BY_VARIANT_INDIVIDUALS_DESCRIPTION = "The body individuals KnockoutByVariant web service parameter";




    public static final String KNOCKOUT_BY_INDIVIDUAL_SUMMARY_ID_DESCRIPTION = "The body id KnockoutByIndividualSummary web service parameter";
    public static final String KNOCKOUT_BY_INDIVIDUAL_SUMMARY_SAMPLE_ID_DESCRIPTION = "The body sampleId KnockoutByIndividualSummary web service parameter";
    public static final String KNOCKOUT_BY_INDIVIDUAL_SUMMARY_MOTHER_ID_DESCRIPTION = "The body motherId KnockoutByIndividualSummary web service parameter";
    public static final String KNOCKOUT_BY_INDIVIDUAL_SUMMARY_MOTHER_SAMPLE_ID_DESCRIPTION = "The body motherSampleId KnockoutByIndividualSummary web service parameter";
    public static final String KNOCKOUT_BY_INDIVIDUAL_SUMMARY_FATHER_ID_DESCRIPTION = "The body fatherId KnockoutByIndividualSummary web service parameter";
    public static final String KNOCKOUT_BY_INDIVIDUAL_SUMMARY_FATHER_SAMPLE_ID_DESCRIPTION = "The body fatherSampleId KnockoutByIndividualSummary web service parameter";
    public static final String KNOCKOUT_BY_INDIVIDUAL_SUMMARY_SEX_DESCRIPTION = "The body sex KnockoutByIndividualSummary web service parameter";
    public static final String KNOCKOUT_BY_INDIVIDUAL_SUMMARY_PHENOTYPES_DESCRIPTION = "The body phenotypes KnockoutByIndividualSummary web service parameter";
    public static final String KNOCKOUT_BY_INDIVIDUAL_SUMMARY_DISORDERS_DESCRIPTION = "The body disorders KnockoutByIndividualSummary web service parameter";
    public static final String KNOCKOUT_BY_INDIVIDUAL_SUMMARY_NUM_PARENTS_DESCRIPTION = "The body numParents KnockoutByIndividualSummary web service parameter";
    public static final String KNOCKOUT_BY_INDIVIDUAL_SUMMARY_GENES_DESCRIPTION = "The body genes KnockoutByIndividualSummary web service parameter";
    public static final String KNOCKOUT_BY_INDIVIDUAL_SUMMARY_VARIANT_STATS_DESCRIPTION = "The body variantStats KnockoutByIndividualSummary web service parameter";


    public static final String RGA_KNOCKOUT_BY_GENE_NUM_INDIVIDUALS_DESCRIPTION = "The body numIndividuals RgaKnockoutByGene web service parameter";
    public static final String RGA_KNOCKOUT_BY_GENE_HAS_NEXT_INDIVIDUAL_DESCRIPTION = "The body hasNextIndividual RgaKnockoutByGene web service parameter";


    public static final String KNOCKOUT_BY_INDIVIDUAL_ID_DESCRIPTION = "The body id KnockoutByIndividual web service parameter";
    public static final String KNOCKOUT_BY_INDIVIDUAL_SAMPLE_ID_DESCRIPTION = "The body sampleId KnockoutByIndividual web service parameter";
    public static final String KNOCKOUT_BY_INDIVIDUAL_MOTHER_ID_DESCRIPTION = "The body motherId KnockoutByIndividual web service parameter";
    public static final String KNOCKOUT_BY_INDIVIDUAL_MOTHER_SAMPLE_ID_DESCRIPTION = "The body motherSampleId KnockoutByIndividual web service parameter";
    public static final String KNOCKOUT_BY_INDIVIDUAL_FATHER_ID_DESCRIPTION = "The body fatherId KnockoutByIndividual web service parameter";
    public static final String KNOCKOUT_BY_INDIVIDUAL_FATHER_SAMPLE_ID_DESCRIPTION = "The body fatherSampleId KnockoutByIndividual web service parameter";
    public static final String KNOCKOUT_BY_INDIVIDUAL_SEX_DESCRIPTION = "The body sex KnockoutByIndividual web service parameter";
    public static final String KNOCKOUT_BY_INDIVIDUAL_PHENOTYPES_DESCRIPTION = "The body phenotypes KnockoutByIndividual web service parameter";
    public static final String KNOCKOUT_BY_INDIVIDUAL_DISORDERS_DESCRIPTION = "The body disorders KnockoutByIndividual web service parameter";
    public static final String KNOCKOUT_BY_INDIVIDUAL_STATS_DESCRIPTION = "The body stats KnockoutByIndividual web service parameter";
    public static final String KNOCKOUT_BY_INDIVIDUAL_GENES_MAP_DESCRIPTION = "The body genesMap KnockoutByIndividual web service parameter";


    public static final String VARIANT_KNOCKOUT_STATS_NUM_PAIRED_COMP_HET_DESCRIPTION = "The body numPairedCompHet VariantKnockoutStats web service parameter";


    public static final String KNOCKOUT_TRANSCRIPT_ID_DESCRIPTION = "The body id KnockoutTranscript web service parameter";
    public static final String KNOCKOUT_TRANSCRIPT_CHROMOSOME_DESCRIPTION = "The body chromosome KnockoutTranscript web service parameter";
    public static final String KNOCKOUT_TRANSCRIPT_START_DESCRIPTION = "The body start KnockoutTranscript web service parameter";
    public static final String KNOCKOUT_TRANSCRIPT_END_DESCRIPTION = "The body end KnockoutTranscript web service parameter";
    public static final String KNOCKOUT_TRANSCRIPT_BIOTYPE_DESCRIPTION = "The body biotype KnockoutTranscript web service parameter";
    public static final String KNOCKOUT_TRANSCRIPT_STRAND_DESCRIPTION = "The body strand KnockoutTranscript web service parameter";
    public static final String KNOCKOUT_TRANSCRIPT_VARIANTS_DESCRIPTION = "The body variants KnockoutTranscript web service parameter";


    public static final String INDIVIDUAL_KNOCKOUT_STATS_MISSING_PARENTS_DESCRIPTION = "The body missingParents IndividualKnockoutStats web service parameter";
    public static final String INDIVIDUAL_KNOCKOUT_STATS_SINGLE_PARENT_DESCRIPTION = "The body singleParent IndividualKnockoutStats web service parameter";
    public static final String INDIVIDUAL_KNOCKOUT_STATS_BOTH_PARENTS_DESCRIPTION = "The body bothParents IndividualKnockoutStats web service parameter";


    public static final String KNOCKOUT_BY_VARIANT_SUMMARY_ID_DESCRIPTION = "The body id KnockoutByVariantSummary web service parameter";
    public static final String KNOCKOUT_BY_VARIANT_SUMMARY_DB_SNP_DESCRIPTION = "The body dbSnp KnockoutByVariantSummary web service parameter";
    public static final String KNOCKOUT_BY_VARIANT_SUMMARY_CHROMOSOME_DESCRIPTION = "The body chromosome KnockoutByVariantSummary web service parameter";
    public static final String KNOCKOUT_BY_VARIANT_SUMMARY_START_DESCRIPTION = "The body start KnockoutByVariantSummary web service parameter";
    public static final String KNOCKOUT_BY_VARIANT_SUMMARY_END_DESCRIPTION = "The body end KnockoutByVariantSummary web service parameter";
    public static final String KNOCKOUT_BY_VARIANT_SUMMARY_LENGTH_DESCRIPTION = "The body length KnockoutByVariantSummary web service parameter";
    public static final String KNOCKOUT_BY_VARIANT_SUMMARY_REFERENCE_DESCRIPTION = "The body reference KnockoutByVariantSummary web service parameter";
    public static final String KNOCKOUT_BY_VARIANT_SUMMARY_ALTERNATE_DESCRIPTION = "The body alternate KnockoutByVariantSummary web service parameter";
    public static final String KNOCKOUT_BY_VARIANT_SUMMARY_TYPE_DESCRIPTION = "The body type KnockoutByVariantSummary web service parameter";
    public static final String KNOCKOUT_BY_VARIANT_SUMMARY_GENES_DESCRIPTION = "The body genes KnockoutByVariantSummary web service parameter";
    public static final String KNOCKOUT_BY_VARIANT_SUMMARY_POPULATION_FREQUENCIES_DESCRIPTION = "The body populationFrequencies KnockoutByVariantSummary web service parameter";
    public static final String KNOCKOUT_BY_VARIANT_SUMMARY_SEQUENCE_ONTOLOGY_TERMS_DESCRIPTION = "The body sequenceOntologyTerms KnockoutByVariantSummary web service parameter";
    public static final String KNOCKOUT_BY_VARIANT_SUMMARY_CLINICAL_SIGNIFICANCES_DESCRIPTION = "The body clinicalSignificances KnockoutByVariantSummary web service parameter";
    public static final String KNOCKOUT_BY_VARIANT_SUMMARY_ALLELE_PAIRS_DESCRIPTION = "The body allelePairs KnockoutByVariantSummary web service parameter";
    public static final String KNOCKOUT_BY_VARIANT_SUMMARY_INDIVIDUAL_STATS_DESCRIPTION = "The body individualStats KnockoutByVariantSummary web service parameter";
    public static final String KNOCKOUT_BY_VARIANT_SUMMARY_TRANSCRIPT_CH_PAIRS_DESCRIPTION = "The body transcriptChPairs KnockoutByVariantSummary web service parameter";


    public static final String KNOCKOUT_BY_GENE_SUMMARY_ID_DESCRIPTION = "The body id KnockoutByGeneSummary web service parameter";
    public static final String KNOCKOUT_BY_GENE_SUMMARY_NAME_DESCRIPTION = "The body name KnockoutByGeneSummary web service parameter";
    public static final String KNOCKOUT_BY_GENE_SUMMARY_CHROMOSOME_DESCRIPTION = "The body chromosome KnockoutByGeneSummary web service parameter";
    public static final String KNOCKOUT_BY_GENE_SUMMARY_START_DESCRIPTION = "The body start KnockoutByGeneSummary web service parameter";
    public static final String KNOCKOUT_BY_GENE_SUMMARY_END_DESCRIPTION = "The body end KnockoutByGeneSummary web service parameter";
    public static final String KNOCKOUT_BY_GENE_SUMMARY_STRAND_DESCRIPTION = "The body strand KnockoutByGeneSummary web service parameter";
    public static final String KNOCKOUT_BY_GENE_SUMMARY_BIOTYPE_DESCRIPTION = "The body biotype KnockoutByGeneSummary web service parameter";
    public static final String KNOCKOUT_BY_GENE_SUMMARY_INDIVIDUAL_STATS_DESCRIPTION = "The body individualStats KnockoutByGeneSummary web service parameter";
    public static final String KNOCKOUT_BY_GENE_SUMMARY_VARIANT_STATS_DESCRIPTION = "The body variantStats KnockoutByGeneSummary web service parameter";


    public static final String PRIVATE_FIELDS_UID_DESCRIPTION = "The body uid PrivateFields web service parameter";




    public static final String INDIVIDUAL_UPDATE_PARAMS_ID_DESCRIPTION = "The body id IndividualUpdateParams web service parameter";
    public static final String INDIVIDUAL_UPDATE_PARAMS_NAME_DESCRIPTION = "The body name IndividualUpdateParams web service parameter";
    public static final String INDIVIDUAL_UPDATE_PARAMS_FATHER_DESCRIPTION = "The body father IndividualUpdateParams web service parameter";
    public static final String INDIVIDUAL_UPDATE_PARAMS_MOTHER_DESCRIPTION = "The body mother IndividualUpdateParams web service parameter";
    public static final String INDIVIDUAL_UPDATE_PARAMS_PARENTAL_CONSANGUINITY_DESCRIPTION = "The body parentalConsanguinity IndividualUpdateParams web service parameter";
    public static final String INDIVIDUAL_UPDATE_PARAMS_LOCATION_DESCRIPTION = "The body location IndividualUpdateParams web service parameter";
    public static final String INDIVIDUAL_UPDATE_PARAMS_SEX_DESCRIPTION = "The body sex IndividualUpdateParams web service parameter";
    public static final String INDIVIDUAL_UPDATE_PARAMS_ETHNICITY_DESCRIPTION = "The body ethnicity IndividualUpdateParams web service parameter";
    public static final String INDIVIDUAL_UPDATE_PARAMS_POPULATION_DESCRIPTION = "The body population IndividualUpdateParams web service parameter";
    public static final String INDIVIDUAL_UPDATE_PARAMS_DATE_OF_BIRTH_DESCRIPTION = "The body dateOfBirth IndividualUpdateParams web service parameter";
    public static final String INDIVIDUAL_UPDATE_PARAMS_KARYOTYPIC_SEX_DESCRIPTION = "The body karyotypicSex IndividualUpdateParams web service parameter";
    public static final String INDIVIDUAL_UPDATE_PARAMS_LIFE_STATUS_DESCRIPTION = "The body lifeStatus IndividualUpdateParams web service parameter";
    public static final String INDIVIDUAL_UPDATE_PARAMS_SAMPLES_DESCRIPTION = "The body samples IndividualUpdateParams web service parameter";
    public static final String INDIVIDUAL_UPDATE_PARAMS_ANNOTATION_SETS_DESCRIPTION = "The body annotationSets IndividualUpdateParams web service parameter";
    public static final String INDIVIDUAL_UPDATE_PARAMS_PHENOTYPES_DESCRIPTION = "The body phenotypes IndividualUpdateParams web service parameter";
    public static final String INDIVIDUAL_UPDATE_PARAMS_DISORDERS_DESCRIPTION = "The body disorders IndividualUpdateParams web service parameter";
    public static final String INDIVIDUAL_UPDATE_PARAMS_QUALITY_CONTROL_DESCRIPTION = "The body qualityControl IndividualUpdateParams web service parameter";


    public static final String INDIVIDUAL_ACL_PARAMS_SAMPLE_DESCRIPTION = "The body sample IndividualAclParams web service parameter";


    public static final String INDIVIDUAL_CREATE_PARAMS_SAMPLES_DESCRIPTION = "The body samples IndividualCreateParams web service parameter";








    public static final String INDIVIDUAL_REFERENCE_PARAM_ID_DESCRIPTION = "The body id IndividualReferenceParam web service parameter";
    public static final String INDIVIDUAL_REFERENCE_PARAM_UUID_DESCRIPTION = "The body uuid IndividualReferenceParam web service parameter";






    public static final String INDIVIDUAL_ACL_UPDATE_PARAMS_INDIVIDUAL_DESCRIPTION = "The body individual IndividualAclUpdateParams web service parameter";
    public static final String INDIVIDUAL_ACL_UPDATE_PARAMS_SAMPLE_DESCRIPTION = "The body sample IndividualAclUpdateParams web service parameter";




    public static final String SAMTOOLS_WRAPPER_PARAMS_COMMAND_DESCRIPTION = "The body command SamtoolsWrapperParams web service parameter";
    public static final String SAMTOOLS_WRAPPER_PARAMS_INPUT_FILE_DESCRIPTION = "The body inputFile SamtoolsWrapperParams web service parameter";
    public static final String SAMTOOLS_WRAPPER_PARAMS_OUTDIR_DESCRIPTION = "The body outdir SamtoolsWrapperParams web service parameter";
    public static final String SAMTOOLS_WRAPPER_PARAMS_SAMTOOLS_PARAMS_DESCRIPTION = "The body samtoolsParams SamtoolsWrapperParams web service parameter";


    public static final String ALIGNMENT_HS_METRICS_PARAMS_BAM_FILE_DESCRIPTION = "The body bamFile AlignmentHsMetricsParams web service parameter";
    public static final String ALIGNMENT_HS_METRICS_PARAMS_BED_FILE_DESCRIPTION = "The body bedFile AlignmentHsMetricsParams web service parameter";
    public static final String ALIGNMENT_HS_METRICS_PARAMS_DICT_FILE_DESCRIPTION = "The body dictFile AlignmentHsMetricsParams web service parameter";
    public static final String ALIGNMENT_HS_METRICS_PARAMS_OUTDIR_DESCRIPTION = "The body outdir AlignmentHsMetricsParams web service parameter";


    public static final String ALIGNMENT_FILE_QUALITY_CONTROL_FAST_QC_METRICS_DESCRIPTION = "The body fastQcMetrics AlignmentFileQualityControl web service parameter";
    public static final String ALIGNMENT_FILE_QUALITY_CONTROL_SAMTOOLS_STATS_DESCRIPTION = "The body samtoolsStats AlignmentFileQualityControl web service parameter";
    public static final String ALIGNMENT_FILE_QUALITY_CONTROL_SAMTOOLS_FLAG_STATS_DESCRIPTION = "The body samtoolsFlagStats AlignmentFileQualityControl web service parameter";
    public static final String ALIGNMENT_FILE_QUALITY_CONTROL_HS_METRICS_DESCRIPTION = "The body hsMetrics AlignmentFileQualityControl web service parameter";


    public static final String ALIGNMENT_FLAG_STATS_PARAMS_FILE_DESCRIPTION = "The body file AlignmentFlagStatsParams web service parameter";
    public static final String ALIGNMENT_FLAG_STATS_PARAMS_OUTDIR_DESCRIPTION = "The body outdir AlignmentFlagStatsParams web service parameter";


    public static final String PICARD_WRAPPER_PARAMS_COMMAND_DESCRIPTION = "The body command PicardWrapperParams web service parameter";
    public static final String PICARD_WRAPPER_PARAMS_OUTDIR_DESCRIPTION = "The body outdir PicardWrapperParams web service parameter";
    public static final String PICARD_WRAPPER_PARAMS_PICARD_PARAMS_DESCRIPTION = "The body picardParams PicardWrapperParams web service parameter";


    public static final String ALIGNMENT_FAST_QC_METRICS_PARAMS_FILE_DESCRIPTION = "The body file AlignmentFastQcMetricsParams web service parameter";
    public static final String ALIGNMENT_FAST_QC_METRICS_PARAMS_OUTDIR_DESCRIPTION = "The body outdir AlignmentFastQcMetricsParams web service parameter";


    public static final String ALIGNMENT_INDEX_PARAMS_FILE_DESCRIPTION = "The body file AlignmentIndexParams web service parameter";
    public static final String ALIGNMENT_INDEX_PARAMS_OVERWRITE_DESCRIPTION = "The body overwrite AlignmentIndexParams web service parameter";


    public static final String FASTQC_WRAPPER_PARAMS_INPUT_FILE_DESCRIPTION = "The body inputFile FastqcWrapperParams web service parameter";
    public static final String FASTQC_WRAPPER_PARAMS_OUTDIR_DESCRIPTION = "The body outdir FastqcWrapperParams web service parameter";
    public static final String FASTQC_WRAPPER_PARAMS_FASTQC_PARAMS_DESCRIPTION = "The body fastqcParams FastqcWrapperParams web service parameter";


    public static final String BWA_WRAPPER_PARAMS_COMMAND_DESCRIPTION = "The body command BwaWrapperParams web service parameter";
    public static final String BWA_WRAPPER_PARAMS_FASTA_FILE_DESCRIPTION = "The body fastaFile BwaWrapperParams web service parameter";
    public static final String BWA_WRAPPER_PARAMS_FASTQ1FILE_DESCRIPTION = "The body fastq1File BwaWrapperParams web service parameter";
    public static final String BWA_WRAPPER_PARAMS_FASTQ2FILE_DESCRIPTION = "The body fastq2File BwaWrapperParams web service parameter";
    public static final String BWA_WRAPPER_PARAMS_OUTDIR_DESCRIPTION = "The body outdir BwaWrapperParams web service parameter";
    public static final String BWA_WRAPPER_PARAMS_BWA_PARAMS_DESCRIPTION = "The body bwaParams BwaWrapperParams web service parameter";


    public static final String ALIGNMENT_STATS_PARAMS_FILE_DESCRIPTION = "The body file AlignmentStatsParams web service parameter";
    public static final String ALIGNMENT_STATS_PARAMS_OUTDIR_DESCRIPTION = "The body outdir AlignmentStatsParams web service parameter";


    public static final String DEEPTOOLS_WRAPPER_PARAMS_COMMAND_DESCRIPTION = "The body command DeeptoolsWrapperParams web service parameter";
    public static final String DEEPTOOLS_WRAPPER_PARAMS_OUTDIR_DESCRIPTION = "The body outdir DeeptoolsWrapperParams web service parameter";
    public static final String DEEPTOOLS_WRAPPER_PARAMS_DEEPTOOLS_PARAMS_DESCRIPTION = "The body deeptoolsParams DeeptoolsWrapperParams web service parameter";


    public static final String ALIGNMENT_QC_PARAMS_BAM_FILE_DESCRIPTION = "The body bamFile AlignmentQcParams web service parameter";
    public static final String ALIGNMENT_QC_PARAMS_BED_FILE_DESCRIPTION = "The body bedFile AlignmentQcParams web service parameter";
    public static final String ALIGNMENT_QC_PARAMS_DICT_FILE_DESCRIPTION = "The body dictFile AlignmentQcParams web service parameter";
    public static final String ALIGNMENT_QC_PARAMS_SKIP_DESCRIPTION = "The body skip AlignmentQcParams web service parameter";
    public static final String ALIGNMENT_QC_PARAMS_OVERWRITE_DESCRIPTION = "The body overwrite AlignmentQcParams web service parameter";
    public static final String ALIGNMENT_QC_PARAMS_OUTDIR_DESCRIPTION = "The body outdir AlignmentQcParams web service parameter";


    public static final String ALIGNMENT_GENE_COVERAGE_STATS_PARAMS_BAM_FILE_DESCRIPTION = "The body bamFile AlignmentGeneCoverageStatsParams web service parameter";
    public static final String ALIGNMENT_GENE_COVERAGE_STATS_PARAMS_GENES_DESCRIPTION = "The body genes AlignmentGeneCoverageStatsParams web service parameter";
    public static final String ALIGNMENT_GENE_COVERAGE_STATS_PARAMS_OUTDIR_DESCRIPTION = "The body outdir AlignmentGeneCoverageStatsParams web service parameter";


    public static final String COVERAGE_INDEX_PARAMS_FILE_DESCRIPTION = "The body file CoverageIndexParams web service parameter";
    public static final String COVERAGE_INDEX_PARAMS_WINDOW_SIZE_DESCRIPTION = "The body windowSize CoverageIndexParams web service parameter";
    public static final String COVERAGE_INDEX_PARAMS_OVERWRITE_DESCRIPTION = "The body overwrite CoverageIndexParams web service parameter";


    public static final String COVERAGE_FILE_QUALITY_CONTROL_GENE_COVERAGE_STATS_DESCRIPTION = "The body geneCoverageStats CoverageFileQualityControl web service parameter";




    public static final String COHORT_NAME_DESCRIPTION = "The body name Cohort web service parameter";


    public static final String COHORT_UPDATE_PARAMS_ID_DESCRIPTION = "The body id CohortUpdateParams web service parameter";
    public static final String COHORT_UPDATE_PARAMS_NAME_DESCRIPTION = "The body name CohortUpdateParams web service parameter";
    public static final String COHORT_UPDATE_PARAMS_TYPE_DESCRIPTION = "The body type CohortUpdateParams web service parameter";
    public static final String COHORT_UPDATE_PARAMS_SAMPLES_DESCRIPTION = "The body samples CohortUpdateParams web service parameter";
    public static final String COHORT_UPDATE_PARAMS_ANNOTATION_SETS_DESCRIPTION = "The body annotationSets CohortUpdateParams web service parameter";




    public static final String COHORT_ACL_UPDATE_PARAMS_COHORT_DESCRIPTION = "The body cohort CohortAclUpdateParams web service parameter";


    public static final String COHORT_GENERATE_PARAMS_ID_DESCRIPTION = "The body id CohortGenerateParams web service parameter";
    public static final String COHORT_GENERATE_PARAMS_NAME_DESCRIPTION = "The body name CohortGenerateParams web service parameter";
    public static final String COHORT_GENERATE_PARAMS_TYPE_DESCRIPTION = "The body type CohortGenerateParams web service parameter";
    public static final String COHORT_GENERATE_PARAMS_ANNOTATION_SETS_DESCRIPTION = "The body annotationSets CohortGenerateParams web service parameter";


    public static final String COHORT_CREATE_PARAMS_ID_DESCRIPTION = "The body id CohortCreateParams web service parameter";
    public static final String COHORT_CREATE_PARAMS_NAME_DESCRIPTION = "The body name CohortCreateParams web service parameter";
    public static final String COHORT_CREATE_PARAMS_TYPE_DESCRIPTION = "The body type CohortCreateParams web service parameter";
    public static final String COHORT_CREATE_PARAMS_SAMPLES_DESCRIPTION = "The body samples CohortCreateParams web service parameter";
    public static final String COHORT_CREATE_PARAMS_ANNOTATION_SETS_DESCRIPTION = "The body annotationSets CohortCreateParams web service parameter";



}
