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

package org.opencb.opencga.catalog.utils;

public class Constants {

    /* ****************************************    Additional QueryOptions parameters    ************************************************/
    /**
     * Boolean used when passing a list of entries. If silent is true, it will try to perform all the actions for all the entries possible.
     * Otherwise, if one of them fails for whatever reason, the response will be an error.
     */
    public static final String SILENT = "silent";

    /**
     * Forces some operations that would not be possible to perform otherwise.
     */
    public static final String FORCE = "force";

    /**
     * Map of actions that will only be used on updates to know how to deal with the update of array objects.
     */
    public static final String ACTIONS = "actions";

    /* ****************************************    Additional parameters                 ************************************************/

    /**
     * Used when deleting a sample.
     *
     * Action to be performed over files that were associated only to the sample to be deleted. Possible actions are NONE, TRASH, DELETE.
     */
    public static final String EMPTY_FILES_ACTION = "emptyFilesAction";

    /**
     * Used when deleting a sample.
     *
     * Boolean indicating if the cohorts associated only to the sample to be deleted should be also deleted.
     */
    public static final String DELETE_EMPTY_COHORTS = "deleteEmptyCohorts";

    /**
     * Used when deleting a file.
     *
     * Skip the trash and perform a physical deletion of local files and folders directly.
     */
    public static final String SKIP_TRASH = "skipTrash";

    /* ****************************************    Variable constants for versioning     ************************************************/
    /**
     * Boolean indicating whether to create a new version of the document containing the updates or update the same document.
     */
    public static final String INCREMENT_VERSION = "incVersion";

    /**
     * Flag indicating to update the references from the document to point to their latest versions available.
     */
    public static final String REFRESH = "refresh";

    /**
     * Numeric parameter containing the current release of the entries.
     */
    public static final String CURRENT_RELEASE = "currentRelease";

    /**
     * Boolean parameter indicating that all the versions are expected to be retrieved.
     */
    public static final String ALL_VERSIONS = "allVersions";


    /* ****************************************    Variable constants for annotations       ********************************************/
    /**
     * String used to include/exclude fields in the query option. It should be used like ANNOTATION.a.b where a.b will be the variable to
     * be included/excluded from the results.
     */
    public static final String ANNOTATION = "annotation";

    /**
     * String used to include/exclude fields in the query option. It should be used like ANNOTATION_SET_NAME.annotation where annotation
     * will be the AnnotationSetName whose annotations will be included/excluded from the results.
     */
    public static final String ANNOTATION_SET_NAME = "annotationSet";

    /**
     * String used to include/exclude fields in the query option. It should be used like VARIABLE_SET.55 where 55 will be the VariableSetId
     * whose annotations will be included/excluded from the results.
     */
    public static final String VARIABLE_SET = "variableSet";

    /**
     * String created in the AnnotationSetManager layer where the different variable types of the variables being queried will be written
     * to simplify the access to the database when the DBAdaptor layer creates parses the query to the actual database query.
     */
    public static final String PRIVATE_ANNOTATION_PARAM_TYPES = "_annotationTypes";

    /* ****************************************    Private attributes       ********************************************/
    /**
     * Reserved key in attributes for OpenCGA attributes.
     */
    public static final String PRIVATE_OPENCGA_ATTRIBUTES = "_opencga";

    /**
     * Key in attributes to contain the list of deleted files that were used as input for a job.
     */
    public static final String JOB_DELETED_INPUT_FILES = "deletedInputFiles";

    /**
     * Key in attributes to contain the list of deleted files that were generated by a job.
     */
    public static final String JOB_DELETED_OUTPUT_FILES = "deletedOutputFiles";

    /**
     * Key in attributes to contain the deleted folder used by a job as the output directory.
     */
    public static final String JOB_DELETED_OUTPUT_DIRECTORY = "deletedOutputFiles";

}
