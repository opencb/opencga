package org.opencb.opencga.catalog.utils;

public class Constants {

    // Variable constants for versioning
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

    // Variable constants for annotations
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

    /**
     * Boolean indicating if the annotations have to be returned flattened or not. Default: false
     */
    public static final String FLATTENED_ANNOTATIONS = "flattenedAnnotations";
}
