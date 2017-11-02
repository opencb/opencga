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

}
