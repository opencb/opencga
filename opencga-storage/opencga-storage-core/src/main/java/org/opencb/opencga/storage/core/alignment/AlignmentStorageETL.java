package org.opencb.opencga.storage.core.alignment;

import org.opencb.opencga.storage.core.StorageETL;

/**
 * Created by pfurio on 31/10/16.
 */
@Deprecated
public abstract class AlignmentStorageETL implements StorageETL {

    protected static final int MINOR_CHUNK_SIZE = 1000;
    protected static final String COVERAGE_DATABASE_NAME = "coverage.db";

}
