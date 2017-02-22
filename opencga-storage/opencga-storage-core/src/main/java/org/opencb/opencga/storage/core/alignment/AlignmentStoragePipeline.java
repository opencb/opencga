package org.opencb.opencga.storage.core.alignment;

import org.opencb.opencga.storage.core.StoragePipeline;

/**
 * Created by pfurio on 31/10/16.
 */
@Deprecated
public abstract class AlignmentStoragePipeline implements StoragePipeline {

    protected static final int MINOR_CHUNK_SIZE = 1000;
    protected static final String COVERAGE_DATABASE_NAME = "coverage.db";

}
