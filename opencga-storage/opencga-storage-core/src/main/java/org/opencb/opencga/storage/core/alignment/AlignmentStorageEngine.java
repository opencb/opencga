package org.opencb.opencga.storage.core.alignment;

import org.opencb.opencga.storage.core.StorageEngine;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.slf4j.LoggerFactory;

/**
 * Created by pfurio on 07/11/16.
 */
public abstract class AlignmentStorageEngine extends StorageEngine<AlignmentDBAdaptor> {

    public AlignmentStorageEngine() {
        this.logger = LoggerFactory.getLogger(AlignmentStorageEngine.class);
    }

    public AlignmentStorageEngine(StorageConfiguration configuration) {
        super(configuration);
        this.logger = LoggerFactory.getLogger(AlignmentStorageEngine.class);
    }

    public AlignmentStorageEngine(String storageEngineId, StorageConfiguration configuration) {
        super(storageEngineId, configuration);
        this.logger = LoggerFactory.getLogger(AlignmentStorageEngine.class);
    }

}
