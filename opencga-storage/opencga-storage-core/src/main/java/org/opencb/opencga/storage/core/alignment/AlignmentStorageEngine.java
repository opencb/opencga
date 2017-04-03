package org.opencb.opencga.storage.core.alignment;

import org.opencb.opencga.storage.core.StorageEngine;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by pfurio on 07/11/16.
 */
public abstract class AlignmentStorageEngine extends StorageEngine<AlignmentDBAdaptor> {

    private Logger logger = LoggerFactory.getLogger(AlignmentStorageEngine.class);

    public AlignmentStorageEngine() {
    }

    public AlignmentStorageEngine(StorageConfiguration configuration) {
        super(configuration);
    }

    public AlignmentStorageEngine(String storageEngineId, StorageConfiguration configuration) {
        super(storageEngineId, configuration);
    }

}
