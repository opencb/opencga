package org.opencb.opencga.storage.core.alignment;

import org.opencb.opencga.storage.core.StorageManager;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.slf4j.LoggerFactory;

/**
 * Created by pfurio on 07/11/16.
 */
public abstract class AlignmentStorageManager extends StorageManager<AlignmentDBAdaptor> {

    public AlignmentStorageManager() {
        this.logger = LoggerFactory.getLogger(AlignmentStorageManager.class);
    }

    public AlignmentStorageManager(StorageConfiguration configuration) {
        super(configuration);
        this.logger = LoggerFactory.getLogger(AlignmentStorageManager.class);
    }

    public AlignmentStorageManager(String storageEngineId, StorageConfiguration configuration) {
        super(storageEngineId, configuration);
        this.logger = LoggerFactory.getLogger(AlignmentStorageManager.class);
    }

}
