package org.opencb.opencga.storage.core.alignment.local;

import org.opencb.opencga.storage.core.StoragePipeline;
import org.opencb.opencga.storage.core.alignment.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageEngine;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;

/**
 * Created by pfurio on 07/11/16.
 */
public class LocalAlignmentStorageEngine extends AlignmentStorageEngine {

    private AlignmentDBAdaptor dbAdaptor;
    private StoragePipeline storagePipeline;

    public LocalAlignmentStorageEngine() {
        super();
        this.storagePipeline = new LocalAlignmentStoragePipeline();
        this.dbAdaptor = new LocalAlignmentDBAdaptor();
    }

    @Override
    public AlignmentDBAdaptor getDBAdaptor() throws StorageEngineException {
        return dbAdaptor;
    }

    @Override
    public void testConnection() throws StorageEngineException {
    }

    @Override
    public StoragePipeline newStoragePipeline(boolean connected) throws StorageEngineException {
        return this.storagePipeline;
    }

}
