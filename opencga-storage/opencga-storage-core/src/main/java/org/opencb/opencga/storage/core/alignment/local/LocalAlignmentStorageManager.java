package org.opencb.opencga.storage.core.alignment.local;

import org.opencb.opencga.storage.core.StorageETL;
import org.opencb.opencga.storage.core.alignment.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageManager;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;

/**
 * Created by pfurio on 07/11/16.
 */
public class LocalAlignmentStorageManager extends AlignmentStorageManager {

    private AlignmentDBAdaptor dbAdaptor;
    private StorageETL storageETL;

    public LocalAlignmentStorageManager() {
        super();
        this.storageETL = new DefaultAlignmentStorageETL();
        this.dbAdaptor = new DefaultAlignmentDBAdaptor();
    }

    @Override
    public AlignmentDBAdaptor getDBAdaptor(String dbName) throws StorageManagerException {
        return dbAdaptor;
    }

    @Override
    public void testConnection() throws StorageManagerException {
    }

    @Override
    public StorageETL newStorageETL(boolean connected) throws StorageManagerException {
        return this.storageETL;
    }

}
