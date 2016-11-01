package org.opencb.opencga.storage.core.alignment.local;

import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.storage.core.StorageETL;
import org.opencb.opencga.storage.core.alignment.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageETL;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageManager;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by pfurio on 31/10/16.
 */
public class DefaultAlignmentStorageManager extends AlignmentStorageManager {

    private Path workspace;

    public DefaultAlignmentStorageManager() {
    }

    public DefaultAlignmentStorageManager(String storageEngineId, StorageConfiguration configuration) {
        super(storageEngineId, configuration);
    }

    public DefaultAlignmentStorageManager(AlignmentStorageETL storageETL, AlignmentDBAdaptor dbAdaptor,
                                          StorageConfiguration configuration, Path workspace) throws IOException {
        super(storageETL, dbAdaptor, configuration);
        FileUtils.checkDirectory(workspace, true);
        this.workspace = workspace;
    }

    @Override
    public AlignmentDBAdaptor getDBAdaptor(String dbName) throws StorageManagerException {
        if (this.dbAdaptor == null) {
            this.dbAdaptor = new DefaultAlignmentDBAdaptor();
        }
        return this.dbAdaptor;
    }

    @Override
    public void testConnection() throws StorageManagerException {
    }

    @Override
    public StorageETL newStorageETL(boolean connected) throws StorageManagerException {
        if (this.storageETL == null) {
            this.storageETL = new DefaultAlignmentStorageETL(getDBAdaptor(), Paths.get("/tmp"));
        }
        return this.storageETL;
    }
}
