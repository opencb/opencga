package org.opencb.opencga.storage.core.variant.dummy;

import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageTest;

import java.io.InputStream;

/**
 * Created on 07/09/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface DummyVariantStorageTest extends VariantStorageTest {

    @Override
    default VariantStorageEngine getVariantStorageEngine() throws Exception {
        try (InputStream is = DummyVariantStorageEngine.class.getClassLoader().getResourceAsStream("storage-configuration.yml")) {
            StorageConfiguration storageConfiguration = StorageConfiguration.load(is);
            DummyVariantStorageEngine storageManager = new DummyVariantStorageEngine();
            storageManager.setConfiguration(storageConfiguration, DummyVariantStorageEngine.STORAGE_ENGINE_ID);
            return storageManager;
        }
    }

    @Override
    default void clearDB(String dbName) throws Exception {
        DummyProjectMetadataAdaptor.clear();
        DummyStudyMetadataDBAdaptor.clear();
    }

}
