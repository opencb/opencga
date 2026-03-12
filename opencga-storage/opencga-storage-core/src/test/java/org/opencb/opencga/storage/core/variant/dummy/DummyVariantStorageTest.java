package org.opencb.opencga.storage.core.variant.dummy;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.config.storage.StorageEngineConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.VariantStorageTest;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotatorFactory;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotatorTest;

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
            storageConfiguration.getVariant().setDefaultEngine(DummyVariantStorageEngine.STORAGE_ENGINE_ID);
            storageConfiguration.getVariant().getEngines().add(new StorageEngineConfiguration()
                    .setId(DummyVariantStorageEngine.STORAGE_ENGINE_ID)
                    .setEngine(DummyVariantStorageEngine.class.getName())
                    .setOptions(new ObjectMap())
            );
            DummyVariantStorageEngine storageManager = new DummyVariantStorageEngine();
            storageManager.setConfiguration(storageConfiguration, DummyVariantStorageEngine.STORAGE_ENGINE_ID, VariantStorageBaseTest.DB_NAME);
            storageManager.getOptions().put(VariantStorageOptions.ANNOTATOR.key(), VariantAnnotatorFactory.AnnotationEngine.OTHER);
            storageManager.getOptions().put(VariantStorageOptions.ANNOTATOR_CLASS.key(), VariantAnnotatorTest.TestCachedCellBaseRestVariantAnnotator.class.getName());
            storageManager.getOptions().put(VariantStorageOptions.SPECIES.key(), "hsapiens");
            storageManager.getOptions().put(VariantStorageOptions.ASSEMBLY.key(), "GRCh37");
            storageManager.getConfiguration().getCellbase().setDataRelease("1");
            return storageManager;
        }
    }

    @Override
    default void clearDB(String dbName) throws Exception {
        DummyVariantStorageEngine.clear();
    }

}
