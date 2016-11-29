package org.opencb.opencga.storage.core.variant.dummy;

import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.variant.VariantStorageETL;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.slf4j.LoggerFactory;

/**
 * Created on 28/11/16.
 *
 * TODO: Use Mockito
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class DummyVariantStorageManager extends VariantStorageManager {


    public static final String STORAGE_ENGINE_ID = "mock";

    @Override
    public VariantDBAdaptor getDBAdaptor(String dbName) throws StorageManagerException {
        return new DummyVariantDBAdaptor(dbName);
    }

    @Override
    public VariantStorageETL newStorageETL(boolean connected) throws StorageManagerException {
        return new DummyVariantStorageETL(getConfiguration(), STORAGE_ENGINE_ID, LoggerFactory.getLogger(DummyVariantStorageETL.class), getDBAdaptor(), getVariantReaderUtils());
    }

    @Override
    public void dropFile(String study, int fileId) throws StorageManagerException {

    }

    @Override
    public void dropStudy(String studyName) throws StorageManagerException {

    }
}
