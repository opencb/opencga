package org.opencb.opencga.storage.core.variant.dummy;

import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.metadata.BatchFileOperation;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageETL;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * Created on 28/11/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class DummyVariantStorageETL extends VariantStorageETL {

    private static final String MOCK_VARIANTS_LOAD_FAIL = "mock.variants.load.fail";

    public DummyVariantStorageETL(StorageConfiguration configuration, String storageEngineId, Logger logger, VariantDBAdaptor dbAdaptor, VariantReaderUtils variantReaderUtils) {
        super(configuration, storageEngineId, logger, dbAdaptor, variantReaderUtils);
    }

    @Override
    protected void securePreLoad(StudyConfiguration studyConfiguration, VariantSource source) throws StorageManagerException {
        super.securePreLoad(studyConfiguration, source);

        List<Integer> fileIds = getOptions().getAsIntegerList(VariantStorageManager.Options.FILE_ID.key());
        BatchFileOperation op = new BatchFileOperation("load", fileIds, 1, BatchFileOperation.Type.LOAD);
        op.addStatus(BatchFileOperation.Status.RUNNING);
        studyConfiguration.getBatches().add(op);
    }

    @Override
    public URI load(URI input) throws IOException, StorageManagerException {
        logger.info("Loading file " + input);
        List<Integer> fileIds = getOptions().getAsIntegerList(VariantStorageManager.Options.FILE_ID.key());
        if (getOptions().getBoolean(MOCK_VARIANTS_LOAD_FAIL)) {
            setStatus(BatchFileOperation.Status.ERROR, "load", fileIds);
        } else {
            setStatus(BatchFileOperation.Status.DONE, "load", fileIds);
        }
        return input;
    }

    @Override
    public URI postLoad(URI input, URI output) throws StorageManagerException {
        logger.info("Post load file " + input);
        return super.postLoad(input, output);
    }

    @Override
    public void securePostLoad(List<Integer> fileIds, StudyConfiguration studyConfiguration) throws StorageManagerException {
        super.securePostLoad(fileIds, studyConfiguration);
        BatchFileOperation.Status status = secureSetStatus(studyConfiguration, BatchFileOperation.Status.READY, "load", fileIds);
        if (status != BatchFileOperation.Status.DONE) {
            logger.warn("Unexpected status " + status);
        }
    }

    @Override
    protected void checkLoadedVariants(URI input, int fileId, StudyConfiguration studyConfiguration, ObjectMap options) throws StorageManagerException {

    }
}
