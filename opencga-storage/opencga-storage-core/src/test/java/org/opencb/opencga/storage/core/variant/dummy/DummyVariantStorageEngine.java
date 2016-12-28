package org.opencb.opencga.storage.core.variant.dummy;

import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.ExportMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotator;
import org.opencb.opencga.storage.core.variant.io.VariantExporter;
import org.opencb.opencga.storage.core.variant.io.VariantImporter;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * Created on 28/11/16.
 *
 * TODO: Use Mockito
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class DummyVariantStorageEngine extends VariantStorageEngine {


    public static final String STORAGE_ENGINE_ID = "dummy";

    @Override
    public String getStorageEngineId() {
        return STORAGE_ENGINE_ID;
    }

    @Override
    public VariantDBAdaptor getDBAdaptor(String dbName) throws StorageEngineException {
        return new DummyVariantDBAdaptor(dbName);
    }

    @Override
    public DummyVariantStoragePipeline newStoragePipeline(boolean connected) throws StorageEngineException {
        return new DummyVariantStoragePipeline(getConfiguration(), STORAGE_ENGINE_ID, LoggerFactory.getLogger(DummyVariantStoragePipeline.class), getDBAdaptor(), getVariantReaderUtils());
    }

    @Override
    public VariantStatisticsManager newVariantStatisticsManager(VariantDBAdaptor dbAdaptor) {
        return super.newVariantStatisticsManager(dbAdaptor);
    }

    @Override
    public VariantExporter newVariantExporter(VariantDBAdaptor dbAdaptor) {
        return super.newVariantExporter(dbAdaptor);
    }

    @Override
    protected VariantImporter newVariantImporter(VariantDBAdaptor dbAdaptor) {
        return new VariantImporter(dbAdaptor) {
            @Override
            public void importData(URI input, ExportMetadata metadata) throws StorageEngineException, IOException {
            }
        };
    }

    @Override
    public VariantAnnotationManager newVariantAnnotationManager(VariantAnnotator annotator, VariantDBAdaptor dbAdaptor) {
        return super.newVariantAnnotationManager(annotator, dbAdaptor);
    }

    @Override
    public void dropFile(String study, int fileId) throws StorageEngineException {

    }

    @Override
    public void dropStudy(String studyName) throws StorageEngineException {

    }
}
