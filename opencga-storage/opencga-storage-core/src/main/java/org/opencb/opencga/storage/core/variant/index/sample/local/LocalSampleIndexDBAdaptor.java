package org.opencb.opencga.storage.core.variant.index.sample.local;

import org.opencb.biodata.models.core.Region;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.utils.iterators.CloseableIterator;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.index.sample.*;
import org.opencb.opencga.storage.core.variant.index.sample.annotation.SampleIndexAnnotationConstructor;
import org.opencb.opencga.storage.core.variant.index.sample.family.FamilyIndexConstructor;
import org.opencb.opencga.storage.core.variant.index.sample.file.SampleIndexConstructor;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexVariant;
import org.opencb.opencga.storage.core.variant.index.sample.query.SingleSampleIndexQuery;

import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;

import java.io.IOException;
import java.nio.file.Path;

public class LocalSampleIndexDBAdaptor extends SampleIndexDBAdaptor {

    private final Path basePath;

    public LocalSampleIndexDBAdaptor(VariantStorageMetadataManager metadataManager, Path basePath) {
        super(metadataManager);
        this.basePath = basePath;
    }

    @Override
    public SampleIndexConstructor newSampleIndexConstructor(VariantStorageEngine engine) throws StorageEngineException {
        return null;
    }

    @Override
    public SampleIndexAnnotationConstructor newSampleIndexAnnotationConstructor(VariantStorageEngine engine) throws StorageEngineException {
        return null;
    }

    @Override
    public FamilyIndexConstructor newSampleIndexFamilyConstructor(VariantStorageEngine engine) throws StorageEngineException {
        return null;
    }

    @Override
    protected VariantDBIterator internalIterator(SingleSampleIndexQuery query, SampleIndexSchema schema) {
        return null;
    }

    @Override
    protected CloseableIterator<SampleIndexVariant> rawInternalIterator(SingleSampleIndexQuery query, SampleIndexSchema schema) {
        return null;
    }

    @Override
    public CloseableIterator<SampleIndexEntry> rawIterator(int study, int sample, Region region,
                                                           SampleIndexSchema schema) throws IOException {
        return null;
    }

    @Override
    protected long count(SingleSampleIndexQuery query) {
        return 0;
    }
}
