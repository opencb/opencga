package org.opencb.opencga.storage.core.variant.index.sample.local;

import org.opencb.biodata.models.core.Region;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.utils.iterators.CloseableIterator;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.index.sample.*;
import org.opencb.opencga.storage.core.variant.index.sample.family.FamilyIndexLoader;
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
    public SampleIndexBuilder newSampleIndexBuilder(VariantStorageEngine engine) throws StorageEngineException {
        return null;
    }

    @Override
    public SampleIndexAnnotationLoader newSampleIndexAnnotationLoader(VariantStorageEngine engine) throws StorageEngineException {
        return null;
    }

    @Override
    public FamilyIndexLoader newSampleIndexFamilyLoader(VariantStorageEngine engine) throws StorageEngineException {
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
