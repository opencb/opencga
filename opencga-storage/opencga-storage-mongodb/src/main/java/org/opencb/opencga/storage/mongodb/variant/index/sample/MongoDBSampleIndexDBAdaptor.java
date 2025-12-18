package org.opencb.opencga.storage.mongodb.variant.index.sample;

import com.google.common.collect.Iterators;
import com.mongodb.client.model.*;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.utils.iterators.CloseableIterator;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.index.sample.SampleIndexDBAdaptor;
import org.opencb.opencga.storage.core.variant.index.sample.annotation.SampleAnnotationIndexer;
import org.opencb.opencga.storage.core.variant.index.sample.family.SampleFamilyIndexer;
import org.opencb.opencga.storage.core.variant.index.sample.genotype.SampleGenotypeIndexer;
import org.opencb.opencga.storage.core.variant.index.sample.genotype.SampleIndexEntryBuilder;
import org.opencb.opencga.storage.core.variant.index.sample.genotype.SampleIndexWriter;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexVariant;
import org.opencb.opencga.storage.core.variant.index.sample.query.SingleSampleIndexQuery;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;
import org.opencb.opencga.storage.mongodb.variant.index.sample.annotation.MongoDBSampleAnnotationIndexer;

import java.io.IOException;
import java.util.*;

public class MongoDBSampleIndexDBAdaptor extends SampleIndexDBAdaptor {

    private final MongoDataStore dataStore;
    private final DocumentToSampleIndexEntryConverter converter = new DocumentToSampleIndexEntryConverter();

    public MongoDBSampleIndexDBAdaptor(MongoDataStore dataStore, VariantStorageMetadataManager metadataManager) {
        super(metadataManager);
        this.dataStore = dataStore;
    }

    @Override
    public SampleGenotypeIndexer newSampleGenotypeIndexer(VariantStorageEngine engine) throws StorageEngineException {
        return null;
    }

    @Override
    public SampleIndexWriter newSampleIndexWriter(int studyId, int fileId, List<Integer> sampleIds,
            SampleIndexSchema schema,
            ObjectMap options, VariantStorageEngine.SplitData splitData)
            throws StorageEngineException {
        return new MongoDBSampleIndexWriter(this, getMetadataManager(), studyId, fileId, sampleIds, schema, options,
                splitData);
    }

    @Override
    public SampleAnnotationIndexer newSampleAnnotationIndexer(VariantStorageEngine engine)
            throws StorageEngineException {
        return new MongoDBSampleAnnotationIndexer(this, engine);
    }

    @Override
    public SampleFamilyIndexer newSampleFamilyIndexer(VariantStorageEngine engine) throws StorageEngineException {
        return null;
    }

    @Override
    public SampleIndexEntryBuilder queryByGtBuilder(int study, int sample, String chromosome, int position,
            SampleIndexSchema schema)
            throws IOException {
        MongoDBCollection collection = getCollection(study, schema.getVersion());
        Document document = collection.find(Filters.eq("_id",
                        DocumentToSampleIndexEntryConverter.buildDocumentId(sample, chromosome, position)), new QueryOptions())
                .first();
        Map<String, TreeSet<SampleIndexVariant>> map;
        if (document == null) {
            map = new HashMap<>();
        } else {
            map = converter.convertToGtVariantMap(document, schema);
        }
        return new SampleIndexEntryBuilder(sample, chromosome, position, schema, map);
    }

    private MongoDBCollection getCollection(int studyId, int version) {
        return dataStore.getCollection(getSampleIndexCollectionName(studyId, version));
    }

    @Override
    protected VariantDBIterator internalIterator(SingleSampleIndexQuery query, SampleIndexSchema schema) {
//        return new MongoDBSingleSampleIndexVariantIterator(query, schema, this, false);
        return null;
    }

    @Override
    protected CloseableIterator<SampleIndexVariant> rawInternalIterator(SingleSampleIndexQuery query,
            SampleIndexSchema schema) {
//        return new MongoDBSingleSampleIndexVariantIterator(query, schema, this, true);
        return null;
    }

    @Override
    public CloseableIterator<SampleIndexEntry> rawIterator(int study, int sample, Region region, SampleIndexSchema schema)
            throws IOException {
        MongoDBCollection collection = getCollection(study, schema.getVersion());
        List<Bson> filters = new ArrayList<>();
        filters.add(Filters.eq(DocumentToSampleIndexEntryConverter.SAMPLE_ID, sample));
        if (region != null) {
            filters.add(Filters.eq(DocumentToSampleIndexEntryConverter.CHROMOSOME, region.getChromosome()));
            filters.add(Filters.gte(DocumentToSampleIndexEntryConverter.BATCH_START, region.getStart()));
            filters.add(Filters.lte(DocumentToSampleIndexEntryConverter.BATCH_START, region.getEnd()));
        }
        MongoDBIterator<Document> mongoIterator = collection.iterator(Filters.and(filters), new QueryOptions(QueryOptions.SORT, "_id"));
        Iterator<SampleIndexEntry> iterator = Iterators.transform(mongoIterator, converter::convertToDataModelType);
        return CloseableIterator.wrap(iterator, mongoIterator);
    }

    @Override
    protected long count(SingleSampleIndexQuery query) {
        return 0;
    }

    public MongoDBCollection createCollectionIfNeeded(int studyId, int version) {
        MongoDBCollection collection = dataStore.getCollection(getSampleIndexCollectionName(studyId, version));
        // TODO: Create indexes if needed
        return collection;
    }

    private String getSampleIndexCollectionName(int studyId, int version) {
        return "sample_index_" + studyId + "_v" + version;
    }

    public void writeEntry(MongoDBCollection collection, SampleIndexEntry sampleIndexEntry) {
        Pair<String, Bson> pair = toUpsertBson(sampleIndexEntry);
        writeEntry(collection, pair);
    }

    public void writeEntry(MongoDBCollection collection, Pair<String, Bson> pair) {
        String documentId = pair.getLeft();
        Bson update = pair.getRight();
        if (update == null) {
            return;
        }
        collection.update(Filters.eq("_id", documentId), update,
                new QueryOptions(MongoDBCollection.UPSERT, true));
    }

    public void writeEntries(MongoDBCollection collection, List<Pair<String, Bson>> entries) {
        List<Bson> queries = new ArrayList<>(entries.size());
        List<Bson> updates = new ArrayList<>(entries.size());
        for (Pair<String, Bson> pair : entries) {
            queries.add(Filters.eq("_id", pair.getLeft()));
            updates.add(pair.getRight());
        }
        collection.update(queries, updates,
                new QueryOptions(MongoDBCollection.UPSERT, true));
    }

    public Pair<String, Bson> toUpsertBson(SampleIndexEntry sampleIndexEntry) {
        return toUpdateBson(sampleIndexEntry, true);
    }

    public Pair<String, Bson> toUpdateBson(SampleIndexEntry sampleIndexEntry) {
        return toUpdateBson(sampleIndexEntry, false);
    }

    public Pair<String, Bson> toUpdateBson(SampleIndexEntry sampleIndexEntry, boolean includeMainFields) {
        Document document = converter.convertToStorageType(sampleIndexEntry);

        List<Bson> updates = new ArrayList<>();
        for (Map.Entry<String, Object> entry : document.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if ("_id".equals(key)) {
                continue;
            }
            if (value != null) {
                if (includeMainFields || key.startsWith(DocumentToSampleIndexEntryConverter.KEY_PREFIX_GT)) {
                    updates.add(Updates.set(key, value));
                }
            }
        }
        Bson update;
        if (updates.isEmpty()) {
            update = null;
        } else {
            update = updates.size() == 1 ? updates.get(0) : Updates.combine(updates);
        }
        return Pair.of(document.getString("_id"), update);
    }

}
