package org.opencb.opencga.storage.mongodb.variant.index.sample.annotation;

import org.apache.commons.lang3.tuple.Pair;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.SampleEntry;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.index.sample.annotation.SampleAnnotationIndexer;
import org.opencb.opencga.storage.core.variant.index.sample.annotation.SampleIndexVariantAnnotationBuilder;
import org.opencb.opencga.storage.core.variant.index.sample.annotation.SampleIndexVariantAnnotationConverter;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexVariantAnnotation;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;
import org.opencb.opencga.storage.mongodb.variant.index.sample.DocumentToSampleIndexEntryConverter;
import org.opencb.opencga.storage.mongodb.variant.index.sample.MongoDBSampleIndexDBAdaptor;

import java.util.*;

public class MongoDBSampleAnnotationIndexer extends SampleAnnotationIndexer {

    private final VariantStorageEngine engine;
    private final MongoDBSampleIndexDBAdaptor mongoAdaptor;
    private final DocumentToSampleIndexEntryConverter converter = new DocumentToSampleIndexEntryConverter();

    public MongoDBSampleAnnotationIndexer(MongoDBSampleIndexDBAdaptor sampleIndexDBAdaptor,
            VariantStorageEngine engine) {
        super(sampleIndexDBAdaptor);
        this.engine = engine;
        this.mongoAdaptor = sampleIndexDBAdaptor;
    }

    @Override
    protected void runBatch(int studyId, List<Integer> samples, int sampleIndexVersion, ObjectMap options)
            throws StorageEngineException {
        List<String> sampleNames = samplesAsNames(studyId, samples);
        String studyName = metadataManager.getStudyName(studyId);
        SampleIndexSchema schema = sampleIndexDBAdaptor.getSchemaFactory()
                .getSchemaForVersion(studyId, sampleIndexVersion);
        SampleIndexVariantAnnotationConverter annotationConverter = new SampleIndexVariantAnnotationConverter(schema);
        MongoDBCollection collection = mongoAdaptor.createCollectionIfNeeded(studyId, sampleIndexVersion);

        Map<String, Set<Integer>> chromosomeToBatches = loadExistingBatches(studyId, sampleIndexVersion, samples);
        if (chromosomeToBatches.isEmpty()) {
            return;
        }

        ProgressLogger progressLogger = new ProgressLogger("Annotating sample index",
                chromosomeToBatches.values().stream().mapToInt(Set::size).sum());

        QueryOptions variantOptions = new QueryOptions()
                .append(QueryOptions.INCLUDE, Arrays.asList(VariantField.ANNOTATION, VariantField.STUDIES_SAMPLES))
                .append(QueryOptions.SORT, true)
                .append("quiet", false);

        Map<Integer, Map<String, SampleIndexVariantAnnotationBuilder>> builders = new HashMap<>();
        for (Map.Entry<String, Set<Integer>> entry : chromosomeToBatches.entrySet()) {
            String chromosome = entry.getKey();
            for (Integer batchStart : entry.getValue()) {
                Region region = new Region(chromosome, batchStart, batchStart + SampleIndexSchema.BATCH_SIZE - 1);
//                Map<Integer, SampleIndexEntry> batchEntries = readEntries(studyId, sampleIndexVersion, samples,
//                        chromosome, batchStart);
//                if (batchEntries.isEmpty()) {
//                    continue;
//                }

                VariantQuery variantQuery = new VariantQuery()
                        .study(studyName)
                        .includeGenotype(true)
                        .region(region);

                if (samples.size() < 50) {
                    variantQuery.put(VariantQueryParam.SAMPLE.key(), sampleNames);
                } else {
                    variantQuery.includeSample(sampleNames);
                }

                try (VariantDBIterator iterator = engine.getDBAdaptor().iterator(variantQuery, variantOptions)) {
                    while (iterator.hasNext()) {
                        Variant variant = iterator.next();
                        SampleIndexVariantAnnotation annotation = annotationConverter.convert(variant.getAnnotation());

                        for (int i = 0; i < samples.size(); i++) {
                            Integer sampleId = samples.get(i);
                            SampleEntry sampleEntry = variant.getStudies().get(0).getSamples().get(i);
                            String gt = sampleEntry.getData().get(0);

                            boolean validGt;
                            if (gt == null || gt.isEmpty()) {
                                gt = GenotypeClass.NA_GT_VALUE;
                                validGt = true;
                            } else {
                                validGt = SampleIndexSchema.isAnnotatedGenotype(gt);
                            }
                            if (validGt) {
                                SampleIndexVariantAnnotationBuilder builder = builders
                                        .computeIfAbsent(sampleId, k -> new HashMap<>())
                                        .computeIfAbsent(gt, g -> new SampleIndexVariantAnnotationBuilder(schema));
                                builder.add(annotation);
                            }
                        }
//                        for (Integer sampleId : batchEntries.keySet()) {
//                            SampleIndexEntry entryValue = batchEntries.get(sampleId);
//                            Map<String, SampleIndexVariantAnnotationBuilder> gtBuilders = builders
//                                    .computeIfAbsent(sampleId, k -> new HashMap<>());
//                            for (String gt : entryValue.getGts().keySet()) {
//                                SampleIndexVariantAnnotationBuilder builder = gtBuilders.computeIfAbsent(gt,
//                                        g -> new SampleIndexVariantAnnotationBuilder(schema));
//                                builder.add(annotation);
//                            }
//                        }
                    }
                } catch (Exception e) {
                    throw new StorageEngineException("Error iterating variants for region " + region, e);
                }

                for (Map.Entry<Integer, Map<String, SampleIndexVariantAnnotationBuilder>> sampleBuilders : builders
                        .entrySet()) {
                    Integer sampleId = sampleBuilders.getKey();
                    Map<String, SampleIndexVariantAnnotationBuilder> buildersMap = sampleBuilders.getValue();

                    SampleIndexEntry sampleIndexEntry = new SampleIndexEntry(sampleId, chromosome, batchStart);
                    for (Map.Entry<String, SampleIndexVariantAnnotationBuilder> builderEntry : buildersMap.entrySet()) {
                        String gt = builderEntry.getKey();
                        SampleIndexVariantAnnotationBuilder builder = builderEntry.getValue();
                        if (!builder.isEmpty()) {
                            sampleIndexEntry.addGtEntry(builder.buildAndReset(gt));
                        }
                    }

//                    for (Map.Entry<String, SampleIndexVariantAnnotationBuilder> builderEntry : buildersMap
//                            .entrySet()) {
//                        SampleIndexEntry.SampleIndexGtEntry gtEntry = entryValue.getGtEntry(builderEntry.getKey());
//                        if (gtEntry != null) {
//                            builderEntry.getValue().buildAndReset(gtEntry);
//                        }
//                    }
                    if (!sampleIndexEntry.getGts().isEmpty()) {
                        mongoAdaptor.writeEntry(collection, sampleIndexEntry);
                    }
                }
                progressLogger.increment(1, () -> " [" + region + "]");
            }
        }
    }

    private Map<String, Set<Integer>> loadExistingBatches(int studyId, int version, List<Integer> samples) {
        Map<String, Set<Integer>> batched = new LinkedHashMap<>();
        MongoDBCollection collection = mongoAdaptor.createCollectionIfNeeded(studyId, version);
        for (Integer sampleId : samples) {
            QueryOptions projection = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList("chromosome", "batchStart"));
            try (MongoDBIterator<Document> iterator = collection.nativeQuery().find(new Document("sampleId", sampleId),
                    projection)) {
                while (iterator.hasNext()) {
                    Document doc = iterator.next();
                    batched.computeIfAbsent(doc.getString("chromosome"), chr -> new TreeSet<>())
                            .add(doc.getInteger("batchStart"));
                }
            }
        }
        return batched;
    }

    private Map<Integer, SampleIndexEntry> readEntries(int studyId, int version, List<Integer> samples,
            String chromosome, int batchStart) {
        MongoDBCollection collection = mongoAdaptor.createCollectionIfNeeded(studyId, version);
        Map<Integer, SampleIndexEntry> entries = new HashMap<>();
        for (Integer sampleId : samples) {
            String id = DocumentToSampleIndexEntryConverter.buildDocumentId(sampleId, chromosome, batchStart);
            Document doc = collection.find(new Document("_id", id), QueryOptions.empty()).first();
            if (doc != null) {
                entries.put(sampleId, converter.convertToDataModelType(doc));
            }
        }
        return entries;
    }

    private List<String> samplesAsNames(int studyId, List<Integer> sampleIds) {
        List<String> names = new ArrayList<>(sampleIds.size());
        for (Integer sampleId : sampleIds) {
            names.add(metadataManager.getSampleName(studyId, sampleId));
        }
        return names;
    }
}
