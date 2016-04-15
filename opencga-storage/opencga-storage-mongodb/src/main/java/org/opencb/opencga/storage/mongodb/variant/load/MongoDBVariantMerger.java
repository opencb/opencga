package org.opencb.opencga.storage.mongodb.variant.load;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantWriteResult;
import org.opencb.opencga.storage.mongodb.variant.VariantMongoDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToSamplesConverter;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyVariantEntryConverter;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToSamplesConverter.UNKNOWN_GENOTYPE;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyVariantEntryConverter.*;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter.STUDIES_FIELD;

/**
 * Created on 07/04/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoDBVariantMerger implements ParallelTaskRunner.Task<Document, MongoDBVariantWriteResult> {

    public static final QueryOptions QUERY_OPTIONS = new QueryOptions();
    private final MongoDBCollection collection;
    private final Integer studyId;
    private final List<Integer> fileIds;
    private Future<Long> futureNumTotalVariants = null;
    private long numTotalVariants;
    private final DocumentToVariantConverter variantConverter;
    private final DocumentToStudyVariantEntryConverter studyConverter;
    private final StudyConfiguration studyConfiguration;

    private final MongoDBVariantWriteResult result;
    private final Map<Integer, LinkedHashMap<String, Integer>> samplesPositionMap;
    private LinkedList<Integer> indexedSamples;

    private final AtomicInteger variantsCount;
    public static final int DEFAULT_LOGING_BATCH_SIZE = 500;
    private long loggingBatchSize;
    private final Logger logger = LoggerFactory.getLogger(MongoDBVariantStageLoader.class);


    public MongoDBVariantMerger(StudyConfiguration sc, List<Integer> fileIds, MongoDBCollection collection,
                                long numTotalVariants) {
        this.collection = collection;
        this.fileIds = fileIds;
        this.numTotalVariants = numTotalVariants;
        studyId = sc.getStudyId();

        Objects.requireNonNull(sc);

        studyConfiguration = sc;
        DocumentToSamplesConverter samplesConverter = new DocumentToSamplesConverter(studyConfiguration);
        studyConverter = new DocumentToStudyVariantEntryConverter(false, samplesConverter);
        variantConverter = new DocumentToVariantConverter(studyConverter, null);
        result = new MongoDBVariantWriteResult();
        samplesPositionMap = new HashMap<>();
        variantsCount = new AtomicInteger(0);
        loggingBatchSize = Math.max(numTotalVariants / 200, DEFAULT_LOGING_BATCH_SIZE);
    }

    public MongoDBVariantMerger(StudyConfiguration sc, List<Integer> fileIds, MongoDBCollection collection,
                                Future<Long> futureNumTotalVariants) {
        this.collection = collection;
        this.fileIds = fileIds;
        this.futureNumTotalVariants = futureNumTotalVariants;
        studyId = sc.getStudyId();

        Objects.requireNonNull(sc);

        studyConfiguration = sc;
        DocumentToSamplesConverter samplesConverter = new DocumentToSamplesConverter(studyConfiguration);
        studyConverter = new DocumentToStudyVariantEntryConverter(false, samplesConverter);
        variantConverter = new DocumentToVariantConverter(studyConverter, null);
        result = new MongoDBVariantWriteResult();
        samplesPositionMap = new HashMap<>();
        variantsCount = new AtomicInteger(0);
        this.numTotalVariants = 0;
        loggingBatchSize = DEFAULT_LOGING_BATCH_SIZE;
    }

    public MongoDBVariantWriteResult getResult() {
        return result;
    }

    @Override
    public List<MongoDBVariantWriteResult> apply(List<Document> batch) {
        return Collections.singletonList(load(batch));
    }

    @Override
    public void post() {
        VariantMongoDBAdaptor.createIndexes(new QueryOptions(), collection);
    }

    public MongoDBVariantWriteResult load(List<Document> variants) {
        return load(variants.stream());
    }

    public MongoDBVariantWriteResult load(Stream<Document> variants) {

        List<Document> inserts = new LinkedList<>();
        List<Bson> queriesExisting = new LinkedList<>();
        List<String> queriesExistingId = new LinkedList<>();
        List<Bson> updatesExisting = new LinkedList<>();
        List<Bson> queriesFillGaps = new LinkedList<>();
        List<String> queriesFillGapsId = new LinkedList<>();
        List<Bson> updatesFillGaps = new LinkedList<>();
        final AtomicInteger skipped = new AtomicInteger();
        final AtomicInteger nonInserted = new AtomicInteger();

        variants.forEach(document -> {
            int size = document.size();
            Variant emptyVar = MongoDBVariantStageLoader.STRING_ID_CONVERTER.convertToDataModelType(document.getString("_id"));
            Document study = document.get(Integer.toString(studyId), Document.class);
            if (study != null) {
                boolean newStudy = study.getBoolean("new", true);
                boolean newVariant = newStudy && size == 2;

                List<Document> fileDocuments = new LinkedList<>();
                Document gts = new Document();

                for (Integer fileId : fileIds) {

                    if (study.containsKey(fileId.toString())) {
                        List duplicatedVariants = study.get(fileId.toString(), List.class);
                        if (duplicatedVariants.size() > 1) {
                            nonInserted.addAndGet(duplicatedVariants.size());
                            addSampleIdsGenotypes(gts, UNKNOWN_GENOTYPE, getSamplesInFile(fileId));
                            continue;
                        }

                        Binary file = ((Binary) duplicatedVariants.get(0));
                        Variant variant = MongoDBVariantStageLoader.VARIANT_CONVERTER_DEFAULT.convertToDataModelType(file);
                        if (variant.getType().equals(VariantType.NO_VARIATION) || variant.getType().equals(VariantType.SYMBOLIC)) {
                            skipped.incrementAndGet();
                            continue;
                        }
                        variant.getStudies().get(0).setSamplesPosition(getSamplesPosition(fileId));
                        Document newDocument = studyConverter.convertToStorageType(variant.getStudies().get(0));

                        fileDocuments.add((Document) newDocument.get(FILES_FIELD, List.class).get(0));

                        for (Map.Entry<String, Object> entry : newDocument.get(GENOTYPES_FIELD, Document.class).entrySet()) {
                            addSampleIdsGenotypes(gts, entry.getKey(), (List<Integer>) entry.getValue());
                        }

//                        queriesExisting.add(Filters.and(Filters.eq("_id", variantConverter.buildStorageId(variant)),
//                                Filters.eq(STUDIES_FIELD + "." + STUDYID_FIELD, studyId)));
//                        updatesExisting.add(Updates.combine(mergeUpdates));

                    } else {
                        addSampleIdsGenotypes(gts, UNKNOWN_GENOTYPE, getSamplesInFile(fileId));
                    }

                }

                if (newVariant) {
                    Document variantDocument = variantConverter.convertToStorageType(emptyVar);
                    variantDocument.append(STUDIES_FIELD,
                            Collections.singletonList(
                                    new Document(STUDYID_FIELD, studyId)
                                            .append(FILES_FIELD, fileDocuments)
                                            .append(GENOTYPES_FIELD, gts)
                            )
                    );

                    inserts.add(variantDocument);
                } else if (newStudy) {
                    Document studyDocument = new Document(STUDYID_FIELD, studyId)
                            .append(FILES_FIELD, fileDocuments)
                            .append(GENOTYPES_FIELD, gts);

                    queriesExisting.add(Filters.eq("_id", variantConverter.buildStorageId(emptyVar)));
                    updatesExisting.add(Updates.push(STUDIES_FIELD, studyDocument));

                } else {
                    queriesExisting.add(Filters.and(Filters.eq("_id", variantConverter.buildStorageId(emptyVar)),
                            Filters.eq(STUDIES_FIELD + "." + STUDYID_FIELD, studyId)));

                    List<Bson> mergeUpdates = new LinkedList<>();

                    for (String gt : gts.keySet()) {
                        mergeUpdates.add(Updates.pushEach(STUDIES_FIELD + ".$." + GENOTYPES_FIELD + "." + gt,
                                gts.get(gt, List.class)));
                    }
                    mergeUpdates.add(Updates.pushEach(STUDIES_FIELD + ".$." + FILES_FIELD, fileDocuments));
                    updatesExisting.add(Updates.combine(mergeUpdates));
                }
            }

        });


        long newVariants = -System.nanoTime();
        if (!inserts.isEmpty()) {
            BulkWriteResult writeResult = collection.insert(inserts, QUERY_OPTIONS).first();
            if (writeResult.getInsertedCount() != inserts.size()) {
                logger.error("(Inserts = " + inserts.size() + ") != (InsertedCount = " + writeResult.getInsertedCount() + " )");
                for (Document insert : inserts) {
                    Long count = collection.count(Filters.eq("_id", insert.get("_id"))).first();
                    if (count != 1) {
                        logger.error("Missing insert " + insert.get("_id"));
                    }
                }
            }
        }
        newVariants += System.nanoTime();
        long existingVariants = -System.nanoTime();
        if (!queriesExisting.isEmpty()) {
            QueryResult<BulkWriteResult> update = collection.update(queriesExisting, updatesExisting, QUERY_OPTIONS);
            if (update.first().getModifiedCount() != queriesExisting.size()) {
                logger.error("(Updated existing variants = " + queriesExisting.size() + " ) != "
                        + " (UpdatedCount = " + update.first().getModifiedCount() + "). MatchedCount:" + update.first().getMatchedCount());
                onError("", update, queriesExisting, queriesExistingId);
            }
        }
        existingVariants += System.nanoTime();
        long fillGapsVariants = -System.nanoTime();
        if (!queriesFillGaps.isEmpty()) {
            QueryResult<BulkWriteResult> update = collection.update(queriesFillGaps, updatesFillGaps, QUERY_OPTIONS);
            if (update.first().getModifiedCount() != queriesFillGaps.size()) {
                onError("fillGaps", update, queriesFillGaps, queriesFillGapsId);
            }
        }
        fillGapsVariants += System.nanoTime();

        MongoDBVariantWriteResult writeResult = new MongoDBVariantWriteResult(inserts.size(), updatesExisting.size(), skipped.get(),
                nonInserted.get(), newVariants, existingVariants, fillGapsVariants);
        synchronized (result) {
            result.merge(writeResult);
        }

        int processedVariants = queriesExisting.size() + inserts.size();
        logProgress(processedVariants);

        return writeResult;

    }

    public void logProgress(int processedVariants) {
        if (numTotalVariants <= 0) {
            try {
                if (futureNumTotalVariants != null && futureNumTotalVariants.isDone()) {
                    numTotalVariants = futureNumTotalVariants.get();
                    loggingBatchSize = Math.max(numTotalVariants / 200, DEFAULT_LOGING_BATCH_SIZE);
                }
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        int previousCount = variantsCount.getAndAdd(processedVariants);
        if ((previousCount + processedVariants) / loggingBatchSize != previousCount / loggingBatchSize) {
            logger.info("Write variants in VARIANTS collection " + (previousCount + processedVariants) + "/" + numTotalVariants + " "
                    + String.format("%.2f%%", ((float) (previousCount + processedVariants)) / numTotalVariants * 100.0));
        }
    }

    public void addSampleIdsGenotypes(Document gts, String genotype, Collection<Integer> sampleIds) {
        if (gts.containsKey(genotype)) {
            gts.get(genotype, List.class).addAll(sampleIds);
        } else {
            gts.put(genotype, new LinkedList<>(sampleIds));
        }
    }

    public void onError(String updateName, QueryResult<BulkWriteResult> update, List<Bson> queries, List<String> queryIds) {
        logger.error("(Updated " + updateName + " variants = " + queries.size() + " ) != "
                + "(UpdatedCount = " + update.first().getModifiedCount() + "). MatchedCount:" + update.first().getMatchedCount());

        for (QueryResult<Document> r : collection.find(queries, null)) {
            if (!r.getResult().isEmpty()) {
                String id = r.first().get("_id", String.class);
                queryIds.remove(id);
            }
        }
        for (String id : queryIds) {
            logger.error("Missing Variant " + id);
        }
    }

    public LinkedList<Integer> getIndexedSamples() {
        if (indexedSamples == null) {
            indexedSamples = new LinkedList<>(StudyConfiguration.getIndexedSamples(studyConfiguration).values());
            indexedSamples.sort(Integer::compareTo);
        }
        return indexedSamples;
    }

    public LinkedHashSet<Integer> getSamplesInFile(Integer fileId) {
        return studyConfiguration.getSamplesInFiles().get(fileId);
    }

    public LinkedHashMap<String, Integer> getSamplesPosition(Integer fileId) {
        LinkedHashMap<String, Integer> samplesPosition;
        if (!samplesPositionMap.containsKey(fileId)) {
            samplesPosition = new LinkedHashMap<>();
            for (Integer sampleId : studyConfiguration.getSamplesInFiles().get(fileId)) {
                samplesPosition.put(studyConfiguration.getSampleIds().inverse().get(sampleId), samplesPosition.size());
            }
        } else {
            samplesPosition = samplesPositionMap.get(fileId);
        }
        return samplesPosition;
    }

}
