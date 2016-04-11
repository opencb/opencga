package org.opencb.opencga.storage.mongodb.variant.load;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantWriteResult;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToSamplesConverter;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyVariantEntryConverter;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToSamplesConverter.*;
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
    private final int numTotalVariants;
    private final DocumentToVariantConverter variantConverter;
    private final DocumentToStudyVariantEntryConverter studyConverter;
    private final StudyConfiguration studyConfiguration;

    private final MongoDBVariantWriteResult result;
    private final Map<Integer, LinkedHashMap<String, Integer>> samplesPositionMap;
    private LinkedList<Integer> indexedSamples;

    private final AtomicInteger variantsCount;
    public static final int LOGING_BATCH_SIZE = 1000;
    private final Logger logger = LoggerFactory.getLogger(MongoDBVariantStageLoader.class);


    public MongoDBVariantMerger(StudyConfiguration sc, List<Integer> fileIds, MongoDBCollection collection, int numTotalVariants) {
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
    }

    public MongoDBVariantWriteResult getResult() {
        return result;
    }

    @Override
    public List<MongoDBVariantWriteResult> apply(List<Document> batch) {
        return Collections.singletonList(load(batch));
    }

    public MongoDBVariantWriteResult load(List<Document> variants) {
        return load(variants.stream());
    }

    public MongoDBVariantWriteResult load(Stream<Document> variants) {

        List<Document> inserts = new LinkedList<>();
        List<Bson> queriesExisting = new LinkedList<>();
        List<Bson> updatesExisting = new LinkedList<>();
        List<Bson> queriesFillGaps = new LinkedList<>();
        List<Bson> updatesFillGaps = new LinkedList<>();
        final int[] skipped = {0};
        final int[] nonInserted = {0};

        variants.forEach(document -> {
            int size = document.size();
            Variant emptyVar = MongoDBVariantStageLoader.STRING_ID_CONVERTER.convertToDataModelType(document.getString("_id"));
            Document study = document.get(Integer.toString(studyId), Document.class);
            if (study != null) {
                boolean newStudy = study.getBoolean("new", true);
                boolean newVariant = newStudy && size == 2;
//                if (newVariant) {
//                    for (Integer fileId : fileIds) {
//                        if (study.containsKey(fileId.toString())) {
//                            Binary file = study.get(fileId.toString(), Binary.class);
//                            Variant variant = MongoDBVariantStageWriter.VARIANT_CONVERTER_DEFAULT.convertToDataModelType(file);
//                            if (variant.getType().equals(VariantType.NO_VARIATION) || variant.getType().equals(VariantType.SYMBOLIC)) {
//                                continue;
//                            }
//                            LinkedHashMap<String, Integer> samplesPosition = new LinkedHashMap<>();
//                            for (Integer sampleId : studyConfiguration.getSamplesInFiles().get(fileId)) {
//                                samplesPosition.put(studyConfiguration.getSampleIds().inverse().get(sampleId), samplesPosition.size());
//                            }
//                            variant.getStudies().get(0).setSamplesPosition(samplesPosition);
//
//                            Document newDocument = variantConverter.convertToStorageType(variant);
//                            inserts.add(newDocument);
//
//                        } else {
//
//                        }
//                    }
//
//                } else {
//                    MongoDBVariantStageWriter.STRING_ID_CONVERTER.convertToDataModelType()
//                    if (newStudy) {
//                        queries.add(Filters.eq("_id", variantConverter.buildStorageId(variant)));
//                    } else {
//                        queries.add(Filters.and(Filters.eq("_id", variantConverter.buildStorageId(variant)),
//                                Filters.eq(STUDIES_FIELD + "." + STUDYID_FIELD, studyId)));
//                    }
//                }

                for (Integer fileId : fileIds) {
                    if (study.containsKey(fileId.toString())) {
                        List duplicatedVariants = study.get(fileId.toString(), List.class);
                        if (duplicatedVariants.size() > 1) {
                            nonInserted[0] += duplicatedVariants.size();
                            System.out.println("duplicatedVariants = " + duplicatedVariants.size());
                            continue;
                        }
                        Binary file = ((Binary) duplicatedVariants.get(0));
                        Variant variant = MongoDBVariantStageLoader.VARIANT_CONVERTER_DEFAULT.convertToDataModelType(file);
                        if (variant.getType().equals(VariantType.NO_VARIATION) || variant.getType().equals(VariantType.SYMBOLIC)) {
                            skipped[0]++;
                            continue;
                        }

                        variant.getStudies().get(0).setSamplesPosition(getSamplesPosition(fileId));

                        if (newVariant) {
                            LinkedList<Integer> indexedSamples = getIndexedSamples();
                            Document newDocument = variantConverter.convertToStorageType(variant);
                            ((Document) newDocument.get(STUDIES_FIELD, List.class).get(0)).get(GENOTYPES_FIELD, Document.class)
                                    .put(UNKNOWN_GENOTYPE, indexedSamples);
                            inserts.add(newDocument);
                        } else {
                            if (newStudy) {
                                Document newDocument = studyConverter.convertToStorageType(variant.getStudies().get(0));
                                queriesExisting.add(Filters.eq("_id", variantConverter.buildStorageId(variant)));
                                updatesExisting.add(Updates.push(STUDIES_FIELD, newDocument));
                            } else {
                                Document newDocument = studyConverter.convertToStorageType(variant.getStudies().get(0));

                                List<Bson> mergeUpdates = new LinkedList<>();

                                mergeUpdates.add(
                                        Updates.push(STUDIES_FIELD + ".$." + FILES_FIELD, newDocument.get(FILES_FIELD, List.class).get(0))
                                );
                                for (Map.Entry<String, Object> entry : newDocument.get(GENOTYPES_FIELD, Document.class).entrySet()) {
                                    mergeUpdates.add(Updates.pushEach(STUDIES_FIELD + ".$." + GENOTYPES_FIELD + "." + entry.getKey(),
                                            (List) entry.getValue()));
                                }

                                queriesExisting.add(Filters.and(Filters.eq("_id", variantConverter.buildStorageId(variant)),
                                        Filters.eq(STUDIES_FIELD + "." + STUDYID_FIELD, studyId)));
                                updatesExisting.add(Updates.combine(mergeUpdates));
                            }
                        }
                    } else {
                        if (newVariant) {
                            logger.error("TODO: New variants and no file");
                            // TODO : Only happens with multi file loading
                        } else if (newStudy) {
                            logger.error("TODO: New study and no file");
                            // TODO : Only happens with multi file loading
                        } else {
                            queriesFillGaps.add(Filters.and(Filters.eq("_id", variantConverter.buildStorageId(emptyVar)),
                                    Filters.eq(STUDIES_FIELD + "." + STUDYID_FIELD, studyId)));
                            updatesFillGaps.add(Updates.pushEach(STUDIES_FIELD + ".$." + GENOTYPES_FIELD + "." + UNKNOWN_GENOTYPE,
                                    new LinkedList<>(getSamplesInFile(fileId))));
                        }
                    }
                }
            }

        });


        long newVariants = -System.nanoTime();
        if (!inserts.isEmpty()) {
            collection.insert(inserts, QUERY_OPTIONS);
        }
        newVariants += System.nanoTime();
        long existingVariants = -System.nanoTime();
        if (!queriesExisting.isEmpty()) {
            collection.update(queriesExisting, updatesExisting, QUERY_OPTIONS);
        }
        existingVariants += System.nanoTime();
        long fillGapsVariants = -System.nanoTime();
        if (!queriesFillGaps.isEmpty()) {
            collection.update(queriesFillGaps, updatesFillGaps, QUERY_OPTIONS);
        }
        fillGapsVariants += System.nanoTime();

        MongoDBVariantWriteResult writeResult = new MongoDBVariantWriteResult(inserts.size(), updatesExisting.size(), skipped[0],
                nonInserted[0], newVariants, existingVariants, fillGapsVariants);
        synchronized (result) {
            result.merge(writeResult);
        }

        int processedVariants = queriesExisting.size() + inserts.size();
        int previousCount = variantsCount.getAndAdd(processedVariants);
        if ((previousCount + processedVariants) / LOGING_BATCH_SIZE != previousCount / LOGING_BATCH_SIZE) {
            logger.info("Write variants in VARIANTS collection " + (previousCount + processedVariants) + "/" + numTotalVariants + " "
                    + String.format("%.2f%%", ((float) (previousCount + processedVariants)) / numTotalVariants * 100.0));
        }

        return writeResult;

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
