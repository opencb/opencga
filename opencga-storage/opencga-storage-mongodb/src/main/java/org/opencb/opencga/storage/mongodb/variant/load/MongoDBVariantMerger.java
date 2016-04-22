package org.opencb.opencga.storage.mongodb.variant.load;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
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
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToSamplesConverter.UNKNOWN_GENOTYPE;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyVariantEntryConverter.*;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter.STUDIES_FIELD;
import static org.opencb.opencga.storage.mongodb.variant.load.MongoDBVariantStageLoader.STRING_ID_CONVERTER;
import static org.opencb.opencga.storage.mongodb.variant.load.MongoDBVariantStageLoader.VARIANT_CONVERTER_DEFAULT;

/**
 * Created on 07/04/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoDBVariantMerger implements ParallelTaskRunner.Task<Document, MongoDBVariantWriteResult> {

    public static final QueryOptions QUERY_OPTIONS = new QueryOptions();
    private final VariantDBAdaptor dbAdaptor;
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
    public static final int DEFAULT_LOGING_BATCH_SIZE = 5000;
    private long loggingBatchSize;
    private final Logger logger = LoggerFactory.getLogger(MongoDBVariantStageLoader.class);
    private final VariantMerger variantMerger;

    private class MongoDBOperations {

        private List<Document> inserts =  new LinkedList<>();

        private List<Bson> queriesExisting = new LinkedList<>();
        private List<String> queriesExistingId = new LinkedList<>();
        private List<Bson> updatesExisting = new LinkedList<>();

        private List<Bson> queriesFillGaps = new LinkedList<>();
        private List<String> queriesFillGapsId = new LinkedList<>();
        private List<Bson> updatesFillGaps = new LinkedList<>();

        private final AtomicInteger skipped = new AtomicInteger();
        private final AtomicInteger nonInserted = new AtomicInteger();

    }

    public MongoDBVariantMerger(VariantDBAdaptor dbAdaptor, StudyConfiguration sc, List<Integer> fileIds, MongoDBCollection collection,
                                long numTotalVariants) {
        this.dbAdaptor = dbAdaptor;
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
        variantMerger = new VariantMerger();
    }

    public MongoDBVariantMerger(VariantDBAdaptor dbAdaptor, StudyConfiguration sc, List<Integer> fileIds, MongoDBCollection collection,
                                Future<Long> futureNumTotalVariants, long aproxNumVariants) {
        this.dbAdaptor = dbAdaptor;
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
        this.numTotalVariants = aproxNumVariants;
        loggingBatchSize = DEFAULT_LOGING_BATCH_SIZE;
        variantMerger = new VariantMerger();
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

        // Set of operations to be executed in the Database
        MongoDBOperations mongoDBOps = new MongoDBOperations();

        Variant previousVariant = null;
        Document previousDocument = null;
        List<Document> overlappedVariants = null;

        for (Document document : variants) {
            Variant emptyVar = STRING_ID_CONVERTER.convertToDataModelType(document.getString("_id"));
            Document study = document.get(Integer.toString(studyId), Document.class);
            if (study != null) {
                if (previousVariant != null && previousVariant.overlapWith(emptyVar, true)) {
                    // If the variant overlaps with the last one, add to the overlappedVariants list.
                    // Do not process any variant!
                    if (overlappedVariants == null) {
                        overlappedVariants = new LinkedList<>();
                        overlappedVariants.add(previousDocument);
                    }
                    overlappedVariants.add(document);
                    previousDocument = document;
                    previousVariant = emptyVar;
                    continue;
                } else {
                    // If the current variant does not overlap with the last one, we can load the previous variant (or region)
                    if (overlappedVariants != null) {
                        processOverlappedVariants(overlappedVariants, mongoDBOps);
                    } else if (previousDocument != null) {
                        processVariant(previousDocument, previousVariant, mongoDBOps);
                    }
                    overlappedVariants = null;
                }

                previousDocument = document;
                previousVariant = emptyVar;
            }

        }

        // Process remaining variants
        if (overlappedVariants != null) {
            processOverlappedVariants(overlappedVariants, mongoDBOps);
        } else if (previousDocument != null) {
            processVariant(previousDocument, previousVariant, mongoDBOps);
        }

        // Execute MongoDB Operations
        MongoDBVariantWriteResult writeResult = executeMongoDBOperations(mongoDBOps);

        return writeResult;

    }

    /**
     * Given a document from the stage collection, transforms the document into a set of MongoDB operations.
     *
     * It may be a new variant document in the database, a new study in the document, or just an update of an existing study variant.
     *
     * @param document          Document to load
     * @param emptyVar          Parsed empty variant of the document. Only chr, pos, ref, alt
     * @param mongoDBOps        Set of MongoDB operations to update
     */
    public void processVariant(Document document, Variant emptyVar, MongoDBOperations mongoDBOps) {
        Document study = document.get(Integer.toString(studyId), Document.class);

        // New variant in the study.
        boolean newStudy = isNewStudy(study);
        // New variant in the collection if new variant and document size is 2 {_id, study}
        boolean newVariant = isNewVariant(document, newStudy);


        List<Document> fileDocuments = new LinkedList<>();
        Document gts = new Document();

        // Loop for each file that have to be merged
        for (Integer fileId : fileIds) {

            // Different actions if the file is present or missing in the document.
            if (study.containsKey(fileId.toString())) {
                //Duplicated documents are treated like missing. Increment the number of duplicated variants
                List<Binary> duplicatedVariants = study.get(fileId.toString(), List.class);
                if (duplicatedVariants.size() > 1) {
                    mongoDBOps.nonInserted.addAndGet(duplicatedVariants.size());
                    addSampleIdsGenotypes(gts, UNKNOWN_GENOTYPE, getSamplesInFile(fileId));
                    System.out.println("duplicatedVariants = " + emptyVar);
                    continue;
                }

                Binary file = duplicatedVariants.get(0);
                Variant variant = VARIANT_CONVERTER_DEFAULT.convertToDataModelType(file);
                if (variant.getType().equals(VariantType.NO_VARIATION) || variant.getType().equals(VariantType.SYMBOLIC)) {
                    mongoDBOps.skipped.incrementAndGet();
                    continue;
                }
                variant.getStudies().get(0).setSamplesPosition(getSamplesPosition(fileId));
                Document newDocument = studyConverter.convertToStorageType(variant.getStudies().get(0));

                fileDocuments.add((Document) newDocument.get(FILES_FIELD, List.class).get(0));

                for (Map.Entry<String, Object> entry : newDocument.get(GENOTYPES_FIELD, Document.class).entrySet()) {
                    addSampleIdsGenotypes(gts, entry.getKey(), (List<Integer>) entry.getValue());
                }

            } else {
                addSampleIdsGenotypes(gts, UNKNOWN_GENOTYPE, getSamplesInFile(fileId));
            }

        }

        if (newStudy) {
            //If it is a new variant for the study, add the already loaded samples as UNKNOWN
            addSampleIdsGenotypes(gts, UNKNOWN_GENOTYPE, getIndexedSamples());
        }

        updateMongoDBOperations(emptyVar, fileDocuments, gts, newStudy, newVariant, mongoDBOps);
    }

    /**
     * Given a list of documents from the stage collection, merge and transforms into a set of MongoDB operations.
     *
     * It may be a new variant document in the database, a new study in the document, or just an update of an existing study variant.
     *
     *
     *
     * @param overlappedVariants    Overlapping documents from Stage collection.
     * @param mongoDBOps            Set of MongoDB operations to update
     */
    public void processOverlappedVariants(List<Document> overlappedVariants, MongoDBOperations mongoDBOps) {

        // Merge documents
        Map<Document, Variant> mergedVariants = mergeOverlappedVariants(overlappedVariants);

        for (Map.Entry<Document, Variant> entry : mergedVariants.entrySet()) {
            Document document = entry.getKey();
            Variant variant = entry.getValue();

            Variant emptyVar = STRING_ID_CONVERTER.convertToDataModelType(document.getString("_id"));
            Document study = document.get(studyId.toString(), Document.class);

            // New variant in the study.
            boolean newStudy = isNewStudy(study);
            // New variant in the collection if new variant and document size is 2 {_id, study}
            boolean newVariant = isNewVariant(document, newStudy);

            Document gts = new Document();
            List<Document> fileDocuments = new LinkedList<>();
            StudyEntry studyEntry = variant.getStudies().get(0);
            // For all the files that are being indexed
            for (Integer fileId : fileIds) {
                FileEntry file = studyEntry.getFile(fileId.toString());
                if (file != null) {
                    Document studyDocument = studyConverter.convertToStorageType(studyEntry, file, getSampleNamesInFile(fileId));
                    studyDocument.get(GENOTYPES_FIELD, Document.class)
                            .forEach((gt, sampleIds) -> addSampleIdsGenotypes(gts, gt, (Collection) sampleIds));
                    fileDocuments.addAll(studyDocument.get(FILES_FIELD, List.class));
                } else {
                    addSampleIdsGenotypes(gts, UNKNOWN_GENOTYPE, getSamplesInFile(fileId));
                }
            }
            // For the rest of the files not indexed, only is this variant is new in this study,
            // add all the already indexed files information, if present in this variant.
            if (newStudy) {
                for (Integer fileId : studyConfiguration.getIndexedFiles()) {
                    FileEntry file = studyEntry.getFile(fileId.toString());
                    if (file != null) {
                        Document studyDocument = studyConverter.convertToStorageType(studyEntry, file, getSampleNamesInFile(fileId));
                        studyDocument.get(GENOTYPES_FIELD, Document.class)
                                .forEach((gt, sampleIds) -> addSampleIdsGenotypes(gts, gt, (Collection) sampleIds));
                        fileDocuments.addAll(studyDocument.get(FILES_FIELD, List.class));
                    } else {
                        addSampleIdsGenotypes(gts, UNKNOWN_GENOTYPE, getSamplesInFile(fileId));
                    }
                }
            }


            updateMongoDBOperations(emptyVar, fileDocuments, gts, newStudy, newVariant, mongoDBOps);
        }
    }
    /**
     * Given a list of overlapped documents from the stage collection, merge resolving the overlapping positions.
     *
     * @see {@link VariantMerger}
     * @param overlappedVariants    Overlapping documents from Stage collection.
     * @return  For each document, its corresponding merged variant
     */
    public Map<Document, Variant> mergeOverlappedVariants(List<Document> overlappedVariants) {
//        System.out.println("--------------------------------");
//        System.out.println("Overlapped region = " + overlappedVariants
//                .stream()
//                .map(doc -> STRING_ID_CONVERTER.convertToDataModelType(doc.getString("_id")))
//                .collect(Collectors.toList()));

        // The overlapping region will be new if any of the variants is new for the study
        boolean newOverlappingRegion = false;
        // The overlapping region will be completely new if ALL the variants are new for the study
        boolean completelyNewOverlappingRegion = true;

        Map<Integer, List<Variant>> variantsPerFile = new HashMap<>();
        for (Integer fileId : fileIds) {
            variantsPerFile.put(fileId, new LinkedList<>());
        }

        // Linked hash map to preserve the order
        Map<Document, Variant> mergedVariants = new LinkedHashMap<>();
        List<Boolean> newStudies = new ArrayList<>(overlappedVariants.size());

        // For each variant, create an empty variant that will be filled by the VariantMerger
        for (Document document : overlappedVariants) {
            Document study = document.get(Integer.toString(studyId), Document.class);

            // New variant in the study.
            boolean newStudy = isNewStudy(study);
            newStudies.add(newStudy);
            // Its a new OverlappingRegion if at least one variant is new in this study
            newOverlappingRegion |= newStudy;
            // Its a completely new OverlappingRegion if all the variants are new in this study
            completelyNewOverlappingRegion &= newStudy;

            Variant var = STRING_ID_CONVERTER.convertToDataModelType(document.getString("_id"));
            // FIXME: Use real format!
            List<String> format = Arrays.asList("GT");
            StudyEntry se = new StudyEntry(studyId.toString(), new LinkedList<>(), format);
            se.setSamplesPosition(new HashMap<>());
            var.addStudyEntry(se);

            mergedVariants.put(document, var);

            for (Integer fileId : fileIds) {
                List<Binary> files = study.get(fileId.toString(), List.class);
                if (files != null && files.size() == 1) {
                    Variant variant = VARIANT_CONVERTER_DEFAULT.convertToDataModelType(files.get(0));
                    variant.getStudies().get(0).setSamplesPosition(getSamplesPosition(fileId));
//                    variantsToMerge.add(variant);
                    variantsPerFile.get(fileId).add(variant);
                }
            }
        }

        List<Variant> variantsToMerge = new LinkedList<>();
        for (Integer fileId : fileIds) {
            List<Variant> variantList = variantsPerFile.get(fileId);
            switch (variantList.size()) {
                case 0:
                    break;
                case 1:
                    variantsToMerge.add(variantList.get(0));
                    break;
                default:
                    logger.warn("Overlapping variants in file {} : {}", fileId, variantList);
//                                    skipped.addAndGet(variantList.size() - 1);
                    variantsToMerge.add(variantList.get(0));
                    break;
            }
        }


        /*
         * If is a new overlapping region and there are some file already indexed
         * Fetch the information from the database regarding the loaded variants of this region.
         *
         *      +---+---+---+---+
         *      | A | B | C | D |
         * +----+---+---+---+---+
         * | V1 | X | X |   |   |
         * +----+---+---+---+---+
         * | V2 | X | X |   | X |
         * +----+---+---+---+---+
         * | V3 |   |   | X |   |
         * +----+---+---+---+---+
         *
         * - Files A and B are loaded
         * - Files C and D are being loaded
         * - Variants V1,V2,V3 are overlapping
         *
         * In order to merge the data properly, we need to get from the server the information about
         * the variants {V1, V2} for the files {A, B}.
         *
         * Because the variants {V1, V2} are already loaded, the information that we need is duplicated
         * in both variants, so we only need to get one of them.
         *
         */
        if (!completelyNewOverlappingRegion && newOverlappingRegion && !studyConfiguration.getIndexedFiles().isEmpty()) {
            int i = 0;
            for (Variant variant : mergedVariants.values()) {
                // If the variant is not new in this study, query to the database for the loaded info.
                if (!newStudies.get(i)) {
                    Region region = new Region(variant.getChromosome(), variant.getStart(), variant.getEnd());
                    QueryResult<Variant> queryResult = dbAdaptor.get(new Query()
                            .append(VariantDBAdaptor.VariantQueryParams.REGION.key(), region)
                            .append(VariantDBAdaptor.VariantQueryParams.ALTERNATE.key(), variant.getAlternate())
                            .append(VariantDBAdaptor.VariantQueryParams.REFERENCE.key(), variant.getReference())
                            .append(VariantDBAdaptor.VariantQueryParams.RETURNED_STUDIES.key(), studyId), null);
                    if (!queryResult.getResult().isEmpty()) {
                        variantsToMerge.add(queryResult.first());
                    } else {
                        throw new IllegalStateException("Variant " + variant + " not found!");
                    }
                    // Because the loaded variants were an overlapped region, all the information required is in every variant.
                    // Fetch only one variant
                    break;
                }
                i++;
            }
        }

        // Finally, merge variants
        for (Variant mergedVariant : mergedVariants.values()) {
            variantMerger.merge(mergedVariant, variantsToMerge);
        }

//        System.out.println("----------------");
//        for (Variant variant : mergedVariants.values()) {
//            System.out.println(variant.toJson());
//        }
//        System.out.println("----------------");
//        System.out.println("--------------------------------");

        return mergedVariants;
    }

    /**
     * Transform the set of genotypes and file objects into a set of mongodb operations.
     *
     * @param emptyVar          Parsed empty variant of the document. Only chr, pos, ref, alt
     * @param fileDocuments     List of files to be updated
     * @param gts               Set of genotypes to be updates
     * @param newStudy          If the variant is new for this study
     * @param newVariant        If the variant was never seen in the database
     * @param mongoDBOperations Set of MongoBD operations to update
     */
    public void updateMongoDBOperations(Variant emptyVar, List<Document> fileDocuments, Document gts, boolean newStudy, boolean newVariant,
                                        MongoDBOperations mongoDBOperations) {

        if (newVariant) {
            // If there where no files and the variant is new, do not insert the variant.
            // It may happen if all the files in the variant where duplicated for this variant.
            if (!fileDocuments.isEmpty()) {

                Document variantDocument = variantConverter.convertToStorageType(emptyVar);
                variantDocument.append(STUDIES_FIELD,
                        Collections.singletonList(
                                new Document(STUDYID_FIELD, studyId)
                                        .append(FILES_FIELD, fileDocuments)
                                        .append(GENOTYPES_FIELD, gts)
                        )
                );
                mongoDBOperations.inserts.add(variantDocument);
            }
        } else if (newStudy) {

            Document studyDocument = new Document(STUDYID_FIELD, studyId)
                    .append(FILES_FIELD, fileDocuments)
                    .append(GENOTYPES_FIELD, gts);

            String id = variantConverter.buildStorageId(emptyVar);
            mongoDBOperations.queriesExistingId.add(id);
            mongoDBOperations.queriesExisting.add(Filters.eq("_id", id));
            mongoDBOperations.updatesExisting.add(Updates.push(STUDIES_FIELD, studyDocument));

        } else {
            String id = variantConverter.buildStorageId(emptyVar);
            List<Bson> mergeUpdates = new LinkedList<>();

            for (String gt : gts.keySet()) {
                List sampleIds = gts.get(gt, List.class);
                mergeUpdates.add(Updates.pushEach(STUDIES_FIELD + ".$." + GENOTYPES_FIELD + "." + gt,
                        sampleIds));
            }
            if (!fileDocuments.isEmpty()) {
                mongoDBOperations.queriesExistingId.add(id);
                mongoDBOperations.queriesExisting.add(Filters.and(Filters.eq("_id", id),
                        Filters.eq(STUDIES_FIELD + "." + STUDYID_FIELD, studyId)));

                mergeUpdates.add(Updates.pushEach(STUDIES_FIELD + ".$." + FILES_FIELD, fileDocuments));
                mongoDBOperations.updatesExisting.add(Updates.combine(mergeUpdates));
            } else {
                mongoDBOperations.queriesFillGapsId.add(id);
                mongoDBOperations.queriesFillGaps.add(Filters.and(Filters.eq("_id", id),
                        Filters.eq(STUDIES_FIELD + "." + STUDYID_FIELD, studyId)));
                mongoDBOperations.updatesFillGaps.add(Updates.combine(mergeUpdates));
            }
        }
    }

    /**
     * Execute the set of mongoDB operations.
     *
     * @param mongoDBOps MongoDB operations to execute
     * @return           MongoDBVariantWriteResult
     */
    public MongoDBVariantWriteResult executeMongoDBOperations(MongoDBOperations mongoDBOps) {
        long newVariants = -System.nanoTime();
        if (!mongoDBOps.inserts.isEmpty()) {
            try {
                BulkWriteResult writeResult = collection.insert(mongoDBOps.inserts, QUERY_OPTIONS).first();
                if (writeResult.getInsertedCount() != mongoDBOps.inserts.size()) {
                    logger.error("(Inserts = " + mongoDBOps.inserts.size() + ") "
                            + "!= (InsertedCount = " + writeResult.getInsertedCount() + ")");
                    for (Document insert : mongoDBOps.inserts) {
                        Long count = collection.count(Filters.eq("_id", insert.get("_id"))).first();
                        if (count != 1) {
                            logger.error("Missing insert " + insert.get("_id"));
                        }
                    }
                }
            } catch (MongoBulkWriteException e) {
                for (Document insert : mongoDBOps.inserts) {
                    System.out.println(insert.get("_id"));
                }
                throw e;
            }
        }
        newVariants += System.nanoTime();
        long existingVariants = -System.nanoTime();
        if (!mongoDBOps.queriesExisting.isEmpty()) {
            QueryResult<BulkWriteResult> update = collection.update(mongoDBOps.queriesExisting, mongoDBOps.updatesExisting, QUERY_OPTIONS);
            if (update.first().getModifiedCount() != mongoDBOps.queriesExisting.size()) {
                onError("existing variants", update, mongoDBOps.queriesExisting, mongoDBOps.queriesExistingId);
            }
        }
        existingVariants += System.nanoTime();
        long fillGapsVariants = -System.nanoTime();
        if (!mongoDBOps.queriesFillGaps.isEmpty()) {
            QueryResult<BulkWriteResult> update = collection.update(mongoDBOps.queriesFillGaps, mongoDBOps.updatesFillGaps, QUERY_OPTIONS);
            if (update.first().getModifiedCount() != mongoDBOps.queriesFillGaps.size()) {
                onError("fill gaps", update, mongoDBOps.queriesFillGaps, mongoDBOps.queriesFillGapsId);
            }
        }
        fillGapsVariants += System.nanoTime();

        MongoDBVariantWriteResult writeResult = new MongoDBVariantWriteResult(mongoDBOps.inserts.size(),
                mongoDBOps.updatesExisting.size() + mongoDBOps.updatesFillGaps.size(),
                mongoDBOps.skipped.get(), mongoDBOps.nonInserted.get(), newVariants, existingVariants, fillGapsVariants);
        synchronized (result) {
            result.merge(writeResult);
        }

        int processedVariants = mongoDBOps.queriesExisting.size() + mongoDBOps.queriesFillGaps.size() + mongoDBOps.inserts.size();
        logProgress(processedVariants);
        return writeResult;
    }

    /**
     * Is a new variant for the study depending on the value of the field {@link MongoDBVariantStageLoader#NEW_STUDY_FIELD}.
     * @param study Study object
     * @return      If this is the first time that the variant has been seen in this study.
     */
    public boolean isNewStudy(Document study) {
        return study.getBoolean(MongoDBVariantStageLoader.NEW_STUDY_FIELD, MongoDBVariantStageLoader.NEW_STUDY_DEFAULT);
    }

    public boolean isNewVariant(Document document, boolean newStudy) {
        // If the document has only the study and the _id field.
        return newStudy && document.size() == 2;
    }

    protected void logProgress(int processedVariants) {
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

    protected void addSampleIdsGenotypes(Document gts, String genotype, Collection<Integer> sampleIds) {
        if (sampleIds.isEmpty()) {
            return;
        }
        if (gts.containsKey(genotype)) {
            gts.get(genotype, List.class).addAll(sampleIds);
        } else {
            gts.put(genotype, new LinkedList<>(sampleIds));
        }
    }

    protected void onError(String updateName, QueryResult<BulkWriteResult> update, List<Bson> queries, List<String> queryIds) {
        logger.error("(Updated " + updateName + " variants = " + queries.size() + " ) != "
                + "(UpdatedCount = " + update.first().getModifiedCount() + "). MatchedCount:" + update.first().getMatchedCount());
        logger.info("QueryIDs: {}", queryIds);
        List<QueryResult<Document>> queryResults = collection.find(queries, null);
        logger.info("Results: ", queryResults.size());

        for (QueryResult<Document> r : queryResults) {
            logger.info("result: ", r);
            if (!r.getResult().isEmpty()) {
                String id = r.first().get("_id", String.class);
                boolean remove = queryIds.remove(id);
                logger.info("remove({}): {}", id, remove);
            }
        }
        for (String id : queryIds) {
            logger.error("Missing Variant " + id);
        }
    }

    protected LinkedList<Integer> getIndexedSamples() {
        if (indexedSamples == null) {
            indexedSamples = new LinkedList<>(StudyConfiguration.getIndexedSamples(studyConfiguration).values());
            indexedSamples.sort(Integer::compareTo);
        }
        return indexedSamples;
    }

    protected LinkedHashSet<Integer> getSamplesInFile(Integer fileId) {
        return studyConfiguration.getSamplesInFiles().get(fileId);
    }

    public Set<String> getSampleNamesInFile(Integer fileId) {
        return getSamplesInFile(fileId)
                .stream()
                .map(sampleId -> studyConfiguration.getSampleIds().inverse().get(sampleId))
                .collect(Collectors.toSet());
    }

    protected LinkedHashMap<String, Integer> getSamplesPosition(Integer fileId) {
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
