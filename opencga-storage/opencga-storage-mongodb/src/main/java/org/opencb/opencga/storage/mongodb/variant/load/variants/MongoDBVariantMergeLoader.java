/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.mongodb.variant.load.variants;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import org.apache.commons.lang3.time.StopWatch;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.load.MongoDBVariantWriteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;
import static org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine.MongoDBVariantOptions.LOADED_GENOTYPES;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyVariantEntryConverter.FILEID_FIELD;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyVariantEntryConverter.FILES_FIELD;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter.STUDIES_FIELD;
import static org.opencb.opencga.storage.mongodb.variant.converters.stage.StageDocumentToVariantConverter.ID_FIELD;
import static org.opencb.opencga.storage.mongodb.variant.converters.stage.StageDocumentToVariantConverter.STUDY_FILE_FIELD;
import static org.opencb.opencga.storage.mongodb.variant.load.stage.MongoDBVariantStageLoader.DUP_KEY_WRITE_RESULT_ERROR_PATTERN;
import static org.opencb.opencga.storage.mongodb.variant.load.stage.MongoDBVariantStageLoader.NEW_STUDY_FIELD;

/**
 * Created on 21/11/16.
 *
 * Loads data into the VARIANTS collection
 *   New variants
 *   New study in a existing variant
 *   New data in a existing study
 * Cleans (if needed/wanted) the STAGE collection.
 *   Removes the files from the indexed field. {@link STUDY_FILE_FIELD}
 *   Sets {studyId}.{fileId} fields to NULL.
 *   Do NOT remove ($unset) the field. See {@link MongoDBVariantMerger#alreadyProcessedStageDocument}
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoDBVariantMergeLoader implements DataWriter<MongoDBOperations> {


    private final Logger logger = LoggerFactory.getLogger(MongoDBVariantMergeLoader.class);
    private static final QueryOptions QUERY_OPTIONS = new QueryOptions();
    private static final QueryOptions UPSERT_AND_RELPACE = new QueryOptions(MongoDBCollection.UPSERT, true)
            .append(MongoDBCollection.REPLACE, true);
    private static final QueryOptions UPSERT = new QueryOptions(MongoDBCollection.UPSERT, true);
    private static final QueryOptions MULTI = new QueryOptions(MongoDBCollection.MULTI, true);


    private final MongoDBCollection studiesCollection;
    private final ProgressLogger progressLogger;
    private final MongoDBCollection variantsCollection;
    private final MongoDBCollection stageCollection;
    private final boolean resume;
    private final boolean cleanWhileLoading;
    private final Integer studyId;
    /** Files to be loaded. */
    private final List<Integer> fileIds;

    // Variables that must be aware of concurrent modification
    private final MongoDBVariantWriteResult result;
    private final Bson cleanStageDuplicated;
    private final Bson cleanStage;

    public MongoDBVariantMergeLoader(MongoDBCollection variantsCollection, MongoDBCollection stageCollection,
                                     MongoDBCollection studiesCollection, StudyConfiguration studyConfiguration, List<Integer> fileIds,
                                     boolean resume, boolean cleanWhileLoading, ProgressLogger progressLogger) {
        this.progressLogger = progressLogger;
        this.variantsCollection = variantsCollection;
        this.stageCollection = stageCollection;
        this.studiesCollection = studiesCollection;
        this.resume = resume;
        this.studyId = studyConfiguration.getStudyId();
        this.fileIds = fileIds;
        this.result = new MongoDBVariantWriteResult();
        result.getGenotypes().addAll(studyConfiguration.getAttributes().getAsStringList(LOADED_GENOTYPES.key()));
        this.cleanWhileLoading = cleanWhileLoading;

        List<String> studyFileToPull = new ArrayList<>(fileIds.size());
        for (Integer fileId : fileIds) {
            studyFileToPull.add(studyId + "_" + fileId);
        }
        List<Bson> cleanStageDuplicatedList = new ArrayList<>(fileIds.size() + 1);
        for (Integer fileId : fileIds) {
            // Can not unset value!
            cleanStageDuplicatedList.add(set(studyId + "." + fileId, null));
        }
        cleanStageDuplicatedList.add(pullAll(STUDY_FILE_FIELD, studyFileToPull));

        List<Bson> cleanStageList = new ArrayList<>(cleanStageDuplicatedList.size() + 1);
        cleanStageList.addAll(cleanStageDuplicatedList);
        cleanStageList.add(set(studyId.toString() + '.' + NEW_STUDY_FIELD, false));

        cleanStageDuplicated = combine(cleanStageDuplicatedList);
        cleanStage = combine(cleanStageList);

    }

    @Override
    public boolean write(List<MongoDBOperations> batch) {
        for (MongoDBOperations mongoDBOperations : batch) {
            executeMongoDBOperations(mongoDBOperations);
        }
        return true;
    }

    public MongoDBVariantWriteResult getResult() {
        return result;
    }

    /**
     * Execute the set of mongoDB operations.
     *
     * @param mongoDBOps MongoDB operations to execute
     * @return           MongoDBVariantWriteResult
     */
    protected MongoDBVariantWriteResult executeMongoDBOperations(MongoDBOperations mongoDBOps) {
        long newVariantsTime = 0; // Impossible to know how much time spend in insert or update in operation "UPSERT"
        StopWatch existingVariants = StopWatch.createStarted();
        long newVariants = 0;
        if (!mongoDBOps.getNewStudy().getQueries().isEmpty()) {
            newVariants = executeMongoDBOperationsNewStudy(mongoDBOps, true);
        }
        existingVariants.stop();
        StopWatch fillGapsVariants = StopWatch.createStarted();
        if (!mongoDBOps.getExistingStudy().getQueries().isEmpty()) {
            QueryResult<BulkWriteResult> update = variantsCollection.update(mongoDBOps.getExistingStudy().getQueries(),
                    mongoDBOps.getExistingStudy().getUpdates(), QUERY_OPTIONS);
            if (update.first().getMatchedCount() != mongoDBOps.getExistingStudy().getQueries().size()) {
                onUpdateError("fill gaps", update, mongoDBOps.getExistingStudy().getQueries(), mongoDBOps.getExistingStudy().getIds());
            }
        }
        fillGapsVariants.stop();

        updateStage(mongoDBOps);

        long updatesNewStudyExistingVariant = mongoDBOps.getNewStudy().getUpdates().size() - newVariants;
        long updatesWithDataExistingStudy = mongoDBOps.getExistingStudy().getUpdates().size() - mongoDBOps.getMissingVariants();
        MongoDBVariantWriteResult writeResult = new MongoDBVariantWriteResult(newVariants,
                updatesNewStudyExistingVariant + updatesWithDataExistingStudy, mongoDBOps.getMissingVariants(),
                mongoDBOps.getOverlappedVariants(), mongoDBOps.getSkipped(), mongoDBOps.getNonInserted(), newVariantsTime,
                existingVariants.getNanoTime(), fillGapsVariants.getNanoTime(), mongoDBOps.getGenotypes());

        boolean updateGenotypes;
        synchronized (result) {
            updateGenotypes = !result.getGenotypes().containsAll(mongoDBOps.getGenotypes());
            result.merge(writeResult);
        }
        if (updateGenotypes) {
            // Update the genotypes as soon as there is a new one detected.
            // Avoid losing values in case of failure.
            logger.debug("Update list of loaded genotypes");
            studiesCollection.update(
                    eq("_id", studyId),
                    addEachToSet("attributes." + LOADED_GENOTYPES.key(), new ArrayList<>(result.getGenotypes())),
                    null);
        }

        // Modifying the stage collection MUST be the latest operation.
        // If there is any failure, we must ensure that we can resume the operation.
        // Once the stage collection is clean, we can not resume the operation.
        if (cleanWhileLoading) {
            cleanStage(mongoDBOps);
        }

        long processedVariants = mongoDBOps.getNewStudy().getQueries().size()
                + mongoDBOps.getExistingStudy().getQueries().size()
                + mongoDBOps.getMissingVariantsNoFillGaps();
        logProgress(processedVariants);
        return writeResult;
    }

    private void updateStage(MongoDBOperations mongoDBOps) {

        MongoDBOperations.StageSecondaryAlternates alternates = mongoDBOps.getSecondaryAlternates();
        if (!alternates.getQueries().isEmpty()) {
            QueryResult<BulkWriteResult> update = stageCollection.update(alternates.getQueries(), alternates.getUpdates(), null);
            if (update.first().getMatchedCount() != alternates.getQueries().size()) {
                onUpdateError("populate secondary alternates", update, alternates.getQueries(), alternates.getIds(), stageCollection);
            }
        }

        long cleanDocuments = 0;
        if (cleanWhileLoading) {
            cleanDocuments = cleanStage(mongoDBOps);
        }

    }

    private long cleanStage(MongoDBOperations mongoDBOps) {
        long modifiedCount = 0;
        if (!mongoDBOps.getDocumentsToCleanStudies().isEmpty()) {
            logger.debug("Clean study {} from stage where all the files {} where duplicated : {}", studyId, fileIds,
                    mongoDBOps.getDocumentsToCleanStudies());
            modifiedCount += stageCollection.update(
                    in(ID_FIELD, mongoDBOps.getDocumentsToCleanStudies()), cleanStageDuplicated, MULTI).first().getModifiedCount();
        }
        if (!mongoDBOps.getDocumentsToCleanFiles().isEmpty()) {
            logger.debug("Cleaning files {} from stage collection", fileIds);
            modifiedCount += stageCollection.update(
                    in(ID_FIELD, mongoDBOps.getDocumentsToCleanFiles()), cleanStage, MULTI).first().getModifiedCount();
        }

        return modifiedCount;
    }

    private int executeMongoDBOperationsNewStudy(MongoDBOperations mongoDBOps, boolean retry) {
        int newVariants = 0;
        MongoDBOperations.NewStudy newStudy = mongoDBOps.getNewStudy();
        try {
            if (resume) {
                // Ensure files exists
                try {
                    if (!newStudy.getVariants().isEmpty()) {
                        newVariants += newStudy.getVariants().size();
                        variantsCollection.insert(newStudy.getVariants(), QUERY_OPTIONS);
                    }
                } catch (MongoBulkWriteException e) {
                    for (BulkWriteError writeError : e.getWriteErrors()) {
                        if (!ErrorCategory.fromErrorCode(writeError.getCode()).equals(ErrorCategory.DUPLICATE_KEY)) {
                            throw e;
                        } else {
                            // Not inserted variant
                            newVariants--;
                        }
                    }
                }

                // Update
                List<Bson> queriesExisting = new ArrayList<>(newStudy.getQueries().size());
                for (Bson bson : newStudy.getQueries()) {
                    queriesExisting.add(and(bson, nin(STUDIES_FIELD + "." + FILES_FIELD + "." + FILEID_FIELD, fileIds)));
                }
                // Update those existing variants
                QueryResult<BulkWriteResult> update = variantsCollection.update(queriesExisting, newStudy.getUpdates(), QUERY_OPTIONS);
                //                if (update.first().getModifiedCount() != mongoDBOps.queriesExisting.size()) {
                //                    // FIXME: Don't know if there is some error inserting. Query already existing?
                //                    onUpdateError("existing variants", update, mongoDBOps.queriesExisting, mongoDBOps.queriesExistingId);
                //                }
            } else {
                QueryResult<BulkWriteResult> update = variantsCollection.update(newStudy.getQueries(), newStudy.getUpdates(), UPSERT);
                if (update.first().getModifiedCount() + update.first().getUpserts().size() != newStudy.getQueries().size()) {
                    onUpdateError("existing variants", update, newStudy.getQueries(), newStudy.getIds());
                }
                // Add upserted documents
                newVariants += update.first().getUpserts().size();
            }
        } catch (MongoBulkWriteException e) {
            // Add upserted documents
            newVariants += e.getWriteResult().getUpserts().size();
            Set<String> duplicatedNonInsertedId = new HashSet<>();
            for (BulkWriteError writeError : e.getWriteErrors()) {
                if (!ErrorCategory.fromErrorCode(writeError.getCode()).equals(ErrorCategory.DUPLICATE_KEY)) {
                    throw e;
                } else {
                    Matcher matcher = DUP_KEY_WRITE_RESULT_ERROR_PATTERN.matcher(writeError.getMessage());
                    if (matcher.find()) {
                        String id = matcher.group(1);
                        duplicatedNonInsertedId.add(id);
                        logger.warn("Catch error : {}",  writeError.toString());
                        logger.warn("DupKey exception inserting '{}'. Retry!", id);
                    } else {
                        logger.error("WriteError with code {} does not match with the pattern {}",
                                writeError.getCode(), DUP_KEY_WRITE_RESULT_ERROR_PATTERN.pattern());
                        throw e;
                    }
                }
            }
            if (retry) {
                // Retry once!
                // With UPSERT=true, this command should never throw DuplicatedKeyException.
                // See https://jira.mongodb.org/browse/SERVER-14322
                // Remove inserted variants
                logger.warn("Retry! " + e);
                Iterator<String> iteratorId = newStudy.getIds().iterator();
                Iterator<?> iteratorQuery = newStudy.getQueries().iterator();
                Iterator<?> iteratorUpdate = newStudy.getUpdates().iterator();
                while (iteratorId.hasNext()) {
                    String id = iteratorId.next();
                    iteratorQuery.next();
                    iteratorUpdate.next();
                    if (!duplicatedNonInsertedId.contains(id)) {
                        iteratorId.remove();
                        iteratorQuery.remove();
                        iteratorUpdate.remove();
                    }
                }
                newVariants += executeMongoDBOperationsNewStudy(mongoDBOps, false);
            } else {
                throw e;
            }
        }
        return newVariants;
    }

    protected void onUpdateError(String updateName, QueryResult<BulkWriteResult> update, List<Bson> queries, List<String> queryIds) {
        onUpdateError(updateName, update, queries, queryIds, variantsCollection);
    }

    protected void onUpdateError(String updateName, QueryResult<BulkWriteResult> update, List<Bson> queries, List<String> queryIds,
                                 MongoDBCollection collection) {
        logger.error("(Updated " + updateName + " variants = " + queries.size() + " ) != "
                + "(ModifiedCount = " + update.first().getModifiedCount() + "). MatchedCount:" + update.first().getMatchedCount());
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
        StringBuilder sb = new StringBuilder("Missing Variant for update : ");
        for (String id : queryIds) {
            logger.error("Missing Variant " + id);
            sb.append(id).append(", ");
        }
        throw new RuntimeException(sb.toString());
    }


    protected void logProgress(long processedVariants) {
        if (progressLogger != null) {
            progressLogger.increment(processedVariants);
        }
    }

    @Override
    public boolean post() {
        VariantMongoDBAdaptor.createIndexes(new QueryOptions(), variantsCollection);
        return true;
    }
//    protected void onInsertError(MongoDBOperations mongoDBOps, BulkWriteResult writeResult) {
//        logger.error("(Inserts = " + mongoDBOps.inserts.size() + ") "
//                + "!= (InsertedCount = " + writeResult.getInsertedCount() + ")");
//
//        StringBuilder sb = new StringBuilder("Missing Variant for insert : ");
//        for (Document insert : mongoDBOps.inserts) {
//            Long count = collection.count(eq("_id", insert.get("_id"))).first();
//            if (count != 1) {
//                logger.error("Missing insert " + insert.get("_id"));
//                sb.append(insert.get("_id")).append(", ");
//            }
//        }
//        throw new RuntimeException(sb.toString());
//    }
}
