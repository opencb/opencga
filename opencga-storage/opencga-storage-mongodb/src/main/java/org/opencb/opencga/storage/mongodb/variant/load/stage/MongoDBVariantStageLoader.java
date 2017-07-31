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

package org.opencb.opencga.storage.mongodb.variant.load.stage;

import com.google.common.collect.ListMultimap;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.storage.mongodb.variant.converters.stage.StageDocumentToVariantConverter;
import org.opencb.opencga.storage.mongodb.variant.converters.stage.VariantToAvroBinaryConverter;
import org.opencb.opencga.storage.mongodb.variant.load.MongoDBVariantWriteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;

/**
 * Created on 07/04/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoDBVariantStageLoader implements DataWriter<ListMultimap<Document, Binary>> {

    public static final String NEW_STUDY_FIELD = "new";
    public static final boolean NEW_STUDY_DEFAULT = true;

    private static final QueryOptions QUERY_OPTIONS = new QueryOptions(MongoDBCollection.UPSERT, true);
    public static final Pattern DUP_KEY_WRITE_RESULT_ERROR_PATTERN = Pattern.compile("^.*dup key: \\{ : \"([^\"]*)\" \\}$");

    private final MongoDBCollection collection;
    private final String fieldName;
    private final boolean resumeStageLoad;
    private final String studyFile;
    private final String studyIdStr;
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDBVariantStageLoader.class);

    private final MongoDBVariantWriteResult writeResult = new MongoDBVariantWriteResult();

    public static final ComplexTypeConverter<Variant, Binary> VARIANT_CONVERTER_DEFAULT = new VariantToAvroBinaryConverter();

    public static final StageDocumentToVariantConverter STAGE_TO_VARIANT_CONVERTER = new StageDocumentToVariantConverter();

    public MongoDBVariantStageLoader(MongoDBCollection collection, int studyId, int fileId, boolean resumeStageLoad) {
        this.collection = collection;
        fieldName = studyId + "." + fileId;
        studyFile = studyId + "_" + fileId;
        studyIdStr = String.valueOf(studyId);
        this.resumeStageLoad = resumeStageLoad;
    }

    @Override
    public boolean pre() {

        Document index = new Document(StageDocumentToVariantConverter.STUDY_FILE_FIELD, 1);
//        index.put(ID_FIELD, 1);
        collection.createIndex(index, new ObjectMap(MongoDBCollection.BACKGROUND, true));

        return true;
    }

    @Override
    public boolean write(List<ListMultimap<Document, Binary>> batch) {
        for (ListMultimap<Document, Binary> map : batch) {
            insert(map);
        }
        return true;
    }

    public MongoDBVariantWriteResult insert(ListMultimap<Document, Binary> ids) {
        final long start = System.nanoTime();

        MongoDBVariantWriteResult result = new MongoDBVariantWriteResult();
        Set<String> retryKeys = updateMongo(ids, result, null);
        if (!retryKeys.isEmpty()) {
            updateMongo(ids, result, retryKeys);
        }

        result.setNewVariantsNanoTime(System.nanoTime() - start);
//        result.setSkippedVariants(skippedVariants);

        synchronized (writeResult) {
            writeResult.merge(result);
        }
        return result;
    }

    /**
     * Given a map of id -> binary[], inserts the binary objects in the stage collection.
     *
     * {
     *     <studyId> : {
     *         <fileId> : [ BinData(), BinData() ]
     *     }
     * }
     *
     * The field <fileId> is an array to detect duplicated variants within the same file.
     *
     * It may happen that an update with upsert:true fail if two different threads try to
     * update the same non existing document.
     * See https://jira.mongodb.org/browse/SERVER-14322
     *
     * In that case, the non inserted values will be returned.
     *
     * @param values        Map with all the values to insert
     * @param result        MongoDBVariantWriteResult to fill
     * @param retryIds      List of IDs to retry. If not null, only will update those documents within this set
     * @return              List of non updated documents.
     * @throws MongoBulkWriteException if the exception was not a DuplicatedKeyException (e:11000)
     */
    private Set<String> updateMongo(ListMultimap<Document, Binary> values, MongoDBVariantWriteResult result, Set<String> retryIds) {

        Set<String> nonInsertedIds = Collections.emptySet();
        if (values.isEmpty()) {
            return nonInsertedIds;
        }
        List<Bson> queries = new LinkedList<>();
        List<Bson> updates = new LinkedList<>();
        for (Document id : values.keySet()) {
            if (retryIds == null || retryIds.contains(id.getString(StageDocumentToVariantConverter.ID_FIELD))) {
                List<Binary> binaryList = values.get(id);
                queries.add(eq(StageDocumentToVariantConverter.ID_FIELD, id.getString(StageDocumentToVariantConverter.ID_FIELD)));
                if (binaryList.size() == 1) {
                    updates.add(combine(resumeStageLoad ? addToSet(fieldName, binaryList.get(0)) : push(fieldName, binaryList.get(0)),
                            addEachToSet(StageDocumentToVariantConverter.STUDY_FILE_FIELD, Arrays.asList(studyIdStr, studyFile)),
                            setOnInsert(StageDocumentToVariantConverter.END_FIELD, id.get(StageDocumentToVariantConverter.END_FIELD)),
                            setOnInsert(StageDocumentToVariantConverter.REF_FIELD, id.get(StageDocumentToVariantConverter.REF_FIELD)),
                            setOnInsert(StageDocumentToVariantConverter.ALT_FIELD, id.get(StageDocumentToVariantConverter.ALT_FIELD))));
                } else {
                    updates.add(combine(resumeStageLoad ? addEachToSet(fieldName, binaryList) : pushEach(fieldName, binaryList),
                            addEachToSet(StageDocumentToVariantConverter.STUDY_FILE_FIELD, Arrays.asList(studyIdStr, studyFile)),
                            setOnInsert(StageDocumentToVariantConverter.END_FIELD, id.get(StageDocumentToVariantConverter.END_FIELD)),
                            setOnInsert(StageDocumentToVariantConverter.REF_FIELD, id.get(StageDocumentToVariantConverter.REF_FIELD)),
                            setOnInsert(StageDocumentToVariantConverter.ALT_FIELD, id.get(StageDocumentToVariantConverter.ALT_FIELD))));
                }
            }
        }

        try {
            final BulkWriteResult mongoResult = collection.update(queries, updates, QUERY_OPTIONS).first();
            result.setNewVariants(mongoResult.getInsertedCount())
                    .setUpdatedVariants(mongoResult.getModifiedCount());
        } catch (MongoBulkWriteException e) {
            result.setNewVariants(e.getWriteResult().getInsertedCount())
                    .setUpdatedVariants(e.getWriteResult().getModifiedCount());


            if (retryIds != null) {
                // If retryIds != null, means that this this was the second attempt to update. In this case, do fail.
                LOGGER.error("BulkWriteErrors when retrying the updates");
                throw e;
            }

            nonInsertedIds = new HashSet<>();
            for (BulkWriteError writeError : e.getWriteErrors()) {
                if (ErrorCategory.fromErrorCode(writeError.getCode()).equals(ErrorCategory.DUPLICATE_KEY)) { //Dup Key error code
                    Matcher matcher = DUP_KEY_WRITE_RESULT_ERROR_PATTERN.matcher(writeError.getMessage());
                    if (matcher.find()) {
                        String id = matcher.group(1);
                        nonInsertedIds.add(id);
                        LOGGER.warn("Catch error : {}",  writeError.toString());
                        LOGGER.warn("DupKey exception inserting '{}'. Retry!", id);
                    } else {
                        LOGGER.error("WriteError with code {} does not match with the pattern {}",
                                writeError.getCode(), DUP_KEY_WRITE_RESULT_ERROR_PATTERN.pattern());
                        throw e;
                    }
                } else {
                    throw e;
                }
            }
        }
        return nonInsertedIds;
    }

    public static long cleanStageCollection(MongoDBCollection stageCollection, int studyId, int fileId) {
        //Delete those studies that have duplicated variants. Those are not inserted, so they are not new variants.
        long modifiedCount = stageCollection.update(
                and(exists(studyId + "." + fileId + ".1"), exists(studyId + "." + NEW_STUDY_FIELD, false)),
                unset(Integer.toString(studyId)),
                new QueryOptions(MongoDBCollection.MULTI, true)).first().getModifiedCount();
        modifiedCount += stageCollection.update(
                exists(studyId + "." + fileId),
                combine(
//                        unset(studyId + "." + fileId),
                        set(studyId + "." + fileId, null),
                        set(studyId + "." + NEW_STUDY_FIELD, false)
                ), new QueryOptions(MongoDBCollection.MULTI, true)).first().getModifiedCount();
        return modifiedCount;
    }

    public static long cleanStageCollection(MongoDBCollection stageCollection, int studyId, List<Integer> fileIds,
                                            Collection<String> chromosomes, MongoDBVariantWriteResult result) {
        boolean removeDuplicatedVariants = result == null || result.getNonInsertedVariants() > 0;
        // Delete those new studies that have duplicated variants. Those are not inserted, so they are not new variants.
        // i.e: For each file, or the file has not been loaded (empty), or the file has more than one element.
        //     { $or : [ { <study>.<file>.0 : {$exists:false} }, { <study>.<file>.1 : {$exists:true} } ] }
        List<Bson> filters = new ArrayList<>();
        long modifiedCount = 0;
        Bson chrFilter;
        if (chromosomes != null && !chromosomes.isEmpty()) {
            List<Bson> chrFilters = new ArrayList<>();
            for (String chromosome : chromosomes) {
                MongoDBVariantStageReader.addChromosomeFilter(chrFilters, chromosome);
            }
            chrFilter = or(chrFilters);
        } else {
            chrFilter = new Document();
        }

        List<String> studyFiles = new ArrayList<>(fileIds.size());
        for (Integer fileId : fileIds) {
            String studyFile = studyId + "_" + fileId;
            studyFiles.add(studyFile);
        }
        if (removeDuplicatedVariants) {
            // TODO: This variants should be removed while loading data. This operation is taking too much time.
            filters.add(exists(studyId + "." + NEW_STUDY_FIELD, false));
            List<Bson> updates = new ArrayList<>(fileIds.size() + 1);
            for (Integer fileId : fileIds) {
                String studyFile = studyId + "_" + fileId;
                // Can not unset value!
                updates.add(set(studyId + "." + fileId, null));
                filters.add(
                        or(
                                ne(StageDocumentToVariantConverter.STUDY_FILE_FIELD, studyFile),
                                and(
                                        eq(StageDocumentToVariantConverter.STUDY_FILE_FIELD, studyFile),
                                        exists(studyId + "." + fileId + ".1")
                                )
                        )
                );
            }
            updates.add(pullAll(StageDocumentToVariantConverter.STUDY_FILE_FIELD, studyFiles));
            Bson filter = and(in(StageDocumentToVariantConverter.STUDY_FILE_FIELD, studyFiles), chrFilter, and(filters));
            LOGGER.info("Clean studies from stage where all the files where duplicated");
            modifiedCount += stageCollection.update(
                    filter, combine(updates),
                    new QueryOptions(MongoDBCollection.MULTI, true)).first().getModifiedCount();
        }

        filters.clear();
        filters.add(in(StageDocumentToVariantConverter.STUDY_FILE_FIELD, studyFiles));
        filters.add(chrFilter);
        List<Bson> updates = new LinkedList<>();
        for (Integer fileId : fileIds) {
            updates.add(unset(studyId + "." + fileId));
//            updates.add(set(studyId + "." + fileId, null));
        }
        updates.add(set(studyId + "." + NEW_STUDY_FIELD, false));
        updates.add(pullAll(StageDocumentToVariantConverter.STUDY_FILE_FIELD, studyFiles));
        LOGGER.info("Cleaning files {} from stage collection", fileIds);
        modifiedCount += stageCollection.update(and(filters), combine(updates),
                new QueryOptions(MongoDBCollection.MULTI, true)).first().getModifiedCount();

        return modifiedCount;
    }


    public MongoDBVariantWriteResult getWriteResult() {
        return writeResult;
    }
}
