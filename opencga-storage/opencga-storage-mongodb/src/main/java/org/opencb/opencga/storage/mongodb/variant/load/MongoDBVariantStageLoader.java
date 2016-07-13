package org.opencb.opencga.storage.mongodb.variant.load;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantWriteResult;
import org.opencb.opencga.storage.mongodb.variant.converters.VariantStringIdComplexTypeConverter;
import org.opencb.opencga.storage.mongodb.variant.converters.stage.VariantToAvroBinaryConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;
import static org.opencb.opencga.storage.mongodb.variant.converters.VariantStringIdComplexTypeConverter.*;

/**
 * Created on 07/04/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoDBVariantStageLoader implements DataWriter<Variant> {

    public static final String NEW_STUDY_FIELD = "new";
    public static final boolean NEW_STUDY_DEFAULT = true;

    private static final QueryOptions QUERY_OPTIONS = new QueryOptions(MongoDBCollection.UPSERT, true);
    public static final Pattern DUP_KEY_WRITE_RESULT_ERROR_PATTERN = Pattern.compile("^.*dup key: \\{ : \"([^\"]*)\" \\}$");

    private final MongoDBCollection collection;
    private final int studyId;
    private final int fileId;
    private final int numTotalVariants;
    private final String fieldName;
    private final boolean resumeStageLoad;
    private final Logger logger = LoggerFactory.getLogger(MongoDBVariantStageLoader.class);

    private final AtomicInteger variantsCount;
    public static final int DEFAULT_LOGING_BATCH_SIZE = 5000;
    private final int loggingBatchSize;
    private final MongoDBVariantWriteResult writeResult = new MongoDBVariantWriteResult();

    public static final ComplexTypeConverter<Variant, Binary> VARIANT_CONVERTER_DEFAULT = new VariantToAvroBinaryConverter();

    public static final VariantStringIdComplexTypeConverter STRING_ID_CONVERTER = new VariantStringIdComplexTypeConverter();

    public MongoDBVariantStageLoader(MongoDBCollection collection, int studyId, int fileId, int numTotalVariants, boolean resumeStageLoad) {
        this.collection = collection;
        this.studyId = studyId;
        this.fileId = fileId;
        this.numTotalVariants = numTotalVariants;
        fieldName = studyId + "." + fileId;
        variantsCount = new AtomicInteger(0);
        loggingBatchSize = Math.max(numTotalVariants / 200, DEFAULT_LOGING_BATCH_SIZE);
        this.resumeStageLoad = resumeStageLoad;
    }

    @Override
    public boolean write(List<Variant> batch) {
        insert(batch);
        return true;
    }

    public MongoDBVariantWriteResult insert(List<Variant> variants) {
        return insert(variants.stream());
    }

    public MongoDBVariantWriteResult insert(Stream<Variant> stream) {
        final long start = System.nanoTime();
        final int[] variantsLocalCount = {0};
        final int[] skippedVariants = {0};

        ListMultimap<Document, Binary> ids = LinkedListMultimap.create();

        stream.forEach(variant -> {
            variantsLocalCount[0]++;
            if (variant.getType().equals(VariantType.NO_VARIATION) || variant.getType().equals(VariantType.SYMBOLIC)) {
                skippedVariants[0]++;
                return;
            }
            Binary binary = VARIANT_CONVERTER_DEFAULT.convertToStorageType(variant);
            Document id = STRING_ID_CONVERTER.convertToStorageType(variant);

            ids.put(id, binary);
        });

        MongoDBVariantWriteResult result = new MongoDBVariantWriteResult();
        Set<String> retryKeys = updateMongo(ids, result, null);
        if (!retryKeys.isEmpty()) {
            updateMongo(ids, result, retryKeys);
        }

        int previousCount = variantsCount.getAndAdd(variantsLocalCount[0]);
        if ((previousCount + variantsLocalCount[0]) / loggingBatchSize != previousCount / loggingBatchSize) {
            logger.info("Write variants in STAGE collection " + (previousCount + variantsLocalCount[0]) + "/" + numTotalVariants + " "
                    + String.format("%.2f%%", ((float) (previousCount + variantsLocalCount[0])) / numTotalVariants * 100.0));
        }

        result.setNewVariantsNanoTime(System.nanoTime() - start)
                .setSkippedVariants(skippedVariants[0]);

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
            if (retryIds == null || retryIds.contains(id.getString("_id"))) {
                List<Binary> binaryList = values.get(id);
                queries.add(eq("_id", id.getString("_id")));
                if (binaryList.size() == 1) {
                    updates.add(combine(resumeStageLoad ? addToSet(fieldName, binaryList.get(0)) : push(fieldName, binaryList.get(0)),
                            setOnInsert(END_FIELD, id.get(END_FIELD)),
                            setOnInsert(REF_FIELD, id.get(REF_FIELD)),
                            setOnInsert(ALT_FIELD, id.get(ALT_FIELD))));
                } else {
                    updates.add(combine(resumeStageLoad ? addEachToSet(fieldName, binaryList) : pushEach(fieldName, binaryList),
                            setOnInsert(END_FIELD, id.get(END_FIELD)),
                            setOnInsert(REF_FIELD, id.get(REF_FIELD)),
                            setOnInsert(ALT_FIELD, id.get(ALT_FIELD))));
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
                logger.error("BulkWriteErrors when retrying the updates");
                throw e;
            }

            nonInsertedIds = new HashSet<>();
            for (BulkWriteError writeError : e.getWriteErrors()) {
                if (ErrorCategory.fromErrorCode(writeError.getCode()).equals(ErrorCategory.DUPLICATE_KEY)) { //Dup Key error code
                    Matcher matcher = DUP_KEY_WRITE_RESULT_ERROR_PATTERN.matcher(writeError.getMessage());
                    if (matcher.find()) {
                        String id = matcher.group(1);
                        nonInsertedIds.add(id);
                        logger.warn("Catch error : {}",  writeError.toString());
                        logger.warn("DupKey exception inserting '{}'. Retry!", id);
                    } else {
                        logger.error("WriteError with code {} does not match with the pattern {}",
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

    public static long cleanStageCollection(MongoDBCollection stageCollection, int studyId, List<Integer> fileIds) {
        return cleanStageCollection(stageCollection, studyId, fileIds, null);
    }

    public static long cleanStageCollection(MongoDBCollection stageCollection, int studyId, List<Integer> fileIds,
                                            Collection<String> chromosomes) {
        // Delete those new studies that have duplicated variants. Those are not inserted, so they are not new variants.
        // i.e: For each file, or the file has not been loaded (empty), or the file has more than one element.
        //     { $or : [ { <study>.<file>.0 : {$exists:false} }, { <study>.<file>.1 : {$exists:true} } ] }
        List<Bson> filters = new ArrayList<>();
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

        filters.add(exists(studyId + "." + NEW_STUDY_FIELD, false));
        for (Integer fileId : fileIds) {
            filters.add(or(exists(studyId + "." + fileId + ".0", false), exists(studyId + "." + fileId + ".1")));
        }
        long modifiedCount = stageCollection.update(
                and(chrFilter, and(filters)), unset(Integer.toString(studyId)),
                new QueryOptions(MongoDBCollection.MULTI, true)).first().getModifiedCount();


        filters.clear();
        List<Bson> updates = new LinkedList<>();
        for (Integer fileId : fileIds) {
            filters.add(exists(studyId + "." + fileId));
//            updates.add(unset(studyId + "." + fileId));
            updates.add(set(studyId + "." + fileId, null));
        }
        updates.add(set(studyId + "." + NEW_STUDY_FIELD, false));
        modifiedCount += stageCollection.update(and(chrFilter, or(filters)), combine(updates),
                new QueryOptions(MongoDBCollection.MULTI, true)).first().getModifiedCount();

        return modifiedCount;
    }


    public MongoDBVariantWriteResult getWriteResult() {
        return writeResult;
    }
}
