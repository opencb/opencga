/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.storage.mongodb.variant;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.mongodb.*;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.io.DataWriter;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.mongodb.MongoDBCollection;
import org.opencb.datastore.mongodb.MongoDataStore;
import org.opencb.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.config.StorageEngineConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.*;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorUtils.QueryOperation;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorUtils.*;

/**
 * @author Ignacio Medina <igmecas@gmail.com>
 * @author Jacobo Coll <jacobo167@gmail.com>
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class VariantMongoDBAdaptor implements VariantDBAdaptor {

    public static final String DEFAULT_TIMEOUT = "dbadaptor.default_timeout";
    public static final String MAX_TIMEOUT = "dbadaptor.max_timeout";
    private final MongoDataStoreManager mongoManager;
    private final MongoDataStore db;
    private final String collectionName;
    private final MongoDBCollection variantsCollection;
    private final VariantSourceMongoDBAdaptor variantSourceMongoDBAdaptor;
    private final ObjectMap configuration;
    private final StorageEngineConfiguration storageEngineConfiguration;
    private final Pattern writeResultErrorPattern = Pattern.compile("^.*dup key: \\{ : \"([^\"]*)\" \\}$");
    private final VariantDBAdaptorUtils utils;

    private StudyConfigurationManager studyConfigurationManager;

    @Deprecated
    private DataWriter dataWriter;

    protected static Logger logger = LoggerFactory.getLogger(VariantMongoDBAdaptor.class);

    public VariantMongoDBAdaptor(MongoCredentials credentials, String variantsCollectionName, String filesCollectionName,
                                 StudyConfigurationManager studyConfigurationManager, StorageEngineConfiguration storageEngineConfiguration)
            throws UnknownHostException {
        // MongoDB configuration
        mongoManager = new MongoDataStoreManager(credentials.getDataStoreServerAddresses());
        db = mongoManager.get(credentials.getMongoDbName(), credentials.getMongoDBConfiguration());
        variantSourceMongoDBAdaptor = new VariantSourceMongoDBAdaptor(credentials, filesCollectionName);
        collectionName = variantsCollectionName;
        variantsCollection = db.getCollection(collectionName);
        this.studyConfigurationManager = studyConfigurationManager;
        this.storageEngineConfiguration = storageEngineConfiguration;
        this.configuration = storageEngineConfiguration == null || this.storageEngineConfiguration.getVariant().getOptions() == null
                ? new ObjectMap()
                : this.storageEngineConfiguration.getVariant().getOptions();
        this.utils = new VariantDBAdaptorUtils(this);
    }


    @Override
    @Deprecated
    public void setDataWriter(DataWriter dataWriter) {
        this.dataWriter = dataWriter;
    }


    @Override
    public QueryResult insert(List<Variant> variants, String studyName, QueryOptions options) {
        StudyConfiguration studyConfiguration = studyConfigurationManager.getStudyConfiguration(studyName, options).first();
        // TODO FILE_ID must be in QueryOptions?
        int fileId = options.getInt(VariantStorageManager.Options.FILE_ID.key());
        boolean includeStats = options.getBoolean(VariantStorageManager.Options.INCLUDE_STATS.key(), VariantStorageManager.Options
                .INCLUDE_STATS.defaultValue());
//        boolean includeSrc = options.getBoolean(VariantStorageManager.Options.INCLUDE_SRC.key(), VariantStorageManager.Options
//                .INCLUDE_SRC.defaultValue());
//        boolean includeGenotypes = options.getBoolean(VariantStorageManager.Options.INCLUDE_GENOTYPES.key(), VariantStorageManager
//                .Options.INCLUDE_GENOTYPES.defaultValue());
//        boolean compressGenotypes = options.getBoolean(VariantStorageManager.Options.COMPRESS_GENOTYPES.key(), VariantStorageManager
// .Options.COMPRESS_GENOTYPES.defaultValue());
//        String defaultGenotype = options.getString(MongoDBVariantStorageManager.DEFAULT_GENOTYPE, "0|0");

        DBObjectToVariantConverter variantConverter = new DBObjectToVariantConverter(null, includeStats ? new
                DBObjectToVariantStatsConverter(studyConfigurationManager) : null);
//        DBObjectToStudyVariantEntryConverter sourceEntryConverter = new DBObjectToStudyVariantEntryConverter(includeSrc,
//                includeGenotypes ? new DBObjectToSamplesConverter(studyConfiguration) : null);
        DBObjectToStudyVariantEntryConverter sourceEntryConverter =
                new DBObjectToStudyVariantEntryConverter(true, new DBObjectToSamplesConverter(studyConfiguration));
        return insert(variants, fileId, variantConverter, sourceEntryConverter, studyConfiguration, getLoadedSamples(fileId,
                studyConfiguration));
    }

    @Override
    public QueryResult delete(Query query, QueryOptions options) {
        QueryBuilder qb = QueryBuilder.start();
        qb = parseQuery(query, qb);
        logger.debug("Delete to be executed: '{}'", qb.get().toString());
        QueryResult queryResult = variantsCollection.remove(qb.get(), options);

        return queryResult;
    }

    @Override
    public QueryResult deleteSamples(String studyName, List<String> sampleNames, QueryOptions options) {
        //TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult deleteFile(String studyName, String fileName, QueryOptions options) {
        //TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult deleteStudy(String studyName, QueryOptions options) {
        if (options == null) {
            options = new QueryOptions();
        }
        StudyConfiguration studyConfiguration = studyConfigurationManager.getStudyConfiguration(studyName, options).first();
        DBObject query = parseQuery(new Query(VariantQueryParams.STUDIES.key(), studyConfiguration.getStudyId()), new QueryBuilder()).get();

        // { $pull : { files : {  sid : <studyId> } } }
        BasicDBObject update = new BasicDBObject(
                "$pull",
                new BasicDBObject(
                        DBObjectToVariantConverter.STUDIES_FIELD,
                        new BasicDBObject(
                                DBObjectToStudyVariantEntryConverter.STUDYID_FIELD, studyConfiguration.getStudyId()
                        )
                )
        );
        QueryResult<WriteResult> result = variantsCollection.update(query, update, new QueryOptions("multi", true));

        logger.debug("deleteStudy: query = {}", query);
        logger.debug("deleteStudy: update = {}", update);
        if (options.getBoolean("purge", false)) {
            BasicDBObject purgeQuery = new BasicDBObject(DBObjectToVariantConverter.STUDIES_FIELD, new BasicDBObject("$size", 0));
            variantsCollection.remove(purgeQuery, new QueryOptions("multi", true));
        }

        return result;
    }

    @Override
    public QueryResult<Variant> get(Query query, QueryOptions options) {
        if (options == null) {
            options = new QueryOptions();
        }
        studyConfigurationManager.setDefaultQueryOptions(options);

        QueryBuilder qb = QueryBuilder.start();
//        parseQueryOptions(options, qb);
        parseQuery(query, qb);
//        DBObject projection = parseProjectionQueryOptions(options);
        DBObject projection = createProjection(query, options);
        logger.debug("Query to be executed: '{}'", qb.get().toString());
        options.putIfAbsent(MongoDBCollection.SKIP_COUNT, true);

        int defaultTimeout = configuration.getInt(DEFAULT_TIMEOUT, 3_000);
        int maxTimeout = configuration.getInt(MAX_TIMEOUT, 30_000);
        int timeout = options.getInt(MongoDBCollection.TIMEOUT, defaultTimeout);
        if (timeout > maxTimeout || timeout < 0) {
            timeout = maxTimeout;
        }
        options.put(MongoDBCollection.TIMEOUT, timeout);

        if (options.getBoolean("mongodb.explain", false)) {
            try (DBCursor dbCursor = variantsCollection.nativeQuery().find(qb.get(), projection, options)) {
                DBObject explain = dbCursor.explain();
                try {
                    System.err.println("mongodb.explain = "
                            + new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(explain));
                } catch (JsonProcessingException ignore) {
                    System.err.println("mongodb.explain = " + explain);
                }
            }
        }
        QueryResult<Variant> queryResult = variantsCollection.find(qb.get(), projection, getDbObjectToVariantConverter(query, options),
                options);
        // set query Id?
        return queryResult;
    }

    @Override
    public List<QueryResult<Variant>> get(List<Query> queries, QueryOptions options) {
        List<QueryResult<Variant>> queryResultList = new ArrayList<>(queries.size());
        for (Query query : queries) {
            QueryResult<Variant> queryResult = get(query, options);
            queryResultList.add(queryResult);
        }
        return queryResultList;
    }


    @Override
    public QueryResult<Long> count(Query query) {
        QueryBuilder qb = QueryBuilder.start();
        parseQuery(query, qb);
        logger.debug("Query to be executed: '{}'", qb.get().toString());
        return variantsCollection.count(qb.get());
    }

    @Override
    public QueryResult distinct(Query query, String field) {
        String documentPath;
        switch (field) {
            case "gene":
            case "ensemblGene":
                documentPath = DBObjectToVariantConverter.ANNOTATION_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.ENSEMBL_GENE_ID_FIELD;
                break;
            case "ensemblTranscript":
                documentPath = DBObjectToVariantConverter.ANNOTATION_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.ENSEMBL_TRANSCRIPT_ID_FIELD;
                break;
            case "ct":
            case "consequence_type":
                documentPath = DBObjectToVariantConverter.ANNOTATION_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.SO_ACCESSION_FIELD;
                break;
            default:
                documentPath = DBObjectToVariantConverter.ANNOTATION_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.GENE_NAME_FIELD;
                break;
        }

        QueryBuilder qb = QueryBuilder.start();
        parseQuery(query, qb);
        return variantsCollection.distinct(documentPath, qb.get());
    }

    @Override
    public VariantDBIterator iterator() {
        return iterator(new Query(), new QueryOptions());
    }

    @Override
    public VariantDBIterator iterator(Query query, QueryOptions options) {
        if (options == null) {
            options = new QueryOptions();
        }
        if (query == null) {
            query = new Query();
        }
        QueryBuilder qb = QueryBuilder.start();
//        parseQueryOptions(options, qb);
        qb = parseQuery(query, qb);
//        DBObject projection = parseProjectionQueryOptions(options);
        DBObject projection = createProjection(query, options);
        DBCursor dbCursor = variantsCollection.nativeQuery().find(qb.get(), projection, options);
        dbCursor.batchSize(options.getInt("batchSize", 100));
        return new VariantMongoDBIterator(dbCursor, getDbObjectToVariantConverter(query, options));
    }

    @Override
    public void forEach(Consumer<? super Variant> action) {
        forEach(new Query(), action, new QueryOptions());
    }

    @Override
    public void forEach(Query query, Consumer<? super Variant> action, QueryOptions options) {
        Objects.requireNonNull(action);
        VariantDBIterator variantDBIterator = iterator(query, options);
        while (variantDBIterator.hasNext()) {
            action.accept(variantDBIterator.next());
        }
    }


    @Override
    public QueryResult getFrequency(Query query, Region region, int regionIntervalSize) {
        // db.variants.aggregate( { $match: { $and: [ {chr: "1"}, {start: {$gt: 251391, $lt: 2701391}} ] }},
        //                        { $group: { _id: { $subtract: [ { $divide: ["$start", 20000] }, { $divide: [{$mod: ["$start", 20000]},
        // 20000] } ] },
        //                                  totalCount: {$sum: 1}}})

        QueryOptions options = new QueryOptions();

        // If interval is not provided is set to the value that returns 200 values
        if (regionIntervalSize <= 0) {
//            regionIntervalSize = options.getInt("interval", (region.getEnd() - region.getStart()) / 200);
            regionIntervalSize = (region.getEnd() - region.getStart()) / 200;
        }

        BasicDBObject start = new BasicDBObject("$gt", region.getStart());
        start.append("$lt", region.getEnd());

        BasicDBList andArr = new BasicDBList();
        andArr.add(new BasicDBObject(DBObjectToVariantConverter.CHROMOSOME_FIELD, region.getChromosome()));
        andArr.add(new BasicDBObject(DBObjectToVariantConverter.START_FIELD, start));

        // Parsing the rest of options
        QueryBuilder qb = new QueryBuilder();
//        DBObject optionsMatch = parseQueryOptions(options, qb).get();
        DBObject optionsMatch = parseQuery(query, qb).get();
        if (!optionsMatch.keySet().isEmpty()) {
            andArr.add(optionsMatch);
        }
        DBObject match = new BasicDBObject("$match", new BasicDBObject("$and", andArr));

//        qb.and("_at.chunkIds").in(chunkIds);
//        qb.and(DBObjectToVariantConverter.END_FIELD).greaterThanEquals(region.getStart());
//        qb.and(DBObjectToVariantConverter.START_FIELD).lessThanEquals(region.getEnd());
//
//        List<String> chunkIds = getChunkIds(region);
//        DBObject regionObject = new BasicDBObject("_at.chunkIds", new BasicDBObject("$in", chunkIds))
//                .append(DBObjectToVariantConverter.END_FIELD, new BasicDBObject("$gte", region.getStart()))
//                .append(DBObjectToVariantConverter.START_FIELD, new BasicDBObject("$lte", region.getEnd()));

        BasicDBList divide1 = new BasicDBList();
        divide1.add("$start");
        divide1.add(regionIntervalSize);

        BasicDBList divide2 = new BasicDBList();
        divide2.add(new BasicDBObject("$mod", divide1));
        divide2.add(regionIntervalSize);

        BasicDBList subtractList = new BasicDBList();
        subtractList.add(new BasicDBObject("$divide", divide1));
        subtractList.add(new BasicDBObject("$divide", divide2));

        BasicDBObject subtract = new BasicDBObject("$subtract", subtractList);
        DBObject totalCount = new BasicDBObject("$sum", 1);
        BasicDBObject g = new BasicDBObject("_id", subtract);
        g.append("features_count", totalCount);
        DBObject group = new BasicDBObject("$group", g);
        DBObject sort = new BasicDBObject("$sort", new BasicDBObject("_id", 1));

//        logger.info("getAllIntervalFrequencies - (>·_·)>");
//        System.out.println(options.toString());
//        System.out.println(match.toString());
//        System.out.println(group.toString());
//        System.out.println(sort.toString());

        long dbTimeStart = System.currentTimeMillis();
        QueryResult output = variantsCollection.aggregate(/*"$histogram", */Arrays.asList(match, group, sort), options);
        long dbTimeEnd = System.currentTimeMillis();

        Map<Long, DBObject> ids = new HashMap<>();
        // Create DBObject for intervals with features inside them
        for (DBObject intervalObj : (List<DBObject>) output.getResult()) {
            Long auxId = Math.round((Double) intervalObj.get("_id")); //is double

            DBObject intervalVisited = ids.get(auxId);
            if (intervalVisited == null) {
                intervalObj.put("_id", auxId);
                intervalObj.put("start", getChunkStart(auxId.intValue(), regionIntervalSize));
                intervalObj.put("end", getChunkEnd(auxId.intValue(), regionIntervalSize));
                intervalObj.put("chromosome", region.getChromosome());
                intervalObj.put("features_count", Math.log((int) intervalObj.get("features_count")));
                ids.put(auxId, intervalObj);
            } else {
                Double sum = (Double) intervalVisited.get("features_count") + Math.log((int) intervalObj.get("features_count"));
                intervalVisited.put("features_count", sum.intValue());
            }
        }

        // Create DBObject for intervals without features inside them
        BasicDBList resultList = new BasicDBList();
        int firstChunkId = getChunkId(region.getStart(), regionIntervalSize);
        int lastChunkId = getChunkId(region.getEnd(), regionIntervalSize);
        DBObject intervalObj;
        for (int chunkId = firstChunkId; chunkId <= lastChunkId; chunkId++) {
            intervalObj = ids.get((long) chunkId);
            if (intervalObj == null) {
                intervalObj = new BasicDBObject();
                intervalObj.put("_id", chunkId);
                intervalObj.put("start", getChunkStart(chunkId, regionIntervalSize));
                intervalObj.put("end", getChunkEnd(chunkId, regionIntervalSize));
                intervalObj.put("chromosome", region.getChromosome());
                intervalObj.put("features_count", 0);
            }
            resultList.add(intervalObj);
        }

        QueryResult queryResult = new QueryResult(region.toString(), ((Long) (dbTimeEnd - dbTimeStart)).intValue(),
                resultList.size(), resultList.size(), null, null, resultList);
        return queryResult;
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) {
        QueryOptions options = new QueryOptions();
        options.put("limit", numResults);
        options.put("count", true);
        options.put("order", (asc) ? 1 : -1); // MongoDB: 1 = ascending, -1 = descending

        return groupBy(query, field, options);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) {
        String documentPath;
        String unwindPath;
        int numUnwinds = 2;
        switch (field) {
            case "gene":
            case "ensemblGene":
                documentPath = DBObjectToVariantConverter.ANNOTATION_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.ENSEMBL_GENE_ID_FIELD;
                unwindPath = DBObjectToVariantConverter.ANNOTATION_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD;

                break;
            case "ct":
            case "consequence_type":
                documentPath = DBObjectToVariantConverter.ANNOTATION_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.SO_ACCESSION_FIELD;
                unwindPath = DBObjectToVariantConverter.ANNOTATION_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD;
                numUnwinds = 3;
                break;
            default:
                documentPath = DBObjectToVariantConverter.ANNOTATION_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.GENE_NAME_FIELD;
                unwindPath = DBObjectToVariantConverter.ANNOTATION_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD;
                break;
        }

        QueryBuilder qb = QueryBuilder.start();
        parseQuery(query, qb);

        boolean count = options != null && options.getBoolean("count", false);
        int order = options != null ? options.getInt("order", -1) : -1;

        DBObject project;
        DBObject projectAndCount;
        if (count) {
            project = new BasicDBObject("$project", new BasicDBObject("field", "$" + documentPath));
            projectAndCount = new BasicDBObject("$project", new BasicDBObject()
                    .append("id", "$_id")
                    .append("_id", 0)
                    .append("count", new BasicDBObject("$size", "$values")));
        } else {
            project = new BasicDBObject("$project", new BasicDBObject()
                    .append("field", "$" + documentPath)
                    //.append("_id._id", "$_id")
                    .append("_id.start", "$" + DBObjectToVariantConverter.START_FIELD)
                    .append("_id.end", "$" + DBObjectToVariantConverter.END_FIELD)
                    .append("_id.chromosome", "$" + DBObjectToVariantConverter.CHROMOSOME_FIELD)
                    .append("_id.alternate", "$" + DBObjectToVariantConverter.ALTERNATE_FIELD)
                    .append("_id.reference", "$" + DBObjectToVariantConverter.REFERENCE_FIELD)
                    .append("_id.ids", "$" + DBObjectToVariantConverter.IDS_FIELD));
            projectAndCount = new BasicDBObject("$project", new BasicDBObject()
                    .append("id", "$_id")
                    .append("_id", 0)
                    .append("values", "$values")
                    .append("count", new BasicDBObject("$size", "$values")));
        }

        DBObject match = new BasicDBObject("$match", qb.get());
        DBObject unwindField = new BasicDBObject("$unwind", "$field");
        DBObject notNull = new BasicDBObject("$match", new BasicDBObject("field", new BasicDBObject("$ne", null)));
        DBObject groupAndAddToSet = new BasicDBObject("$group", new BasicDBObject("_id", "$field")
                .append("values", new BasicDBObject("$addToSet", "$_id"))); // sum, count, avg, ...?
        DBObject sort = new BasicDBObject("$sort", new BasicDBObject("count", order)); // 1 = ascending, -1 = descending
        DBObject skip = null;
        if (options != null && options.getInt("skip", -1) > 0) {
            skip = new BasicDBObject("$skip", options.getInt("skip", -1));
        }
        DBObject limit = new BasicDBObject("$limit",
                options != null && options.getInt("limit", -1) > 0 ? options.getInt("limit") : 10);

        List<DBObject> operations = new LinkedList<>();
        operations.add(match);
        operations.add(project);
        for (int i = 0; i < numUnwinds; i++) {
            operations.add(unwindField);
        }
        operations.add(notNull);
        operations.add(groupAndAddToSet);
        operations.add(projectAndCount);
        operations.add(sort);
        if (skip != null) {
            operations.add(skip);
        }
        operations.add(limit);
        logger.debug("db." + collectionName + ".aggregate( " + operations + " )");
        QueryResult<DBObject> queryResult = variantsCollection.aggregate(operations, options);

//            List<Map<String, Object>> results = new ArrayList<>(queryResult.getResult().size());
//            results.addAll(queryResult.getResult().stream().map(dbObject -> new ObjectMap("id", dbObject.get("_id")).append("count",
// dbObject.get("count"))).collect(Collectors.toList()));
        List<Map> results = queryResult.getResult().stream().map(DBObject::toMap).collect(Collectors.toList());

        return new QueryResult<>(queryResult.getId(), queryResult.getDbTime(), queryResult.getNumResults(), queryResult
                .getNumTotalResults(), queryResult.getWarningMsg(), queryResult.getErrorMsg(), results);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) {
        String warningMsg = "Unimplemented VariantMongoDBAdaptor::groupBy list of fields. Using field[0] : '" + fields.get(0) + "'";
        logger.warn(warningMsg);
        QueryResult queryResult = groupBy(query, fields.get(0), options);
        queryResult.setWarningMsg(warningMsg);
        return queryResult;
    }

    @Override
    public Map<Integer, List<Integer>> getReturnedSamples(Query query, QueryOptions options) {

        List<Integer> studyIds = utils.getStudyIds(query.getAsList(VariantQueryParams.RETURNED_STUDIES.key()), options);
        if (studyIds.isEmpty()) {
            studyIds = utils.getStudyIds(getStudyConfigurationManager().getStudyNames(options), options);
        }

        List<String> returnedSamples = query.getAsStringList(VariantQueryParams.RETURNED_SAMPLES.key())
                .stream().map(s -> s.contains(":") ? s.split(":")[1] : s).collect(Collectors.toList());
        LinkedHashSet<String> returnedSamplesSet = new LinkedHashSet<>(returnedSamples);

        Map<Integer, List<Integer>> samples = new HashMap<>(studyIds.size());
        for (Integer studyId : studyIds) {
            StudyConfiguration sc = getStudyConfigurationManager().getStudyConfiguration(studyId, options).first();
            LinkedHashMap<String, Integer> returnedSamplesPosition = StudyConfiguration.getReturnedSamplesPosition(sc, returnedSamplesSet);
            List<Integer> sampleNames = Arrays.asList(new Integer[returnedSamplesPosition.size()]);
            returnedSamplesPosition.forEach((sample, position) -> sampleNames.set(position, sc.getSampleIds().get(sample)));
            samples.put(studyId, sampleNames);
        }

        return samples;
    }

    @Override
    public QueryResult addStats(List<VariantStatsWrapper> variantStatsWrappers, String studyName, QueryOptions queryOptions) {
        return updateStats(variantStatsWrappers, studyName, queryOptions);
    }

    @Override
    public QueryResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, String studyName, QueryOptions options) {
        return updateStats(variantStatsWrappers, studyConfigurationManager.getStudyConfiguration(studyName, options).first(), options);
    }

    @Override
    public QueryResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, StudyConfiguration studyConfiguration,
                                   QueryOptions options) {
        DBCollection coll = db.getDb().getCollection(collectionName);
        BulkWriteOperation pullBuilder = coll.initializeUnorderedBulkOperation();
        BulkWriteOperation pushBuilder = coll.initializeUnorderedBulkOperation();

        long start = System.nanoTime();
        DBObjectToVariantStatsConverter statsConverter = new DBObjectToVariantStatsConverter(studyConfigurationManager);
//        VariantSource variantSource = queryOptions.get(VariantStorageManager.VARIANT_SOURCE, VariantSource.class);
        DBObjectToVariantConverter variantConverter = getDbObjectToVariantConverter(new Query(), options);
        boolean overwrite = options.getBoolean(VariantStorageManager.Options.OVERWRITE_STATS.key(), false);
        //TODO: Use the StudyConfiguration to change names to ids

        // TODO make unset of 'st' if already present?
        for (VariantStatsWrapper wrapper : variantStatsWrappers) {
            Map<String, VariantStats> cohortStats = wrapper.getCohortStats();
            Iterator<VariantStats> iterator = cohortStats.values().iterator();
            VariantStats variantStats = iterator.hasNext() ? iterator.next() : null;
            List<DBObject> cohorts = statsConverter.convertCohortsToStorageType(cohortStats, studyConfiguration.getStudyId());   // TODO
            // remove when we remove fileId
//            List cohorts = statsConverter.convertCohortsToStorageType(cohortStats, variantSource.getStudyId());   // TODO use when we
// remove fileId

            // add cohorts, overwriting old values if that cid, fid and sid already exists: remove and then add
            // db.variants.update(
            //      {_id:<id>},
            //      {$pull:{st:{cid:{$in:["Cohort 1","cohort 2"]}, fid:{$in:["file 1", "file 2"]}, sid:{$in:["study 1", "study 2"]}}}}
            // )
            // db.variants.update(
            //      {_id:<id>},
            //      {$push:{st:{$each: [{cid:"Cohort 1", fid:"file 1", ... , defaultValue:3},{cid:"Cohort 2", ... , defaultValue:3}] }}}
            // )

            if (!cohorts.isEmpty()) {
                String id = variantConverter.buildStorageId(wrapper.getChromosome(), wrapper.getPosition(),
                        variantStats.getRefAllele(), variantStats.getAltAllele());


                DBObject find = new BasicDBObject("_id", id);
                if (overwrite) {
                    List<BasicDBObject> idsList = new ArrayList<>(cohorts.size());
                    for (DBObject cohort : cohorts) {
                        BasicDBObject ids = new BasicDBObject()
                                .append(DBObjectToVariantStatsConverter.COHORT_ID, cohort.get(DBObjectToVariantStatsConverter.COHORT_ID))
                                .append(DBObjectToVariantStatsConverter.STUDY_ID, cohort.get(DBObjectToVariantStatsConverter.STUDY_ID));
                        idsList.add(ids);
                    }
                    DBObject update = new BasicDBObject("$pull",
                            new BasicDBObject(DBObjectToVariantConverter.STATS_FIELD,
                                    new BasicDBObject("$or", idsList)));

                    pullBuilder.find(find).updateOne(update);
                }

                DBObject push = new BasicDBObject("$push",
                        new BasicDBObject(DBObjectToVariantConverter.STATS_FIELD,
                                new BasicDBObject("$each", cohorts)));

                pushBuilder.find(find).update(push);
            }
        }

        // TODO handle if the variant didn't had that studyId in the files array
        // TODO check the substitution is done right if the stats are already present
        if (overwrite) {
            pullBuilder.execute();
        }
        BulkWriteResult writeResult = pushBuilder.execute();
        int writes = writeResult.getModifiedCount();


        return new QueryResult<>("", ((int) (System.nanoTime() - start)), writes, writes, "", "", Collections.singletonList(writeResult));
    }

    @Override
    public QueryResult deleteStats(String studyName, String cohortName, QueryOptions options) {
        StudyConfiguration studyConfiguration = studyConfigurationManager.getStudyConfiguration(studyName, options).first();
        int cohortId = studyConfiguration.getCohortIds().get(cohortName);

        // { st : { $elemMatch : {  sid : <studyId>, cid : <cohortId> } } }
        DBObject query = new BasicDBObject(DBObjectToVariantConverter.STATS_FIELD,
                new BasicDBObject("$elemMatch",
                        new BasicDBObject(DBObjectToVariantStatsConverter.STUDY_ID, studyConfiguration.getStudyId())
                                .append(DBObjectToVariantStatsConverter.COHORT_ID, cohortId)));

        // { $pull : { st : {  sid : <studyId>, cid : <cohortId> } } }
        BasicDBObject update = new BasicDBObject(
                "$pull",
                new BasicDBObject(DBObjectToVariantConverter.STATS_FIELD,
                        new BasicDBObject(DBObjectToVariantStatsConverter.STUDY_ID, studyConfiguration.getStudyId())
                                .append(DBObjectToVariantStatsConverter.COHORT_ID, cohortId)
                )
        );
        logger.debug("deleteStats: query = {}", query);
        logger.debug("deleteStats: update = {}", update);

        return variantsCollection.update(query, update, new QueryOptions("multi", true));
    }

    @Override
    public QueryResult addAnnotations(List<VariantAnnotation> variantAnnotations, QueryOptions queryOptions) {
        logger.warn("Unimplemented VariantMongoDBAdaptor::addAnnotations. Using \"VariantMongoDBAdaptor::updateAnnotations\"");
        return updateAnnotations(variantAnnotations, queryOptions);
    }

    @Override
    public QueryResult updateAnnotations(List<VariantAnnotation> variantAnnotations, QueryOptions queryOptions) {
        DBCollection coll = db.getDb().getCollection(collectionName);
        BulkWriteOperation builder = coll.initializeUnorderedBulkOperation();

        long start = System.nanoTime();
        DBObjectToVariantConverter variantConverter = getDbObjectToVariantConverter(new Query(), queryOptions);
        for (VariantAnnotation variantAnnotation : variantAnnotations) {
            String id = variantConverter.buildStorageId(variantAnnotation.getChromosome(), variantAnnotation.getStart(),
                    variantAnnotation.getReference(), variantAnnotation.getAlternate());
            DBObject find = new BasicDBObject("_id", id);
            DBObjectToVariantAnnotationConverter converter = new DBObjectToVariantAnnotationConverter();
            DBObject convertedVariantAnnotation = converter.convertToStorageType(variantAnnotation);
            DBObject update = new BasicDBObject("$set", new BasicDBObject(DBObjectToVariantConverter.ANNOTATION_FIELD + ".0",
                    convertedVariantAnnotation));
            builder.find(find).updateOne(update);
        }
        BulkWriteResult writeResult = builder.execute();

        return new QueryResult<>("", ((int) (System.nanoTime() - start)), 1, 1, "", "", Collections.singletonList(writeResult));
    }

    @Override
    public QueryResult deleteAnnotation(String annotationId, Query query, QueryOptions queryOptions) {
        if (queryOptions == null) {
            queryOptions = new QueryOptions();
        }
        DBObject dbQuery = parseQuery(query, new QueryBuilder()).get();
        logger.debug("deleteAnnotation: query = {}", dbQuery);

        DBObject update = new BasicDBObject("$set", new BasicDBObject(DBObjectToVariantConverter.ANNOTATION_FIELD + ".0", null));
        logger.debug("deleteAnnotation: update = {}", update);
        return variantsCollection.update(dbQuery, update, new QueryOptions("multi", true));
    }


    @Override
    public boolean close() {
        mongoManager.close(db.getDatabaseName());
        return true;
    }

    private QueryBuilder parseQuery(Query query, QueryBuilder builder) {
        if (query != null) {
            /** VARIANT PARAMS **/
            if (query.get(VariantQueryParams.CHROMOSOME.key()) != null && !query.getString(VariantQueryParams.CHROMOSOME.key()).isEmpty()) {
                List<String> chromosomes = query.getAsStringList(VariantQueryParams.CHROMOSOME.key());
                LinkedList<String> regions = new LinkedList<>(query.getAsStringList(VariantQueryParams.REGION.key()));
                regions.addAll(chromosomes);
                query.put(VariantQueryParams.REGION.key(), regions);
            }

            if (query.get(VariantQueryParams.REGION.key()) != null && !query.getString(VariantQueryParams.REGION.key()).isEmpty()) {
                List<String> stringList = query.getAsStringList(VariantQueryParams.REGION.key());
                List<Region> regions = new ArrayList<>(stringList.size());
                for (String reg : stringList) {
                    Region region = Region.parseRegion(reg);
                    regions.add(region);
                }
                getRegionFilter(regions, builder);
            }

            if (query.get(VariantQueryParams.ID.key()) != null && !query.getString(VariantQueryParams.ID.key()).isEmpty()) {
                String ids = query.getString(VariantQueryParams.ID.key());
                addQueryStringFilter(DBObjectToVariantConverter.ANNOTATION_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.XREFS_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.XREF_ID_FIELD, ids, builder, QueryOperation.OR);
                addQueryStringFilter(DBObjectToVariantConverter.IDS_FIELD, ids, builder, QueryOperation.OR);
            }

            if (query.containsKey(VariantQueryParams.GENE.key())) {
                String xrefs = query.getString(VariantQueryParams.GENE.key());
                addQueryStringFilter(DBObjectToVariantConverter.ANNOTATION_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.XREFS_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.XREF_ID_FIELD, xrefs, builder, QueryOperation.OR);
            }

            if (query.containsKey(VariantQueryParams.REFERENCE.key()) && query.getString(VariantQueryParams.REFERENCE.key()) != null) {
                addQueryStringFilter(DBObjectToVariantConverter.REFERENCE_FIELD, query.getString(VariantQueryParams.REFERENCE.key()),
                        builder, QueryOperation.AND);
            }

            if (query.containsKey(VariantQueryParams.ALTERNATE.key()) && query.getString(VariantQueryParams.ALTERNATE.key()) != null) {
                addQueryStringFilter(DBObjectToVariantConverter.ALTERNATE_FIELD, query.getString(VariantQueryParams.ALTERNATE.key()),
                        builder, QueryOperation.AND);
            }

            if (query.containsKey(VariantQueryParams.TYPE.key()) && !query.getString(VariantQueryParams.TYPE.key()).isEmpty()) {
                addQueryStringFilter(DBObjectToVariantConverter.TYPE_FIELD, query.getString(VariantQueryParams.TYPE.key()), builder,
                        QueryOperation.AND);
            }

            /** ANNOTATION PARAMS **/
            if (query.containsKey(VariantQueryParams.ANNOTATION_EXISTS.key())) {
                builder.and(DBObjectToVariantConverter.ANNOTATION_FIELD + "." + DBObjectToVariantAnnotationConverter.ANNOT_ID_FIELD);
                builder.exists(query.getBoolean(VariantQueryParams.ANNOTATION_EXISTS.key()));
            }

            if (query.containsKey(VariantQueryParams.ANNOT_XREF.key())) {
                String xrefs = query.getString(VariantQueryParams.ANNOT_XREF.key());
                addQueryStringFilter(DBObjectToVariantConverter.ANNOTATION_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.XREFS_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.XREF_ID_FIELD, xrefs, builder, QueryOperation.AND);
            }

            if (query.containsKey(VariantQueryParams.ANNOT_CONSEQUENCE_TYPE.key())) {
                String value = query.getString(VariantQueryParams.ANNOT_CONSEQUENCE_TYPE.key());
                value = value.replace("SO:", "");
                addQueryIntegerFilter(DBObjectToVariantConverter.ANNOTATION_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.SO_ACCESSION_FIELD, value, builder, QueryOperation.AND);
            }

            if (query.containsKey(VariantQueryParams.ANNOT_BIOTYPE.key())) {
                String biotypes = query.getString(VariantQueryParams.ANNOT_BIOTYPE.key());
                addQueryStringFilter(DBObjectToVariantConverter.ANNOTATION_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.BIOTYPE_FIELD, biotypes, builder, QueryOperation.AND);
            }

            if (query.containsKey(VariantQueryParams.POLYPHEN.key())) {
                addCompListQueryFilter(DBObjectToVariantConverter.ANNOTATION_FIELD
                                + "." + DBObjectToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                                + "." + DBObjectToVariantAnnotationConverter.POLYPHEN_FIELD
                                + "." + DBObjectToVariantAnnotationConverter.SCORE_SCORE_FIELD,
                        query.getString(VariantQueryParams.POLYPHEN.key()), builder);
            }

            if (query.containsKey(VariantQueryParams.SIFT.key())) {
                addCompListQueryFilter(DBObjectToVariantConverter.ANNOTATION_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.SIFT_FIELD + "."
                        + DBObjectToVariantAnnotationConverter.SCORE_SCORE_FIELD, query.getString(VariantQueryParams.SIFT.key()), builder);
            }

            if (query.containsKey(VariantQueryParams.PROTEIN_SUBSTITUTION.key())) {
                String value = query.getString(VariantQueryParams.PROTEIN_SUBSTITUTION.key());
                addScoreFilter(DBObjectToVariantConverter.ANNOTATION_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.PROTEIN_SUBSTITUTION_SCORE_FIELD, value, builder,
                        VariantQueryParams.PROTEIN_SUBSTITUTION);
            }

            if (query.containsKey(VariantQueryParams.CONSERVATION.key())) {
                String value = query.getString(VariantQueryParams.CONSERVATION.key());
                addScoreFilter(DBObjectToVariantConverter.ANNOTATION_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.CONSERVED_REGION_SCORE_FIELD, value, builder,
                        VariantQueryParams.CONSERVATION);
            }

            if (query.containsKey(VariantQueryParams.ALTERNATE_FREQUENCY.key())) {
                String value = query.getString(VariantQueryParams.ALTERNATE_FREQUENCY.key());
                addFrequencyFilter(DBObjectToVariantConverter.ANNOTATION_FIELD
                                + "." + DBObjectToVariantAnnotationConverter.POPULATION_FREQUENCIES_FIELD,
                        DBObjectToVariantAnnotationConverter.POPULATION_FREQUENCY_ALTERNATE_FREQUENCY_FIELD, value, builder,
                        VariantQueryParams.ALTERNATE_FREQUENCY); // Same
                // method addFrequencyFilter is used for reference and allele frequencies. Need to provide the field
                // (reference/alternate) where to check the frequency
            }

            if (query.containsKey(VariantQueryParams.REFERENCE_FREQUENCY.key())) {
                String value = query.getString(VariantQueryParams.REFERENCE_FREQUENCY.key());
                addFrequencyFilter(DBObjectToVariantConverter.ANNOTATION_FIELD
                                + "." + DBObjectToVariantAnnotationConverter.POPULATION_FREQUENCIES_FIELD,
                        DBObjectToVariantAnnotationConverter.POPULATION_FREQUENCY_REFERENCE_FREQUENCY_FIELD, value, builder,
                        VariantQueryParams.REFERENCE_FREQUENCY); // Same
                // method addFrequencyFilter is used for reference and allele frequencies. Need to provide the field
                // (reference/alternate) where to check the frequency
            }

            if (query.containsKey(VariantQueryParams.POPULATION_MINOR_ALLELE_FREQUENCY.key())) {
                String value = query.getString(VariantQueryParams.POPULATION_MINOR_ALLELE_FREQUENCY.key());
                addFrequencyFilter(DBObjectToVariantConverter.ANNOTATION_FIELD + "."
                        + DBObjectToVariantAnnotationConverter.POPULATION_FREQUENCIES_FIELD,
                        value, builder, VariantQueryParams.POPULATION_MINOR_ALLELE_FREQUENCY,
                        (v, queryBuilder) -> {
                            String op = getOperator(v);
                            String obj = v.replaceFirst(op, "");

                            double aDouble = Double.parseDouble(obj);
                            switch(op) {
                                case "<":
                                    queryBuilder.or(
                                            QueryBuilder.start(DBObjectToVariantAnnotationConverter.
                                                    POPULATION_FREQUENCY_REFERENCE_FREQUENCY_FIELD).lessThan(aDouble).get(),
                                            QueryBuilder.start(DBObjectToVariantAnnotationConverter.
                                                    POPULATION_FREQUENCY_ALTERNATE_FREQUENCY_FIELD).lessThan(aDouble).get()
                                    );
                                    break;
                                case "<=":
                                    queryBuilder.or(
                                            QueryBuilder.start(DBObjectToVariantAnnotationConverter.
                                                    POPULATION_FREQUENCY_REFERENCE_FREQUENCY_FIELD).lessThanEquals(aDouble).get(),
                                            QueryBuilder.start(DBObjectToVariantAnnotationConverter.
                                                    POPULATION_FREQUENCY_ALTERNATE_FREQUENCY_FIELD).lessThanEquals(aDouble).get()
                                    );
                                    break;
                                case ">":
                                    queryBuilder.and(DBObjectToVariantAnnotationConverter.
                                                    POPULATION_FREQUENCY_REFERENCE_FREQUENCY_FIELD).greaterThan(aDouble)
                                            .and(DBObjectToVariantAnnotationConverter.
                                                    POPULATION_FREQUENCY_ALTERNATE_FREQUENCY_FIELD).greaterThan(aDouble);
                                    break;
                                case ">=":
                                    queryBuilder.and(DBObjectToVariantAnnotationConverter.
                                                    POPULATION_FREQUENCY_REFERENCE_FREQUENCY_FIELD).greaterThanEquals(aDouble)
                                            .and(DBObjectToVariantAnnotationConverter.
                                                    POPULATION_FREQUENCY_ALTERNATE_FREQUENCY_FIELD).greaterThanEquals(aDouble);
                                    break;
                                default:
                                    throw new IllegalArgumentException("Unsupported operator '" + op + "'");
                            }
                        });
            }

            /** STUDIES **/
            QueryBuilder studyBuilder = QueryBuilder.start();
            final StudyConfiguration defaultStudyConfiguration;
            if (query.containsKey(VariantQueryParams.STUDIES.key())) { // && !options.getList("studies").isEmpty() && !options.getListAs
                // ("studies", String.class).get(0).isEmpty()) {
                String value = objectToString(query.get(VariantQueryParams.STUDIES.key()));

                this.<Integer>addQueryFilter(DBObjectToVariantConverter.STUDIES_FIELD + "." + DBObjectToStudyVariantEntryConverter
                        .STUDYID_FIELD, value, builder, QueryOperation.AND, studyName -> {
                    try {
                        return Integer.parseInt(studyName);
                    } catch (NumberFormatException e) {
                        QueryResult<StudyConfiguration> result = studyConfigurationManager.getStudyConfiguration(studyName, null);
                        if (result.getResult().isEmpty()) {
                            throw VariantQueryException.studyNotFound(studyName);
                        }
                        return result.first().getStudyId();
                    }
                });

                List<Integer> studyIds = utils.getStudyIds(Arrays.asList(value.split(",|;")), null);
                if (studyIds.size() == 1) {
                    defaultStudyConfiguration = studyConfigurationManager.getStudyConfiguration(studyIds.get(0), null).first();
                } else {
                    defaultStudyConfiguration = null;
                }
                String studyIdsCsv = studyIds.stream().map(Object::toString).collect(Collectors.joining(","));
                this.addQueryIntegerFilter(DBObjectToStudyVariantEntryConverter.STUDYID_FIELD, studyIdsCsv, studyBuilder,
                        QueryOperation.AND);

            } else {
                List<String> studyNames = studyConfigurationManager.getStudyNames(null);
                if (studyNames != null && studyNames.size() == 1) {
                    defaultStudyConfiguration = studyConfigurationManager.getStudyConfiguration(studyNames.get(0), null).first();
                } else {
                    defaultStudyConfiguration = null;
                }
            }

            if (query.containsKey(VariantQueryParams.FILES.key())) { // && !options.getList("files").isEmpty() && !options.getListAs
                // ("files", String.class).get(0).isEmpty()) {
                addQueryFilter(DBObjectToStudyVariantEntryConverter.FILES_FIELD + "." + DBObjectToStudyVariantEntryConverter.FILEID_FIELD,
                        objectToString(query.get(VariantQueryParams.FILES.key())), studyBuilder, QueryOperation.AND, file -> {
                            if (file.contains(":")) {
                                String[] studyFile = file.split(":");
                                QueryResult<StudyConfiguration> queryResult =
                                        studyConfigurationManager.getStudyConfiguration(studyFile[0], null);
                                if (queryResult.getResult().isEmpty()) {
                                    throw VariantQueryException.studyNotFound(studyFile[0]);
                                }
                                return queryResult.first().getFileIds().get(studyFile[1]);
                            } else {
                                try {
                                    return Integer.parseInt(file);
                                } catch (NumberFormatException e) {
                                    if (defaultStudyConfiguration != null) {
                                        return defaultStudyConfiguration.getFileIds().get(file);
                                    } else {
                                        List<String> studyNames = studyConfigurationManager.getStudyNames(null);
                                        throw new VariantQueryException("Unknown file \"" + file + "\". "
                                                + "Please, specify the study belonging."
                                                + (studyNames == null ? "" : " Available studies: " + studyNames));
                                    }
                                }
                            }
                        });
            }

            if (query.containsKey(VariantQueryParams.GENOTYPE.key())) {
                String sampleGenotypesCSV = query.getString(VariantQueryParams.GENOTYPE.key());

                // we may need to know the study type
//                studyConfigurationManager.getStudyConfiguration(1, null).getResult().get(0).
                String[] sampleGenotypesArray = sampleGenotypesCSV.split(AND);
                for (String sampleGenotypes : sampleGenotypesArray) {
                    String[] sampleGenotype = sampleGenotypes.split(IS);
                    if (sampleGenotype.length != 2 && sampleGenotype.length != 3) {
                        throw VariantQueryException.malformedParam(VariantQueryParams.GENOTYPE, sampleGenotypes);
//                        throw new IllegalArgumentException("Malformed genotype query \"" + sampleGenotypes + "\". Expected "
//                                + "[<study>:]<sample>:<genotype>[,<genotype>]*");
                    }

                    int sampleId;
                    final String genotypes;
                    if (sampleGenotype.length == 3) {  //Expect to be as <study>:<sample>
                        String study = sampleGenotype[0];
                        String sample = sampleGenotype[1];
                        genotypes = sampleGenotype[2];
                        QueryResult<StudyConfiguration> queryResult = studyConfigurationManager.getStudyConfiguration(study, null);
                        if (queryResult.getResult().isEmpty()) {
                            throw VariantQueryException.studyNotFound(study);
                        }
                        if (!queryResult.first().getSampleIds().containsKey(sample)) {
                            throw VariantQueryException.sampleNotFound(sample, study);
                        }
                        sampleId = queryResult.first().getSampleIds().get(sample);
                    } else {
                        String sample = sampleGenotype[0];
                        genotypes = sampleGenotype[1];
                        sampleId = utils.getSampleId(sample, defaultStudyConfiguration);
                    }

                    String[] genotypesArray = genotypes.split(OR);
                    QueryBuilder genotypesBuilder = QueryBuilder.start();
                    for (String genotype : genotypesArray) {
                        if ("0/0".equals(genotype) || "0|0".equals(genotype)) {
                            QueryBuilder andBuilder = QueryBuilder.start();
                            List<String> otherGenotypes = Arrays.asList(
                                    "0/1", "1/0", "1/1", "-1/-1",
                                    "0|1", "1|0", "1|1", "-1|-1",
                                    "0|2", "2|0", "2|1", "1|2", "2|2",
                                    "0/2", "2/0", "2/1", "1/2", "2/2",
                                    DBObjectToSamplesConverter.UNKNOWN_GENOTYPE);
                            for (String otherGenotype : otherGenotypes) {
                                andBuilder.and(new BasicDBObject(DBObjectToStudyVariantEntryConverter.GENOTYPES_FIELD
                                        + "." + otherGenotype,
                                        new BasicDBObject("$not", new BasicDBObject("$elemMatch", new BasicDBObject("$eq", sampleId)))));
                            }
                            genotypesBuilder.or(andBuilder.get());
                        } else {
                            String s = DBObjectToStudyVariantEntryConverter.GENOTYPES_FIELD
                                    + "." + DBObjectToSamplesConverter.genotypeToStorageType(genotype);
                            //or [ {"samp.0|0" : { $elemMatch : { $eq : <sampleId> } } } ]
                            genotypesBuilder.or(new BasicDBObject(s, new BasicDBObject("$elemMatch", new BasicDBObject("$eq", sampleId))));
                        }
                    }
                    studyBuilder.and(genotypesBuilder.get());
                }
            }

            // If Study Query is used then we add a elemMatch query
            DBObject studyQuery = studyBuilder.get();
            if (studyQuery.keySet().size() != 0) {
                builder.and(DBObjectToVariantConverter.STUDIES_FIELD).elemMatch(studyQuery);
            }

            /** STATS PARAMS **/
            if (query.get(VariantQueryParams.COHORTS.key()) != null && !query.getString(VariantQueryParams.COHORTS.key()).isEmpty()) {
                addQueryFilter(DBObjectToVariantConverter.STATS_FIELD
                                + "." + DBObjectToVariantStatsConverter.COHORT_ID,
                        query.getString(VariantQueryParams.COHORTS.key()), builder, QueryOperation.AND,
                        s -> {
                            try {
                                return Integer.parseInt(s);
                            } catch (NumberFormatException ignore) {
                                int indexOf = s.lastIndexOf(":");
                                if (defaultStudyConfiguration == null && indexOf < 0) {
                                    throw VariantQueryException.malformedParam(VariantQueryParams.COHORTS, s, "Expected {study}:{cohort}");
                                } else {
                                    String study;
                                    String cohort;
                                    Integer studyId;
                                    Integer cohortId;
                                    if (defaultStudyConfiguration != null && indexOf < 0) {
                                        cohort = s;
                                        cohortId = getInteger(cohort);
                                        if (cohortId == null) {
                                            cohortId = defaultStudyConfiguration.getCohortIds().get(cohort);
                                        }
                                    } else {
                                        study = s.substring(0, indexOf);
                                        cohort = s.substring(indexOf + 1);
                                        studyId = getInteger(study);
                                        cohortId = getInteger(cohort);

                                        if (studyId == null) {
                                            StudyConfiguration studyConfiguration =
                                                    studyConfigurationManager.getStudyConfiguration(study, null).first();
                                            studyId = studyConfiguration.getStudyId();
                                            if (cohortId == null) {
                                                cohortId = studyConfiguration.getCohortIds().get(cohort);
                                            }
                                        } else if (cohortId == null) {
                                            StudyConfiguration studyConfiguration =
                                                    studyConfigurationManager.getStudyConfiguration(studyId, null).first();
                                            cohortId = studyConfiguration.getCohortIds().get(cohort);
                                        }
                                    }
                                    if (cohortId == null) {
                                        throw new VariantQueryException("Unknown cohort \"" + s + "\"");
                                    }
                                    return cohortId;
                                }
                            }
                        });
            }

            if (query.get(VariantQueryParams.STATS_MAF.key()) != null && !query.getString(VariantQueryParams.STATS_MAF.key()).isEmpty()) {
                addStatsFilterList(DBObjectToVariantStatsConverter.MAF_FIELD, query.getString(VariantQueryParams.STATS_MAF.key()),
                        builder, defaultStudyConfiguration);
            }

            if (query.get(VariantQueryParams.STATS_MGF.key()) != null && !query.getString(VariantQueryParams.STATS_MGF.key()).isEmpty()) {
                addStatsFilterList(DBObjectToVariantStatsConverter.MGF_FIELD, query.getString(VariantQueryParams.STATS_MGF.key()),
                        builder, defaultStudyConfiguration);
            }

            if (query.get(VariantQueryParams.MISSING_ALLELES.key()) != null && !query.getString(VariantQueryParams.MISSING_ALLELES.key())
                    .isEmpty()) {
                addStatsFilterList(DBObjectToVariantStatsConverter.MISSALLELE_FIELD, query.getString(VariantQueryParams.MISSING_ALLELES
                        .key()), builder, defaultStudyConfiguration);
            }

            if (query.get(VariantQueryParams.MISSING_GENOTYPES.key()) != null && !query.getString(VariantQueryParams.MISSING_GENOTYPES
                    .key()).isEmpty()) {
                addStatsFilterList(DBObjectToVariantStatsConverter.MISSGENOTYPE_FIELD, query.getString(VariantQueryParams
                        .MISSING_GENOTYPES.key()), builder, defaultStudyConfiguration);
            }

            if (query.get("numgt") != null && !query.getString("numgt").isEmpty()) {
                for (String numgt : query.getAsStringList("numgt")) {
                    String[] split = numgt.split(":");
                    addCompQueryFilter(
                            DBObjectToVariantConverter.STATS_FIELD + "." + DBObjectToVariantStatsConverter.NUMGT_FIELD + "." + split[0],
                            split[1], builder);
                }
            }
        }

        logger.info("Find = " + builder.get());
        return builder;
    }

    private DBObject createProjection(Query query, QueryOptions options) {
        DBObject projection = new BasicDBObject();

        if (options == null) {
            options = new QueryOptions();
        }

        if (options.containsKey("sort")) {
            if (options.getBoolean("sort")) {
                options.put("sort", new BasicDBObject(DBObjectToVariantConverter.CHROMOSOME_FIELD, 1).append(DBObjectToVariantConverter
                        .START_FIELD, 1));
            } else {
                options.remove("sort");
            }
        }

        List<String> includeList = options.getAsStringList("include");
        if (!includeList.isEmpty()) { //Include some
            for (String s : includeList) {
                String key = DBObjectToVariantConverter.toShortFieldName(s);
                if (key != null) {
                    projection.put(key, 1);
                } else {
                    logger.warn("Unknown include field: {}", s);
                }
            }
        } else { //Include all
            for (String values : DBObjectToVariantConverter.FIELDS_MAP.values()) {
                projection.put(values, 1);
            }
            if (options.containsKey("exclude")) { // Exclude some
                List<String> excludeList = options.getAsStringList("exclude");
                for (String s : excludeList) {
                    String key = DBObjectToVariantConverter.toShortFieldName(s);
                    if (key != null) {
                        projection.removeField(key);
                    } else {
                        logger.warn("Unknown exclude field: {}", s);
                    }
                }
            }
        }

        if (query.containsKey(VariantQueryParams.RETURNED_FILES.key()) && projection.containsField(DBObjectToVariantConverter
                .STUDIES_FIELD)) {
            List<Integer> files = query.getAsIntegerList(VariantQueryParams.RETURNED_FILES.key());
            projection.put(
                    DBObjectToVariantConverter.STUDIES_FIELD,
                    new BasicDBObject(
                            "$elemMatch",
                            new BasicDBObject(
                                    DBObjectToStudyVariantEntryConverter.FILES_FIELD + "." + DBObjectToStudyVariantEntryConverter
                                            .FILEID_FIELD,
                                    new BasicDBObject(
                                            "$in",
                                            files
                                    )
                            )
                    )
            );
        }
        if (query.containsKey(VariantQueryParams.RETURNED_STUDIES.key())
                && projection.containsField(DBObjectToVariantConverter.STUDIES_FIELD)) {
            List<Integer> studiesIds = utils.getStudyIds(query.getAsList(VariantQueryParams.RETURNED_STUDIES.key()), options);
//            List<Integer> studies = query.getAsIntegerList(VariantQueryParams.RETURNED_STUDIES.key());
            if (!studiesIds.isEmpty()) {
                projection.put(
                        DBObjectToVariantConverter.STUDIES_FIELD,
                        new BasicDBObject(
                                "$elemMatch",
                                new BasicDBObject(
                                        DBObjectToStudyVariantEntryConverter.STUDYID_FIELD,
                                        new BasicDBObject(
                                                "$in",
                                                studiesIds
                                        )
                                )
                        )
                );
            }
        }

        logger.debug("Projection: {}", projection);
        return projection;
    }

    /**
     * Two steps insertion:
     * First check that the variant and study exists making an update.
     * For those who doesn't exist, pushes a study with the file and genotype information
     * <p>
     * The documents that throw a "dup key" exception are those variants that exist and have the study.
     * Then, only for those variants, make a second update.
     * <p>
     * *An interesting idea would be to invert this actions depending on the number of already inserted variants.
     *
     * @param data Variants to insert
     * @param fileId File ID
     * @param variantConverter Variant converter to be used
     * @param variantSourceEntryConverter Variant source converter to be used
     * @param studyConfiguration Configuration for the study
     * @param loadedSampleIds Other loaded sampleIds EXCEPT those that are going to be loaded
     * @return QueryResult object
     */
    QueryResult<MongoDBVariantWriteResult> insert(List<Variant> data, int fileId, DBObjectToVariantConverter variantConverter,
                                                  DBObjectToStudyVariantEntryConverter variantSourceEntryConverter,
                                                  StudyConfiguration studyConfiguration, List<Integer> loadedSampleIds) {

        MongoDBVariantWriteResult writeResult = new MongoDBVariantWriteResult();
        long startTime = System.currentTimeMillis();
        if (data.isEmpty()) {
            return new QueryResult<>("insertVariants", 0, 1, 1, "", "", Collections.singletonList(writeResult));
        }
        List<DBObject> queries = new ArrayList<>(data.size());
        List<DBObject> updates = new ArrayList<>(data.size());
        // Use a multiset instead of a normal set, to keep tracking of duplicated variants
        Multiset<String> nonInsertedVariants = HashMultiset.create();
        String fileIdStr = Integer.toString(fileId);
        List<String> extraFields = studyConfiguration.getAttributes().getAsStringList(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS
                .key());
//        {
        Map missingSamples = Collections.emptyMap();
        String defaultGenotype = studyConfiguration.getAttributes().getString(MongoDBVariantStorageManager.DEFAULT_GENOTYPE, "");
        if (defaultGenotype.equals(DBObjectToSamplesConverter.UNKNOWN_GENOTYPE)) {
            logger.debug("Do not need fill gaps. DefaultGenotype is UNKNOWN_GENOTYPE({}).");
        } else if (!loadedSampleIds.isEmpty()) {
            missingSamples = new BasicDBObject(DBObjectToSamplesConverter.UNKNOWN_GENOTYPE, loadedSampleIds);   // ?/?
        }
        List<Object> missingOtherValues = new ArrayList<>(loadedSampleIds.size());
        for (int i = 0; i < loadedSampleIds.size(); i++) {
            missingOtherValues.add(DBObjectToSamplesConverter.UNKNOWN_FIELD);
        }
        for (Variant variant : data) {
            if (variant.getType().equals(VariantType.NO_VARIATION)) {
                //Storage-MongoDB is not able to store NON VARIANTS
                writeResult.setSkippedVariants(writeResult.getSkippedVariants() + 1);
                continue;
            } else if (variant.getType().equals(VariantType.SYMBOLIC)) {
                logger.warn("Skip symbolic variant " + variant.toString());
                writeResult.setSkippedVariants(writeResult.getSkippedVariants() + 1);
                continue;
            }
            String id = variantConverter.buildStorageId(variant);
            for (StudyEntry studyEntry : variant.getStudies()) {
                if (studyEntry.getFiles().size() == 0 || !studyEntry.getFiles().get(0).getFileId().equals(fileIdStr)) {
                    continue;
                }
                int studyId = studyConfiguration.getStudyId();
                DBObject study = variantSourceEntryConverter.convertToStorageType(studyEntry);
                DBObject genotypes = (DBObject) study.get(DBObjectToStudyVariantEntryConverter.GENOTYPES_FIELD);
                if (genotypes != null) {        //If genotypes is null, genotypes are not suppose to be loaded
                    genotypes.putAll(missingSamples);   //Add missing samples
                    for (String extraField : extraFields) {
                        List<Object> otherFieldValues = (List<Object>) study.get(extraField.toLowerCase());
                        otherFieldValues.addAll(0, missingOtherValues);
                    }
                }
                DBObject push = new BasicDBObject(DBObjectToVariantConverter.STUDIES_FIELD, study);
                BasicDBObject update = new BasicDBObject()
                        .append("$push", push)
                        .append("$setOnInsert", variantConverter.convertToStorageType(variant));
                if (variant.getIds() != null && !variant.getIds().isEmpty() && !variant.getIds().iterator().next().isEmpty()) {
                    update.put("$addToSet", new BasicDBObject(DBObjectToVariantConverter.IDS_FIELD, new BasicDBObject("$each",
                            variant.getIds())));
                }
                // { _id: <variant_id>, "studies.sid": {$ne: <studyId> } }
                //If the variant exists and contains the study, this find will fail, will try to do the upsert, and throw a
                // duplicated key exception.
                queries.add(new BasicDBObject("_id", id).append(DBObjectToVariantConverter.STUDIES_FIELD
                                + "." + DBObjectToStudyVariantEntryConverter.STUDYID_FIELD,
                        new BasicDBObject("$ne", studyId)));
                updates.add(update);
            }
        }
        //
        if (!queries.isEmpty()) {
            QueryOptions options = new QueryOptions("upsert", true);
            options.put("multi", false);
            BulkWriteResult bulkWriteResult;
            try {
                bulkWriteResult = variantsCollection.update(queries, updates, options).first();
            } catch (BulkWriteException e) {
                bulkWriteResult = e.getWriteResult();
                for (BulkWriteError writeError : e.getWriteErrors()) {
                    if (writeError.getCode() == 11000) { //Dup Key error code
                        Matcher matcher = writeResultErrorPattern.matcher(writeError.getMessage());
                        if (matcher.find()) {
                            String id = matcher.group(1);
                            nonInsertedVariants.add(id);
                        } else {
                            throw e;
                        }
                    } else {
                        throw e;
                    }
                }
            }

            writeResult.setNewDocuments(bulkWriteResult.getUpserts().size());
            writeResult.setUpdatedObjects(bulkWriteResult.getModifiedCount());
//                writeResult.setNewDocuments(data.size() - nonInsertedVariants.size() - writeResult.getSkippedVariants());
            queries.clear();
            updates.clear();
        }
//        }

        for (Variant variant : data) {
            variant.setAnnotation(null);
            String id = variantConverter.buildStorageId(variant);

            if (nonInsertedVariants != null && !nonInsertedVariants.contains(id)) {
                continue;   //Already inserted variant
            }

            for (StudyEntry studyEntry : variant.getStudies()) {
                if (studyEntry.getFiles().size() == 0 || !studyEntry.getFiles().get(0).getFileId().equals(fileIdStr)) {
                    continue;
                }

                DBObject studyObject = variantSourceEntryConverter.convertToStorageType(studyEntry);
                DBObject genotypes = (DBObject) studyObject.get(DBObjectToStudyVariantEntryConverter.GENOTYPES_FIELD);
                DBObject push = new BasicDBObject();
                if (genotypes != null) { //If genotypes is null, genotypes are not suppose to be loaded
                    for (String genotype : genotypes.keySet()) {
                        push.put(DBObjectToVariantConverter.STUDIES_FIELD + ".$." + DBObjectToStudyVariantEntryConverter.GENOTYPES_FIELD
                                + "." + genotype, new BasicDBObject("$each", genotypes.get(genotype)));
                    }
                    for (String extraField : extraFields) {
                        List values = (List) studyObject.get(extraField.toLowerCase());
                        push.put(DBObjectToVariantConverter.STUDIES_FIELD + ".$." + extraField.toLowerCase(),
                                new BasicDBObject("$each", values).append("$position", loadedSampleIds.size()));
                    }
                } else {
                    push.put(DBObjectToVariantConverter.STUDIES_FIELD + ".$." + DBObjectToStudyVariantEntryConverter.GENOTYPES_FIELD,
                            Collections.emptyMap());
                }
                push.put(DBObjectToVariantConverter.STUDIES_FIELD + ".$." + DBObjectToStudyVariantEntryConverter.FILES_FIELD, ((List)
                        studyObject.get(DBObjectToStudyVariantEntryConverter.FILES_FIELD)).get(0));
                BasicDBObject update = new BasicDBObject(new BasicDBObject("$push", push));

                queries.add(new BasicDBObject("_id", id)
                        .append(DBObjectToVariantConverter.STUDIES_FIELD
                                + '.' + DBObjectToStudyVariantEntryConverter.STUDYID_FIELD, studyConfiguration.getStudyId())
                        .append(DBObjectToVariantConverter.STUDIES_FIELD
                                + '.' + DBObjectToStudyVariantEntryConverter.FILES_FIELD
                                + '.' + DBObjectToStudyVariantEntryConverter.FILEID_FIELD, new BasicDBObject("$ne", fileId))
                );
                updates.add(update);

            }
        }

        if (!queries.isEmpty()) {
            QueryOptions options = new QueryOptions("upsert", false);
            options.put("multi", false);
            QueryResult<BulkWriteResult> update = variantsCollection.update(queries, updates, options);
            // Can happen that nonInsertedVariantsNum != queries.size() != nonInsertedVariants.size() if there was
            // a duplicated variant.
            writeResult.setNonInsertedVariants(nonInsertedVariants.size() - update.first().getMatchedCount());
            writeResult.setUpdatedObjects(writeResult.getUpdatedObjects() + update.first().getModifiedCount());
        }

        return new QueryResult<>("insertVariants", ((int) (System.currentTimeMillis() - startTime)), 1, 1, "", "",
                Collections.singletonList(writeResult));
    }

    QueryResult<WriteResult> fillFileGaps(int fileId, List<String> chromosomes, List<Integer> fileSampleIds, StudyConfiguration
            studyConfiguration) {
        // { "studies.sid" : <studyId>, "studies.files.fid" : { $ne : <fileId> } },
        // { $push : {
        //      "studies.$.gt.?/?" : {$each : [ <fileSampleIds> ] }
        // } }
        if (studyConfiguration.getAttributes().getAsStringList(MongoDBVariantStorageManager.DEFAULT_GENOTYPE, "")
                .equals(Collections.singletonList(DBObjectToSamplesConverter.UNKNOWN_GENOTYPE))
                && studyConfiguration.getAttributes().getAsStringList(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS.key())
                .isEmpty()) {
            logger.debug("Do not need fill gaps. DefaultGenotype is UNKNOWN_GENOTYPE({}).", DBObjectToSamplesConverter.UNKNOWN_GENOTYPE);
            return new QueryResult<>();
        }

        List<Integer> loadedSamples = getLoadedSamples(fileId, studyConfiguration);

        DBObject query = new BasicDBObject();
        if (chromosomes != null && !chromosomes.isEmpty()) {
            query.put(DBObjectToVariantConverter.CHROMOSOME_FIELD, new BasicDBObject("$in", chromosomes));
        }

        query.put(DBObjectToVariantConverter.STUDIES_FIELD, new BasicDBObject("$elemMatch",
                new BasicDBObject(
                        DBObjectToStudyVariantEntryConverter.STUDYID_FIELD,
                        studyConfiguration.getStudyId())
                        .append(DBObjectToStudyVariantEntryConverter.FILES_FIELD + "." + DBObjectToStudyVariantEntryConverter.FILEID_FIELD,
                                new BasicDBObject("$ne", fileId)
                        )
        ));

        BasicDBObject push = new BasicDBObject()
                .append(DBObjectToVariantConverter.STUDIES_FIELD
                        + ".$." + DBObjectToStudyVariantEntryConverter.GENOTYPES_FIELD
                        + "." + DBObjectToSamplesConverter.UNKNOWN_GENOTYPE, new BasicDBObject("$each", fileSampleIds));

        List<Object> missingOtherValues = new ArrayList<>(fileSampleIds.size());
        for (int size = fileSampleIds.size(); size > 0; size--) {
            missingOtherValues.add(DBObjectToSamplesConverter.UNKNOWN_FIELD);
        }
        List<String> extraFields = studyConfiguration.getAttributes().getAsStringList(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS
                .key());
        for (String extraField : extraFields) {
            push.put(DBObjectToVariantConverter.STUDIES_FIELD + ".$." + extraField.toLowerCase(),
                    new BasicDBObject("$each", missingOtherValues).append("$position", loadedSamples.size())
            );
        }

        BasicDBObject update = new BasicDBObject("$push", push);

        QueryOptions queryOptions = new QueryOptions("multi", true);
        logger.debug("FillGaps find : {}", query);
        logger.debug("FillGaps update : {}", update);
        return variantsCollection.update(query, update, queryOptions);
    }

    private DBObjectToVariantConverter getDbObjectToVariantConverter(Query query, QueryOptions options) {
        studyConfigurationManager.setDefaultQueryOptions(options);
        List<Integer> studyIds = utils.getStudyIds(query.getAsList(VariantQueryParams.STUDIES.key(), ",|;"), options);

        DBObjectToSamplesConverter samplesConverter;
        if (studyIds.isEmpty()) {
            samplesConverter = new DBObjectToSamplesConverter(studyConfigurationManager, null);
        } else {
            List<StudyConfiguration> studyConfigurations = new LinkedList<>();
            for (Integer studyId : studyIds) {
                QueryResult<StudyConfiguration> queryResult = studyConfigurationManager.getStudyConfiguration(studyId, options);
                if (queryResult.getResult().isEmpty()) {
                    throw VariantQueryException.studyNotFound(studyId);
//                    throw new IllegalArgumentException("Couldn't find studyConfiguration for StudyId '" + studyId + "'");
                } else {
                    studyConfigurations.add(queryResult.first());
                }
            }
            samplesConverter = new DBObjectToSamplesConverter(studyConfigurations);
        }
        if (query.containsKey(VariantQueryParams.UNKNOWN_GENOTYPE.key())) {
            samplesConverter.setReturnedUnknownGenotype(query.getString(VariantQueryParams.UNKNOWN_GENOTYPE.key()));
        }
        if (query.containsKey(VariantQueryParams.RETURNED_SAMPLES.key())) {
            //Remove the studyName, if any
            samplesConverter.setReturnedSamples(query.getAsStringList(VariantQueryParams.RETURNED_SAMPLES.key())
                    .stream().map(s -> s.contains(":") ? s.split(":")[1] : s).collect(Collectors.toList()));
        }
        DBObjectToStudyVariantEntryConverter sourceEntryConverter = new DBObjectToStudyVariantEntryConverter(false,
                query.containsKey(VariantQueryParams.RETURNED_FILES.key())
                        ? query.getAsIntegerList(VariantQueryParams.RETURNED_FILES.key())
                        : null,
                samplesConverter
        );
        sourceEntryConverter.setStudyConfigurationManager(studyConfigurationManager);
        return new DBObjectToVariantConverter(sourceEntryConverter, new DBObjectToVariantStatsConverter(studyConfigurationManager));
    }

    @Deprecated
    private QueryBuilder addQueryStringFilter(String key, String value, final QueryBuilder builder) {
        return addQueryStringFilter(key, value, builder, QueryOperation.AND);
    }

    private QueryBuilder addQueryStringFilter(String key, String value, final QueryBuilder builder, QueryOperation op) {
        return this.addQueryFilter(key, value, builder, op, Function.identity());
    }

    private QueryBuilder addQueryIntegerFilter(String key, String value, final QueryBuilder builder, QueryOperation op) {
        return this.<Integer>addQueryFilter(key, value, builder, op, elem -> {
            try {
                return Integer.parseInt(elem);
            } catch (NumberFormatException e) {
                throw new VariantQueryException("Unable to parse int " + elem, e);
            }
        });
    }

    private <T> QueryBuilder addQueryFilter(String key, String value, final QueryBuilder builder, QueryOperation op,
                                            Function<String, T> map) {
        QueryOperation operation = checkOperator(value);
        QueryBuilder auxBuilder;
        if (op == QueryOperation.OR) {
            auxBuilder = QueryBuilder.start();
        } else {
            auxBuilder = builder;
        }

        if (operation == null) {
            if (value.startsWith("!")) {
                auxBuilder.and(key).notEquals(map.apply(value.substring(1)));
            } else {
                auxBuilder.and(key).is(map.apply(value));
            }
        } else if (operation == QueryOperation.OR) {
            String[] array = value.split(OR);
            List<T> list = new ArrayList<>(array.length);
            for (String elem : array) {
                if (elem.startsWith("!")) {
                    throw new VariantQueryException("Unable to use negate (!) operator in OR sequences (<it_1>(,<it_n>)*)");
                } else {
                    list.add(map.apply(elem));
                }
            }
            auxBuilder.and(key).in(list);
        } else {
            //Split in two lists: positive and negative
            String[] array = value.split(AND);
            List<T> listIs = new ArrayList<>(array.length);
            List<T> listNotIs = new ArrayList<>(array.length);

            for (String elem : array) {
                if (elem.startsWith("!")) {
                    listNotIs.add(map.apply(elem.substring(1)));
                } else {
                    listIs.add(map.apply(elem));
                }
            }

            if (!listIs.isEmpty()) {    //Can not use method "is" because it will be overwritten with the "notEquals" or "notIn" method
                auxBuilder.and(key).all(listIs);
            }
            if (listNotIs.size() == 1) {
                auxBuilder.and(key).notEquals(listNotIs.get(0));
            } else if (listNotIs.size() > 1) {
                auxBuilder.and(key).notIn(listNotIs);
            }

        }

        if (op == QueryOperation.OR) {
            builder.or(auxBuilder.get());
        }
        return builder;
    }

    /**
     * Accept a list of comparative filters separated with "," or ";" with the expression:
     * {OPERATION}{VALUE}, where the accepted operations are: <, <=, >, >=, =, ==, !=, ~=.
     *
     * @param key
     * @param value
     * @param builder
     * @return
     */
    private QueryBuilder addCompListQueryFilter(String key, String value, QueryBuilder builder) {
        QueryOperation op = checkOperator(value);
        List<String> values = splitValue(value, op);

        QueryBuilder compBuilder;
        if (op == QueryOperation.OR) {
            compBuilder = QueryBuilder.start();
        } else {
            compBuilder = builder;
        }

        for (String elem : values) {
            addCompQueryFilter(key, elem, compBuilder);
        }

        if (op == QueryOperation.OR) {
            builder.or(compBuilder.get());
        }
        return builder;
    }

    private QueryBuilder addCompQueryFilter(String key, String value, QueryBuilder builder) {
        String op = getOperator(value);
        String obj = value.replaceFirst(op, "");

        switch (op) {
            case "<":
                builder.and(key).lessThan(Double.parseDouble(obj));
                break;
            case "<=":
                builder.and(key).lessThanEquals(Double.parseDouble(obj));
                break;
            case ">":
                builder.and(key).greaterThan(Double.parseDouble(obj));
                break;
            case ">=":
                builder.and(key).greaterThanEquals(Double.parseDouble(obj));
                break;
            case "=":
            case "==":
                builder.and(key).is(Double.parseDouble(obj));
                break;
            case "!=":
                builder.and(key).notEquals(Double.parseDouble(obj));
                break;
            case "~=":
                builder.and(key).regex(Pattern.compile(obj));
                break;
            default:
                break;
        }
        return builder;
    }

    private String getOperator(String value) {
        String op = value.substring(0, 2);
        op = op.replaceFirst("[0-9]", "");
        return op;
    }

    /**
     * Accepts a list of filters separated with "," or ";" with the expression: {SCORE}{OPERATION}{VALUE}.
     *
     * @param key     ProteinScore schema field
     * @param value   Value to parse
     * @param builder QueryBuilder
     * @param conservation
     * @return QueryBuilder
     */
    private QueryBuilder addScoreFilter(String key, String value, QueryBuilder builder, VariantQueryParams conservation) {
        final List<String> list;
        QueryOperation operation = checkOperator(value);
        list = splitValue(value, operation);

        List<DBObject> dbObjects = new ArrayList<>();
        for (String elem : list) {
            String[] populationFrequency = splitKeyValue(elem);
            if (populationFrequency.length != 2) {
                logger.error("Bad score filter: " + elem);
                throw VariantQueryException.malformedParam(conservation, value);
            }
            QueryBuilder scoreBuilder = new QueryBuilder();
            scoreBuilder.and(DBObjectToVariantAnnotationConverter.SCORE_SOURCE_FIELD).is(populationFrequency[0]);
            addCompQueryFilter(DBObjectToVariantAnnotationConverter.SCORE_SCORE_FIELD, populationFrequency[1], scoreBuilder);
            dbObjects.add(new BasicDBObject(key, new BasicDBObject("$elemMatch", scoreBuilder.get())));
        }
        if (!dbObjects.isEmpty()) {
            if (operation == null || operation == QueryOperation.AND) {
                builder.and(dbObjects.toArray(new DBObject[dbObjects.size()]));
            } else {
                builder.and(new BasicDBObject("$or", dbObjects));
            }
        }
        return builder;
    }

    /**
     * Accepts a list of filters separated with "," or ";" with the expression:
     * {STUDY}:{POPULATION}{OPERATION}{VALUE}.
     *
     * @param key                  PopulationFrequency schema field
     * @param alleleFrequencyField Allele frequency schema field
     * @param value                Value to parse
     * @param builder              QueryBuilder
     * @param queryParam           QueryParam filter
     * @return QueryBuilder
     */
    private QueryBuilder addFrequencyFilter(String key, String alleleFrequencyField, String value, QueryBuilder builder,
                                            VariantQueryParams queryParam) {
        return addFrequencyFilter(key, value, builder, queryParam, (v, qb) -> addCompQueryFilter(alleleFrequencyField, v, qb));
    }

    /**
     * Accepts a list of filters separated with "," or ";" with the expression:
     *      {STUDY}:{POPULATION}{OPERATION}{VALUE}.
     *
     * @param key                   PopulationFrequency schema field
     * @param value                 Value to parse
     * @param builder               QueryBuilder
     * @param addFilter             For complex filter
     * @return                      QueryBuilder
     */
    private QueryBuilder addFrequencyFilter(String key, String value, QueryBuilder builder, VariantQueryParams queryParam,
                                            BiConsumer<String, QueryBuilder> addFilter) {

        final List<String> list;
        QueryOperation operation = checkOperator(value);
        list = splitValue(value, operation);

        List<DBObject> dbObjects = new ArrayList<>();
        for (String elem : list) {
            String[] split = elem.split(IS);
            if (split.length != 2) {
                logger.error("Bad population frequency filter: " + elem);
                throw VariantQueryException.malformedParam(queryParam, value);
                //new IllegalArgumentException("Bad population frequency filter: " + elem);
            }
            String study = split[0];
            String population = split[1];
            String[] populationFrequency = splitKeyValue(population);
            logger.debug("populationFrequency = " + Arrays.toString(populationFrequency));

            QueryBuilder frequencyBuilder = new QueryBuilder();
            frequencyBuilder.and(DBObjectToVariantAnnotationConverter.POPULATION_FREQUENCY_STUDY_FIELD).is(study);
            frequencyBuilder.and(DBObjectToVariantAnnotationConverter.POPULATION_FREQUENCY_POP_FIELD).is(populationFrequency[0]);
            addFilter.accept(populationFrequency[1], frequencyBuilder);
            dbObjects.add(new BasicDBObject(key, new BasicDBObject("$elemMatch", frequencyBuilder.get())));
        }
        if (!dbObjects.isEmpty()) {
            if (operation == null || operation == QueryOperation.AND) {
                builder.and(dbObjects.toArray(new DBObject[dbObjects.size()]));
            } else {
                builder.and(new BasicDBObject("$or", dbObjects));
            }
        }
        return builder;
    }

    /**
     * Accept filters separated with "," or ";" with the expression:
     * [{STUDY}:]{COHORT}{OPERATION}{VALUE}.
     * Where STUDY is optional if defaultStudyConfiguration is provided
     *
     * @param key                       Stats field to filter
     * @param values                    Values to parse
     * @param builder                   QueryBuilder
     * @param defaultStudyConfiguration
     */
    private void addStatsFilterList(String key, String values, QueryBuilder builder, StudyConfiguration defaultStudyConfiguration) {
        QueryOperation op = checkOperator(values);
        List<String> valuesList = splitValue(values, op);
        List<DBObject> statsQueries = new LinkedList<>();
        for (String value : valuesList) {
            statsQueries.add(addStatsFilter(key, value, new QueryBuilder(), defaultStudyConfiguration).get());
        }

        if (!statsQueries.isEmpty()) {
            if (op == QueryOperation.OR) {
                builder.or(statsQueries.toArray(new DBObject[statsQueries.size()]));
            } else {
                builder.and(statsQueries.toArray(new DBObject[statsQueries.size()]));
            }
        }
    }

    /**
     * Accepts filters with the expresion: [{STUDY}:]{COHORT}{OPERATION}{VALUE}.
     * Where STUDY is optional if defaultStudyConfiguration is provided
     *
     * @param key                       Stats field to filter
     * @param value                     Value to parse
     * @param builder                   QueryBuilder
     * @param defaultStudyConfiguration
     */
    private QueryBuilder addStatsFilter(String key, String value, QueryBuilder builder, StudyConfiguration defaultStudyConfiguration) {
        if (value.contains(":") || defaultStudyConfiguration != null) {
            Integer studyId;
            Integer cohortId;
            String operatorValue;
            if (value.contains(":")) {
                String[] studyValue = value.split(":");
                String[] cohortValue = splitKeyValue(studyValue[1]);
                String study = studyValue[0];
                String cohort = cohortValue[0];
                operatorValue = cohortValue[1];
                studyId = getInteger(study);
                cohortId = getInteger(cohort);
                if (studyId == null) {
                    StudyConfiguration studyConfiguration = studyConfigurationManager.getStudyConfiguration(study, null).first();
                    studyId = studyConfiguration.getStudyId();
                    if (cohortId == null) {
                        cohortId = studyConfiguration.getCohortIds().get(cohort);
                    }
                } else if (cohortId == null) {
                    StudyConfiguration studyConfiguration = studyConfigurationManager.getStudyConfiguration(studyId, null).first();
                    cohortId = studyConfiguration.getCohortIds().get(cohort);
                }
            } else {
                String study = defaultStudyConfiguration.getStudyName();
                studyId = defaultStudyConfiguration.getStudyId();
                String[] cohortValue = splitKeyValue(value);
                String cohort = cohortValue[0];
                cohortId = getInteger(cohort);
                if (cohortId == null) {
                    cohortId = defaultStudyConfiguration.getCohortIds().get(cohort);
                }
                operatorValue = cohortValue[1];
            }

            QueryBuilder statsBuilder = new QueryBuilder();
            statsBuilder.and(DBObjectToVariantStatsConverter.STUDY_ID).is(studyId);
            if (cohortId != null) {
                statsBuilder.and(DBObjectToVariantStatsConverter.COHORT_ID).is(cohortId);
            }
            addCompQueryFilter(key, operatorValue, statsBuilder);
            builder.and(DBObjectToVariantConverter.STATS_FIELD).elemMatch(statsBuilder.get());
        } else {
            addCompQueryFilter(DBObjectToVariantConverter.STATS_FIELD + "." + key, value, builder);
        }
        return builder;
    }

    /**
     * Parses the string to integer number.
     * <p>
     * Returns null if the string was not an integer.
     *
     * @param study
     * @return
     */
    private Integer getInteger(String study) {
        Integer integer;
        try {
            integer = Integer.parseInt(study);
        } catch (NumberFormatException ignored) {
            integer = null;
        }
        return integer;
    }

    private QueryBuilder getRegionFilter(Region region, QueryBuilder builder) {
        List<String> chunkIds = getChunkIds(region);
        builder.and(DBObjectToVariantConverter.AT_FIELD + '.' + DBObjectToVariantConverter.CHUNK_IDS_FIELD).in(chunkIds);
        builder.and(DBObjectToVariantConverter.END_FIELD).greaterThanEquals(region.getStart());
        builder.and(DBObjectToVariantConverter.START_FIELD).lessThanEquals(region.getEnd());
        return builder;
    }

    private QueryBuilder getRegionFilter(List<Region> regions, QueryBuilder builder) {
        if (regions != null && !regions.isEmpty()) {
            DBObject[] objects = new DBObject[regions.size()];
            int i = 0;
            for (Region region : regions) {
                if (region.getEnd() - region.getStart() < 1000000) {
                    List<String> chunkIds = getChunkIds(region);
                    DBObject regionObject = new BasicDBObject(DBObjectToVariantConverter.AT_FIELD + '.' + DBObjectToVariantConverter
                            .CHUNK_IDS_FIELD,
                            new BasicDBObject("$in", chunkIds));
                    if (region.getEnd() != Integer.MAX_VALUE) {
                        regionObject.put(DBObjectToVariantConverter.START_FIELD, new BasicDBObject("$lte", region.getEnd()));
                    }
                    if (region.getStart() != 0) {
                        regionObject.put(DBObjectToVariantConverter.END_FIELD, new BasicDBObject("$gte", region.getStart()));
                    }
                    objects[i] = regionObject;
                } else {
                    DBObject regionObject = new BasicDBObject(DBObjectToVariantConverter.CHROMOSOME_FIELD, region.getChromosome());
                    if (region.getEnd() != Integer.MAX_VALUE) {
                        regionObject.put(DBObjectToVariantConverter.START_FIELD, new BasicDBObject("$lte", region.getEnd()));
                    }
                    if (region.getStart() != 0) {
                        regionObject.put(DBObjectToVariantConverter.END_FIELD, new BasicDBObject("$gte", region.getStart()));
                    }
                    objects[i] = regionObject;
                }
                i++;
            }
            builder.or(objects);
        }
        return builder;
    }

    /* Query util methods */
    /**
     * Parses the string to integer number.
     * <p>
     * Returns null if the string was not an integer.
     */
    private Integer parseInteger(String string) {
        Integer integer;
        try {
            integer = Integer.parseInt(string);
        } catch (NumberFormatException ignored) {
            integer = null;
        }
        return integer;
    }

    /**
     * Get the object as an integer. If it's a list, will be returned as a CSV
     */
    private String objectToString(Object objectValue) {
        String value;
        if (objectValue instanceof String) {
            value = ((String) objectValue);
        } else if (objectValue instanceof List) {
            value = ((List<Object>) objectValue).stream().map(String::valueOf).collect(Collectors.joining(","));
        } else {
            value = String.valueOf(objectValue);
        }
        return value;
    }

    /**
     * Splits the string with the specified operation.
     */
    private List<String> splitValue(String value, QueryOperation operation) {
        List<String> list;
        if (operation == null) {
            list = Collections.singletonList(value);
        } else if (operation == QueryOperation.AND) {
            list = Arrays.asList(value.split(AND));
        } else {
            list = Arrays.asList(value.split(OR));
        }
        return list;
    }

    void createIndexes(QueryOptions options) {
        logger.info("Start creating indexes");

        DBObject onBackground = new BasicDBObject("background", true);
        DBObject backgroundAndSparse = new BasicDBObject("background", true).append("sparse", true);
        variantsCollection.createIndex(new BasicDBObject(DBObjectToVariantConverter.AT_FIELD + '.'
                + DBObjectToVariantConverter.CHUNK_IDS_FIELD, 1), onBackground);
        variantsCollection.createIndex(new BasicDBObject(DBObjectToVariantConverter.CHROMOSOME_FIELD, 1)
                .append(DBObjectToVariantConverter.START_FIELD, 1)
                .append(DBObjectToVariantConverter.END_FIELD, 1), onBackground);
        variantsCollection.createIndex(new BasicDBObject(DBObjectToVariantConverter.IDS_FIELD, 1), onBackground);
        variantsCollection.createIndex(new BasicDBObject(DBObjectToVariantConverter.STUDIES_FIELD
                + "." + DBObjectToStudyVariantEntryConverter.STUDYID_FIELD, 1)
                .append(DBObjectToVariantConverter.STUDIES_FIELD
                        + "." + DBObjectToStudyVariantEntryConverter.FILES_FIELD
                        + "." + DBObjectToStudyVariantEntryConverter.FILEID_FIELD, 1), onBackground);
        variantsCollection.createIndex(new BasicDBObject(DBObjectToVariantConverter.ANNOTATION_FIELD
                + "." + DBObjectToVariantAnnotationConverter.XREFS_FIELD
                + "." + DBObjectToVariantAnnotationConverter.XREF_ID_FIELD, 1), onBackground);
        variantsCollection.createIndex(new BasicDBObject(DBObjectToVariantConverter.ANNOTATION_FIELD
                + "." + DBObjectToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                + "." + DBObjectToVariantAnnotationConverter.SO_ACCESSION_FIELD, 1), onBackground);
        variantsCollection.createIndex(new BasicDBObject(DBObjectToVariantConverter.ANNOTATION_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.POPULATION_FREQUENCIES_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.POPULATION_FREQUENCY_STUDY_FIELD, 1)
                        .append(DBObjectToVariantConverter.ANNOTATION_FIELD
                                + "." + DBObjectToVariantAnnotationConverter.POPULATION_FREQUENCIES_FIELD
                                + "." + DBObjectToVariantAnnotationConverter.POPULATION_FREQUENCY_POP_FIELD, 1)
                        .append(DBObjectToVariantConverter.ANNOTATION_FIELD
                                + "." + DBObjectToVariantAnnotationConverter.POPULATION_FREQUENCIES_FIELD
                                + "." + DBObjectToVariantAnnotationConverter.POPULATION_FREQUENCY_ALTERNATE_FREQUENCY_FIELD, 1),
                backgroundAndSparse);
        variantsCollection.createIndex(new BasicDBObject(DBObjectToVariantConverter.ANNOTATION_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.CLINICAL_DATA_FIELD + ".clinvar.clinicalSignificance", 1),
                backgroundAndSparse);
        variantsCollection.createIndex(new BasicDBObject(DBObjectToVariantConverter.STATS_FIELD + "." + DBObjectToVariantStatsConverter
                .MAF_FIELD, 1), onBackground);
        variantsCollection.createIndex(new BasicDBObject(DBObjectToVariantConverter.STATS_FIELD + "." + DBObjectToVariantStatsConverter
                .MGF_FIELD, 1), onBackground);

        logger.debug("sent order to create indices");
    }


    /**
     * This method split a typical key-value param such as 'sift<=0.2' in an array ["sift", "<=0.2"].
     * This implementation can and probably should be improved.
     *
     * @param keyValue The keyvalue parameter to be split
     * @return An array with 2 positions for the key and value
     */
    private String[] splitKeyValue(String keyValue) {
        String[] keyValueArray = new String[2];
        String[] arr = keyValue.replaceAll("==", " ")
                .replaceAll(">=", " ")
                .replaceAll("<=", " ")
                .replaceAll("!=", " ")
                .replaceAll("~=", " ")
                .replaceAll("=", " ")
                .replaceAll("<", " ")
                .replaceAll(">", " ")
                .split(" ");
        keyValueArray[0] = arr[0];
        keyValueArray[1] = keyValue.replaceAll(arr[0], "");
        return keyValueArray;
    }

    /* *******************
     * Auxiliary methods *
     * *******************/
    private List<String> getChunkIds(Region region) {
        List<String> chunkIds = new LinkedList<>();

        int chunkSize = (region.getEnd() - region.getStart() > VariantMongoDBWriter.CHUNK_SIZE_BIG)
                ? VariantMongoDBWriter.CHUNK_SIZE_BIG
                : VariantMongoDBWriter.CHUNK_SIZE_SMALL;
        int ks = chunkSize / 1000;
        int chunkStart = region.getStart() / chunkSize;
        int chunkEnd = region.getEnd() / chunkSize;

        for (int i = chunkStart; i <= chunkEnd; i++) {
            String chunkId = region.getChromosome() + "_" + i + "_" + ks + "k";
            chunkIds.add(chunkId);
        }
        return chunkIds;
    }

    private int getChunkId(int position, int chunksize) {
        return position / chunksize;
    }

    private int getChunkStart(int id, int chunksize) {
        return (id == 0) ? 1 : id * chunksize;
    }

    private int getChunkEnd(int id, int chunksize) {
        return (id * chunksize) + chunksize - 1;
    }




    /* OLD METHODS*/
    @Deprecated
    private QueryBuilder parseQueryOptions(QueryOptions options, QueryBuilder builder) {
        if (options != null) {
            if (options.containsKey("sort")) {
                if (options.getBoolean("sort")) {
                    options.put("sort", new BasicDBObject(DBObjectToVariantConverter.CHROMOSOME_FIELD, 1)
                            .append(DBObjectToVariantConverter.START_FIELD, 1));
                } else {
                    options.remove("sort");
                }
            }

            /** GENOMIC REGION **/
            if (options.containsKey(VariantQueryParams.REGION.key()) && !options.getString(VariantQueryParams.REGION.key()).isEmpty()) {
                List<String> stringList = options.getAsStringList(VariantQueryParams.REGION.key());
                List<Region> regions = new ArrayList<>(stringList.size());
                for (String reg : stringList) {
                    Region region = Region.parseRegion(reg);
                    regions.add(region);
                }
                getRegionFilter(regions, builder);
            }

            if (options.containsKey(VariantQueryParams.GENE.key())) {
                List<String> xrefs = options.getAsStringList(VariantQueryParams.GENE.key());
                addQueryListFilter(DBObjectToVariantConverter.ANNOTATION_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.XREFS_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.XREF_ID_FIELD, xrefs, builder, QueryOperation.OR);
            }

            if (options.getString(VariantQueryParams.ID.key()) != null && !options.getString(VariantQueryParams.ID.key()).isEmpty()) {
                //) && !options.getString("id").isEmpty()) {
                List<String> ids = options.getAsStringList(VariantQueryParams.ID.key());
                addQueryListFilter(DBObjectToVariantConverter.ANNOTATION_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.XREFS_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.XREF_ID_FIELD, ids, builder, QueryOperation.OR);
                addQueryListFilter(DBObjectToVariantConverter.IDS_FIELD, ids, builder, QueryOperation.OR);
            }

            /** VARIANT **/
            if (options.containsKey(VariantQueryParams.TYPE.key())) { // && !options.getString("type").isEmpty()) {
                addQueryStringFilter(DBObjectToVariantConverter.TYPE_FIELD, options.getString(VariantQueryParams.TYPE.key()), builder);
            }

            if (options.containsKey(VariantQueryParams.REFERENCE.key()) && options.getString(VariantQueryParams.REFERENCE.key()) != null) {
                addQueryStringFilter(DBObjectToVariantConverter.REFERENCE_FIELD, options.getString(VariantQueryParams.REFERENCE.key()),
                        builder);
            }

            if (options.containsKey(VariantQueryParams.ALTERNATE.key()) && options.getString(VariantQueryParams.ALTERNATE.key()) != null) {
                addQueryStringFilter(DBObjectToVariantConverter.ALTERNATE_FIELD, options.getString(VariantQueryParams.ALTERNATE.key()),
                        builder);
            }

            /** ANNOTATION **/
            if (options.containsKey(VariantQueryParams.ANNOTATION_EXISTS.key())) {
                builder.and(DBObjectToVariantConverter.ANNOTATION_FIELD).exists(options.getBoolean(VariantQueryParams.ANNOTATION_EXISTS
                        .key()));
            }

            if (options.containsKey(VariantQueryParams.ANNOT_XREF.key())) {
                List<String> xrefs = options.getAsStringList(VariantQueryParams.ANNOT_XREF.key());
                addQueryListFilter(DBObjectToVariantConverter.ANNOTATION_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.XREFS_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.XREF_ID_FIELD, xrefs, builder, QueryOperation.AND);
            }

            if (options.containsKey(VariantQueryParams.ANNOT_CONSEQUENCE_TYPE.key())) {
                List<String> cts = new ArrayList<>(options.getAsStringList(VariantQueryParams.ANNOT_CONSEQUENCE_TYPE.key()));
                List<Integer> ctsInteger = new ArrayList<>(cts.size());
                Iterator<String> iterator = cts.iterator();
                while (iterator.hasNext()) {
                    String ct = iterator.next();
                    if (ct.startsWith("SO:")) {
                        ct = ct.substring(3);
                    }
                    try {
                        ctsInteger.add(Integer.parseInt(ct));
                    } catch (NumberFormatException e) {
                        logger.error("Error parsing integer ", e);
                        iterator.remove();  //Remove the malformed query params.
                    }
                }
                options.put(VariantQueryParams.ANNOT_CONSEQUENCE_TYPE.key(), cts); //Replace the QueryOption without the malformed query
                // params
                addQueryListFilter(DBObjectToVariantConverter.ANNOTATION_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.SO_ACCESSION_FIELD, ctsInteger, builder, QueryOperation.AND);
            }

            if (options.containsKey(VariantQueryParams.ANNOT_BIOTYPE.key())) {
                List<String> biotypes = options.getAsStringList(VariantQueryParams.ANNOT_BIOTYPE.key());
                addQueryListFilter(DBObjectToVariantConverter.ANNOTATION_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.BIOTYPE_FIELD, biotypes, builder, QueryOperation.AND);
            }

            if (options.containsKey(VariantQueryParams.POLYPHEN.key())) {
                addCompQueryFilter(DBObjectToVariantConverter.ANNOTATION_FIELD
                                + "." + DBObjectToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                                + "." + DBObjectToVariantAnnotationConverter.POLYPHEN_FIELD + "."
                                + DBObjectToVariantAnnotationConverter.SCORE_SCORE_FIELD,
                        options.getString(VariantQueryParams.POLYPHEN.key()),
                        builder);
            }

            if (options.containsKey(VariantQueryParams.SIFT.key())) {
                addCompQueryFilter(DBObjectToVariantConverter.ANNOTATION_FIELD
                                + "." + DBObjectToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                                + "." + DBObjectToVariantAnnotationConverter.SIFT_FIELD + "."
                                + DBObjectToVariantAnnotationConverter.SCORE_SCORE_FIELD, options.getString(VariantQueryParams.SIFT.key()),
                        builder);
            }

            if (options.containsKey(VariantQueryParams.PROTEIN_SUBSTITUTION.key())) {
                List<String> list = new ArrayList<>(options.getAsStringList(VariantQueryParams.PROTEIN_SUBSTITUTION.key()));
                addScoreFilter(DBObjectToVariantConverter.ANNOTATION_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.PROTEIN_SUBSTITUTION_SCORE_FIELD, list, builder);
                options.put(VariantQueryParams.PROTEIN_SUBSTITUTION.key(), list); //Replace the QueryOption without the malformed query
            }

            if (options.containsKey(VariantQueryParams.CONSERVATION.key())) {
                List<String> list = new ArrayList<>(options.getAsStringList(VariantQueryParams.CONSERVATION.key()));
                addScoreFilter(DBObjectToVariantConverter.ANNOTATION_FIELD
                        + "." + DBObjectToVariantAnnotationConverter.CONSERVED_REGION_SCORE_FIELD, list, builder);
                options.put(VariantQueryParams.CONSERVATION.key(), list); //Replace the QueryOption without the malformed query params
            }

            if (options.containsKey(VariantQueryParams.ALTERNATE_FREQUENCY.key())) {
                List<String> list = new ArrayList<>(options.getAsStringList(VariantQueryParams.ALTERNATE_FREQUENCY.key()));
                addFrequencyFilter(DBObjectToVariantConverter.ANNOTATION_FIELD
                                + "." + DBObjectToVariantAnnotationConverter.POPULATION_FREQUENCIES_FIELD,
                        DBObjectToVariantAnnotationConverter.POPULATION_FREQUENCY_ALTERNATE_FREQUENCY_FIELD, list, builder); // Same
            }

            if (options.containsKey(VariantQueryParams.REFERENCE_FREQUENCY.key())) {
                List<String> list = new ArrayList<>(options.getAsStringList(VariantQueryParams.REFERENCE_FREQUENCY.key()));
                addFrequencyFilter(DBObjectToVariantConverter.ANNOTATION_FIELD
                                + "." + DBObjectToVariantAnnotationConverter.POPULATION_FREQUENCIES_FIELD,
                        DBObjectToVariantAnnotationConverter.POPULATION_FREQUENCY_REFERENCE_FREQUENCY_FIELD, list, builder); // Same
            }

            /** STATS **/
            if (options.get(VariantQueryParams.STATS_MAF.key()) != null
                    && !options.getString(VariantQueryParams.STATS_MAF.key()).isEmpty()) {
                addCompQueryFilter(DBObjectToVariantConverter.STATS_FIELD + "." + DBObjectToVariantStatsConverter.MAF_FIELD,
                        options.getString(VariantQueryParams.STATS_MAF.key()), builder);
            }

            if (options.get(VariantQueryParams.STATS_MGF.key()) != null
                    && !options.getString(VariantQueryParams.STATS_MGF.key()).isEmpty()) {
                addCompQueryFilter(DBObjectToVariantConverter.STATS_FIELD + "." + DBObjectToVariantStatsConverter.MGF_FIELD,
                        options.getString(VariantQueryParams.STATS_MGF.key()), builder);
            }

            if (options.get(VariantQueryParams.MISSING_ALLELES.key()) != null
                    && !options.getString(VariantQueryParams.MISSING_ALLELES.key()).isEmpty()) {
                addCompQueryFilter(DBObjectToVariantConverter.STATS_FIELD + "." + DBObjectToVariantStatsConverter.MISSALLELE_FIELD,
                        options.getString(VariantQueryParams.MISSING_ALLELES.key()), builder);
            }

            if (options.get(VariantQueryParams.MISSING_GENOTYPES.key()) != null && !options.getString(VariantQueryParams
                    .MISSING_GENOTYPES.key()).isEmpty()) {
                addCompQueryFilter(DBObjectToVariantConverter.STATS_FIELD + "." + DBObjectToVariantStatsConverter.MISSGENOTYPE_FIELD,
                        options.getString(VariantQueryParams.MISSING_GENOTYPES.key()), builder);
            }

            if (options.get("numgt") != null && !options.getString("numgt").isEmpty()) {
                for (String numgt : options.getAsStringList("numgt")) {
                    String[] split = numgt.split(":");
                    addCompQueryFilter(DBObjectToVariantConverter.STATS_FIELD
                            + "." + DBObjectToVariantStatsConverter.NUMGT_FIELD + "." + split[0], split[1], builder);
                }
            }

            /** FILES **/
            QueryBuilder fileBuilder = QueryBuilder.start();

            if (options.containsKey(VariantQueryParams.STUDIES.key())) { // && !options.getList("studies").isEmpty() && !options
                addQueryListFilter(DBObjectToStudyVariantEntryConverter.STUDYID_FIELD,
                        options.getAsIntegerList(VariantQueryParams.STUDIES.key()), fileBuilder, QueryOperation.AND);
            }

            if (options.containsKey(VariantQueryParams.FILES.key())) { // && !options.getList("files").isEmpty() && !options.getListAs
                addQueryListFilter(DBObjectToStudyVariantEntryConverter.FILES_FIELD
                                + "." + DBObjectToStudyVariantEntryConverter.FILEID_FIELD,
                        options.getAsIntegerList(VariantQueryParams.FILES.key()),
                        fileBuilder, QueryOperation.AND);
            }

            if (options.containsKey(VariantQueryParams.GENOTYPE.key())) {
                String sampleGenotypesCSV = options.getString(VariantQueryParams.GENOTYPE.key());

//                String AND = ",";
//                String OR = ";";
//                String IS = ":";
//                String AND = "AND";
//                String OR = "OR";
//                String IS = ":";

                String[] sampleGenotypesArray = sampleGenotypesCSV.split(AND);
                for (String sampleGenotypes : sampleGenotypesArray) {
                    String[] sampleGenotype = sampleGenotypes.split(IS);
                    if (sampleGenotype.length != 2) {
                        continue;
                    }
                    int sample = Integer.parseInt(sampleGenotype[0]);
                    String[] genotypes = sampleGenotype[1].split(OR);
                    QueryBuilder genotypesBuilder = QueryBuilder.start();
                    for (String genotype : genotypes) {
                        String s = DBObjectToStudyVariantEntryConverter.GENOTYPES_FIELD
                                + "." + DBObjectToSamplesConverter.genotypeToStorageType(genotype);
                        //or [ {"samp.0|0" : { $elemMatch : { $eq : <sampleId> } } } ]
                        genotypesBuilder.or(new BasicDBObject(s, new BasicDBObject("$elemMatch", new BasicDBObject("$eq", sample))));
                    }
                    fileBuilder.and(genotypesBuilder.get());
                }
            }

            DBObject fileQuery = fileBuilder.get();
            if (fileQuery.keySet().size() != 0) {
                builder.and(DBObjectToVariantConverter.STUDIES_FIELD).elemMatch(fileQuery);
            }
        }

        logger.debug("Find = " + builder.get());
        return builder;
    }

    @Deprecated
    private DBObject parseProjectionQueryOptions(QueryOptions options) {
        DBObject projection = new BasicDBObject();

        if (options == null) {
            return projection;
        }

        List<String> includeList = options.getAsStringList("include");
        if (!includeList.isEmpty()) { //Include some
            for (String s : includeList) {
                String key = DBObjectToVariantConverter.toShortFieldName(s);
                if (key != null) {
                    projection.put(key, 1);
                } else {
                    logger.warn("Unknown include field: {}", s);
                }
            }
        } else { //Include all
            for (String values : DBObjectToVariantConverter.FIELDS_MAP.values()) {
                projection.put(values, 1);
            }
            if (options.containsKey("exclude")) { // Exclude some
                List<String> excludeList = options.getAsStringList("exclude");
                for (String s : excludeList) {
                    String key = DBObjectToVariantConverter.toShortFieldName(s);
                    if (key != null) {
                        projection.removeField(key);
                    } else {
                        logger.warn("Unknown exclude field: {}", s);
                    }
                }
            }
        }

        if (options.containsKey(VariantQueryParams.RETURNED_FILES.key())
                && projection.containsField(DBObjectToVariantConverter.STUDIES_FIELD)) {
            int file = options.getInt(VariantQueryParams.RETURNED_FILES.key());
            projection.put(DBObjectToVariantConverter.STUDIES_FIELD, new BasicDBObject("$elemMatch",
                    new BasicDBObject(DBObjectToStudyVariantEntryConverter.FILES_FIELD
                            + "." + DBObjectToStudyVariantEntryConverter.FILEID_FIELD, file)));
        }
        logger.debug("Projection: {}", projection);
        return projection;
    }

    @Deprecated
    private QueryBuilder addQueryListFilter(String key, List<?> values, QueryBuilder builder, QueryOperation op) {
        if (values != null) {
            if (values.size() == 1) {
                if (op == QueryOperation.AND) {
                    builder.and(key).is(values.get(0));
                } else {
                    builder.or(QueryBuilder.start(key).is(values.get(0)).get());
                }
            } else if (!values.isEmpty()) {
                if (op == QueryOperation.AND) {
                    builder.and(key).in(values);
                } else {
                    builder.or(QueryBuilder.start(key).in(values).get());
                }
            }
        }
        return builder;
    }

    @Deprecated
    private QueryBuilder addScoreFilter(String key, List<String> list, QueryBuilder builder) {
        return addScoreFilter(key, list.stream().collect(Collectors.joining(OR)), builder, VariantQueryParams.CONSERVATION);
    }

    @Deprecated
    private QueryBuilder addFrequencyFilter(String key, String alleleFrequencyField, List<String> list, QueryBuilder builder) {
        return addFrequencyFilter(key, alleleFrequencyField, list.stream().collect(Collectors.joining(OR)), builder,
                VariantQueryParams.ALTERNATE_FREQUENCY);
    }

    @Override
    @Deprecated
    public QueryResult<Variant> getAllVariants(QueryOptions options) {
        QueryBuilder qb = QueryBuilder.start();
        parseQueryOptions(options, qb);
        DBObject projection = parseProjectionQueryOptions(options);
        logger.debug("Query to be executed {}", qb.get().toString());

        return variantsCollection.find(qb.get(), projection, getDbObjectToVariantConverter(new Query(options), options), options);
    }

    @Override
    @Deprecated
    public QueryResult<Variant> getVariantById(String id, QueryOptions options) {
        if (options == null) {
            options = new QueryOptions(VariantQueryParams.ID.key(), id);
        } else {
            options.addToListOption(VariantQueryParams.ID.key(), id);
        }

        QueryBuilder qb = QueryBuilder.start();
        parseQueryOptions(options, qb);
        DBObject projection = parseProjectionQueryOptions(options);
        logger.debug("Query to be executed {}", qb.get().toString());

        QueryResult<Variant> queryResult = variantsCollection.find(qb.get(), projection, getDbObjectToVariantConverter(new Query(options)
                , options), options);
        queryResult.setId(id);
        return queryResult;
    }

    @Override
    @Deprecated
    public List<QueryResult<Variant>> getAllVariantsByIdList(List<String> idList, QueryOptions options) {
        List<QueryResult<Variant>> allResults = new ArrayList<>(idList.size());
        for (String r : idList) {
            QueryResult<Variant> queryResult = getVariantById(r, options);
            allResults.add(queryResult);
        }
        return allResults;
    }

    @Override
    @Deprecated
    public QueryResult<Variant> getAllVariantsByRegion(Region region, QueryOptions options) {
        QueryBuilder qb = QueryBuilder.start();
        getRegionFilter(region, qb);
        parseQueryOptions(options, qb);
        DBObject projection = parseProjectionQueryOptions(options);
        if (options == null) {
            options = new QueryOptions();
        }
        QueryResult<Variant> queryResult = variantsCollection.find(qb.get(), projection,
                getDbObjectToVariantConverter(new Query(options), options), options);
        queryResult.setId(region.toString());
        return queryResult;
    }

    @Override
    @Deprecated
    public List<QueryResult<Variant>> getAllVariantsByRegionList(List<Region> regionList, QueryOptions options) {
        List<QueryResult<Variant>> allResults;
        if (options == null) {
            options = new QueryOptions();
        }

        // If the users asks to sort the results, do it by chromosome and start
        if (options.getBoolean("sort", false)) {
            options.put("sort", new BasicDBObject("chr", 1).append("start", 1));
        }

        // If the user asks to merge the results, run only one query,
        // otherwise delegate in the method to query regions one by one
        if (options.getBoolean("merge", false)) {
            options.add(VariantQueryParams.REGION.key(), regionList);
            allResults = Collections.singletonList(getAllVariants(options));
        } else {
            allResults = new ArrayList<>(regionList.size());
            for (Region r : regionList) {
                QueryResult queryResult = getAllVariantsByRegion(r, options);
                queryResult.setId(r.toString());
                allResults.add(queryResult);
            }
        }
        return allResults;
    }

    @Override
    @Deprecated
    public QueryResult getVariantFrequencyByRegion(Region region, QueryOptions options) {
        // db.variants.aggregate( { $match: { $and: [ {chr: "1"}, {start: {$gt: 251391, $lt: 2701391}} ] }},
        //                        { $group: { _id: { $subtract: [ { $divide: ["$start", 20000] }, { $divide: [{$mod: ["$start", 20000]},
        // 20000] } ] },
        //                                  totalCount: {$sum: 1}}})
        if (options == null) {
            options = new QueryOptions();
        }

        int interval = options.getInt("interval", 20000);
        BasicDBObject start = new BasicDBObject("$gt", region.getStart());
        start.append("$lt", region.getEnd());

        BasicDBList andArr = new BasicDBList();
        andArr.add(new BasicDBObject(DBObjectToVariantConverter.CHROMOSOME_FIELD, region.getChromosome()));
        andArr.add(new BasicDBObject(DBObjectToVariantConverter.START_FIELD, start));

        // Parsing the rest of options
        QueryBuilder qb = new QueryBuilder();
        DBObject optionsMatch = parseQueryOptions(options, qb).get();
        if (!optionsMatch.keySet().isEmpty()) {
            andArr.add(optionsMatch);
        }
        DBObject match = new BasicDBObject("$match", new BasicDBObject("$and", andArr));

        BasicDBList divide1 = new BasicDBList();
        divide1.add("$start");
        divide1.add(interval);

        BasicDBList divide2 = new BasicDBList();
        divide2.add(new BasicDBObject("$mod", divide1));
        divide2.add(interval);

        BasicDBList subtractList = new BasicDBList();
        subtractList.add(new BasicDBObject("$divide", divide1));
        subtractList.add(new BasicDBObject("$divide", divide2));
        BasicDBObject substract = new BasicDBObject("$subtract", subtractList);
        DBObject totalCount = new BasicDBObject("$sum", 1);
        BasicDBObject g = new BasicDBObject("_id", substract);
        g.append("features_count", totalCount);
        DBObject group = new BasicDBObject("$group", g);

        DBObject sort = new BasicDBObject("$sort", new BasicDBObject("_id", 1));
        long dbTimeStart = System.currentTimeMillis();
        QueryResult output = variantsCollection.aggregate(/*"$histogram", */Arrays.asList(match, group, sort), options);
        long dbTimeEnd = System.currentTimeMillis();

        Map<Long, DBObject> ids = new HashMap<>();
        // Create DBObject for intervals with features inside them
        for (DBObject intervalObj : (List<DBObject>) output.getResult()) {
            Long auxId = Math.round((Double) intervalObj.get("_id")); //is double

            DBObject intervalVisited = ids.get(auxId);
            if (intervalVisited == null) {
                intervalObj.put("_id", auxId);
                intervalObj.put("start", getChunkStart(auxId.intValue(), interval));
                intervalObj.put("end", getChunkEnd(auxId.intValue(), interval));
                intervalObj.put("chromosome", region.getChromosome());
                intervalObj.put("features_count", Math.log((int) intervalObj.get("features_count")));
                ids.put(auxId, intervalObj);
            } else {
                Double sum = (Double) intervalVisited.get("features_count") + Math.log((int) intervalObj.get("features_count"));
                intervalVisited.put("features_count", sum.intValue());
            }
        }

        // Create DBObject for intervals without features inside them
        BasicDBList resultList = new BasicDBList();
        int firstChunkId = getChunkId(region.getStart(), interval);
        int lastChunkId = getChunkId(region.getEnd(), interval);
        DBObject intervalObj;
        for (int chunkId = firstChunkId; chunkId <= lastChunkId; chunkId++) {
            intervalObj = ids.get((long) chunkId);
            if (intervalObj == null) {
                intervalObj = new BasicDBObject();
                intervalObj.put("_id", chunkId);
                intervalObj.put("start", getChunkStart(chunkId, interval));
                intervalObj.put("end", getChunkEnd(chunkId, interval));
                intervalObj.put("chromosome", region.getChromosome());
                intervalObj.put("features_count", 0);
            }
            resultList.add(intervalObj);
        }

        QueryResult queryResult = new QueryResult(region.toString(), ((Long) (dbTimeEnd - dbTimeStart)).intValue(),
                resultList.size(), resultList.size(), null, null, resultList);
        return queryResult;
    }

    @Override
    @Deprecated
    public QueryResult groupBy(String field, QueryOptions options) {
        String documentPath;
        switch (field) {
            case "gene":
            case "ensemblGene":
                documentPath = DBObjectToVariantConverter.ANNOTATION_FIELD + "." + DBObjectToVariantAnnotationConverter
                        .CONSEQUENCE_TYPE_FIELD + "." + DBObjectToVariantAnnotationConverter.ENSEMBL_GENE_ID_FIELD;
                break;
            case "ct":
            case "consequence_type":
                documentPath = DBObjectToVariantConverter.ANNOTATION_FIELD + "." + DBObjectToVariantAnnotationConverter
                        .CONSEQUENCE_TYPE_FIELD + "." + DBObjectToVariantAnnotationConverter.SO_ACCESSION_FIELD;
                break;
            default:
                documentPath = DBObjectToVariantConverter.ANNOTATION_FIELD + "." + DBObjectToVariantAnnotationConverter
                        .CONSEQUENCE_TYPE_FIELD + "." + DBObjectToVariantAnnotationConverter.GENE_NAME_FIELD;
                break;
        }
        QueryBuilder qb = QueryBuilder.start();
        parseQueryOptions(options, qb);
        DBObject match = new BasicDBObject("$match", qb.get());
        DBObject project = new BasicDBObject("$project", new BasicDBObject("field", "$" + documentPath));
        DBObject unwind = new BasicDBObject("$unwind", "$field");
        DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "$field")
                .append("count", new BasicDBObject("$sum", 1))); // sum, count, avg, ...?
        DBObject sort = new BasicDBObject("$sort", new BasicDBObject("count", options != null ? options.getInt("order", -1) : -1)); // 1
        // = ascending, -1 = descending
        DBObject limit = new BasicDBObject("$limit", options != null && options.getInt("limit", -1) > 0 ? options.getInt("limit") : 10);
        return variantsCollection.aggregate(Arrays.asList(match, project, unwind, group, sort, limit), options);
    }

    @Override
    @Deprecated
    public VariantSourceDBAdaptor getVariantSourceDBAdaptor() {
        return variantSourceMongoDBAdaptor;
    }

    @Override
    public StudyConfigurationManager getStudyConfigurationManager() {
        return studyConfigurationManager;
    }

    @Override
    public void setStudyConfigurationManager(StudyConfigurationManager studyConfigurationManager) {
        this.studyConfigurationManager = studyConfigurationManager;
    }

    @Override
    @Deprecated
    public VariantDBIterator iterator(QueryOptions options) {
        return iterator(new Query(options), options);
    }

    public static List<Integer> getLoadedSamples(int fileId, StudyConfiguration studyConfiguration) {
        List<Integer> loadedSampleIds = new LinkedList<>();
        for (Integer indexedFile : studyConfiguration.getIndexedFiles()) {
            if (indexedFile.equals(fileId)) {
                continue;
            } else {
                loadedSampleIds.addAll(studyConfiguration.getSamplesInFiles().get(indexedFile));
            }
        }
        loadedSampleIds.removeAll(studyConfiguration.getSamplesInFiles().get(fileId));
        return loadedSampleIds;
    }

    @Override
    @Deprecated
    public QueryResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, int studyId, QueryOptions queryOptions) {
        DBCollection coll = db.getDb().getCollection(collectionName);
        BulkWriteOperation builder = coll.initializeUnorderedBulkOperation();
        long start = System.nanoTime();
        DBObjectToVariantStatsConverter statsConverter = new DBObjectToVariantStatsConverter(studyConfigurationManager);
        int fileId = queryOptions.getInt(VariantStorageManager.Options.FILE_ID.key());
        DBObjectToVariantConverter variantConverter = getDbObjectToVariantConverter(new Query(queryOptions), queryOptions);
        //TODO: Use the StudyConfiguration to change names to ids

        // TODO make unset of 'st' if already present?
        for (VariantStatsWrapper wrapper : variantStatsWrappers) {
            Map<String, VariantStats> cohortStats = wrapper.getCohortStats();
            Iterator<VariantStats> iterator = cohortStats.values().iterator();
            VariantStats variantStats = iterator.hasNext() ? iterator.next() : null;
            List<DBObject> cohorts = statsConverter.convertCohortsToStorageType(cohortStats, studyId);   // TODO remove when we remove
            if (!cohorts.isEmpty()) {
                String id = variantConverter.buildStorageId(wrapper.getChromosome(), wrapper.getPosition(),
                        variantStats.getRefAllele(), variantStats.getAltAllele());

                List<String> cohortIds = new ArrayList<>(cohorts.size());
                List<Integer> fileIds = new ArrayList<>(cohorts.size());
                List<Integer> studyIds = new ArrayList<>(cohorts.size());
                for (DBObject cohort : cohorts) {
                    cohortIds.add((String) cohort.get(DBObjectToVariantStatsConverter.COHORT_ID));
                    studyIds.add((Integer) cohort.get(DBObjectToVariantStatsConverter.STUDY_ID));
                }

                DBObject find = new BasicDBObject("_id", id);
                DBObject update = new BasicDBObject("$pull",
                        new BasicDBObject(DBObjectToVariantConverter.STATS_FIELD,
                                new BasicDBObject()
                                        .append(
                                                DBObjectToVariantStatsConverter.STUDY_ID,
                                                new BasicDBObject("$in", studyIds))
                                        .append(
                                                DBObjectToVariantStatsConverter.COHORT_ID,
                                                new BasicDBObject("$in", cohortIds))));

                builder.find(find).updateOne(update);

                DBObject push = new BasicDBObject("$push",
                        new BasicDBObject(DBObjectToVariantConverter.STATS_FIELD,
                                new BasicDBObject("$each", cohorts)));

                builder.find(find).update(push);
            }
        }
        // TODO handle if the variant didn't had that studyId in the files array
        // TODO check the substitution is done right if the stats are already present
        BulkWriteResult writeResult = builder.execute();
        int writes = writeResult.getModifiedCount();
        return new QueryResult<>("", ((int) (System.nanoTime() - start)), writes, writes, "", "", Collections.singletonList(writeResult));
    }

}
