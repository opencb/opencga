package org.opencb.opencga.storage.mongodb.variant;

import com.mongodb.*;
import java.net.UnknownHostException;
import java.util.*;
import java.util.regex.Pattern;

import org.opencb.biodata.models.feature.Region;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.io.DataWriter;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.mongodb.MongoDBCollection;
import org.opencb.datastore.mongodb.MongoDBConfiguration;
import org.opencb.datastore.mongodb.MongoDataStore;
import org.opencb.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantSourceDBAdaptor;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class VariantMongoDBAdaptor implements VariantDBAdaptor {


    private final MongoDataStoreManager mongoManager;
    private final MongoDataStore db;
    private final DBObjectToVariantConverter variantConverter;
    private final DBObjectToVariantSourceEntryConverter archivedVariantFileConverter;
    private final String collectionName;
    private final VariantSourceMongoDBAdaptor variantSourceMongoDBAdaptor;

    private DataWriter dataWriter;

    protected static Logger logger = LoggerFactory.getLogger(VariantMongoDBAdaptor.class);

    public VariantMongoDBAdaptor(MongoCredentials credentials, String variantsCollectionName, String filesCollectionName) 
            throws UnknownHostException {
        // Mongo configuration
        mongoManager = new MongoDataStoreManager(credentials.getMongoHost(), credentials.getMongoPort());
        MongoDBConfiguration mongoDBConfiguration = MongoDBConfiguration.builder()
                .add("username", credentials.getUsername())
                .add("password", credentials.getPassword() != null ? new String(credentials.getPassword()) : null).build();
        db = mongoManager.get(credentials.getMongoDbName(), mongoDBConfiguration);

        variantSourceMongoDBAdaptor = new VariantSourceMongoDBAdaptor(credentials, filesCollectionName);

        collectionName = variantsCollectionName;
        
        // Converters from DBObject to Java classes
        // TODO Allow to configure depending on the type of study?
        archivedVariantFileConverter = new DBObjectToVariantSourceEntryConverter(true, 
                new DBObjectToVariantStatsConverter(), credentials, filesCollectionName);
        variantConverter = new DBObjectToVariantConverter(archivedVariantFileConverter);
    }


    @Override
    public void setDataWriter(DataWriter dataWriter) {
        this.dataWriter = dataWriter;
    }

    @Override
    public QueryResult getAllVariants(QueryOptions options) {
        MongoDBCollection coll = db.getCollection(collectionName);

        QueryBuilder qb = QueryBuilder.start();
        parseQueryOptions(options, qb);
        DBObject projection = parseProjectionQueryOptions(options);
        logger.debug("Query to be executed {}", qb.get().toString());

        return coll.find(qb.get(), projection, variantConverter, options);
    }


    @Override
    public QueryResult getVariantById(String id, QueryOptions options) {
        MongoDBCollection coll = db.getCollection(collectionName);

//        BasicDBObject query = new BasicDBObject(DBObjectToVariantConverter.ID_FIELD, id);
        if(options == null) {
            options = new QueryOptions(ID, id);
        }else {
            if(options.get(ID) == null) {
                options.put(ID, id);
            }else {
                options.put(ID, options.get(ID)+","+id);
            }
        }

        QueryBuilder qb = QueryBuilder.start();
        parseQueryOptions(options, qb);
        DBObject projection = parseProjectionQueryOptions(options);
        logger.debug("Query to be executed {}", qb.get().toString());

//        return coll.find(query, options, variantConverter);
        return coll.find(qb.get(), projection, variantConverter, options);
    }

    @Override
    public List<QueryResult> getAllVariantsByIdList(List<String> idList, QueryOptions options) {
        List<QueryResult> allResults = new ArrayList<>(idList.size());
        for (String r : idList) {
            QueryResult queryResult = getVariantById(r, options);
            allResults.add(queryResult);
        }
        return allResults;
    }


    @Override
    public QueryResult getAllVariantsByRegion(Region region, QueryOptions options) {
        MongoDBCollection coll = db.getCollection(collectionName);

        QueryBuilder qb = QueryBuilder.start();
        getRegionFilter(region, qb);
        parseQueryOptions(options, qb);
        DBObject projection = parseProjectionQueryOptions(options);

        if (options == null) {
            options = new QueryOptions();
        }
        options.add("sort", new BasicDBObject("chr", 1).append("start", 1));
        return coll.find(qb.get(), projection, variantConverter, options);
    }

    @Override
    public List<QueryResult> getAllVariantsByRegionList(List<Region> regionList, QueryOptions options) {
        List<QueryResult> allResults = new ArrayList<>(regionList.size());
        // If the user asks to merge the results, run only one query,
        // otherwise delegate in the method to query regions one by one
        if (options.getBoolean(MERGE, false)) {
            MongoDBCollection coll = db.getCollection(collectionName);
            QueryBuilder qb = QueryBuilder.start();
            getRegionFilter(regionList, qb);
            parseQueryOptions(options, qb);
            DBObject projection = parseProjectionQueryOptions(options);
            allResults.add(coll.find(qb.get(), projection, variantConverter, options));
        } else {
            for (Region r : regionList) {
                QueryResult queryResult = getAllVariantsByRegion(r, options);
                allResults.add(queryResult);
            }
        }
        return allResults;
    }

    @Override
    public QueryResult getAllVariantsByRegionAndStudies(Region region, List<String> studyId, QueryOptions options) {
        MongoDBCollection coll = db.getCollection(collectionName);

        // Aggregation for filtering when more than one study is present
        QueryBuilder qb = QueryBuilder.start(DBObjectToVariantConverter.FILES_FIELD + "." + DBObjectToVariantSourceEntryConverter.STUDYID_FIELD).in(studyId);
        getRegionFilter(region, qb);
        parseQueryOptions(options, qb);

        DBObject match = new BasicDBObject("$match", qb.get());
        DBObject unwind = new BasicDBObject("$unwind", "$" + DBObjectToVariantConverter.FILES_FIELD);
        DBObject match2 = new BasicDBObject("$match", 
                new BasicDBObject(DBObjectToVariantConverter.FILES_FIELD + "." + DBObjectToVariantSourceEntryConverter.STUDYID_FIELD, 
                        new BasicDBObject("$in", studyId)));

        logger.debug("Query to be executed {}", qb.get().toString());

        return coll.aggregate(/*"$variantsRegionStudies", */Arrays.asList(match, unwind, match2), options);
    }

    @Override
    public QueryResult getVariantFrequencyByRegion(Region region, QueryOptions options) {
        // db.variants.aggregate( { $match: { $and: [ {chr: "1"}, {start: {$gt: 251391, $lt: 2701391}} ] }}, 
        //                        { $group: { _id: { $subtract: [ { $divide: ["$start", 20000] }, { $divide: [{$mod: ["$start", 20000]}, 20000] } ] }, 
        //                                  totalCount: {$sum: 1}}})
        MongoDBCollection coll = db.getCollection(collectionName);

        if(options == null) {
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
        if(!optionsMatch.keySet().isEmpty()) {
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

//        logger.info("getAllIntervalFrequencies - (>·_·)>");
//        System.out.println(options.toString());
//
//        System.out.println(match.toString());
//        System.out.println(group.toString());
//        System.out.println(sort.toString());

        long dbTimeStart = System.currentTimeMillis();
        QueryResult output = coll.aggregate(/*"$histogram", */Arrays.asList(match, group, sort), options);
        long dbTimeEnd = System.currentTimeMillis();

        Map<Long, DBObject> ids = new HashMap<>();
        // Create DBObject for intervals with features inside them
        for (DBObject intervalObj : (List<DBObject>) output.getResult()) {
            Long _id = Math.round((Double) intervalObj.get("_id"));//is double

            DBObject intervalVisited = ids.get(_id);
            if (intervalVisited == null) {
                intervalObj.put("_id", _id);
                intervalObj.put("start", getChunkStart(_id.intValue(), interval));
                intervalObj.put("end", getChunkEnd(_id.intValue(), interval));
                intervalObj.put("chromosome", region.getChromosome());
                intervalObj.put("features_count", Math.log((int) intervalObj.get("features_count")));
                ids.put(_id, intervalObj);
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
    public QueryResult getAllVariantsByGene(String geneName, QueryOptions options) {
        MongoDBCollection coll = db.getCollection(collectionName);

        QueryBuilder qb = QueryBuilder.start("_at.gn").all(Arrays.asList(geneName));
        parseQueryOptions(options, qb);
        DBObject projection = parseProjectionQueryOptions(options);
        return coll.find(qb.get(), projection, variantConverter, options);
    }


    @Override
    public QueryResult groupBy(String field, QueryOptions options) {
        MongoDBCollection coll = db.getCollection(collectionName);

        String documentPath;
        switch (field) {
            case "gene":
                documentPath = "_at.gn";
                break;
            case "ct":
            case "consequence_type":
                documentPath = "_at.ct";
                break;
            default:
                documentPath = "_at.gn";
                break;
        }

        QueryBuilder qb = QueryBuilder.start();
        parseQueryOptions(options, qb);

        DBObject match = new BasicDBObject("$match", qb.get());
        DBObject project = new BasicDBObject("$project", new BasicDBObject("field", "$"+documentPath));
        DBObject unwind = new BasicDBObject("$unwind", "$field");
        DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "$field")
//                .append("field", "$field")
                .append("count", new BasicDBObject("$sum", 1))); // sum, count, avg, ...?
        DBObject sort = new BasicDBObject("$sort", new BasicDBObject("count", (options != null) ? options.getInt("order", -1) : -1)); // 1 = ascending, -1 = descending
        DBObject limit = new BasicDBObject("$limit", (options != null) ? options.getInt("limit", 10) : 10);

        return coll.aggregate(/*"$field", */Arrays.asList(match, project, unwind, group, sort, limit), options);
    }

    @Override
    public QueryResult getMostAffectedGenes(int numGenes, QueryOptions options) {
        return getGenesRanking(numGenes, -1, options);
    }

    @Override
    public QueryResult getLeastAffectedGenes(int numGenes, QueryOptions options) {
        return getGenesRanking(numGenes, 1, options);
    }

    private QueryResult getGenesRanking(int numGenes, int order, QueryOptions options) {
        // db.variants.aggregate( { $project : { genes : "$_at.gn"} },
        //                        { $unwind : "$genes"},
        //                        { $group : { _id : "$genes", count: { $sum : 1 } }},
        //                        { $sort : { "count" : -1 }},
        //                        { $limit : 10 } )
        MongoDBCollection coll = db.getCollection(collectionName);

        QueryBuilder qb = QueryBuilder.start();
        parseQueryOptions(options, qb);

        DBObject match = new BasicDBObject("$match", qb.get());
        DBObject project = new BasicDBObject("$project", new BasicDBObject("genes", "$_at.gn"));
        DBObject unwind = new BasicDBObject("$unwind", "$genes");
        DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "$genes").append("count", new BasicDBObject( "$sum", 1)));
        DBObject sort = new BasicDBObject("$sort", new BasicDBObject("count", order)); // 1 = ascending, -1 = descending
        DBObject limit = new BasicDBObject("$limit", numGenes);

        return coll.aggregate(/*"$effects.geneName", */Arrays.asList(match, project, unwind, group, sort, limit), options);
    }


    @Override
    public QueryResult getTopConsequenceTypes(int numConsequenceTypes, QueryOptions options) {
        return getConsequenceTypesRanking(numConsequenceTypes, -1, options);
    }

    @Override
    public QueryResult getBottomConsequenceTypes(int numConsequenceTypes, QueryOptions options) {
        return getConsequenceTypesRanking(numConsequenceTypes, 1, options);
    }

    private QueryResult getConsequenceTypesRanking(int numConsequenceTypes, int order, QueryOptions options) {
        MongoDBCollection coll = db.getCollection(collectionName);

        QueryBuilder qb = QueryBuilder.start();
        parseQueryOptions(options, qb);

        DBObject match = new BasicDBObject("$match", qb.get());
        DBObject project = new BasicDBObject("$project", new BasicDBObject("so", "$_at.ct"));
        DBObject unwind = new BasicDBObject("$unwind", "$so");
        DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "$so").append("count", new BasicDBObject( "$sum", 1)));
        DBObject sort = new BasicDBObject("$sort", new BasicDBObject("count", order)); // 1 = ascending, -1 = descending
        DBObject limit = new BasicDBObject("$limit", numConsequenceTypes);

        return coll.aggregate(/*"$effects.so", */Arrays.asList(match, project, unwind, group, sort, limit), options);
    }

    @Override
    public VariantSourceDBAdaptor getVariantSourceDBAdaptor() {
        return variantSourceMongoDBAdaptor;
    }

    @Override
    public VariantDBIterator iterator() {
        MongoDBCollection coll = db.getCollection(collectionName);

        DBCursor dbCursor = coll.nativeQuery().find(new BasicDBObject(), new QueryOptions());
        return new VariantMongoDBIterator(dbCursor, variantConverter);
    }

    @Override
    public VariantDBIterator iterator(QueryOptions options) {
        MongoDBCollection coll = db.getCollection(collectionName);

        QueryBuilder qb = QueryBuilder.start();
        parseQueryOptions(options, qb);
        DBObject projection = parseProjectionQueryOptions(options);
        DBCursor dbCursor = coll.nativeQuery().find(qb.get(), projection, options);
        return new VariantMongoDBIterator(dbCursor, variantConverter);
    }

    @Override
    public QueryResult updateAnnotations(List<VariantAnnotation> variantAnnotations, QueryOptions queryOptions) {

        DBCollection coll = db.getDb().getCollection(collectionName);
        BulkWriteOperation builder = coll.initializeUnorderedBulkOperation();

        long start = System.nanoTime();
        for (VariantAnnotation variantAnnotation : variantAnnotations) {
            String id = variantConverter.buildStorageId(variantAnnotation.getChromosome(), variantAnnotation.getStart(),
                    variantAnnotation.getReferenceAllele(), variantAnnotation.getAlternativeAllele());
            DBObject find = new BasicDBObject("_id", id);
            DBObjectToVariantAnnotationConverter converter = new DBObjectToVariantAnnotationConverter();
            DBObject convertedVariantAnnotation = converter.convertToStorageType(variantAnnotation);
//            System.out.println("convertedVariantAnnotation = " + convertedVariantAnnotation);
            DBObject update = new BasicDBObject("$set", new BasicDBObject(DBObjectToVariantConverter.ANNOTATION_FIELD,
                    convertedVariantAnnotation));
            builder.find(find).updateOne(update);
        }

        BulkWriteResult writeResult = builder.execute();

        return new QueryResult<>("", ((int) (System.nanoTime() - start)), 1, 1, "", "", Collections.singletonList(writeResult));
    }

    @Override
    public QueryResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, QueryOptions queryOptions) {
        DBCollection coll = db.getDb().getCollection(collectionName);
        BulkWriteOperation builder = coll.initializeUnorderedBulkOperation();

        long start = System.nanoTime();
        DBObjectToVariantStatsConverter statsConverter = new DBObjectToVariantStatsConverter();
        VariantSource variantSource = queryOptions.get(VariantStorageManager.VARIANT_SOURCE, VariantSource.class);
        // TODO make unset of 'st' if already present?
        for (VariantStatsWrapper wrapper : variantStatsWrappers) {
            VariantStats variantStats = null;
            Map<String, VariantStats> cohortStats = wrapper.getCohortStats();
            List<DBObject> cohortsStats = new LinkedList<>();
            for (Map.Entry<String, VariantStats> variantStatsEntry : cohortStats.entrySet()) {
                variantStats = variantStatsEntry.getValue();
                DBObject variantStatsDBObject = statsConverter.convertToStorageType(variantStats);
                variantStatsDBObject.put(DBObjectToVariantStatsConverter.COHORT_ID, variantStatsEntry.getKey());
                cohortsStats.add(variantStatsDBObject);
            }
            if (variantStats != null) {
                String id = variantConverter.buildStorageId(wrapper.getChromosome(), wrapper.getPosition(),
                        variantStats.getRefAllele(), variantStats.getAltAllele());


                DBObject find = new BasicDBObject("_id", id)
                        .append(DBObjectToVariantConverter.FILES_FIELD + "." + DBObjectToVariantSourceEntryConverter.STUDYID_FIELD
                                , variantSource.getStudyId())
                        .append(DBObjectToVariantConverter.FILES_FIELD + "." + DBObjectToVariantSourceEntryConverter.FILEID_FIELD
                                , variantSource.getFileId());

                DBObject update = new BasicDBObject("$set", new BasicDBObject(
                        DBObjectToVariantConverter.FILES_FIELD + ".$." + DBObjectToVariantSourceConverter.STATS_FIELD
                        , cohortsStats));

                builder.find(find).updateOne(update);
            }
        }

        // TODO handle if the variant didn't had that studyId in the files array
        // TODO check the substitution is done right if the stats are already present
        BulkWriteResult writeResult = builder.execute();
        int writes = writeResult.getModifiedCount();

        return new QueryResult<>("", ((int) (System.nanoTime() - start)), writes, writes, "", "", Collections.singletonList(writeResult));
    }

    @Override
    public boolean close() {
        mongoManager.close(db.getDatabaseName());
        return true;
    }

    private QueryBuilder parseQueryOptions(QueryOptions options, QueryBuilder builder) {
        if (options != null) {


            /** GENOMIC REGION **/

            if (options.getString(ID) != null && !options.getString(ID).isEmpty()) { //) && !options.getString("id").isEmpty()) {
                List<String> ids = getStringList(options.get(ID));
                addQueryListFilter(DBObjectToVariantConverter.ANNOTATION_FIELD + "." +
                        DBObjectToVariantAnnotationConverter.XREFS_FIELD + "." +
                        DBObjectToVariantAnnotationConverter.XREF_ID_FIELD
                        , ids, builder, QueryOperation.OR);

                addQueryListFilter(DBObjectToVariantConverter.ID_FIELD
                        , ids, builder, QueryOperation.OR);
            }

            if (options.containsKey(REGION) && !options.getString(REGION).isEmpty()) {
//                getRegionFilter(Region.parseRegion(options.getString("region")), builder);
                List<String> stringList = getStringList(options.get(REGION));

                DBObject[] objects = new DBObject[stringList.size()];
                int i = 0;
                for (String reg : stringList) {
                    Region region = Region.parseRegion(reg);
                    List<String> chunkIds = getChunkIds(region);
                    DBObject regionObject = new BasicDBObject("_at.chunkIds", new BasicDBObject("$in", chunkIds))
                            .append(DBObjectToVariantConverter.END_FIELD, new BasicDBObject("$gte", region.getStart()))
                            .append(DBObjectToVariantConverter.START_FIELD, new BasicDBObject("$lte", region.getEnd()));
                    objects[i] = regionObject;
                    i++;
                }
                builder.or(objects);

            }

            if (options.containsKey(GENE)) {
                List<String> xrefs = getStringList(options.get(GENE));
                addQueryListFilter(DBObjectToVariantConverter.ANNOTATION_FIELD + "." +
                        DBObjectToVariantAnnotationConverter.XREFS_FIELD + "." +
                        DBObjectToVariantAnnotationConverter.XREF_ID_FIELD
                        , xrefs, builder, QueryOperation.OR);
            }

            if (options.containsKey(CHROMOSOME)) {
                List<String> chromosome = getStringList(options.get(CHROMOSOME));
                addQueryListFilter(DBObjectToVariantConverter.CHROMOSOME_FIELD
                        , chromosome, builder, QueryOperation.OR);
            }

            /** VARIANT **/

            if (options.containsKey(TYPE)) { // && !options.getString("type").isEmpty()) {
                addQueryStringFilter(DBObjectToVariantConverter.TYPE_FIELD, options.getString(TYPE), builder);
            }

            if (options.containsKey(REFERENCE) && options.getString(REFERENCE) != null) {
                addQueryStringFilter(DBObjectToVariantConverter.REFERENCE_FIELD, options.getString(REFERENCE), builder);
            }

            if (options.containsKey(ALTERNATE) && options.getString(ALTERNATE) != null) {
                addQueryStringFilter(DBObjectToVariantConverter.ALTERNATE_FIELD, options.getString(ALTERNATE), builder);
            }

            /** ANNOTATION **/

            if (options.containsKey(ANNOTATION_EXISTS)) {
                builder.and(DBObjectToVariantConverter.ANNOTATION_FIELD).exists(options.getBoolean(ANNOTATION_EXISTS));
            }

            if (options.containsKey(ANNOT_XREF)) {
                List<String> xrefs = getStringList(options.get(ANNOT_XREF));
                addQueryListFilter(DBObjectToVariantConverter.ANNOTATION_FIELD + "." +
                        DBObjectToVariantAnnotationConverter.XREFS_FIELD + "." +
                        DBObjectToVariantAnnotationConverter.XREF_ID_FIELD
                        , xrefs, builder, QueryOperation.AND);
            }

            if (options.containsKey(ANNOT_CONSEQUENCE_TYPE)) {
//                List<Integer> cts = getIntegersList(options.get(ANNOT_CONSEQUENCE_TYPE));
                List<String> cts = getStringList(options.get(ANNOT_CONSEQUENCE_TYPE));
                List<Integer> ctsInteger = new ArrayList<>(cts.size());
                for (String ct : cts) {
                    if(ct.startsWith("SO:")) {
                        ct = ct.substring(3);
                    }
                    try {
                        ctsInteger.add(Integer.parseInt(ct));
                    } catch (NumberFormatException e) {
                        logger.error("Error parsing integer ", e);
                    }
                }
                addQueryListFilter(DBObjectToVariantConverter.ANNOTATION_FIELD + "." +
                        DBObjectToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD + "." +
                        DBObjectToVariantAnnotationConverter.SO_ACCESSION_FIELD
                        , ctsInteger, builder, QueryOperation.AND);
            }

            if (options.containsKey(ANNOT_BIOTYPE)) {
                List<String> biotypes = getStringList(options.get(ANNOT_BIOTYPE));
                addQueryListFilter(DBObjectToVariantConverter.ANNOTATION_FIELD + "." +
                        DBObjectToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD + "." +
                        DBObjectToVariantAnnotationConverter.BIOTYPE_FIELD
                        , biotypes, builder, QueryOperation.AND);
            }

            if (options.containsKey(POLYPHEN)) {
                addCompQueryFilter(DBObjectToVariantConverter.ANNOTATION_FIELD + "." +
                        DBObjectToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD + "." +
                        DBObjectToVariantAnnotationConverter.POLYPHEN_FIELD + "." +
                        DBObjectToVariantAnnotationConverter.SCORE_SCORE_FIELD, options.getString(POLYPHEN), builder);
            }

            if (options.containsKey(SIFT)) {
                addCompQueryFilter(DBObjectToVariantConverter.ANNOTATION_FIELD + "." +
                        DBObjectToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD + "." +
                        DBObjectToVariantAnnotationConverter.SIFT_FIELD + "." +
                        DBObjectToVariantAnnotationConverter.SCORE_SCORE_FIELD, options.getString(SIFT), builder);
            }

            if (options.containsKey(PROTEIN_SUBSTITUTION)) {
                List<String> list = getStringList(options.get(PROTEIN_SUBSTITUTION));
                addScoreFilter(DBObjectToVariantConverter.ANNOTATION_FIELD + "." +
                        DBObjectToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD + "." +
                        DBObjectToVariantAnnotationConverter.PROTEIN_SUBSTITUTION_SCORE_FIELD, list, builder);
            }

            if (options.containsKey(CONSERVED_REGION)) {
                List<String> list = getStringList(options.get(CONSERVED_REGION));
                addScoreFilter(DBObjectToVariantConverter.ANNOTATION_FIELD + "." +
                        DBObjectToVariantAnnotationConverter.CONSERVED_REGION_SCORE_FIELD, list, builder);
            }


            /** FILES **/
            QueryBuilder fileBuilder = QueryBuilder.start();

            if (options.containsKey(STUDIES)) { // && !options.getList("studies").isEmpty() && !options.getListAs("studies", String.class).get(0).isEmpty()) {
                addQueryListFilter(
                        DBObjectToVariantSourceEntryConverter.STUDYID_FIELD, getStringList(options.get(STUDIES)),
                        fileBuilder, QueryOperation.AND);
            }

            if (options.containsKey(FILES)) { // && !options.getList("files").isEmpty() && !options.getListAs("files", String.class).get(0).isEmpty()) {
                addQueryListFilter(
                        DBObjectToVariantSourceConverter.FILEID_FIELD, getStringList(options.get(FILES)),
                                fileBuilder, QueryOperation.AND);
            }

            if (options.get(MAF) != null && !options.getString(MAF).isEmpty()) {
                addCompQueryFilter(
                        DBObjectToVariantSourceEntryConverter.STATS_FIELD + "." + DBObjectToVariantStatsConverter.MAF_FIELD,
                        options.getString(MAF), fileBuilder);
            }

            if (options.get(MGF) != null && !options.getString(MGF).isEmpty()) {
                addCompQueryFilter(
                        DBObjectToVariantSourceEntryConverter.STATS_FIELD + "." + DBObjectToVariantStatsConverter.MGF_FIELD,
                        options.getString(MGF), fileBuilder);
            }

            if (options.get(MISSING_ALLELES) != null && !options.getString(MISSING_ALLELES).isEmpty()) {
                addCompQueryFilter(
                        DBObjectToVariantSourceEntryConverter.STATS_FIELD + "." + DBObjectToVariantStatsConverter.MISSALLELE_FIELD,
                        options.getString(MISSING_ALLELES), fileBuilder);
            }

            if (options.get(MISSING_GENOTYPES) != null && !options.getString(MISSING_GENOTYPES).isEmpty()) {
                addCompQueryFilter(
                        DBObjectToVariantSourceEntryConverter.STATS_FIELD + "." + DBObjectToVariantStatsConverter.MISSGENOTYPE_FIELD,
                        options.getString(MISSING_GENOTYPES), fileBuilder);
            }

            if (options.get("numgt") != null && !options.getString("numgt").isEmpty()) {
                for (String numgt : getStringList(options.get("numgt"))) {
                    String[] split = numgt.split(":");
                    addCompQueryFilter(
                            DBObjectToVariantSourceEntryConverter.STATS_FIELD + "." + DBObjectToVariantStatsConverter.NUMGT_FIELD + "." + split[0],
                            split[1], fileBuilder);
                }
            }

//            if (options.get("freqgt") != null && !options.getString("freqgt").isEmpty()) {
//                for (String freqgt : getStringList(options.get("freqgt"))) {
//                    String[] split = freqgt.split(":");
//                    addCompQueryFilter(
//                            DBObjectToVariantSourceEntryConverter.STATS_FIELD + "." + DBObjectToVariantStatsConverter.FREQGT_FIELD + "." + split[0],
//                            split[1], fileBuilder);
//                }
//            }



            if (options.containsKey(GENOTYPE)) {
                String sampleGenotypesCSV = options.getString(GENOTYPE);

//                String AND = ",";
//                String OR = ";";
//                String IS = ":";

//                String AND = "AND";
//                String OR = "OR";
//                String IS = ":";

                String AND = ";";
                String OR = ",";
                String IS = ":";

                String[] sampleGenotypesArray = sampleGenotypesCSV.split(AND);
                for (String sampleGenotypes : sampleGenotypesArray) {
                    String[] sampleGenotype = sampleGenotypes.split(IS);
                    if(sampleGenotype.length != 2) {
                        continue;
                    }
                    int sample = Integer.parseInt(sampleGenotype[0]);
                    String[] genotypes = sampleGenotype[1].split(OR);
                    QueryBuilder genotypesBuilder = QueryBuilder.start();
                    for (String genotype : genotypes) {
                        String s = DBObjectToVariantSourceEntryConverter.SAMPLES_FIELD + "." + genotype;
                        //or [ {"samp.0|0" : { $elemMatch : { $eq : <sampleId> } } } ]
                        genotypesBuilder.or(new BasicDBObject(s, new BasicDBObject("$elemMatch", new BasicDBObject("$eq", sample))));
                    }
                    fileBuilder.and(genotypesBuilder.get());
                }
            }

            DBObject fileQuery = fileBuilder.get();
            if (fileQuery.keySet().size() != 0) {
                builder.and(DBObjectToVariantConverter.FILES_FIELD).elemMatch(fileQuery);
            }
        }

        logger.debug("Find = " + builder.get());
        return builder;
    }

    /**
     * when the tags "include" or "exclude" The names are the same as the members of Variant.
     * @param options
     * @return
     */
    private DBObject parseProjectionQueryOptions(QueryOptions options) {
        DBObject projection = new BasicDBObject();

        if(options == null) {
            return projection;
        }

        List<String> includeList = getStringList(options.get("include"));
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
            for (String values : DBObjectToVariantConverter.fieldsMap.values()) {
                projection.put(values, 1);
            }
            if (options.containsKey("exclude")) { // Exclude some
                List<String> excludeList = getStringList(options.get("exclude"));
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

        if (options.containsKey(FILE_ID) && projection.containsField(DBObjectToVariantConverter.FILES_FIELD)) {
//            List<String> files = options.getListAs(FILES, String.class);
            String file = options.getString(FILE_ID);
            projection.put(
                    DBObjectToVariantConverter.FILES_FIELD,
                    new BasicDBObject(
                            "$elemMatch",
                            new BasicDBObject(
                                    DBObjectToVariantSourceEntryConverter.FILEID_FIELD,
                                    file
//                                    new BasicDBObject(
//                                            "$in",
//                                            files
//                                    )
                            )
                    )
            );
        }

        logger.debug("Projection: {}", projection);
        return projection;
    }

    private enum QueryOperation {
        AND, OR
    }

    private QueryBuilder addQueryStringFilter(String key, String value, QueryBuilder builder) {
        if(value != null && !value.isEmpty()) {
            if(value.indexOf(",") == -1) {
                builder.and(key).is(value);
            }else {
                String[] values = value.split(",");
                builder.and(key).in(values);
            }
        }
        return builder;
    }

    private QueryBuilder addQueryListFilter(String key, List<?> values, QueryBuilder builder, QueryOperation op) {
        if (values != null)
            if (values.size() == 1) {
                if(op == QueryOperation.AND) {
                    builder.and(key).is(values.get(0));
                } else {
                    builder.or(QueryBuilder.start(key).is(values.get(0)).get());
                }
            } else if (!values.isEmpty()) {
                if(op == QueryOperation.AND) {
                    builder.and(key).in(values);
                } else {
                    builder.or(QueryBuilder.start(key).in(values).get());
                }
            }
        return builder;
    }

    private QueryBuilder addCompQueryFilter(String key, String value, QueryBuilder builder) {
        String op = value.substring(0, 2);
        op = op.replaceFirst("[0-9]", "");
        String obj = value.replaceFirst(op, "");

        switch(op) {
            case "<":
                builder.and(key).lessThan(Float.parseFloat(obj));
                break;
            case "<=":
                builder.and(key).lessThanEquals(Float.parseFloat(obj));
                break;
            case ">":
                builder.and(key).greaterThan(Float.parseFloat(obj));
                break;
            case ">=":
                builder.and(key).greaterThanEquals(Float.parseFloat(obj));
                break;
            case "=":
            case "==":
                builder.and(key).is(Float.parseFloat(obj));
                break;
            case "!=":
                builder.and(key).notEquals(Float.parseFloat(obj));
                break;
            case "~=":
                builder.and(key).regex(Pattern.compile(obj));
                break;
        }
        return builder;
    }

    private QueryBuilder addScoreFilter(String key, List<String> list, QueryBuilder builder) {
//        ArrayList<DBObject> and = new ArrayList<>(list.size());
//        DBObject[] ands = new DBObject[list.size()];
        List<DBObject> ands = new ArrayList<>();
        for (String elem : list) {
            String[] split = elem.split(":");
            if (split.length == 2) {
                String source = split[0];
                String score = split[1];
                QueryBuilder scoreBuilder = new QueryBuilder();
                scoreBuilder.and(DBObjectToVariantAnnotationConverter.SCORE_SOURCE_FIELD).is(source);
                addCompQueryFilter(DBObjectToVariantAnnotationConverter.SCORE_SCORE_FIELD
                        , score, scoreBuilder);
//                builder.and(key).elemMatch(scoreBuilder.get());
                ands.add(new BasicDBObject(key, new BasicDBObject("$elemMatch", scoreBuilder.get())));
            } else {
                logger.error("Bad score filter: " + elem);
            }
        }
        if (!ands.isEmpty()) {
            builder.and(ands.toArray(new DBObject[ands.size()]));
        }
        return builder;
    }

    private QueryBuilder getRegionFilter(Region region, QueryBuilder builder) {
        List<String> chunkIds = getChunkIds(region);
        builder.and("_at.chunkIds").in(chunkIds);
        builder.and(DBObjectToVariantConverter.END_FIELD).greaterThanEquals(region.getStart());
        builder.and(DBObjectToVariantConverter.START_FIELD).lessThanEquals(region.getEnd());
        return builder;
    }

    private QueryBuilder getRegionFilter(List<Region> regions, QueryBuilder builder) {
        DBObject[] objects = new DBObject[regions.size()];

        int i = 0;
        for (Region region : regions) {
            List<String> chunkIds = getChunkIds(region);
            DBObject regionObject = new BasicDBObject("_at.chunkIds", new BasicDBObject("$in", chunkIds))
                    .append(DBObjectToVariantConverter.END_FIELD, new BasicDBObject("$gte", region.getStart()))
                    .append(DBObjectToVariantConverter.START_FIELD, new BasicDBObject("$lte", region.getEnd()));
            objects[i] = regionObject;
            i++;
        }
        builder.or(objects);
        return builder;
    }

    /* *******************
     * Auxiliary methods *
     * *******************/

    private List<String> getChunkIds(Region region) {
        List<String> chunkIds = new LinkedList<>();

        int chunkSize = (region.getEnd() - region.getStart() > VariantMongoDBWriter.CHUNK_SIZE_BIG) ?
                VariantMongoDBWriter.CHUNK_SIZE_BIG : VariantMongoDBWriter.CHUNK_SIZE_SMALL;
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


    private List<String> getStringList(Object value) {
        List<String> stringList;
        List list;
        list = getList(value);
        stringList = new LinkedList<>();
        for (Object o : list) {
            stringList.add(o.toString());
        }
        return stringList;
    }

    private List<Integer> getIntegersList(Object value) {
        List<Integer> integerList;
        integerList = new LinkedList<>();
        List list = getList(value);
        for (Object o : list) {
            int i;
            if (o instanceof Integer) {
                i = (int) o;
            } else {
                i = Integer.parseInt(o.toString());
            }
            integerList.add(i);
        }
        return integerList;
    }

    private List getList(Object value) {
        return getList(value, ",");
    }

    private List getList(Object value, String separator) {
        List list;
        if (value instanceof List) {
            list = (List) value;
        } else if (value == null) {
            return Collections.emptyList();
        } else {
            list = Arrays.asList(value.toString().split(separator));
        }
        return list;
    }

//    private QueryBuilder getReferenceFilter(String reference, QueryBuilder builder) {
//        return builder.and(DBObjectToVariantConverter.REFERENCE_FIELD).is(reference);
//    }
//
//    private QueryBuilder getAlternateFilter(String alternate, QueryBuilder builder) {
//        return builder.and(DBObjectToVariantConverter.ALTERNATE_FIELD).is(alternate);
//    }
//
//    private QueryBuilder getVariantTypeFilter(String type, QueryBuilder builder) {
//        return builder.and(DBObjectToVariantConverter.TYPE_FIELD).is(type.toUpperCase());
//    }
//
//    private QueryBuilder getEffectFilter(List<String> effects, QueryBuilder builder) {
//        return builder.and(DBObjectToVariantConverter.EFFECTS_FIELD + "." + DBObjectToVariantConverter.SOTERM_FIELD).in(effects);
//    }
//
//    private QueryBuilder getStudyFilter(List<String> studies, QueryBuilder builder) {
//        return builder.and(DBObjectToVariantConverter.FILES_FIELD + "." + DBObjectToArchivedVariantFileConverter.STUDYID_FIELD).in(studies);
//    }
//
//    private QueryBuilder getFileFilter(List<String> files, QueryBuilder builder) {
//        return builder.and(DBObjectToVariantConverter.FILES_FIELD + "." + DBObjectToVariantSourceConverter.FILEID_FIELD).in(files);
//    }
//
//    private QueryBuilder getMafFilter(float maf, ComparisonOperator op, QueryBuilder builder) {
//        return op.apply(DBObjectToVariantConverter.FILES_FIELD + "." + DBObjectToArchivedVariantFileConverter.STATS_FIELD
//                + "." + DBObjectToVariantStatsConverter.MAF_FIELD, maf, builder);
//    }
//
//    private QueryBuilder getMissingAllelesFilter(int missingAlleles, ComparisonOperator op, QueryBuilder builder) {
//        return op.apply(DBObjectToVariantConverter.FILES_FIELD + "." + DBObjectToArchivedVariantFileConverter.STATS_FIELD
//                + "." + DBObjectToVariantStatsConverter.MISSALLELE_FIELD, missingAlleles, builder);
//    }
//
//    private QueryBuilder getMissingGenotypesFilter(int missingGenotypes, ComparisonOperator op, QueryBuilder builder) {
//        return op.apply(DBObjectToVariantConverter.FILES_FIELD + "." + DBObjectToArchivedVariantFileConverter.STATS_FIELD
//                + "." + DBObjectToVariantStatsConverter.MISSGENOTYPE_FIELD, missingGenotypes, builder);
//    }


    /* *******************
     *  Auxiliary types  *
     * *******************/

//    private enum ComparisonOperator {
//        LT("<") {
//            @Override
//            QueryBuilder apply(String key, Object value, QueryBuilder builder) {
//                return builder.and(key).lessThan(value);
//            }
//        },
//
//        LTE("<=") {
//            @Override
//            QueryBuilder apply(String key, Object value, QueryBuilder builder) {
//                return builder.and(key).lessThanEquals(value);
//            }
//        },
//
//        GT(">") {
//            @Override
//            QueryBuilder apply(String key, Object value, QueryBuilder builder) {
//                return builder.and(key).greaterThan(value);
//            }
//        },
//
//        GTE(">=") {
//            @Override
//            QueryBuilder apply(String key, Object value, QueryBuilder builder) {
//                return builder.and(key).greaterThanEquals(value);
//            }
//        },
//
//        EQ("=") {
//            @Override
//            QueryBuilder apply(String key, Object value, QueryBuilder builder) {
//                return builder.and(key).is(value);
//            }
//        },
//
//        NEQ("=/=") {
//            @Override
//            QueryBuilder apply(String key, Object value, QueryBuilder builder) {
//                return builder.and(key).notEquals(value);
//            }
//        };
//
//        private final String symbol;
//
//        private ComparisonOperator(String symbol) {
//            this.symbol = symbol;
//        }
//
//        @Override
//        public String toString() {
//            return symbol;
//        }
//
//        abstract QueryBuilder apply(String key, Object value, QueryBuilder builder);
//
//        // Returns Operation for string, or null if string is invalid
//        private static final Map<String, ComparisonOperator> stringToEnum = new HashMap<>();
//        static { // Initialize map from constant name to enum constant
//            for (ComparisonOperator op : values()) {
//                stringToEnum.put(op.toString(), op);
//            }
//        }
//
//        public static ComparisonOperator fromString(String symbol) {
//            return stringToEnum.get(symbol);
//        }
//
//    }


//    @Override
//    public List<VariantStats> getRecordsStats(Map<String, String> options) {
//        return null;
//    }
//
//    @Override
//    public List<VariantEffect> getEffect(Map<String, String> options) {
//        return null;
//    }
//
//    @Override
//    public VariantAnalysisInfo getAnalysisInfo(Map<String, String> options) {
//        return null;
//    }
//
//    private String buildRowkey(String chromosome, String position) {
//        if (chromosome.length() > 2) {
//            if (chromosome.substring(0, 2).equals("chr")) {
//                chromosome = chromosome.substring(2);
//            }
//        }
//        if (chromosome.length() < 2) {
//            chromosome = "0" + chromosome;
//        }
//        if (position.length() < 10) {
//            while (position.length() < 10) {
//                position = "0" + position;
//            }
//        }
//        return chromosome + "_" + position;
//    }
//
//    @Override
//    public List<VariantInfo> getRecords(Map<String, String> options) {
//        return null;
//    }

//    public QueryResult<VariantInfo> getRecordsMongo(int page, int start, int limit, MutableInt count, Map<String, String> options) {
//
//        long startTime = System.currentTimeMillis();
//        QueryResult<VariantInfo> queryResult = new QueryResult<>();
//
//        List<VariantInfo> res = new ArrayList<>();
//        String studyId = options.get("studyId");
//        DBCollection coll = db.getCollection("variants");
//
//        DBObject elemMatch = new BasicDBObject("studyId", studyId);
//        DBObject query = new BasicDBObject();
//        BasicDBList orList = new BasicDBList();
//
//        Map<String, List<String>> sampleGenotypes = processSamplesGT(options);
//
//        System.out.println("map = " + options);
//
//        if (options.containsKey("region_list") && !options.get("region_list").equals("")) {
//            String[] regions = options.get("region_list").split(",");
//            Pattern pattern = Pattern.compile("(\\w+):(\\d+)-(\\d+)");
//            Matcher matcher, matcherChr;
//
//            for (int i = 0; i < regions.length; i++) {
//                String region = regions[i];
//                matcher = pattern.matcher(region);
//                if (matcher.find()) {
//                    String chr = matcher.group(1);
//                    int s = Integer.valueOf(matcher.group(2));
//                    int e = Integer.valueOf(matcher.group(3));
//
//                    DBObject regionClause = new BasicDBObject("chr", chr);
//                    regionClause.put("pos", new BasicDBObject("$gte", s).append("$lte", e));
//                    orList.add(regionClause);
//                } else {
//
//                    Pattern patternChr = Pattern.compile("(\\w+)");
//                    matcherChr = patternChr.matcher(region);
//
//                    if (matcherChr.find()) {
//                        String chr = matcherChr.group();
//                        DBObject regionClause = new BasicDBObject("chr", chr);
//                        orList.add(regionClause);
//                    }
//
//                }
//            }
//            query.put("$or", orList);
//
//        } else if (options.containsKey("genes") && !options.get("genes").equals("")) {
//            orList = processGeneList(options.get("genes"));
//            if (orList.ks() > 0) {
//                query.put("$or", orList);
//            } else {
//                queryResult.setWarningMsg("Wrong gene name");
//                queryResult.setResult(res);
//                queryResult.setNumResults(res.ks());
//                return queryResult;
//            }
//        }
//
//        if (options.containsKey("conseq_type") && !options.get("conseq_type").equals("")) {
//            String[] cts = options.get("conseq_type").split(",");
//
//            BasicDBList ctList = new BasicDBList();
//            for (String ct : cts) {
//                ctList.add(ct);
//            }
//            elemMatch.put("effects", new BasicDBObject("$in", ctList));
//        }
//
//        if (sampleGenotypes.ks() > 0) {
//            for (Map.Entry<String, List<String>> entry : sampleGenotypes.entrySet()) {
//                BasicDBList gtList = new BasicDBList();
//                for (String gt : entry.getValue()) {
//                    gtList.add(gt);
//                }
//                elemMatch.put("samples." + entry.getKey() + ".GT", new BasicDBObject("$in", gtList));
//            }
//        }
//
//        if (options.containsKey("miss_gt") && !options.get("miss_gt").equalsIgnoreCase("")) {
//            Integer val = Integer.valueOf(options.get("miss_gt"));
//            Object missGt = getMongoOption(options.get("option_miss_gt"), val);
//            elemMatch.put("stats.missGenotypes", missGt);
//        }
//
//        if (options.containsKey("maf_1000g_controls") && !options.get("maf_1000g_controls").equalsIgnoreCase("")) {
//            elemMatch.put("attributes.1000G_maf", new BasicDBObject("$lte", options.get("maf_1000g_controls")));
//        }
//
//        query.put("studies", new BasicDBObject("$elemMatch", elemMatch));
//
//        System.out.println("#############################");
//        System.out.println(query);
//        System.out.println("#############################");
//
//        long dbStart = System.currentTimeMillis();
//
//        DBObject sort = null;
//        DBCursor cursor;
//
//        if (options.containsKey("sort")) {
//            sort = getQuerySort(options.get("sort"));
//
//            System.out.println(sort);
//
//            cursor = coll.find(query).sort(sort).skip(start).limit(limit);
//        } else {
//            cursor = coll.find(query).skip(start).limit(limit);
//        }
//
//        count.setValue(cursor.count());
//
//        queryResult.setDbTime(dbStart - System.currentTimeMillis());
//
//        for (DBObject obj : cursor) {
//
//            BasicDBObject elem = (BasicDBObject) obj;
//            VariantInfo vi = new VariantInfo();
//            VariantStats vs = new VariantStats();
//
//            String chr = elem.getString("chr");
//            int pos = elem.getInt("pos");
//
//            vi.setChromosome(chr);
//            vi.setPosition(pos);
//
//            BasicDBList studies = (BasicDBList) elem.get("studies");
//
//            Iterator<Object> it = studies.iterator();
//            while (it.hasNext()) {
//                BasicDBObject study = (BasicDBObject) it.next();
//
//                if (study.getString("studyId").equalsIgnoreCase(studyId)) {
//
//                    BasicDBObject stats = (BasicDBObject) study.get("stats");
//
//                    String ref = study.getString("ref");
//                    BasicDBList alt = (BasicDBList) study.get("alt");
//                    vi.setRef(ref);
//                    vi.setAlt(Joiner.on(",").join(alt.toArray()));
//                    vs.setMaf((float) stats.getDouble("maf"));
//                    vs.setMgf((float) stats.getDouble("mgf"));
//                    vs.setMafAllele(stats.getString("alleleMaf"));
//                    vs.setMgfGenotype(stats.getString("genotypeMaf"));
//                    vs.setMissingAlleles(stats.getInt("missAllele"));
//                    vs.setMissingGenotypes(stats.getInt("missGenotypes"));
//                    vs.setMendelianErrors(stats.getInt("mendelErr"));
//                    vs.setCasesPercentDominant((float) stats.getDouble("casesPercentDominant"));
//                    vs.setControlsPercentDominant((float) stats.getDouble("controlsPercentDominant"));
//                    vs.setCasesPercentRecessive((float) stats.getDouble("casesPercentRecessive"));
//                    vs.setControlsPercentRecessive((float) stats.getDouble("controlsPercentRecessive"));
//
//                    BasicDBObject samples = (BasicDBObject) study.get("samples");
//
//                    for (String sampleName : samples.keySet()) {
//
//                        DBObject sample = (DBObject) samples.get(sampleName);
//
//                        if (sample.containsField("GT")) {
//                            String sampleGT = (String) sample.get("GT");
//                            vi.addSammpleGenotype(sampleName, sampleGT);
//                        }
//
//                    }
//
//                    vi.setSnpid((String) study.get("snpId"));
//
//                    if (study.containsField("effects")) {
//                        BasicDBList conseqTypes = (BasicDBList) study.get("effects");
//                        conseqTypes.remove("");
//                        String cts = Joiner.on(",").join(conseqTypes.iterator());
//                        vi.addConsequenceTypes(cts);
//                    }
//
//                    if (study.containsField("genes")) {
//                        BasicDBList genesList = (BasicDBList) study.get("genes");
//                        String genes = Joiner.on(",").join(genesList.iterator());
//                        vi.addGenes(genes);
//                    }
//
//                    if (study.containsField("attributes")) {
//
//                        BasicDBObject attr = (BasicDBObject) study.get("attributes");
//
//                        if (attr.containsField("1000G_maf")) {
//                            vi.addControl("1000G_maf", (String) attr.get("1000G_maf"));
//                            vi.addControl("1000G_amaf", (String) attr.get("1000G_amaf"));
//                            vi.addControl("1000G_gt", (String) attr.get("1000G_gt"));
//                        }
//
//                        if (attr.containsField("EVS_maf")) {
//                            vi.addControl("EVS_maf", (String) attr.get("EVS_maf"));
//                            vi.addControl("EVS_amaf", (String) attr.get("EVS_amaf"));
//                            vi.addControl("EVS_gt", (String) attr.get("EVS_gt"));
//                        }
//
//                    }
//                    continue;
//                }
//            }
//            vi.addStats(vs);
//            res.add(vi);
//        }
//
//        queryResult.setResult(res);
//        queryResult.setTime(startTime - System.currentTimeMillis());
//
//        return queryResult;
//    }
//
//    private DBObject getQuerySort(String sort) {
//
//        DBObject res = new BasicDBObject();
//
//        //  sort=[{"property":"stats_id_snp","direction":"ASC"}],
////        Pattern pattern = Pattern.compile("(\\w+):(\\d+)-(\\d+)");
//        Pattern pattern = Pattern.compile("\"property\":\"(\\w+)\",\"direction\":\"(ASC|DESC)\"");
//        Matcher matcher = pattern.matcher(sort);
//        if (matcher.find()) {
//
//            String field = matcher.group(1);
//            String direction = matcher.group(2);
//
//            int dir = 1;
//
//            if (direction.equalsIgnoreCase("ASC")) {
//                dir = 1;
//            } else if (direction.equalsIgnoreCase("DESC")) {
//                dir = -1;
//            }
//
//            switch (field) {
//                case "chromosome":
//                    res.put("chr", dir);
//                    res.put("pos", dir);
//                    break;
//                case "snpid":
//                    res.put("studies.snpId", dir);
//                    break;
//                case "consecuente_types":
//                    res.put("studies.effects", dir);
//                    break;
//                case "genes":
//                    res.put("studies.genes.1", dir);
//                    break;
//            }
//
//        }
//
//        return res;
//    }
//
//    private Object getMongoOption(String option, float val) {
//
//        Object res = null;
//
//        switch (option) {
//            case ("<"):
//                res = new BasicDBObject("$lt", val);
//                break;
//            case ("<="):
//                res = new BasicDBObject("$lte", val);
//                break;
//            case (">"):
//                res = new BasicDBObject("$gt", val);
//                break;
//            case (">="):
//                res = new BasicDBObject("$gte", val);
//                break;
//            case ("="):
//                res = val;
//                break;
//            case ("!="):
//                res = new BasicDBObject("$ne", val);
//                break;
//        }
//
//        return res;
//    }
//
//    private BasicDBList processGeneList(String genes) {
//
//        BasicDBList list = new BasicDBList();
//
//        Client wsRestClient = Client.create();
//        WebResource webResource = wsRestClient.resource("http://ws.bioinfo.cipf.es/cellbase/rest/latest/hsa/feature/gene/");
//
//        ObjectMapper mapper = new ObjectMapper();
//
//        String response = webResource.path(genes).path("info").queryParam("of", "json").get(String.class);
//
//        try {
//            JsonNode actualObj = mapper.readTree(response);
//            Iterator<JsonNode> it = actualObj.iterator();
//            Iterator<JsonNode> aux;
//
//            while (it.hasNext()) {
//                JsonNode node = it.next();
//                if (node.isArray()) {
//
//                    aux = node.iterator();
//                    while (aux.hasNext()) {
//                        JsonNode auxNode = aux.next();
//
//                        DBObject regionClause = new BasicDBObject("chr", auxNode.get("chromosome").asText());
//                        regionClause.put("pos", new BasicDBObject("$gte", auxNode.get("start").asInt()).append("$lte", auxNode.get("end").asInt()));
//                        list.add(regionClause);
//
//                    }
//
//                }
//            }
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        return list;
//
//    }
//
//    private Map<String, List<String>> processSamplesGT(Map<String, String> options) {
//        Map<String, List<String>> samplesGenotypes = new LinkedHashMap<>(10);
//        List<String> genotypesList;
//
//        String key, val;
//        for (Map.Entry<String, String> entry : options.entrySet()) {
//            key = entry.getKey();
//            val = entry.getValue();
//
//            if (key.startsWith("sampleGT_")) {
//                String sampleName = key.replace("sampleGT_", "").replace("[]", "");
//                String[] genotypes = val.split(",");
//
//                if (samplesGenotypes.containsKey(sampleName)) {
//                    genotypesList = samplesGenotypes.get(sampleName);
//                } else {
//
//                    genotypesList = new ArrayList<>();
//                    samplesGenotypes.put(sampleName, genotypesList);
//                }
//
//                for (int i = 0; i < genotypes.length; i++) {
//
//                    genotypesList.add(genotypes[i]);
//                }
//
//            }
//
//        }
//        return samplesGenotypes;
//    }
//
//    public QueryResult<VariantAnalysisInfo> getAnalysisInfo(String studyId) {
//
//        long start = System.currentTimeMillis();
//        QueryResult<VariantAnalysisInfo> qres = new QueryResult<>();
//        VariantAnalysisInfo vi = new VariantAnalysisInfo();
//
//        DBCollection coll = db.getCollection("studies");
//        DBCollection collV = db.getCollection("variants");
//
//        long dbStart = System.currentTimeMillis();
//        DBObject study = coll.findOne(new BasicDBObject("name", studyId));
//
//        if (study != null) {
//            Iterator<Object> it = ((BasicDBList) study.get("samples")).iterator();
//
//            while (it.hasNext()) {
//                vi.addSample((String) it.next());
//            }
//
//            BasicDBObject gs = (BasicDBObject) study.get("globalStats");
//
//            for (String elem : gs.keySet()) {
//                if (!elem.equalsIgnoreCase("consequenceTypes")) {
//                    double val = gs.getDouble(elem);
//                    vi.addGlobalStats(elem, val);
//                } else {
//
//                    BasicDBObject cts = (BasicDBObject) gs.get("consequenceTypes");
//                    for (String ct : cts.keySet()) {
//                        vi.addConsequenceType(ct, cts.getInt(ct));
//                    }
//                }
//            }
//        }
//        qres.setDbTime(System.currentTimeMillis() - dbStart);
//
//        qres.setResult(Arrays.asList(vi));
//
//        qres.setTime(System.currentTimeMillis() - start);
//
//        return qres;
//
//    }

}
