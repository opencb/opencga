package org.opencb.opencga.storage.mongodb.variant;

import com.mongodb.*;
import java.net.UnknownHostException;
import java.util.*;
import java.util.regex.Pattern;

import org.opencb.biodata.models.feature.Region;
import org.opencb.biodata.models.variant.Variant;
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
    private DBObjectToVariantConverter variantConverter;
    private DBObjectToVariantSourceEntryConverter variantSourceEntryConverter;
    private final String collectionName;
    private final VariantSourceMongoDBAdaptor variantSourceMongoDBAdaptor;

    private DataWriter dataWriter;

    protected static Logger logger = LoggerFactory.getLogger(VariantMongoDBAdaptor.class);

    public VariantMongoDBAdaptor(MongoCredentials credentials, String variantsCollectionName, String filesCollectionName)
            throws UnknownHostException {
        // Mongo configuration
        mongoManager = new MongoDataStoreManager(credentials.getDataStoreServerAddresses());
        db = mongoManager.get(credentials.getMongoDbName(), credentials.getMongoDBConfiguration());
        variantSourceMongoDBAdaptor = new VariantSourceMongoDBAdaptor(credentials, filesCollectionName);

        collectionName = variantsCollectionName;
        
        // Converters from DBObject to Java classes
        // TODO Allow to configure depending on the type of study?
        variantSourceEntryConverter = new DBObjectToVariantSourceEntryConverter(
                true,
                new DBObjectToSamplesConverter(credentials, filesCollectionName)
        );
        variantConverter = new DBObjectToVariantConverter(variantSourceEntryConverter, new DBObjectToVariantStatsConverter());
    }


    @Override
    public void setDataWriter(DataWriter dataWriter) {
        this.dataWriter = dataWriter;
    }

    @Override
    public void setConstantSamples(String sourceEntry) {
        List<String> samples = null;
        QueryResult samplesBySource = variantSourceMongoDBAdaptor.getSamplesBySource(sourceEntry, null);    // TODO jmmut: check when we remove fileId
        if(samplesBySource.getResult().isEmpty()) {
            logger.error("setConstantSamples(): couldn't find samples in source {} " + sourceEntry);
        } else {
            samples = (List<String>) samplesBySource.getResult().get(0);
        }
        
        variantSourceEntryConverter = new DBObjectToVariantSourceEntryConverter(
                true,
                new DBObjectToSamplesConverter(samples)
        );
        
        variantConverter = new DBObjectToVariantConverter(variantSourceEntryConverter, new DBObjectToVariantStatsConverter());
    }

    @Override
    public QueryResult<Variant> getAllVariants(QueryOptions options) {
        MongoDBCollection coll = db.getCollection(collectionName);

        QueryBuilder qb = QueryBuilder.start();
        parseQueryOptions(options, qb);
        DBObject projection = parseProjectionQueryOptions(options);
        logger.debug("Query to be executed {}", qb.get().toString());

        return coll.find(qb.get(), projection, variantConverter, options);
    }


    @Override
    public QueryResult<Variant> getVariantById(String id, QueryOptions options) {
        MongoDBCollection coll = db.getCollection(collectionName);

//        BasicDBObject query = new BasicDBObject(DBObjectToVariantConverter.ID_FIELD, id);

        if(options == null) {
            options = new QueryOptions(ID, id);
        } else {
            options.addToListOption(ID, id);
        }

        QueryBuilder qb = QueryBuilder.start();
        parseQueryOptions(options, qb);
        DBObject projection = parseProjectionQueryOptions(options);
        logger.debug("Query to be executed {}", qb.get().toString());

//        return coll.find(query, options, variantConverter);
        QueryResult<Variant> queryResult = coll.find(qb.get(), projection, variantConverter, options);
        queryResult.setId(id);
        return queryResult;
    }

    @Override
    public List<QueryResult<Variant>> getAllVariantsByIdList(List<String> idList, QueryOptions options) {
        List<QueryResult<Variant>> allResults = new ArrayList<>(idList.size());
        for (String r : idList) {
            QueryResult<Variant> queryResult = getVariantById(r, options);
            allResults.add(queryResult);
        }
        return allResults;
    }


    @Override
    public QueryResult<Variant> getAllVariantsByRegion(Region region, QueryOptions options) {
        MongoDBCollection coll = db.getCollection(collectionName);

        QueryBuilder qb = QueryBuilder.start();
        getRegionFilter(region, qb);
        parseQueryOptions(options, qb);
        DBObject projection = parseProjectionQueryOptions(options);

        if (options == null) {
            options = new QueryOptions();
        }
        options.add("sort", new BasicDBObject("chr", 1).append("start", 1));
        QueryResult<Variant> queryResult = coll.find(qb.get(), projection, variantConverter, options);
        queryResult.setId(region.toString());
        return queryResult;
    }

    @Override
    public List<QueryResult<Variant>> getAllVariantsByRegionList(List<Region> regionList, QueryOptions options) {
        List<QueryResult<Variant>> allResults;
        if (options == null) {
            options = new QueryOptions();
        }
        // If the user asks to merge the results, run only one query,
        // otherwise delegate in the method to query regions one by one
        if (options.getBoolean(MERGE, false)) {
            options.add(REGION, regionList);
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

        QueryBuilder qb = QueryBuilder.start();
        if(options == null) {
            options = new QueryOptions(GENE, geneName);
        } else {
            options.addToListOption(GENE, geneName);
        }
        options.put(GENE, geneName);
        parseQueryOptions(options, qb);
        DBObject projection = parseProjectionQueryOptions(options);
        QueryResult<Variant> queryResult = coll.find(qb.get(), projection, variantConverter, options);
        queryResult.setId(geneName);
        return queryResult;
    }


    @Override
    public QueryResult groupBy(String field, QueryOptions options) {
        MongoDBCollection coll = db.getCollection(collectionName);

        String documentPath;
        switch (field) {
            case "gene":
            default:
                documentPath = DBObjectToVariantConverter.ANNOTATION_FIELD + "." + DBObjectToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD + "." + DBObjectToVariantAnnotationConverter.GENE_NAME_FIELD;
                break;
            case "ensemblGene":
                documentPath = DBObjectToVariantConverter.ANNOTATION_FIELD + "." + DBObjectToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD + "." + DBObjectToVariantAnnotationConverter.ENSEMBL_GENE_ID_FIELD;
                break;
            case "ct":
            case "consequence_type":
                documentPath = DBObjectToVariantConverter.ANNOTATION_FIELD + "." + DBObjectToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD + "." + DBObjectToVariantAnnotationConverter.SO_ACCESSION_FIELD;
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
        DBObject sort = new BasicDBObject("$sort", new BasicDBObject("count", options != null ? options.getInt("order", -1) : -1)); // 1 = ascending, -1 = descending
        DBObject limit = new BasicDBObject("$limit", options != null && options.getInt("limit", -1) > 0 ? options.getInt("limit") : 10);

        return coll.aggregate(Arrays.asList(match, project, unwind, group, sort, limit), options);
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
        if (options == null) {
            options = new QueryOptions();
        }
        options.put("limit", numGenes);
        options.put("order", order);

        return groupBy("gene", options);
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
        if (options == null) {
            options = new QueryOptions();
        }
        options.put("limit", numConsequenceTypes);
        options.put("order", order);

        return groupBy("ct", options);
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
        dbCursor.batchSize(options.getInt("batchSize", 100));
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
            Map<String, VariantStats> cohortStats = wrapper.getCohortStats();
            Iterator<VariantStats> iterator = cohortStats.values().iterator();
            VariantStats variantStats = iterator.hasNext()? iterator.next() : null;
            List<DBObject> cohorts = statsConverter.convertCohortsToStorageType(cohortStats, variantSource.getStudyId(), variantSource.getFileId());   // TODO jmmut: remove when we remove fileId
//            List cohorts = statsConverter.convertCohortsToStorageType(cohortStats, variantSource.getStudyId());   // TODO jmmut: use when we remove fileId
            
            if (!cohorts.isEmpty()) {
                String id = variantConverter.buildStorageId(wrapper.getChromosome(), wrapper.getPosition(),
                        variantStats.getRefAllele(), variantStats.getAltAllele());
                
                for (DBObject cohort : cohorts) {   // remove already present elements one by one. TODO improve this. pullAll requires exact match and addToSet does not overwrite, we would need a putToSet
                    DBObject find = new BasicDBObject("_id", id)
                            .append(
                                    DBObjectToVariantConverter.STATS_FIELD + "." + DBObjectToVariantStatsConverter.STUDY_ID,
                                    cohort.get(DBObjectToVariantStatsConverter.STUDY_ID));
                    DBObject update = new BasicDBObject("$pull",
                            new BasicDBObject(DBObjectToVariantConverter.STATS_FIELD, 
                                    new BasicDBObject(DBObjectToVariantStatsConverter.STUDY_ID, 
                                            cohort.get(DBObjectToVariantStatsConverter.STUDY_ID))));

                    builder.find(find).updateOne(update);
                }
                
                DBObject push = new BasicDBObject("$push", 
                        new BasicDBObject(DBObjectToVariantConverter.STATS_FIELD,
                                new BasicDBObject("$each", cohorts)));
                
                builder.find(new BasicDBObject("_id", id)).update(push);
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

            if (options.containsKey("sort")) {
                if (options.getBoolean("sort")) {
                    options.put("sort", new BasicDBObject("chr", 1).append("start", 1));
                } else {
                    options.remove("sort");
                }
            }

            /** GENOMIC REGION **/

            if (options.getString(ID) != null && !options.getString(ID).isEmpty()) { //) && !options.getString("id").isEmpty()) {
                List<String> ids = options.getAsStringList(ID);
                addQueryListFilter(DBObjectToVariantConverter.ANNOTATION_FIELD + "." +
                        DBObjectToVariantAnnotationConverter.XREFS_FIELD + "." +
                        DBObjectToVariantAnnotationConverter.XREF_ID_FIELD
                        , ids, builder, QueryOperation.OR);

                addQueryListFilter(DBObjectToVariantConverter.IDS_FIELD
                        , ids, builder, QueryOperation.OR);
            }

            if (options.containsKey(REGION) && !options.getString(REGION).isEmpty()) {
                List<String> stringList = options.getAsStringList(REGION);
                List<Region> regions = new ArrayList<>(stringList.size());
                for (String reg : stringList) {
                    Region region = Region.parseRegion(reg);
                    regions.add(region);
                }
                getRegionFilter(regions, builder);
            }

            if (options.containsKey(GENE)) {
                List<String> xrefs = options.getAsStringList(GENE);
                addQueryListFilter(DBObjectToVariantConverter.ANNOTATION_FIELD + "." +
                        DBObjectToVariantAnnotationConverter.XREFS_FIELD + "." +
                        DBObjectToVariantAnnotationConverter.XREF_ID_FIELD
                        , xrefs, builder, QueryOperation.OR);
            }

            if (options.containsKey(CHROMOSOME)) {
                List<String> chromosome = options.getAsStringList(CHROMOSOME);
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
                List<String> xrefs = options.getAsStringList(ANNOT_XREF);
                addQueryListFilter(DBObjectToVariantConverter.ANNOTATION_FIELD + "." +
                        DBObjectToVariantAnnotationConverter.XREFS_FIELD + "." +
                        DBObjectToVariantAnnotationConverter.XREF_ID_FIELD
                        , xrefs, builder, QueryOperation.AND);
            }

            if (options.containsKey(ANNOT_CONSEQUENCE_TYPE)) {
//                List<Integer> cts = getIntegersList(options.get(ANNOT_CONSEQUENCE_TYPE));
                List<String> cts = new ArrayList<>(options.getAsStringList(ANNOT_CONSEQUENCE_TYPE));
                List<Integer> ctsInteger = new ArrayList<>(cts.size());
                for (Iterator<String> iterator = cts.iterator(); iterator.hasNext(); ) {
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
                options.put(ANNOT_CONSEQUENCE_TYPE, cts); //Replace the QueryOption without the malformed query params
                addQueryListFilter(DBObjectToVariantConverter.ANNOTATION_FIELD + "." +
                        DBObjectToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD + "." +
                        DBObjectToVariantAnnotationConverter.SO_ACCESSION_FIELD
                        , ctsInteger, builder, QueryOperation.AND);
            }

            if (options.containsKey(ANNOT_BIOTYPE)) {
                List<String> biotypes = options.getAsStringList(ANNOT_BIOTYPE);
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
                List<String> list = new ArrayList<>(options.getAsStringList(PROTEIN_SUBSTITUTION));
                addScoreFilter(DBObjectToVariantConverter.ANNOTATION_FIELD + "." +
                        DBObjectToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD + "." +
                        DBObjectToVariantAnnotationConverter.PROTEIN_SUBSTITUTION_SCORE_FIELD, list, builder);
                options.put(PROTEIN_SUBSTITUTION, list); //Replace the QueryOption without the malformed query params
            }

            if (options.containsKey(CONSERVED_REGION)) {
                List<String> list = new ArrayList<>(options.getAsStringList(CONSERVED_REGION));
                addScoreFilter(DBObjectToVariantConverter.ANNOTATION_FIELD + "." +
                        DBObjectToVariantAnnotationConverter.CONSERVED_REGION_SCORE_FIELD, list, builder);
                options.put(PROTEIN_SUBSTITUTION, list); //Replace the QueryOption without the malformed query params
            }

            /** STATS **/

            if (options.get(MAF) != null && !options.getString(MAF).isEmpty()) {
                addCompQueryFilter(
                        DBObjectToVariantConverter.STATS_FIELD + "." + DBObjectToVariantStatsConverter.MAF_FIELD,
                        options.getString(MAF), builder);
            }

            if (options.get(MGF) != null && !options.getString(MGF).isEmpty()) {
                addCompQueryFilter(
                        DBObjectToVariantConverter.STATS_FIELD + "." + DBObjectToVariantStatsConverter.MGF_FIELD,
                        options.getString(MGF), builder);
            }

            if (options.get(MISSING_ALLELES) != null && !options.getString(MISSING_ALLELES).isEmpty()) {
                addCompQueryFilter(
                        DBObjectToVariantConverter.STATS_FIELD + "." + DBObjectToVariantStatsConverter.MISSALLELE_FIELD,
                        options.getString(MISSING_ALLELES), builder);
            }

            if (options.get(MISSING_GENOTYPES) != null && !options.getString(MISSING_GENOTYPES).isEmpty()) {
                addCompQueryFilter(
                        DBObjectToVariantConverter.STATS_FIELD + "." + DBObjectToVariantStatsConverter.MISSGENOTYPE_FIELD,
                        options.getString(MISSING_GENOTYPES), builder);
            }

            if (options.get("numgt") != null && !options.getString("numgt").isEmpty()) {
                for (String numgt : options.getAsStringList("numgt")) {
                    String[] split = numgt.split(":");
                    addCompQueryFilter(
                            DBObjectToVariantConverter.STATS_FIELD + "." + DBObjectToVariantStatsConverter.NUMGT_FIELD + "." + split[0],
                            split[1], builder);
                }
            }

//            if (options.get("freqgt") != null && !options.getString("freqgt").isEmpty()) {
//                for (String freqgt : getStringList(options.get("freqgt"))) {
//                    String[] split = freqgt.split(":");
//                    addCompQueryFilter(
//                            DBObjectToVariantSourceEntryConverter.STATS_FIELD + "." + DBObjectToVariantStatsConverter.FREQGT_FIELD + "." + split[0],
//                            split[1], builder);
//                }
//            }


            /** FILES **/
            QueryBuilder fileBuilder = QueryBuilder.start();

            if (options.containsKey(STUDIES)) { // && !options.getList("studies").isEmpty() && !options.getListAs("studies", String.class).get(0).isEmpty()) {
                addQueryListFilter(
                        DBObjectToVariantSourceEntryConverter.STUDYID_FIELD, options.getAsStringList(STUDIES),
                        fileBuilder, QueryOperation.AND);
            }

            if (options.containsKey(FILES)) { // && !options.getList("files").isEmpty() && !options.getListAs("files", String.class).get(0).isEmpty()) {
                addQueryListFilter(
                        DBObjectToVariantSourceConverter.FILEID_FIELD, options.getAsStringList(FILES),
                        fileBuilder, QueryOperation.AND);
            }

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
            for (String values : DBObjectToVariantConverter.fieldsMap.values()) {
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
        for (Iterator<String> iterator = list.iterator(); iterator.hasNext(); ) {
            String elem = iterator.next();
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
                iterator.remove(); //Remove the malformed query params.
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


}
