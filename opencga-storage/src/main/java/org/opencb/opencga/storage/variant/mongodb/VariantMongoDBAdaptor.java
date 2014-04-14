package org.opencb.opencga.storage.variant.mongodb;

import com.mongodb.*;
import java.net.UnknownHostException;
import java.util.*;
import org.opencb.biodata.models.feature.Region;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.mongodb.MongoDBCollection;
import org.opencb.datastore.mongodb.MongoDBConfiguration;
import org.opencb.datastore.mongodb.MongoDataStore;
import org.opencb.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.lib.auth.MongoCredentials;
import org.opencb.opencga.storage.variant.VariantDBAdaptor;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 * @author Alejandro Aleman Ramos <aaleman@cipf.es>
 */
public class VariantMongoDBAdaptor implements VariantDBAdaptor {

    private final MongoDataStoreManager mongoManager;
    private final MongoDataStore db;

    public VariantMongoDBAdaptor(MongoCredentials credentials) throws UnknownHostException {
        // Mongo configuration
        mongoManager = new MongoDataStoreManager(credentials.getMongoHost(), credentials.getMongoPort());
        MongoDBConfiguration mongoDBConfiguration = MongoDBConfiguration.builder().add("username", "biouser").add("password", "biopass").build();
        db = mongoManager.get(credentials.getMongoDbName(), mongoDBConfiguration);
    }

    @Override
    public QueryResult getAllVariantsByRegion(Region region, QueryOptions options) {
        MongoDBCollection coll = db.getCollection("variants");
        
        QueryBuilder qb = QueryBuilder.start();
        getRegionFilter(region, qb);
        
        if (options != null) {
            if (options.containsKey("type")) {
                getVariantTypeFilter(options.getString("type"), qb);
            }
        }
        
        return coll.find(qb.get(), options);
    }

    @Override
    public List<QueryResult> getAllVariantsByRegionList(List<Region> regions, QueryOptions options) {
        List<QueryResult> allResults = new LinkedList<>();
        for (Region r : regions) {
            QueryResult queryResult = getAllVariantsByRegion(r, options);
            allResults.add(queryResult);
        }
        return allResults;
    }

    @Override
    public QueryResult getAllVariantsByRegionAndStudies(Region region, List<String> studyId, QueryOptions options) {
        MongoDBCollection coll = db.getCollection("variants");

        // Aggregation for filtering when more than one study is present
        QueryBuilder qb = QueryBuilder.start("files.studyId").in(studyId);
        getRegionFilter(region, qb);
        
        if (options != null) {
            if (options.containsKey("type")) {
                getVariantTypeFilter(options.getString("type"), qb);
            }
        }
        
        DBObject match = new BasicDBObject("$match", qb.get());
        DBObject unwind = new BasicDBObject("$unwind", "$files");
        DBObject match2 = new BasicDBObject("$match", new BasicDBObject("files.studyId", new BasicDBObject("$in", studyId)));
        
        return coll.aggregate("$variantsRegionStudies", Arrays.asList(match, unwind, match2), options);
    }


    @Override
    public QueryResult getAllVariantsByGene(String geneName, QueryOptions options) {
        MongoDBCollection coll = db.getCollection("variants");

        // TODO Should the gene name be a first-order attribute of the variant?
        QueryBuilder qb = QueryBuilder.start("effects.geneName").is(geneName);
        if (options != null) {
            if (options.containsKey("type")) {
                getVariantTypeFilter(options.getString("type"), qb);
            }
        }
        return coll.find(qb.get(), options);
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
        // db.variants.aggregate( {$project : { genes : "$effects.geneName"} },
        //                        { $unwind : "$genes"},
        //                        { $group : { _id : "$genes", count: { $sum : 1 } }},
        //                        { $sort : { "count" : -1 }},
        //                        { $limit : 10 } )
        MongoDBCollection coll = db.getCollection("variants");
        
        QueryBuilder qb = QueryBuilder.start();
        if (options != null) {
            if (options.containsKey("type")) {
                getVariantTypeFilter(options.getString("type"), qb);
            }
        }
        
        DBObject match = new BasicDBObject("$match", qb.get());
        DBObject project = new BasicDBObject("$project", new BasicDBObject("genes", "$effects.geneName"));
        DBObject unwind = new BasicDBObject("$unwind", "$genes");
        DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "$genes").append("count", new BasicDBObject( "$sum", 1)));
        DBObject sort = new BasicDBObject("$sort", new BasicDBObject("count", order)); // 1 = ascending, -1 = descending
        DBObject limit = new BasicDBObject("$limit", numGenes);
        
        return coll.aggregate("$effects.geneName", Arrays.asList(match, project, unwind, group, sort, limit), options);
    }

    @Override
    public QueryResult getVariantById(String id, QueryOptions options) {
        MongoDBCollection coll = db.getCollection("variants");

        BasicDBObject query = new BasicDBObject("id", id);
        return coll.find(query, options);
    }

    @Override
    public List<QueryResult> getVariantsByIdList(List<String> ids, QueryOptions options) {
        List<QueryResult> allResults = new LinkedList<>();
        for (String r : ids) {
            QueryResult queryResult = getVariantById(r, options);
            allResults.add(queryResult);
        }
        return allResults;
    }

    @Override
    public boolean close() {
        db.close();
        return true;
    }

    private List<String> getChunkIds(Region region) {
        List<String> chunkIds = new LinkedList<>();
        
        int chunkSize = (region.getEnd() - region.getStart() > VariantMongoWriter.CHUNK_SIZE_BIG) ?
                VariantMongoWriter.CHUNK_SIZE_BIG : VariantMongoWriter.CHUNK_SIZE_SMALL;
        int ks = chunkSize / 1000;
        int chunkStart = region.getStart() / chunkSize;
        int chunkEnd = region.getEnd() / chunkSize;
        
        for (int i = chunkStart; i <= chunkEnd; i++) {
            String chunkId = region.getChromosome() + "_" + i + "_" + ks + "k";
            chunkIds.add(chunkId);
        }
        
        return chunkIds;
    }
    
    private QueryBuilder getRegionFilter(Region region, QueryBuilder builder) {
        List<String> chunkIds = getChunkIds(region);
        builder.and("chunkIds").in(chunkIds);
        builder.and("end").greaterThanEquals(region.getStart()).and("start").lessThanEquals(region.getEnd());
        return builder;
    }
    
    private QueryBuilder getVariantTypeFilter(String type, QueryBuilder builder) {
        return builder.and("type").is(type.toUpperCase());
    }
    
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
