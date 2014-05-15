package org.opencb.opencga.storage.variant;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.mongodb.*;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.opencb.commons.bioformats.feature.Region;
import org.opencb.commons.bioformats.variant.Variant;
import org.opencb.commons.bioformats.variant.json.VariantAnalysisInfo;
import org.opencb.commons.bioformats.variant.json.VariantInfo;
import org.opencb.commons.bioformats.variant.utils.effect.VariantEffect;
import org.opencb.commons.bioformats.variant.utils.stats.VariantStats;
import org.opencb.commons.containers.QueryResult;
import org.opencb.commons.containers.map.QueryOptions;
import org.opencb.opencga.lib.auth.MongoCredentials;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Alejandro Aleman Ramos <aaleman@cipf.es>
 */
public class VariantMongoQueryBuilder implements VariantQueryBuilder {

    private final MongoClient mongoClient;
    private final DB db;

    public VariantMongoQueryBuilder(MongoCredentials credentials)
            throws MasterNotRunningException, ZooKeeperConnectionException, UnknownHostException {

        // Mongo configuration
        mongoClient = new MongoClient(credentials.getMongoHost());
        db = mongoClient.getDB(credentials.getMongoDbName());
    }


    @Override
    public QueryResult getAllVariantsByRegion(Region region, String studyName, QueryOptions options) {
        Long start, end, dbstart, dbend;
        start = System.currentTimeMillis();
        QueryResult<DBObject> queryResult = new QueryResult<>(
                String.format("%s:%d-%d", region.getChromosome(), region.getStart(), region.getEnd()));
        List<DBObject> results = new LinkedList<>();

        boolean includeSamples;
        boolean includeStats;
        boolean includeEffects;


        if (!options.containsKey("samples") && !options.containsKey("stats") && !options.containsKey("effects")) {
            includeSamples = true;
            includeStats = true;
            includeEffects = true;
        } else {
            includeSamples = options.containsKey("samples") && options.getBoolean("samples");
            includeStats = options.containsKey("stats") && options.getBoolean("stats");
            includeEffects = options.containsKey("effects") && options.getBoolean("effects");
        }

        String startRow = buildRowkey(region.getChromosome(), Long.toString(region.getStart()));
        String stopRow = buildRowkey(region.getChromosome(), Long.toString(region.getEnd()));
//            HTable table = new HTable(admin.getConfiguration(), tableName);
        dbstart = System.currentTimeMillis();
//            Scan regionScan = new Scan(startRow.getBytes(), stopRow.getBytes());
//            ResultScanner scanres = table.getScanner(regionScan);
        dbend = System.currentTimeMillis();
        queryResult.setDbTime(dbend - dbstart);

        DBCollection coll = db.getCollection("variants");


        // Iterate over results and, optionally, their samples and statistics

        DBObject query = new BasicDBObject();
        DBObject match = new BasicDBObject("sources.sourceId", studyName);
        query.put("$match", match);
        DBObject unwind = new BasicDBObject("$unwind", "$sources");
        DBObject match2 = new BasicDBObject("$match", new BasicDBObject("sources.studyId", studyName));
        // db.variants.aggregate(
//        {$match: {'studies.studyId': 'ale'}},
//        {$project: {"studies.effects":1,'studies.studyId':1 }},
//        {$unwind:'$studies'}, {$match: {'studies.studyId':'ale'}},
//        {$project: {effects: '$studies.effects'}},
//        {$unwind: '$effects'},
//        {$group: {_id:'$effects', count:{$sum:1}}})

        System.out.println(query);
        System.out.println(unwind);
        System.out.println(match2);
        AggregationOutput cursor = coll.aggregate(query, unwind, match2);


        //  db.variants.aggregate({ "$match" : { "studies.studyId" : "test9"}}, { "$unwind" : "$studies"} , { "$match" : { "studies.studyId" : "test9"}} ,{$skip: 900},  {$limit: 5})


        for (DBObject obj : cursor.results()) {
            results.add(obj);
        }
        end = System.currentTimeMillis();
        queryResult.setDbTime(end - dbstart);

        queryResult.setResult(results);
        queryResult.setNumResults(results.size());
        end = System.currentTimeMillis();
        queryResult.setTime(end - start);
        return queryResult;
    }

    @Override
    public List<QueryResult> getAllVariantsByRegionList(List<Region> regions, String studyName, QueryOptions options) {
        List<QueryResult> allResults = new LinkedList<>();
        for (Region r : regions) {
            QueryResult queryResult = getAllVariantsByRegion(r, studyName, options);
            allResults.add(queryResult);
        }
        return allResults;
    }

    @Override
    public QueryResult getVariantsHistogramByRegion(Region region, String studyName, boolean histogramLogarithm, int histogramMax) {
        return null;
    }

    @Override
    public QueryResult getStatsByVariant(Variant variant, QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult getSimpleStatsByVariant(Variant variant, QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult getEffectsByVariant(Variant variant, QueryOptions options) {
        return null;
    }

    @Override
    public List<VariantStats> getRecordsStats(Map<String, String> options) {
        return null;
    }

    @Override
    public List<VariantEffect> getEffect(Map<String, String> options) {
        return null;
    }

    @Override
    public VariantAnalysisInfo getAnalysisInfo(Map<String, String> options) {
        return null;
    }

    @Override
    public boolean close() {

        mongoClient.close();
        return true;
    }

    private String buildRowkey(String chromosome, String position) {
        return chromosome + "_" + position;
    }

    @Override
    public List<VariantInfo> getRecords(Map<String, String> options) {
        return null;
    }

    public QueryResult<VariantInfo> getRecordsMongo(int page, int start, int limit, MutableInt count, Map<String, String> options) {

        long startTime = System.currentTimeMillis();
        QueryResult<VariantInfo> queryResult = new QueryResult<>();

        List<VariantInfo> res = new ArrayList<>();
        String sourceId = options.get("studyId");
        DBCollection coll = db.getCollection("variants");

        BasicDBObject elemMatch = new BasicDBObject("sourceId", sourceId);
        DBObject query = new BasicDBObject();
        BasicDBList orList = new BasicDBList();

        Map<String, List<String>> sampleGenotypes = processSamplesGT(options);


        System.out.println("map = " + options);

        if (options.containsKey("region_list") && !options.get("region_list").equals("")) {
            String[] regions = options.get("region_list").split(",");
            Pattern pattern = Pattern.compile("(\\w+):(\\d+)-(\\d+)");
            Matcher matcher, matcherChr;

            for (int i = 0; i < regions.length; i++) {
                String region = regions[i];
                matcher = pattern.matcher(region);
                if (matcher.find()) {
                    String chr = matcher.group(1);
                    int s = Integer.valueOf(matcher.group(2));
                    int e = Integer.valueOf(matcher.group(3));

                    DBObject regionClause = new BasicDBObject("chr", chr);
                    regionClause.put("pos", new BasicDBObject("$gte", s).append("$lte", e));
                    orList.add(regionClause);
                } else {

                    Pattern patternChr = Pattern.compile("(\\w+)");
                    matcherChr = patternChr.matcher(region);

                    if (matcherChr.find()) {
                        String chr = matcherChr.group();
                        DBObject regionClause = new BasicDBObject("chr", chr);
                        orList.add(regionClause);
                    }

                }
            }
            query.put("$or", orList);

        } else if (options.containsKey("genes") && !options.get("genes").equals("")) {
            orList = processGeneList(options.get("genes"));
            if (orList.size() > 0) {
                query.put("$or", orList);
            } else {
                queryResult.setWarningMsg("Wrong gene name");
                queryResult.setResult(res);
                queryResult.setNumResults(res.size());
                return queryResult;
            }
        }

        if (options.containsKey("conseq_type") && !options.get("conseq_type").equals("")) {
            String[] cts = options.get("conseq_type").split(",");

            BasicDBList ctList = new BasicDBList();
            for (String ct : cts) {
                ctList.add(ct);
            }
            elemMatch.put("effects", new BasicDBObject("$in", ctList));
        }

        if (sampleGenotypes.size() > 0) {
            for (Map.Entry<String, List<String>> entry : sampleGenotypes.entrySet()) {
                BasicDBList gtList = new BasicDBList();
                for (String gt : entry.getValue()) {
                    gtList.add(gt);
                }
                elemMatch.put("samples." + entry.getKey() + ".GT", new BasicDBObject("$in", gtList));
            }
        }

        if (options.containsKey("miss_gt") && !options.get("miss_gt").equalsIgnoreCase("")) {
            Integer val = Integer.valueOf(options.get("miss_gt"));
            Object missGt = getMongoOption(options.get("option_miss_gt"), val);
            elemMatch.put("stats.missGenotypes", missGt);
        }

        BasicDBList andControls = new BasicDBList();

        if (options.containsKey("maf_1000g_controls") && !options.get("maf_1000g_controls").equalsIgnoreCase("")) {
            BasicDBList or = new BasicDBList();
            or.add(new BasicDBObject("attributes.1000G_maf", new BasicDBObject("$exists", false)));
            or.add(new BasicDBObject("attributes.1000G_maf", new BasicDBObject("$lte", options.get("maf_1000g_controls"))));

            andControls.add(new BasicDBObject("$or", or));
        }

        if (options.containsKey("maf_1000g_afr_controls") && !options.get("maf_1000g_afr_controls").equalsIgnoreCase("")) {
            BasicDBList or = new BasicDBList();
            or.add(new BasicDBObject("attributes.1000G_AFR_maf", new BasicDBObject("$exists", false)));
            or.add(new BasicDBObject("attributes.1000G_AFR_maf", new BasicDBObject("$lte", options.get("maf_1000g_afr_controls"))));

            andControls.add(new BasicDBObject("$or", or));
        }

        if (options.containsKey("maf_1000g_asi_controls") && !options.get("maf_1000g_asi_controls").equalsIgnoreCase("")) {
            BasicDBList or = new BasicDBList();
            or.add(new BasicDBObject("attributes.1000G_ASI_maf", new BasicDBObject("$exists", false)));
            or.add(new BasicDBObject("attributes.1000G_ASI_maf", new BasicDBObject("$lte", options.get("maf_1000g_asi_controls"))));

            andControls.add(new BasicDBObject("$or", or));
        }

        if (options.containsKey("maf_evs_controls") && !options.get("maf_evs_controls").equalsIgnoreCase("")) {
            BasicDBList or = new BasicDBList();
            or.add(new BasicDBObject("attributes.EVS_maf", new BasicDBObject("$exists", false)));
            or.add(new BasicDBObject("attributes.EVS_maf", new BasicDBObject("$lte", options.get("maf_evs_controls"))));

            andControls.add(new BasicDBObject("$or", or));

        }

        if (options.containsKey("maf_bier_controls") && !options.get("maf_bier_controls").equalsIgnoreCase("")) {
            BasicDBList or = new BasicDBList();
            or.add(new BasicDBObject("attributes.BIER_maf", new BasicDBObject("$exists", false)));
            or.add(new BasicDBObject("attributes.BIER_maf", new BasicDBObject("$lte", options.get("maf_bier_controls"))));

            andControls.add(new BasicDBObject("$or", or));
        }

        if (andControls.size() > 0) {
            elemMatch.append("$and", andControls);
        }

        query.put("sources", new BasicDBObject("$elemMatch", elemMatch));

        System.out.println("#############################");
        System.out.println(query);
        System.out.println("#############################");

        long dbStart = System.currentTimeMillis();

        DBObject sort = null;
        DBCursor cursor;

        if (options.containsKey("sort")) {
            sort = getQuerySort(options.get("sort"));
            cursor = coll.find(query).sort(sort).skip(start).limit(limit);
        } else {
            cursor = coll.find(query).skip(start).limit(limit);
        }

        count.setValue(cursor.count());

        queryResult.setDbTime(dbStart - System.currentTimeMillis());

        for (DBObject obj : cursor) {

            BasicDBObject elem = (BasicDBObject) obj;
            VariantInfo vi = new VariantInfo();
            VariantStats vs = new VariantStats();

            String chr = elem.getString("chr");
            int pos = elem.getInt("pos");

            vi.setChromosome(chr);
            vi.setPosition(pos);

            BasicDBList studies = (BasicDBList) elem.get("sources");

            Iterator<Object> it = studies.iterator();
            while (it.hasNext()) {
                BasicDBObject study = (BasicDBObject) it.next();

                if (study.getString("sourceId").equalsIgnoreCase(sourceId)) {

                    BasicDBObject stats = (BasicDBObject) study.get("stats");

                    String ref = study.getString("ref");
                    BasicDBList alt = (BasicDBList) study.get("alt");
                    vi.setRef(ref);
                    vi.setAlt(Joiner.on(",").join(alt.toArray()));
                    vs.setMaf((float) stats.getDouble("maf"));
                    vs.setMgf((float) stats.getDouble("mgf"));
                    vs.setMafAllele(stats.getString("alleleMaf"));
                    vs.setMgfAllele(stats.getString("genotypeMaf"));
                    vs.setMissingAlleles(stats.getInt("missAllele"));
                    vs.setMissingGenotypes(stats.getInt("missGenotypes"));
                    vs.setMendelinanErrors(stats.getInt("mendelErr"));
                    vs.setCasesPercentDominant((float) stats.getDouble("casesPercentDominant"));
                    vs.setControlsPercentDominant((float) stats.getDouble("controlsPercentDominant"));
                    vs.setCasesPercentRecessive((float) stats.getDouble("casesPercentRecessive"));
                    vs.setControlsPercentRecessive((float) stats.getDouble("controlsPercentRecessive"));

                    BasicDBObject samples = (BasicDBObject) study.get("samples");

                    for (String sampleName : samples.keySet()) {

                        DBObject sample = (DBObject) samples.get(sampleName);

                        if (sample.containsField("GT")) {
                            String sampleGT = (String) sample.get("GT");
                            vi.addSammpleGenotype(sampleName, sampleGT);
                        }

                    }

                    vi.setSnpid((String) study.get("snpId"));

                    if (study.containsField("effects")) {
                        BasicDBList conseqTypes = (BasicDBList) study.get("effects");
                        conseqTypes.remove("");
                        String cts = Joiner.on(",").join(conseqTypes.iterator());
                        vi.addConsequenceTypes(cts);
                    }

                    if (study.containsField("genes")) {
                        BasicDBList genesList = (BasicDBList) study.get("genes");
                        String genes = Joiner.on(",").join(genesList.iterator());
                        vi.addGenes(genes);
                    }

                    if (study.containsField("attributes")) {

                        BasicDBObject attr = (BasicDBObject) study.get("attributes");

                        if (attr.containsField("1000G_maf")) {
                            vi.addControl("1000G_maf", (String) attr.get("1000G_maf"));
                            vi.addControl("1000G_amaf", (String) attr.get("1000G_amaf"));
                            vi.addControl("1000G_gt", (String) attr.get("1000G_gt"));
                        }

                        if (attr.containsField("1000G_ASI_maf")) {
                            vi.addControl("1000G-ASI_maf", (String) attr.get("1000G_ASI_maf"));
                            vi.addControl("1000G-ASI_amaf", (String) attr.get("1000G_ASI_amaf"));
                            vi.addControl("1000G-ASI_gt", (String) attr.get("1000G_ASI_gt"));
                        }

                        if (attr.containsField("1000G_AFR_maf")) {
                            vi.addControl("1000G-AFR_maf", (String) attr.get("1000G_AFR_maf"));
                            vi.addControl("1000G-AFR_amaf", (String) attr.get("1000G_AFR_amaf"));
                            vi.addControl("1000G-AFR_gt", (String) attr.get("1000G_AFR_gt"));
                        }

                        if (attr.containsField("1000G_AME_maf")) {
                            vi.addControl("1000G-AME_maf", (String) attr.get("1000G_AME_maf"));
                            vi.addControl("1000G-AME_amaf", (String) attr.get("1000G_AME_amaf"));
                            vi.addControl("1000G-AME_gt", (String) attr.get("1000G_AME_gt"));
                        }

                        if (attr.containsField("1000G_EUR_maf")) {
                            vi.addControl("1000G-EUR_maf", (String) attr.get("1000G_EUR_maf"));
                            vi.addControl("1000G-EUR_amaf", (String) attr.get("1000G_EUR_amaf"));
                            vi.addControl("1000G-EUR_gt", (String) attr.get("1000G_EUR_gt"));
                        }

                        if (attr.containsField("EVS_maf")) {
                            vi.addControl("EVS_maf", (String) attr.get("EVS_maf"));
                            vi.addControl("EVS_amaf", (String) attr.get("EVS_amaf"));
                            vi.addControl("EVS_gt", (String) attr.get("EVS_gt"));
                        }

                        if (attr.containsField("BIER_maf")) {
                            vi.addControl("BIER_maf", (String) attr.get("BIER_maf"));
                            vi.addControl("BIER_amaf", (String) attr.get("BIER_amaf"));
                            vi.addControl("BIER_gt", (String) attr.get("BIER_gt"));
                        }

                        if (attr.containsField("PolyphenScore")) {
                            vi.setPolyphen_score(Double.parseDouble(attr.getString("PolyphenScore")));
                            vi.setPolyphen_effect(Integer.parseInt(attr.getString("PolyphenEffect")));
                        }

                        if (attr.containsField("SIFTScore")) {
                            vi.setSift_score(Double.parseDouble(attr.getString("SIFTScore")));
                            vi.setSift_effect(Integer.parseInt(attr.getString("SIFTEffect")));
                        }

                    }
                    continue;
                }
            }
            vi.addStats(vs);
            res.add(vi);
        }

        queryResult.setResult(res);
        queryResult.setTime(startTime - System.currentTimeMillis());

        return queryResult;
    }

    private DBObject getQuerySort(String sort) {

        DBObject res = new BasicDBObject();

        //  sort=[{"property":"stats_id_snp","direction":"ASC"}],
//        Pattern pattern = Pattern.compile("(\\w+):(\\d+)-(\\d+)");
        Pattern pattern = Pattern.compile("\"property\":\"(\\w+)\",\"direction\":\"(ASC|DESC)\"");
        Matcher matcher = pattern.matcher(sort);
        if (matcher.find()) {

            String field = matcher.group(1);
            String direction = matcher.group(2);

            int dir = 1;

            if (direction.equalsIgnoreCase("ASC")) {
                dir = 1;
            } else if (direction.equalsIgnoreCase("DESC")) {
                dir = -1;
            }

            switch (field) {
                case "chromosome":
                    res.put("chr", dir);
                    res.put("pos", dir);
                    break;
                case "snpid":
                    res.put("sources.snpId", dir);
                    break;
                case "consecuente_types":
                    res.put("sources.effects", dir);
                    break;
                case "genes":
                    res.put("sources.genes.1", dir);
                    break;
                case "polyphen_score":
                    res.put("sources.attributes.PolyphenScore", dir);
                    break;
                case "sift_score":
                    res.put("sources.attributes.SIFTScore", dir);
                    break;
            }

        }


        return res;
    }

    private Object getMongoOption(String option, float val) {

        Object res = null;

        switch (option) {
            case ("<"):
                res = new BasicDBObject("$lt", val);
                break;
            case ("<="):
                res = new BasicDBObject("$lte", val);
                break;
            case (">"):
                res = new BasicDBObject("$gt", val);
                break;
            case (">="):
                res = new BasicDBObject("$gte", val);
                break;
            case ("="):
                res = val;
                break;
            case ("!="):
                res = new BasicDBObject("$ne", val);
                break;
        }


        return res;
    }

    private BasicDBList processGeneList(String genes) {

        BasicDBList list = new BasicDBList();

        Client wsRestClient = Client.create();
        WebResource webResource = wsRestClient.resource("http://ws.bioinfo.cipf.es/cellbase/rest/latest/hsa/feature/gene/");

        ObjectMapper mapper = new ObjectMapper();

        String response = webResource.path(genes).path("info").queryParam("of", "json").get(String.class);


        try {
            JsonNode actualObj = mapper.readTree(response);
            Iterator<JsonNode> it = actualObj.iterator();
            Iterator<JsonNode> aux;

            while (it.hasNext()) {
                JsonNode node = it.next();
                if (node.isArray()) {

                    aux = node.iterator();
                    while (aux.hasNext()) {
                        JsonNode auxNode = aux.next();

                        DBObject regionClause = new BasicDBObject("chr", auxNode.get("chromosome").asText());
                        regionClause.put("pos", new BasicDBObject("$gte", auxNode.get("start").asInt()).append("$lte", auxNode.get("end").asInt()));
                        list.add(regionClause);

                    }

                }
            }


        } catch (IOException e) {
            e.printStackTrace();
        }

        return list;

    }

    private Map<String, List<String>> processSamplesGT(Map<String, String> options) {
        Map<String, List<String>> samplesGenotypes = new LinkedHashMap<>(10);
        List<String> genotypesList;

        String key, val;
        for (Map.Entry<String, String> entry : options.entrySet()) {
            key = entry.getKey();
            val = entry.getValue();

            if (key.startsWith("sampleGT_")) {
                String sampleName = key.replace("sampleGT_", "").replace("[]", "");
                String[] genotypes = val.split(",");

                if (samplesGenotypes.containsKey(sampleName)) {
                    genotypesList = samplesGenotypes.get(sampleName);
                } else {

                    genotypesList = new ArrayList<>();
                    samplesGenotypes.put(sampleName, genotypesList);
                }


                for (int i = 0; i < genotypes.length; i++) {

                    genotypesList.add(genotypes[i]);
                }

            }

        }
        return samplesGenotypes;
    }


    public QueryResult<VariantAnalysisInfo> getAnalysisInfo(String studyId) {

        long start = System.currentTimeMillis();
        QueryResult<VariantAnalysisInfo> qres = new QueryResult<>();
        VariantAnalysisInfo vi = new VariantAnalysisInfo();


        DBCollection coll = db.getCollection("sources");
        DBCollection collV = db.getCollection("variants");

        long dbStart = System.currentTimeMillis();
        DBObject study = coll.findOne(new BasicDBObject("alias", studyId));

        if (study != null) {
            Iterator<Object> it = ((BasicDBList) study.get("samples")).iterator();

            while (it.hasNext()) {
                vi.addSample((String) it.next());
            }

            BasicDBObject gs = (BasicDBObject) study.get("globalStats");

            for (String elem : gs.keySet()) {
                if (!elem.equalsIgnoreCase("consequenceTypes")) {
                    double val = gs.getDouble(elem);
                    vi.addGlobalStats(elem, val);
                } else {

                    BasicDBObject cts = (BasicDBObject) gs.get("consequenceTypes");
                    for (String ct : cts.keySet()) {
                        vi.addConsequenceType(ct, cts.getInt(ct));
                    }
                }
            }
        }
        qres.setDbTime(System.currentTimeMillis() - dbStart);

        qres.setResult(Arrays.asList(vi));

        qres.setTime(System.currentTimeMillis() - start);


        return qres;


    }

}
