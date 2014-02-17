package org.opencb.opencga.storage.variant;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.mongodb.*;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import org.apache.commons.lang.StringUtils;
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
        DBObject match = new BasicDBObject("studies.studyId", studyName);
        query.put("$match", match);
        DBObject unwind = new BasicDBObject("$unwind", "$studies");
        DBObject match2 = new BasicDBObject("$match", new BasicDBObject("studies.studyId", studyName));
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
        if (chromosome.length() > 2) {
            if (chromosome.substring(0, 2).equals("chr")) {
                chromosome = chromosome.substring(2);
            }
        }
        if (chromosome.length() < 2) {
            chromosome = "0" + chromosome;
        }
        if (position.length() < 10) {
            while (position.length() < 10) {
                position = "0" + position;
            }
        }
        return chromosome + "_" + position;
    }

    @Override
    public List<VariantInfo> getRecords(Map<String, String> options) {
        return null;
    }

    public QueryResult<VariantInfo> getRecordsMongo(Map<String, String> options) {

        long start = System.currentTimeMillis();
        QueryResult<VariantInfo> queryResult = new QueryResult<>();

        List<VariantInfo> res = new ArrayList<>();
        String studyId = options.get("studyId");
        DBCollection coll = db.getCollection("variants");

        DBObject elemMatch = new BasicDBObject("studyId", studyId);
        DBObject query = new BasicDBObject();
        BasicDBList orList = new BasicDBList();

        Map<String, List<String>> sampleGenotypes = processSamplesGT(options);

        if (options.containsKey("region_list") && !options.get("region_list").equals("")) {
            String[] regions = options.get("region_list").split(",");
            Pattern pattern = Pattern.compile("(\\w+):(\\d+)-(\\d+)");
            Matcher matcher;
            query.put("$or", orList);

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

        if (options.containsKey("conseq_type[]") && !options.get("conseq_type[]").equals("")) {
            String[] cts = options.get("conseq_type[]").split(",");
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

        query.put("studies", new BasicDBObject("$elemMatch", elemMatch));

/*
        if (options.containsKey("mend_error") && !options.get("mend_error").equals("")) {
            int val = Integer.parseInt(options.get("mend_error"));
            String opt = getMongoOption(options.get("option_mend_error"));
            elemMatch.put("stats.mendelErr", new BasicDBObject(opt, val));
        }

//        if (options.containsKey("is_indel") && options.get("is_indel").equalsIgnoreCase("on")) {
//            whereClauses.add("variant_stats.is_indel=1");
//        }
//
        if (options.containsKey("maf") && !options.get("maf").equals("")) {
            float val = Float.parseFloat(options.get("maf"));
            elemMatch.put("stats.maf", getMongoOption(options.get("option_maf"), val));
        }

        if (options.containsKey("mgf") && !options.get("mgf").equals("")) {
            float val = Float.parseFloat(options.get("mgf"));
            String opt = getMongoOption(options.get("option_mgf"));
            elemMatch.put("stats.mgf", new BasicDBObject(opt, val));
        }

        if (options.containsKey("miss_allele") && !options.get("miss_allele").equals("")) {
            int val = Integer.parseInt(options.get("miss_allele"));
            String opt = getMongoOption(options.get("option_miss_allele"));
            elemMatch.put("stats.missAllele", new BasicDBObject(opt, val));
        }

        if (options.containsKey("miss_gt") && !options.get("miss_gt").equals("")) {
            int val = Integer.parseInt(options.get("miss_gt"));
            String opt = getMongoOption(options.get("option_miss_gt"));
            elemMatch.put("stats.missGenotypes", new BasicDBObject(opt, val));
        }

        if (options.containsKey("cases_percent_dominant") && !options.get("cases_percent_dominant").equals("")) {
            float val = Float.parseFloat(options.get("cases_percent_dominant"));
            String opt = getMongoOption(options.get("option_cases_dom"));
            elemMatch.put("stats.casesPercentDominant", new BasicDBObject(opt, val));
        }

        if (options.containsKey("controls_percent_dominant") && !options.get("controls_percent_dominant").equals("")) {
            float val = Float.parseFloat(options.get("controls_percent_dominant"));
            String opt = getMongoOption(options.get("option_controls_dom"));
            elemMatch.put("stats.controlsPercentDominant", new BasicDBObject(opt, val));
        }

        if (options.containsKey("cases_percent_recessive") && !options.get("cases_percent_recessive").equals("")) {
            float val = Float.parseFloat(options.get("cases_percent_recessive"));
            String opt = getMongoOption(options.get("option_cases_rec"));
            elemMatch.put("stats.casesPercentRecessive", new BasicDBObject(opt, val));
        }

        if (options.containsKey("controls_percent_recessive") && !options.get("controls_percent_recessive").equals("")) {
            float val = Float.parseFloat(options.get("controls_percent_recessive"));
            String opt = getMongoOption(options.get("option_controls_rec"));
            elemMatch.put("stats.controlsPercentRecessive", new BasicDBObject(opt, val));
        }
        */

        System.out.println("#############################");
        System.out.println(query);
        System.out.println("#############################");

        long dbStart = System.currentTimeMillis();

        DBCursor cursor = coll.find(query); //.limit(25);

        queryResult.setDbTime(dbStart - System.currentTimeMillis());

        for (DBObject obj : cursor) {

            BasicDBObject elem = (BasicDBObject) obj;
            VariantInfo vi = new VariantInfo();
            VariantStats vs = new VariantStats();

            String chr = elem.getString("chr");
            int pos = elem.getInt("pos");

            vi.setChromosome(chr);
            vi.setPosition(pos);

            BasicDBList studies = (BasicDBList) elem.get("studies");

            Iterator<Object> it = studies.iterator();
            while (it.hasNext()) {
                BasicDBObject study = (BasicDBObject) it.next();

                if (study.getString("studyId").equalsIgnoreCase(studyId)) {

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

                    continue;

                }
            }
            vi.addStats(vs);
            res.add(vi);
        }

        queryResult.setResult(res);
        queryResult.setNumResults(res.size());
        queryResult.setTime(start - System.currentTimeMillis());

        return queryResult;
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


}
