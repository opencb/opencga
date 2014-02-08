package org.opencb.opencga.storage.variant;

import com.mongodb.*;
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

import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
    public List<VariantInfo> getRecords(Map<String, String> options) {
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
}
