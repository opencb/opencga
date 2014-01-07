package org.opencb.opencga.storage.variant;

import com.google.protobuf.InvalidProtocolBufferException;
import com.mongodb.*;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.opencb.commons.bioformats.feature.Genotype;
import org.opencb.commons.bioformats.feature.Region;
import org.opencb.commons.bioformats.variant.Variant;
import org.opencb.commons.bioformats.variant.json.VariantAnalysisInfo;
import org.opencb.commons.bioformats.variant.json.VariantInfo;
import org.opencb.commons.bioformats.variant.utils.effect.VariantEffect;
import org.opencb.commons.bioformats.variant.utils.stats.VariantStats;
import org.opencb.commons.containers.QueryResult;
import org.opencb.commons.containers.map.QueryOptions;
import org.opencb.opencga.lib.auth.MonbaseCredentials;

/**
 * @author Jesus Rodriguez <jesusrodrc@gmail.com>
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */


public class VariantMonbaseQueryBuilder implements VariantQueryBuilder {
    
    private String tableName;
    private String effectTableName;
    private HBaseAdmin admin;
    private MongoClient mongoClient;
    private DB db;
    private Variant partialResult;

    private MonbaseCredentials monbaseCredentials;

    public static final Charset CHARSET_UTF_8 = Charset.forName("UTF-8");


    public VariantMonbaseQueryBuilder(String species, MonbaseCredentials credentials) 
            throws MasterNotRunningException, ZooKeeperConnectionException, UnknownHostException {
        this.monbaseCredentials = credentials;
        this.tableName = species;
        this.effectTableName = species + "effect";
        
        // HBase configuration
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.master", credentials.getHbaseMasterHost() + ":" + credentials.getHbaseMasterPort());
        config.set("hbase.zookeeper.quorum", credentials.getHbaseZookeeperQuorum());
        config.set("hbase.zookeeper.property.clientPort", String.valueOf(credentials.getHbaseZookeeperClientPort()));
        admin = new HBaseAdmin(config);

        // Mongo configuration
        mongoClient = new MongoClient(credentials.getMongoHost());
        db = mongoClient.getDB(credentials.getMongoDbName());
    }

    
    @Override
    public QueryResult<Variant> getAllVariantsByRegion(Region region, String studyName, QueryOptions options) {
        Long start, end, dbstart, dbend;
        start = System.currentTimeMillis();
        QueryResult<Variant> queryResult = new QueryResult<>(
                String.format("%s:%d-%d", region.getChromosome(), region.getStart(), region.getEnd()));
        List<Variant> results =  new LinkedList<>();
        
        boolean includeSamples;
        boolean includeStats;
        boolean includeEffects;
        if(!options.containsKey("samples") && !options.containsKey("stats") && !options.containsKey("effects")){
            includeSamples = true;
            includeStats = true;
            includeEffects = true;
        } else {
            includeSamples = options.containsKey("samples") && options.getBoolean("samples");
            includeStats = options.containsKey("stats") && options.getBoolean("stats");
            includeEffects = options.containsKey("effects") &&  options.getBoolean("effects");
        }

        try {
            String startRow = buildRowkey(region.getChromosome(), Long.toString(region.getStart()));
            String stopRow = buildRowkey(region.getChromosome(), Long.toString(region.getEnd()));
            HTable table = new HTable(admin.getConfiguration(), tableName);
            dbstart = System.currentTimeMillis();
            Scan regionScan = new Scan(startRow.getBytes(), stopRow.getBytes());
            ResultScanner scanres =  table.getScanner(regionScan);
            dbend = System.currentTimeMillis();
            queryResult.setDbTime(dbend-dbstart);

            // Iterate over variants and, optionally, their samples and statistics
            for (Result result : scanres) {
                String[] rowkeyParts = new String(result.getRow(), CHARSET_UTF_8).split("_");
                String chromosome = rowkeyParts[0];
                int position = Integer.parseInt(rowkeyParts[1]);
                // TODO Reimplement chromosome padding
                if (chromosome.startsWith("0")) {
                    chromosome = chromosome.substring(1);
                }

                // Get basic variant fields from Protocol Buffers message
                NavigableMap<byte[], byte[]> infoMap = result.getFamilyMap("i".getBytes());
                byte[] byteInfo = infoMap.get((studyName + "_data").getBytes());
                VariantFieldsProtos.VariantInfo protoInfo = VariantFieldsProtos.VariantInfo.parseFrom(byteInfo);
                String reference = protoInfo.getReference();
                String alternate = StringUtils.join(protoInfo.getAlternateList(), ",");
                String format = StringUtils.join(protoInfo.getFormatList(), ":");
                Variant variant = new Variant(chromosome, position, reference, alternate);

                // Set samples if requested
                if (includeSamples) {
                    NavigableMap<byte[], byte[]> sampleMap = result.getFamilyMap("d".getBytes());
                    Map<String, Map<String, String>> resultSampleMap = new HashMap<>();

                    // Set samples
                    for (byte[] s : sampleMap.keySet()) {
                        String sampleName = (new String(s, CHARSET_UTF_8)).replaceAll(studyName + "_", "");
                        VariantFieldsProtos.VariantSample sample = VariantFieldsProtos.VariantSample.parseFrom(sampleMap.get(s));
                        String sample1 = sample.getSample();
                        String[] values = sample1.split(":");
                        String[] fields = format.split(":");
                        Map<String, String> singleSampleMap = new HashMap<>();
                        for (int i = 0; i < fields.length; i++) {
                            singleSampleMap.put(fields[i], values[i]);
                        }
                        resultSampleMap.put(sampleName, singleSampleMap);
                    }
                    
                    variant.setSampleData(resultSampleMap);
                }

                // Set stats if requested
                if (includeStats) {
                    byte[] byteStats = infoMap.get((studyName + "_stats").getBytes());
                    VariantFieldsProtos.VariantStats protoStats = VariantFieldsProtos.VariantStats.parseFrom(byteStats);
                    VariantStats variantStats = new VariantStats(chromosome, position, reference, alternate,
                            protoStats.getMaf(),
                            protoStats.getMgf(),
                            protoStats.getMafAllele(),
                            protoStats.getMgfGenotype(),
                            protoStats.getMissingAlleles(),
                            protoStats.getMissingGenotypes(),
                            protoStats.getMendelianErrors(),
                            protoStats.getIsIndel(),
                            protoStats.getCasesPercentDominant(),
                            protoStats.getControlsPercentDominant(),
                            protoStats.getCasesPercentRecessive(),
                            protoStats.getControlsPercentRecessive());
                    variant.setStats(variantStats);
                }

                if (includeEffects) {
                    QueryResult<VariantEffect> queryEffects = getEffectsByVariant(variant, options);
                    variant.setEffect(queryEffects.getResult());
                }
                
                results.add(variant);
            }
        } catch (IOException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
        queryResult.setResult(results);
        queryResult.setNumResults(results.size());
        end = System.currentTimeMillis();
        queryResult.setTime(end-start);
        return queryResult;
    }

    @Override
    public QueryResult getVariantsHistogramByRegion(Region region, String studyName, boolean histogramLogarithm, int histogramMax) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public QueryResult<VariantStats> getStatsByVariant(Variant variant, QueryOptions options) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public QueryResult<VariantStats> getSimpleStatsByVariant(Variant variant, QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult getEffectsByVariant(Variant variant, QueryOptions options) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public QueryResult getSimpleVariantsByRegion(Region region, String studyName, QueryOptions options){
        Long start, end, dbstart, dbend;
        start = System.currentTimeMillis();
        boolean includeSamples;
        boolean includeStats;
        boolean includeEffects;
        if(!options.containsKey("samples") && !options.containsKey("stats") && !options.containsKey("effects")){
            includeSamples = true;
            includeStats = true;
            includeEffects = true;
        }
        else{
            includeSamples = options.containsKey("samples") && options.getBoolean("samples");
            includeStats = options.containsKey("stats") && options.getBoolean("stats");
            includeEffects = options.containsKey("effects") &&  options.getBoolean("effects");
        }

        QueryResult<Variant> queryResult = new QueryResult<>(String.format("%s:%d-%d", region.getChromosome(), region.getStart(), region.getEnd()));
        List<Variant> result = new ArrayList<>();
        String startRow = buildRowkey(region.getChromosome(), Long.toString(region.getStart()));
        String stopRow = buildRowkey(region.getChromosome(), Long.toString(region.getEnd()));
        BasicDBObject compare = new BasicDBObject("_id", new BasicDBObject("$gte", startRow).append("$lte", stopRow)).append("studies._id", studyName);
        DBCollection collection = db.getCollection("variants");
        dbstart = System.currentTimeMillis();
        Iterator<DBObject> dbresults = collection.find(compare, new BasicDBObject("studies.$", 1));
        dbend = System.currentTimeMillis();
        while (dbresults.hasNext()) {
            DBObject variant = dbresults.next();
            String position = variant.get("_id").toString();
            String[] pos = position.split("_");
            String varchr = pos[0];
            String inner_position = pos[1];
            BasicDBList studies = (BasicDBList) variant.get("studies");
            BasicDBObject st = (BasicDBObject) studies.get(0);
            String ref = (String) st.get("ref");
            StringBuilder alt = new StringBuilder();
            for (String s : (ArrayList<String>) st.get("alt")) {
                alt.append(s);
            }
            System.out.println(alt);
            if (varchr.startsWith("0")) {
                varchr = varchr.substring(1);
            }
            while (inner_position.startsWith("0")) {
                inner_position = inner_position.substring(1);
            }

            Variant partial = new Variant(varchr, Integer.parseInt(inner_position), ref, alt.toString());
            if(includeStats){
                VariantStats stats = new VariantStats();
                BasicDBObject mongoStats = (BasicDBObject) st.get("stats");
                stats.setMaf((float) (double) mongoStats.get("MAF"));
                stats.setMafAllele((String) mongoStats.get("allele_maf"));
                stats.setMissingGenotypes((int) mongoStats.get("missing"));
                List<org.opencb.commons.bioformats.feature.Genotype> geno = new ArrayList<>();
                for (BasicDBObject s : (List<BasicDBObject>) mongoStats.get("genotype_count")) {
                    for (String str : s.keySet()) {
                        Genotype genotype = new Genotype(str);
                        genotype.setCount((int) s.get(str));
                        geno.add(genotype);
                    }
                }
                stats.setGenotypes(geno);
                partial.setStats(stats);
            }
            result.add(partial);
        }

        queryResult.setResult(result);
        queryResult.setNumResults(result.size());
        end = System.currentTimeMillis();
        queryResult.setTime(end-start);
        return queryResult;
    }

    private List<Variant> buildVariantByRegion(String startChr, long startPosition, long stopPosition, String study, QueryOptions options) {
        List<Variant> results = new ArrayList<>();
        boolean includeSamples = options.containsKey("samples") && options.getBoolean("samples");
        boolean includeStats = options.containsKey("stats") && options.getBoolean("stats");
        boolean includeEffects = options.containsKey("effects") &&  options.getBoolean("effects");


        return results;
    }

//    private List<Variant> getRegionHBase(String startChr, long startPosition, long stopPosition, String study) {
//        Map<String, Variant> resultsMap = new HashMap<>();
//
//        try {
//            String startRow = buildRowkey(startChr, Long.toString(startPosition));
//            String stopRow = buildRowkey(startChr, Long.toString(stopPosition));
//            HTable table = new HTable(admin.getConfiguration(), tableName);
//            effectTableName = tableName + "effect";
//            HTable effectTable = new HTable(admin.getConfiguration(), effectTableName);
//            Scan region = new Scan(startRow.getBytes(), stopRow.getBytes());
//
//            // Iterate over variant and its statistics
//            for (Result result : table.getScanner(region)) {
//                String position = new String(result.getRow(), CHARSET_UTF_8);
//                String[] aux = position.split("_");
//                String inner_position = aux[1];
//                String chr = aux[0];
//                //position parsing
//                if (chr.startsWith("0")) {
//                    chr = chr.substring(1);
//                }
//                while (inner_position.startsWith("0")) {
//                    inner_position = inner_position.substring(1);
//                }
//                List<VariantFieldsProtos.VariantSample> samples = new LinkedList<>();
//                NavigableMap<byte[], byte[]> infoMap = result.getFamilyMap("i".getBytes());
//                byte[] byteStats = infoMap.get((study + "_stats").getBytes());
//                VariantFieldsProtos.VariantStats stats = VariantFieldsProtos.VariantStats.parseFrom(byteStats);
//                byte[] byteInfo = infoMap.get((study + "_data").getBytes());
//                VariantFieldsProtos.VariantInfo info = VariantFieldsProtos.VariantInfo.parseFrom(byteInfo);
//                String alternate = StringUtils.join(info.getAlternateList(), ", ");
//                String reference = info.getReference();
//                partialResult = new Variant(chr, Integer.parseInt(inner_position), reference, alternate);
//                String format = StringUtils.join(info.getFormatList(), ":");
//                NavigableMap<byte[], byte[]> sampleMap = result.getFamilyMap("d".getBytes());
//                Map<String, Map<String, String>> resultSampleMap = new HashMap<>();
//                // Set samples
//                String[] fields = format.split(":");
//                for (byte[] s : sampleMap.keySet()) {
//                    String quality = (new String(s, CHARSET_UTF_8)).replaceAll(study + "_", "");
//                    VariantFieldsProtos.VariantSample sample = VariantFieldsProtos.VariantSample.parseFrom(sampleMap.get(s));
//                    String sample1 = sample.getSample();
//                    String[] values = sample1.split(":");
//                    Map<String, String> singleSampleMap = new HashMap<>();
//                    for (int i = 0; i < fields.length; i++) {
//                        singleSampleMap.put(fields[i], values[i]);
//                    }
//                    resultSampleMap.put(quality, singleSampleMap);
//
//                }
//
//                // Set stats
//                VariantStats variantStats = new VariantStats(chr, Integer.parseInt(inner_position), reference, alternate,
//                        stats.getMaf(),
//                        stats.getMgf(),
//                        stats.getMafAllele(),
//                        stats.getMgfGenotype(),
//                        stats.getMissingAlleles(),
//                        stats.getMissingGenotypes(),
//                        stats.getMendelianErrors(),
//                        stats.getIsIndel(),
//                        stats.getCasesPercentDominant(),
//                        stats.getControlsPercentDominant(),
//                        stats.getCasesPercentRecessive(),
//                        stats.getControlsPercentRecessive());
//                partialResult.setStats(variantStats);
//                resultsMap.put(new String(result.getRow(), CHARSET_UTF_8), partialResult);
//            }
//
//            // Iterate over variant effects
//            for (Result r : effectTable.getScanner(region)) {
//                if (!r.isEmpty()) {
//                    NavigableMap<byte[], byte[]> effectMap = r.getFamilyMap("e".getBytes());
//                    partialResult = resultsMap.get(new String(r.getRow(), CHARSET_UTF_8));
//                    List<String> alts = new ArrayList<>();
//                    if (partialResult.getAlternate().length() >= 2) {
//                        for (String s : partialResult.getAlternate().split(",")) {
//                            alts.add(s);
//                        }
//                    } else {
//                        alts.add(partialResult.getAlternate());
//                    }
//                    for (String alt : alts) {
//                        String s = partialResult.getReference() + "_" + alt;
//                        if (effectMap.containsKey(s.getBytes())) {
//                            VariantEffectProtos.EffectInfo effectInfo = VariantEffectProtos.EffectInfo.parseFrom(effectMap.get(s.getBytes()));
//                            VariantEffect variantEffect = new VariantEffect(
//                                    partialResult.getChromosome(),
//                                    (int) partialResult.getPosition(),
//                                    partialResult.getReference(),
//                                    partialResult.getAlternate(),
//                                    effectInfo.getFeatureId(),
//                                    effectInfo.getFeatureName(),
//                                    effectInfo.getFeatureType(),
//                                    effectInfo.getFeatureBiotype(),
//                                    effectInfo.getFeatureChromosome(),
//                                    effectInfo.getFeatureStart(),
//                                    effectInfo.getFeatureEnd(),
//                                    effectInfo.getFeatureStrand(),
//                                    effectInfo.getSnpId(),
//                                    effectInfo.getAncestral(),
//                                    effectInfo.getAlternative(),
//                                    effectInfo.getGeneId(),
//                                    effectInfo.getTranscriptId(),
//                                    effectInfo.getGeneName(),
//                                    effectInfo.getConsequenceType(),
//                                    effectInfo.getConsequenceTypeObo(),
//                                    effectInfo.getConsequenceTypeDesc(),
//                                    effectInfo.getConsequenceTypeType(),
//                                    effectInfo.getAaPosition(),
//                                    effectInfo.getAminoacidChange(),
//                                    effectInfo.getCodonChange()
//                            );
//                            partialResult.addEffect(variantEffect);
//                            resultsMap.put(new String(r.getRow(), CHARSET_UTF_8), partialResult);
//                        }
//                    }
//                }
//            }
//        } catch (InvalidProtocolBufferException e) {
//            e.printStackTrace();
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        } catch (IOException e) {
//            e.printStackTrace();
//            System.err.println(e.getClass().getName() + ": " + e.getMessage());
//        }
//        List<Variant> results = new ArrayList<>(resultsMap.values());
//        return results;
//    }

//    public List<Variant> getRegionMongo(String chr, long startPosition, long stopPosition, String study, QueryOptions options) {
//        String startRow = buildRowkey(chr, Long.toString(startPosition));
//        String stopRow = buildRowkey(chr, Long.toString(stopPosition));
//        BasicDBObject compare = new BasicDBObject("_id", new BasicDBObject("$gte", startRow).append("$lte", stopRow)).append("studies._id", study);
//        DBCollection collection = db.getCollection("variants");
//        Iterator<DBObject> result = collection.find(compare, new BasicDBObject("studies.$", 1));
//        List<Variant> results = new ArrayList<>();
//        while (result.hasNext()) {
//            DBObject variant = result.next();
//            String position = variant.get("_id").toString();
//            String[] pos = position.split("_");
//            String varchr = pos[0];
//            String inner_position = pos[1];
//            BasicDBList studies = (BasicDBList) variant.get("studies");
//            BasicDBObject st = (BasicDBObject) studies.get(0);
//            String ref = (String) st.get("ref");
//            StringBuilder alt = new StringBuilder();
//            for (String s : (ArrayList<String>) st.get("alt")) {
//                alt.append(s);
//            }
//            System.out.println(alt);
//            if (varchr.startsWith("0")) {
//                varchr = varchr.substring(1);
//            }
//            while (inner_position.startsWith("0")) {
//                inner_position = inner_position.substring(1);
//            }
//
//            Variant partial = new Variant(varchr, Integer.parseInt(inner_position), ref, alt.toString());
//            if(includeStats){
//                VariantStats stats = new VariantStats();
//                BasicDBObject mongoStats = (BasicDBObject) st.get("stats");
//                stats.setMaf((float) (double) mongoStats.get("MAF"));
//                stats.setMafAllele((String) mongoStats.get("allele_maf"));
//                stats.setMissingGenotypes((int) mongoStats.get("missing"));
//                List<org.opencb.commons.bioformats.feature.Genotype> geno = new ArrayList<>();
//                for (BasicDBObject s : (List<BasicDBObject>) mongoStats.get("genotype_count")) {
//                    for (String str : s.keySet()) {
//                        Genotype genotype = new Genotype(str);
//                        genotype.setCount((int) s.get(str));
//                        geno.add(genotype);
//                    }
//                }
//                stats.setGenotypes(geno);
//                partial.setStats(stats);
//            }
//            results.add(partial);
//        }
//        return results;
//    }

    public List<Variant> getRecordSimpleStats(String study, int missing_gt, float maf, String maf_allele) {
        BasicDBObject compare = new BasicDBObject("studies.stats.allele_maf", maf_allele).append("studies.stats.MAF", maf).append("studies.stats.missing", missing_gt);
        List<Get> hbaseQuery = new ArrayList<>();
        DBCollection collection = db.getCollection("variants");
        Iterator<DBObject> result = collection.find(compare);
        String chromosome = new String();
        while (result.hasNext()) {
            DBObject variant = result.next();
            String position = variant.get("_id").toString();
            //hbase query construction
            Get get = new Get(position.getBytes());
            hbaseQuery.add(get);
        }
        //Complete results, from HBase

        tableName = study;
        effectTableName = tableName + "effect";
        Map<String, Variant> resultsMap = new HashMap<>();

        try {
            HTable table = new HTable(admin.getConfiguration(), tableName);
            HTable effectTable = new HTable(admin.getConfiguration(), effectTableName);
            Result[] hbaseResultEffect = effectTable.get(hbaseQuery);
            Result[] hbaseResultStats = table.get(hbaseQuery);

//            List<Variant> results = new LinkedList<>();
            for (Result r : hbaseResultStats) {
                String position = new String(r.getRow(), CHARSET_UTF_8);
                String[] aux = position.split("_");
                String inner_position = aux[1];
                String chr = aux[0];
                //position parsing
                if (chr.startsWith("0")) {
                    chr = chr.substring(1);
                }
                while (inner_position.startsWith("0")) {
                    inner_position = inner_position.substring(1);
                }
                List<VariantFieldsProtos.VariantSample> samples = new LinkedList<>();
                NavigableMap<byte[], byte[]> infoMap = r.getFamilyMap("i".getBytes());
                byte[] byteStats = infoMap.get((study + "_stats").getBytes());
                VariantFieldsProtos.VariantStats stats = VariantFieldsProtos.VariantStats.parseFrom(byteStats);
                byte[] byteInfo = infoMap.get((study + "_data").getBytes());
                VariantFieldsProtos.VariantInfo info = VariantFieldsProtos.VariantInfo.parseFrom(byteInfo);
                String alternate = StringUtils.join(info.getAlternateList(), ", ");
                String reference = info.getReference();
                partialResult = new Variant(chr, Integer.parseInt(inner_position), reference, alternate);
                String format = StringUtils.join(info.getFormatList(), ":");
                NavigableMap<byte[], byte[]> sampleMap = r.getFamilyMap("d".getBytes());
                Map<String, Map<String, String>> resultSampleMap = new HashMap<>();
//                StringBuilder sampleRaw = new StringBuilder();
                for (byte[] s : sampleMap.keySet()) {
                    String qual = (new String(s, CHARSET_UTF_8)).replaceAll(study + "_", "");
                    VariantFieldsProtos.VariantSample sample = VariantFieldsProtos.VariantSample.parseFrom(sampleMap.get(s));
                    String sample1 = sample.getSample();
                    String[] values = sample1.split(":");
                    String[] fields = format.split(":");
                    Map<String, String> singleSampleMap = new HashMap<>();
                    for (int i = 0; i < fields.length; i++) {
                        singleSampleMap.put(fields[i], values[i]);
                    }
                    resultSampleMap.put(qual, singleSampleMap);

                }
                VariantStats variantStats = new VariantStats(chromosome, Integer.parseInt(inner_position), reference, alternate,
                        stats.getMaf(),
                        stats.getMgf(),
                        stats.getMafAllele(),
                        stats.getMgfGenotype(),
                        stats.getMissingAlleles(),
                        stats.getMissingGenotypes(),
                        stats.getMendelianErrors(),
                        stats.getIsIndel(),
                        stats.getCasesPercentDominant(),
                        stats.getControlsPercentDominant(),
                        stats.getCasesPercentRecessive(),
                        stats.getControlsPercentRecessive());
                partialResult.setStats(variantStats);
                resultsMap.put(new String(r.getRow(), CHARSET_UTF_8), partialResult);
            }

            for (Result r : hbaseResultEffect) {
                if (!r.isEmpty()) {
                    NavigableMap<byte[], byte[]> effectMap = r.getFamilyMap("e".getBytes());
                    partialResult = resultsMap.get(new String(r.getRow(), CHARSET_UTF_8));
                    System.out.println("Recuperado " + partialResult.toString());
                    String s = partialResult.getReference() + "_" + partialResult.getAlternate();
                    VariantEffectProtos.EffectInfo effectInfo = VariantEffectProtos.EffectInfo.parseFrom(effectMap.get(s.getBytes()));
                    VariantEffect variantEffect = new VariantEffect(
                            partialResult.getChromosome(),
                            (int) partialResult.getPosition(),
                            partialResult.getReference(),
                            partialResult.getAlternate(),
                            effectInfo.getFeatureId(),
                            effectInfo.getFeatureName(),
                            effectInfo.getFeatureType(),
                            effectInfo.getFeatureBiotype(),
                            effectInfo.getFeatureChromosome(),
                            effectInfo.getFeatureStart(),
                            effectInfo.getFeatureEnd(),
                            effectInfo.getFeatureStrand(),
                            effectInfo.getSnpId(),
                            effectInfo.getAncestral(),
                            effectInfo.getAlternative(),
                            effectInfo.getGeneId(),
                            effectInfo.getTranscriptId(),
                            effectInfo.getGeneName(),
                            effectInfo.getConsequenceType(),
                            effectInfo.getConsequenceTypeObo(),
                            effectInfo.getConsequenceTypeDesc(),
                            effectInfo.getConsequenceTypeType(),
                            effectInfo.getAaPosition(),
                            effectInfo.getAminoacidChange(),
                            effectInfo.getCodonChange()
                    );
                    resultsMap.put(new String(r.getRow(), CHARSET_UTF_8), partialResult);
                }
            }
        } catch (InvalidProtocolBufferException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        } catch (IOException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }

        List<Variant> results = new ArrayList<>(resultsMap.values());
        return results;
    }

    @Override
    public List<VariantInfo> getRecords(Map<String, String> options) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<VariantStats> getRecordsStats(Map<String, String> options) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<VariantEffect> getEffect(Map<String, String> options) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public VariantAnalysisInfo getAnalysisInfo(Map<String, String> options) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
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
