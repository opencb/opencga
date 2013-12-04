package org.opencb.opencga.storage.variant;

import com.google.protobuf.InvalidProtocolBufferException;
import com.mongodb.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.opencb.commons.bioformats.variant.Variant;
import org.opencb.commons.bioformats.variant.json.VariantAnalysisInfo;
import org.opencb.commons.bioformats.variant.json.VariantInfo;
import org.opencb.commons.bioformats.variant.utils.effect.VariantEffect;
import org.opencb.commons.bioformats.variant.utils.stats.VariantStats;

import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: jrodriguez
 * Date: 11/15/13
 * Time: 12:37 PM
 * To change this template use File | Settings | File Templates.
 */



public class VariantMonbaseQueryMaker implements VariantQueryMaker{
    private List<Scan> regionScans;
    private List<ResultScanner> regionResults;
    private List<ResultScanner> regionEffect;
    private String study;
    private String tableName;
    private String effectTableName;
    private HTable table;
    private HTable effectTable;
    private Configuration config;
    private HBaseAdmin admin;
    private MongoClient mongoClient;
    private DB database;
    private List<Get> hbaseQuery;
    private Result[] hbaseResultStats;
    private Result[] hbaseResultEffect;
    private ResultScanner effectScan;
    private ResultScanner statsScan;
    private List<Variant> results;
    private Variant partialResult;
    private Map<String, Variant> resultsMap;

    public VariantMonbaseQueryMaker(String server, String dbname){
        try{
            mongoClient = new MongoClient(server);
            DB db = mongoClient.getDB(dbname);
            //HBase connection
            Configuration config = HBaseConfiguration.create();
            config.set("hbase.master", "172.24.79.30:60010");
            config.set("hbase.zookeeper.quorum", "172.24.79.30");
            config.set("hbase.zookeeper.property.clientPort", "2181");
            admin = new HBaseAdmin(config);
            tableName = dbname;

        }catch (UnknownHostException ex) {
            Logger.getLogger(VariantVcfMonbaseDataWriter.class.getName()).log(Level.SEVERE, null, ex);
        }catch (MasterNotRunningException | ZooKeeperConnectionException ex) {
            Logger.getLogger(VariantVcfMonbaseDataWriter.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public List<Variant> getRegionHbase(String startChr, String stopChr, int startPosition, int stopPosition, String study){
        try{
            String startRow = buildRowkey(startChr, Integer.toString(startPosition));
            String stopRow = buildRowkey(stopChr, Integer.toString(stopPosition));
            table = new HTable(admin.getConfiguration(), tableName);
            effectTableName = tableName + "effect";
            effectTable = new HTable(admin.getConfiguration(), effectTableName);
            Scan region = new Scan(startRow.getBytes(), stopRow.getBytes());
            effectScan = effectTable.getScanner(region);
            statsScan = table.getScanner(region);
            for(Result r : statsScan){
                String position = new String(r.getRow(), "US-ASCII");
                String[] aux  = position.split("_");
                String inner_position = aux[1];
                String chr = aux[0];
                //position parsing
                if(chr.startsWith("0")){
                    chr = chr.substring(1);
                }
                while(inner_position.startsWith("0")){
                    inner_position = inner_position.substring(1);
                }
                List <VariantFieldsProtos.VariantSample> samples = new LinkedList<>();
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
                Map <String, Map<String, String>> resultSampleMap = new HashMap<>();
                StringBuilder sampleRaw = new StringBuilder();
                for(byte[] s : sampleMap.keySet()){
                    String qual = (new String(s, "US-ASCII")).replaceAll(study+"_","");
                    VariantFieldsProtos.VariantSample sample = VariantFieldsProtos.VariantSample.parseFrom(sampleMap.get(s));
                    String sample1 = sample.getSample();
                    String[] values = sample1.split(":");
                    String[] fields = format.split(":");
                    Map<String, String> singleSampleMap = new HashMap<>();
                    for(int i=0;i<fields.length;i++){
                        singleSampleMap.put(fields[i], values[i]);
                    }
                    resultSampleMap.put(qual, singleSampleMap);

                }
                VariantStats variantStats = new VariantStats(chr, Integer.parseInt(inner_position), reference, alternate,
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
                resultsMap.put(new String(r.getRow(), "US-ASCII"), partialResult);
            }
            for(Result r : effectScan){
                if(!r.isEmpty()){
                    NavigableMap<byte[], byte[]> effectMap = r.getFamilyMap("e".getBytes());
                    partialResult = resultsMap.get(new String(r.getRow(), "US-ASCII"));
                    System.out.println("Recuperado" + partialResult.toString());
                    String s = partialResult.getReference()+"_"+partialResult.getAlternate();
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
                    resultsMap.put(new String(r.getRow(), "US-ASCII"), partialResult);
                }
            }
        }catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }catch (IOException e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
        for(String r : resultsMap.keySet()){
            System.out.println(r);
            results.add(resultsMap.get(r));
        }
        return results;
    }

    public List<Variant> getRegionMongo(String startChr, String stopChr, int startPosition, int stopPosition, String study){
        String startRow = buildRowkey(startChr, Integer.toString(startPosition));
        String stopRow = buildRowkey(stopChr, Integer.toString(stopPosition));
        BasicDBObject compare = new BasicDBObject("_id", new BasicDBObject("$ge", startRow).append("$le", stopRow));
        DBCollection collection = database.getCollection("variants");
        Iterator<DBObject> result = collection.find(compare);
        List<Variant> results = new ArrayList<>();
        while(result.hasNext()){
            DBObject variant = result.next();
            String position = variant.get("_id").toString();
            String[] pos = position.split("_");
            String chr = pos[0];
            String inner_position = pos[1];
            String reference = (String)variant.get("ref");
            StringBuilder alt = new StringBuilder();
            for(String s: (String[])variant.get("alt")){
                alt.append(s);
            }
            if(chr.startsWith("0")){
                chr = chr.substring(1);
            }
            while(inner_position.startsWith("0")){
                inner_position = inner_position.substring(1);
            }

            Variant partial = new Variant(chr, Integer.parseInt(position), reference, alt.toString());
            results.add(partial);
        }
        return results;
    }

    public List<Variant> getRecordSimpleStats(String study, int missing_gt, float maf, String maf_allele){
        BasicDBObject compare = new BasicDBObject("studies.stats.allele_maf", maf_allele).append("studies.stats.MAF", maf).append("studies.stats.missing", missing_gt);
        hbaseQuery =   new ArrayList<>();
        DBCollection collection = database.getCollection("variants");
        Iterator<DBObject> result = collection.find(compare);
        String chromosome = new String();
        while(result.hasNext()){
            DBObject variant = result.next();
            String position = variant.get("_id").toString();
            //hbase query construction
            Get get = new Get(position.getBytes());
            hbaseQuery.add(get);
        }
        //Complete results, from HBase

        tableName = study;
        effectTableName = tableName + "effect";
        try{
            table = new HTable(admin.getConfiguration(), tableName);
            effectTable = new HTable(admin.getConfiguration(), effectTableName);
            hbaseResultEffect = effectTable.get(hbaseQuery);
            hbaseResultStats = table.get(hbaseQuery);
            resultsMap = new HashMap<>();
            results = new LinkedList<>();
            for(Result r : hbaseResultStats){
                String position = new String(r.getRow(), "US-ASCII");
                String[] aux  = position.split("_");
                String inner_position = aux[1];
                String chr = aux[0];
                //position parsing
                if(chr.startsWith("0")){
                    chr = chr.substring(1);
                }
                while(inner_position.startsWith("0")){
                    inner_position = inner_position.substring(1);
                }
                List <VariantFieldsProtos.VariantSample> samples = new LinkedList<>();
                NavigableMap<byte[], byte[]> infoMap = r.getFamilyMap("i".getBytes());
                byte[] byteStats = infoMap.get("miEstudio_stats".getBytes());
                VariantFieldsProtos.VariantStats stats = VariantFieldsProtos.VariantStats.parseFrom(byteStats);
                byte[] byteInfo = infoMap.get("miEstudio_data".getBytes());
                VariantFieldsProtos.VariantInfo info = VariantFieldsProtos.VariantInfo.parseFrom(byteInfo);
                String alternate = StringUtils.join(info.getAlternateList(), ", ");
                String reference = info.getReference();
                partialResult = new Variant(chr, Integer.parseInt(inner_position), reference, alternate);
                String format = StringUtils.join(info.getFormatList(), ":");
                NavigableMap<byte[], byte[]> sampleMap = r.getFamilyMap("d".getBytes());
                Map <String, Map<String, String>> resultSampleMap = new HashMap<>();
                StringBuilder sampleRaw = new StringBuilder();
                for(byte[] s : sampleMap.keySet()){
                    String qual = (new String(s, "US-ASCII")).replaceAll(study+"_","");
                    VariantFieldsProtos.VariantSample sample = VariantFieldsProtos.VariantSample.parseFrom(sampleMap.get(s));
                    String sample1 = sample.getSample();
                    String[] values = sample1.split(":");
                    String[] fields = format.split(":");
                    Map<String, String> singleSampleMap = new HashMap<>();
                    for(int i=0;i<fields.length;i++){
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
                resultsMap.put(new String(r.getRow(), "US-ASCII"), partialResult);
            }
            for(Result r : hbaseResultEffect){
                  if(!r.isEmpty()){
                    NavigableMap<byte[], byte[]> effectMap = r.getFamilyMap("e".getBytes());
                    partialResult = resultsMap.get(new String(r.getRow(), "US-ASCII"));
                    System.out.println("Recuperado" + partialResult.toString());
                    String s = partialResult.getReference()+"_"+partialResult.getAlternate();
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
                    resultsMap.put(new String(r.getRow(), "US-ASCII"), partialResult);
                  }
                }
            }catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
                System.err.println(e.getClass().getName() + ": " + e.getMessage());
            }catch (IOException e) {
                e.printStackTrace();
                System.err.println(e.getClass().getName() + ": " + e.getMessage());
            }
        for(String r : resultsMap.keySet()){
            System.out.println(r);
            results.add(resultsMap.get(r));
        }
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

    //Connect to mongodb
    private DB mongoConnect(String database){
        try{
            mongoClient = new MongoClient("localhost");
            DB db = mongoClient.getDB("cgonzalez");
            return db;
        }catch (UnknownHostException ex) {
            Logger.getLogger(VariantVcfMonbaseDataWriter.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
    }
}

