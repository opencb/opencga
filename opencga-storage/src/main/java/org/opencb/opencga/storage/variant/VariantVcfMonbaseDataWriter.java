package org.opencb.opencga.storage.variant;

import com.mongodb.*;

import java.io.IOException;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.commons.bioformats.feature.Genotype;
import org.opencb.commons.bioformats.variant.VariantStudy;
import org.opencb.commons.bioformats.variant.utils.effect.VariantEffect;
import org.opencb.commons.bioformats.variant.utils.stats.*;
import org.opencb.commons.bioformats.variant.vcf4.VcfRecord;
import org.opencb.commons.bioformats.variant.vcf4.io.VariantDBWriter;
import org.opencb.opencga.lib.auth.MonbaseCredentials;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 * @author Jesus Rodriguez <jesusrodrc@gmail.com>
 */
public class VariantVcfMonbaseDataWriter implements VariantDBWriter<VcfRecord> {

    private final byte[] infoColumnFamily = "i".getBytes();
    private final byte[] dataColumnFamily = "d".getBytes();
    private String tableName;
    private String studyName;

    private HBaseAdmin admin;
    private HTable variantTable;
    private HTable effectTable;
    private Map<String, Put> putMap;
    private Map<String, Set<Put>> effectPutMap;

    private MongoClient mongoClient;
    private DB db;
    private DBCollection studyCollection;
    private DBCollection variantCollection;

    private MonbaseCredentials credentials;


    public VariantVcfMonbaseDataWriter(String study, String species, MonbaseCredentials credentials) {
        if (credentials == null) {
            throw new IllegalArgumentException("Credentials for accessing the database must be specified");
        }
        this.studyName = study;
        this.tableName = species;
        this.putMap = new HashMap<>();
        this.effectPutMap = new HashMap<>();
        this.credentials = credentials;
    }

    @Override
    public boolean open() {
        try {
            // HBase configuration
            Configuration config = HBaseConfiguration.create();
            config.set("hbase.master", credentials.getHbaseMasterHost() + ":" + credentials.getHbaseMasterPort());
            config.set("hbase.zookeeper.quorum", credentials.getHbaseZookeeperQuorum());
            config.set("hbase.zookeeper.property.clientPort", String.valueOf(credentials.getHbaseZookeeperClientPort()));
            admin = new HBaseAdmin(config);

            // Mongo configuration
            MongoClientOptions.Builder mongoOptions = MongoClientOptions.builder();
            mongoOptions.autoConnectRetry(true);
            mongoClient = new MongoClient(credentials.getMongoHost());
            db = mongoClient.getDB(credentials.getMongoDbName());
        } catch (UnknownHostException ex) {
            Logger.getLogger(VariantVcfMonbaseDataWriter.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        } catch (MasterNotRunningException | ZooKeeperConnectionException ex) {
            Logger.getLogger(VariantVcfMonbaseDataWriter.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }

        return admin != null && db != null;
    }

    @Override
    public boolean pre() {
        try {
            // HBase variant table creation (one per species)
            if (!admin.tableExists(tableName)) {
                HTableDescriptor newTable = new HTableDescriptor(tableName.getBytes());
                // Add column family for samples
                HColumnDescriptor samplesDescriptor = new HColumnDescriptor(dataColumnFamily);
                samplesDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
                newTable.addFamily(samplesDescriptor);
                // Add column family for the raw main columns and statistics
                HColumnDescriptor statsDescriptor = new HColumnDescriptor(infoColumnFamily);
                statsDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
                newTable.addFamily(statsDescriptor);
                // Create table
                admin.createTable(newTable);
            }
            variantTable = new HTable(admin.getConfiguration(), tableName);
            variantTable.setAutoFlush(false, true);

            // HBase effect table creation (one per species)
            String tableEffectName = tableName + "effect";
            if (!admin.tableExists(tableEffectName)) {
                HTableDescriptor newEffectTable = new HTableDescriptor(tableEffectName.getBytes());
                // Add column family for effect
                HColumnDescriptor effectDescriptor = new HColumnDescriptor("e".getBytes());
                effectDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
                newEffectTable.addFamily(effectDescriptor);
                // Create effect table
                admin.createTable(newEffectTable);
            }
            effectTable = new HTable(admin.getConfiguration(), tableEffectName);
            effectTable.setAutoFlush(false, true);

            // Mongo collection creation
            studyCollection = db.getCollection("studies");
            variantCollection = db.getCollection("variants");

            return variantTable != null && studyCollection != null && effectTable != null && variantCollection != null;
        } catch (IOException ex) {
            Logger.getLogger(VariantVcfMonbaseDataWriter.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
    }

    @Override
    public boolean writeHeader(String string) {
        return true; //throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean writeBatch(List<VcfRecord> data) {
        // Generate the Put objects
        Put auxPut;
        for (VcfRecord v : data) {
            String rowkey = buildRowkey(v.getChromosome(), String.valueOf(v.getPosition()));
            VariantFieldsProtos.VariantInfo info = buildInfoProto(v);
            byte[] qualdata = (studyName + "_data").getBytes();
            if (putMap.get(rowkey) != null) {
                auxPut = putMap.get(rowkey);
                auxPut.add(infoColumnFamily, qualdata, info.toByteArray());
                putMap.put(rowkey, auxPut);
            } else {
                auxPut = new Put(rowkey.getBytes());
                auxPut.add(infoColumnFamily, qualdata, info.toByteArray());
                putMap.put(rowkey, auxPut);
            }

            for (String s : v.getSampleNames()) {
                VariantFieldsProtos.VariantSample.Builder sp = VariantFieldsProtos.VariantSample.newBuilder();
                sp.setSample(v.getSampleRawData(s));
                VariantFieldsProtos.VariantSample sample = sp.build();
                byte[] qual = (studyName + "_" + s).getBytes();
                if (putMap.get(rowkey) != null) {
                    auxPut = putMap.get(rowkey);
                    auxPut.add(dataColumnFamily, qual, sample.toByteArray());
                    putMap.put(rowkey, auxPut);
                } else {
                    auxPut = new Put(rowkey.getBytes());
                    auxPut.add(dataColumnFamily, qual, sample.toByteArray());
                    putMap.put(rowkey, auxPut);
                }
            }
            //Insert relationship variant-study in Mongo
            BasicDBObject query = new BasicDBObject("position", rowkey);
            query.put("studies.studyId", studyName);

            if (variantCollection.count(query) == 0) {
                // Insert relationship variant-study in Mongo
                BasicDBObject mongoStudy = new BasicDBObject("studyId", studyName).append("ref", v.getReference()).append("alt", v.getAltAlleles());
                BasicDBObject mongoVariant = new BasicDBObject().append("$addToSet", new BasicDBObject("studies", mongoStudy));
                BasicDBObject query2 = new BasicDBObject("position", rowkey);
                query2.put("chr", v.getChromosome());
                query2.put("pos", v.getPosition());
                WriteResult wr = variantCollection.update(query2, mongoVariant, true, false);
                if (!wr.getLastError().ok()) {
                    // TODO If not correct, retry?
                    return false;
                }

            }
        }

        DBObject indexPosition = new BasicDBObject("position", 1);
        DBObject indexChrPos = new BasicDBObject("chr", 1);
        indexChrPos.put("pos", 1);

        variantCollection.ensureIndex(indexPosition);
        variantCollection.ensureIndex(indexChrPos);

        return true;
    }

    @Override
    public boolean writeVariantStats(List<VariantStats> data) {
        for (VariantStats v : data) {
            String rowkey = buildRowkey(v.getChromosome(), String.valueOf(v.getPosition()));
            VariantFieldsProtos.VariantStats stats = buildStatsProto(v);
            byte[] qualifier = (studyName + "_stats").getBytes();
            Put put2 = new Put(Bytes.toBytes(rowkey));
            put2.add(infoColumnFamily, qualifier, stats.toByteArray());
            putMap.put(rowkey, put2);

            // Generate genotype counts
            ArrayList<BasicDBObject> genotypeCounts = new ArrayList<>();
            for (Genotype g : v.getGenotypes()) {
                BasicDBObject genotype = new BasicDBObject();
                String count = g.getAllele1() + "/" + g.getAllele2();
                genotype.append(count, g.getCount());
                genotypeCounts.add(genotype);
            }

            //db.variants.aggregate({$match : {position : '01_0000100000'}},
            //                      {$unwind: "$studies"},
            //                      { $match : {"studies.studyId": "testStudy1", "studies.stats" : { $exists : true}}})

            // Search for already existing study
            DBObject match = new BasicDBObject("$match", new BasicDBObject("position", rowkey));
            DBObject unwind = new BasicDBObject("$unwind", "$studies");
            DBObject match2_fields = new BasicDBObject("studies.studyId", studyName);
            match2_fields.put("studies.stats", new BasicDBObject("$exists", true));
            DBObject match2 = new BasicDBObject("$match", match2_fields);

            AggregationOutput agg_output = variantCollection.aggregate(match, unwind, match2);

            if (!agg_output.results().iterator().hasNext()) {
                // Add stats to study
                BasicDBObject mongoStats = new BasicDBObject("maf", v.getMaf()).append("alleleMaf", v.getMafAllele()).append(
                        "missing", v.getMissingGenotypes()).append("genotypeCount", genotypeCounts);
                BasicDBObject item = new BasicDBObject("studies.$.stats", mongoStats);
                BasicDBObject action = new BasicDBObject("$set", item);

                BasicDBObject query = new BasicDBObject("position", rowkey);
                query.put("studies.studyId", studyName);

                WriteResult wr = variantCollection.update(query, action, true, false);
                if (!wr.getLastError().ok()) {
                    // TODO If not correct, retry?
                    return false;
                }
             }
         }
        // Save results in HBase
        save(variantTable, putMap);
        return true;
    }

    @Override
    public boolean writeGlobalStats(VariantGlobalStats vgs) {
        return true; //throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean writeSampleStats(VariantSampleStats vss) {
        return true; //throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean writeSampleGroupStats(VariantSampleGroupStats vsgs) throws IOException {
        return true;//throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean writeVariantGroupStats(VariantGroupStats vvgs) throws IOException {
        return true;//throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean writeVariantEffect(List<VariantEffect> list) {
        Map<String, Set<String>> mongoPutMap = new HashMap<>();

        for (VariantEffect v : list) {
            String position = buildRowkey(v.getChromosome(), String.valueOf(v.getPosition()));
            VariantEffectProtos.EffectInfo effectProto = buildEffectProto(v);
            String rowkey = position + ":" + v.getReferenceAllele() + "_" + v.getAlternativeAllele();
            String qualifier = effectProto.getConsequenceTypeObo();
            //TODO Insert in the map for HBase storage
            Put effectPut = new Put(Bytes.toBytes(rowkey));
            effectPut.add("e".getBytes(), qualifier.getBytes(), effectProto.toByteArray());
            Set<Put> putSet = effectPutMap.get(rowkey);
            if(putSet==null){
                putSet = new HashSet<>();
                effectPutMap.put(rowkey, putSet);
            }
            putSet.add(effectPut);

            // Insert in the map for Mongo storage
            Set<String> positionSet = mongoPutMap.get(position);
            if (positionSet == null) {
                positionSet = new HashSet<>();
                mongoPutMap.put(position, positionSet);
            }
            positionSet.add(effectProto.getConsequenceTypeObo());
        }
        // Insert in HBase
        saveEffectHBase(effectTable, effectPutMap);

        // TODO Insert in Mongo
        saveEffectMongo(variantCollection, mongoPutMap);

        return true;
    }

    @Override
    public boolean writeStudy(VariantStudy study) {
        String timeStamp = new SimpleDateFormat("dd/mm/yyyy").format(Calendar.getInstance().getTime());
        BasicDBObject studyMongo = new BasicDBObject("name", study.getName())
                .append("alias", study.getAlias())
                .append("date", timeStamp)
                .append("authors", study.getAuthors())
                .append("samples", study.getSamples())
                .append("description", study.getDescription())
                .append("sources", study.getSources());

        VariantGlobalStats global = study.getStats();
        if (global != null) {
            DBObject globalStats = new BasicDBObject("samplesCount", global.getSamplesCount())
                    .append("variantsCount", global.getVariantsCount())
                    .append("snpCount", global.getSnpsCount())
                    .append("indelCount", global.getIndelsCount())
                    .append("passCount", global.getPassCount())
                    .append("transitionsCount", global.getTransitionsCount())
                    .append("transversionsCount", global.getTransversionsCount())
                    .append("biallelicsCount", global.getBiallelicsCount())
                    .append("multiallelicsCount", global.getMultiallelicsCount())
                    .append("accumulatedQuality", global.getAccumQuality());
            studyMongo = studyMongo.append("globalStats", globalStats);
        } else {
            // TODO Notify?
        }

        // TODO Save pedigree information

        Map<String, String> meta = study.getMetadata();
        DBObject metadataMongo = new BasicDBObjectBuilder()
                .add("header", meta.get("variantFileHeader"))
                .get();
        studyMongo = studyMongo.append("metadata", metadataMongo);

        DBObject query = new BasicDBObject("name", study.getName());
        WriteResult wr = studyCollection.update(query, studyMongo, true, false);
        return wr.getLastError().ok(); // TODO Is this a proper return statement?
    }

    @Override
    public boolean post() {
        try {
            variantTable.flushCommits();
            effectTable.flushCommits();
        } catch (IOException ex) {
            Logger.getLogger(VariantVcfMonbaseDataWriter.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }

        return true;
    }

    @Override
    public boolean close() {
        try {
            admin.close();
            variantTable.close();
            effectTable.close();
        } catch (IOException e) {
            Logger.getLogger(VariantVcfMonbaseDataWriter.class.getName()).log(Level.SEVERE, null, e);
            return false;
        }

        return true;
    }

    /*
     * ProtocolBuffers objects construction
     */
    private VariantFieldsProtos.VariantInfo buildInfoProto(VcfRecord v) {
        String[] format = parseFormat(v.getFormat());
        String[] filter = parseFilter(v.getFilter());
        String[] info = parseInfo(v.getInfo());
        String[] alternate = parseAlternate(v.getAlternate());
        VariantFieldsProtos.VariantInfo.Builder infoBuilder = VariantFieldsProtos.VariantInfo.newBuilder();
        infoBuilder.setQuality(v.getQuality());
        infoBuilder.setReference(v.getReference());
        if (format != null) {
            for (String s : format) {
                infoBuilder.addFormat(s);
            }
        }
        if (alternate != null) {
            for (String s : alternate) {
                infoBuilder.addAlternate(s);
            }
        }
        if (filter != null) {
            for (String s : filter) {
                infoBuilder.addFilters(s);
            }
        }
        if (info != null) {
            for (String s : info) {
                infoBuilder.addInfo(s);
            }
        }
        return infoBuilder.build();
    }

    private VariantFieldsProtos.VariantStats buildStatsProto(VariantStats v) {
        VariantFieldsProtos.VariantStats.Builder stats = VariantFieldsProtos.VariantStats.newBuilder();
        stats.setNumAlleles(v.getNumAlleles());
        stats.setMafAllele(v.getMafAllele());
        stats.setMgfGenotype(v.getMgfAllele());
        stats.setMaf(v.getMaf());
        stats.setMgf(v.getMgf());
        for (int a : v.getAllelesCount()) {
            stats.addAllelesCount(a);
        }
        for (int a : v.getGenotypesCount()) {
            stats.addGenotypesCount(a);
        }
        for (float a : v.getAllelesFreq()) {
            stats.addAllelesFreq(a);
        }
        for (float a : v.getGenotypesFreq()) {
            stats.addGenotypesFreq(a);
        }
        stats.setMissingAlleles(v.getMissingAlleles());
        stats.setMissingGenotypes(v.getMissingGenotypes());
        stats.setMendelianErrors(v.getMendelinanErrors());
        stats.setIsIndel(v.isIndel());
        stats.setCasesPercentDominant(v.getCasesPercentDominant());
        stats.setControlsPercentDominant(v.getControlsPercentDominant());
        stats.setCasesPercentRecessive(v.getCasesPercentRecessive());
        stats.setControlsPercentRecessive(v.getControlsPercentRecessive());
        //stats.setHardyWeinberg(v.getHw().getpValue());
        return stats.build();
    }

    private VariantEffectProtos.EffectInfo buildEffectProto(VariantEffect v) {
        VariantEffectProtos.EffectInfo.Builder effect = VariantEffectProtos.EffectInfo.newBuilder();
        effect.setReference(v.getReferenceAllele());
        effect.setAlternative(v.getAlternativeAllele());
        effect.setChromosome(v.getChromosome());
        effect.setPosition(v.getPosition());
        effect.setFeatureId(v.getFeatureId());
        effect.setFeatureName(v.getFeatureName());
        effect.setFeatureBiotype(v.getFeatureBiotype());
        effect.setFeatureChromosome(v.getFeatureChromosome());
        effect.setFeatureStart(v.getFeatureStart());
        effect.setFeatureEnd(v.getFeatureEnd());
        effect.setSnpId(v.getSnpId());
        effect.setAncestral(v.getAncestral());
        effect.setGeneId(v.getGeneId());
        effect.setTranscriptId(v.getTranscriptId());
        effect.setGeneName(v.getGeneName());
        effect.setConsequenceType(v.getConsequenceType());
        effect.setConsequenceTypeObo(v.getConsequenceTypeObo());
        effect.setConsequenceTypeDesc(v.getConsequenceTypeDesc());
        effect.setConsequenceTypeType(v.getConsequenceTypeType());
        effect.setAaPosition(v.getAaPosition());
        effect.setAminoacidChange(v.getAminoacidChange());
        effect.setCodonChange(v.getCodonChange());
        return effect.build();
    }

    /*
     * Auxiliary functions
     */
    private String[] parseFormat(String format) {
        return format.split(":");
    }

    private String[] parseFilter(String filter) {
        return filter.equals(".") ? null : filter.split(";");
    }

    private String[] parseInfo(String info) {
        return info.equals(".") ? null : info.split(";");
    }

    private String[] parseAlternate(String alternate) {
        return alternate.equals(".") ? null : alternate.split(",");
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

    private void save(HTable table, Map<String, Put> putMap) {
        try {
            table.put(new LinkedList(putMap.values()));
            putMap.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private void saveEffectHBase(HTable table, Map<String, Set<Put>> putMap) {
        try{
            List<Put> effectPutList = new LinkedList<>();
            for (Map.Entry<String, Set<Put>> entry : putMap.entrySet()) {
                effectPutList.addAll(entry.getValue());
//                for(Put put : entry.getValue()){
//                    effectPutList.add(put);
//                }
            }
            table.put(effectPutList);
            putMap.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void saveEffectMongo(DBCollection collection, Map<String, Set<String>> putMap) {
        for (Map.Entry<String, Set<String>> entry : putMap.entrySet()) {
            BasicDBObject query = new BasicDBObject("position", entry.getKey());
            query.put("studies.studyId", studyName);

            DBObject item = new BasicDBObject("studies.$.effects", entry.getValue());
            BasicDBObject action = new BasicDBObject("$set", item);

            WriteResult wr = variantCollection.update(query, action, true, false);
        }
        putMap.clear();
    }
}
