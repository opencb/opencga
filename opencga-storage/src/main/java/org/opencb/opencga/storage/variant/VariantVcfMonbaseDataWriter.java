package org.opencb.opencga.storage.variant;

import com.mongodb.*;
import java.io.IOException;
import java.net.UnknownHostException;
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
import org.opencb.commons.bioformats.variant.Variant;
import org.opencb.commons.bioformats.variant.VariantFactory;
import org.opencb.commons.bioformats.variant.utils.effect.VariantEffect;
import org.opencb.commons.bioformats.variant.utils.stats.VariantStats;
import org.opencb.opencga.lib.auth.MonbaseCredentials;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 * @author Jesus Rodriguez <jesusrodrc@gmail.com>
 */
public class VariantVcfMonbaseDataWriter extends VariantDBWriter {

    private final byte[] infoColumnFamily = "i".getBytes();
    private final byte[] dataColumnFamily = "d".getBytes();
    private String tableName;
    private String studyName;

    private HBaseAdmin admin;
    private HTable variantTable;
    private HTable effectTable;
    private Map<String, Put> putMap;
    private Map<String, Put> effectPutMap;
    private Map<String, BasicDBObject> mongoMap;

    private MongoClient mongoClient;
    private DB db;
    private DBCollection studyCollection;
    private DBCollection variantCollection;

    private MonbaseCredentials credentials;

    private boolean includeStats;
    private boolean includeEffect;
    private boolean includeSamples;


    public VariantVcfMonbaseDataWriter(String study, String species, MonbaseCredentials credentials) {
        if (credentials == null) {
            throw new IllegalArgumentException("Credentials for accessing the database must be specified");
        }
        this.studyName = study;
        this.tableName = species;
        this.putMap = new HashMap<>();
        this.effectPutMap = new HashMap<>();
        this.mongoMap = new HashMap<>();
        this.credentials = credentials;

        this.includeEffect(false);
        this.includeStats(false);
        this.includeSamples(false);
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
    public boolean write(Variant variant) {
        return write(Arrays.asList(variant));
    }

    @Override
    public boolean write(List<Variant> data) {
        buildBatchRaw(data);
        buildStatsRaw(data);
        buildEffectRaw(data);
        buildBatchIndex(data);
        return writeBatch(data);
    }

    @Override
    boolean writeBatch(List<Variant> data) {
        // TODO Better error checking! Probably doing more variant-by-variant inserts
        try {
            // Insert raw variant data
            // TODO Track which ones were successful
            variantTable.put(new LinkedList(putMap.values()));
            putMap.clear();
            
            // Insert effect raw data
            // TODO Track which ones were successful
            effectTable.put(new LinkedList(effectPutMap.values()));
            effectPutMap.clear();
            
            // Insert indexes
            // TODO Track which ones were successful
            for (Variant v : data) {
                String rowkey = buildRowkey(v.getChromosome(), String.valueOf(v.getPosition()));
                BasicDBObject mongoStudy = mongoMap.get(rowkey);
                BasicDBObject mongoVariant = new BasicDBObject().append("$push", new BasicDBObject("studies", mongoStudy));
                BasicDBObject query = new BasicDBObject("position", rowkey);
                WriteResult wr = variantCollection.update(query, mongoVariant, true, false);
                if (!wr.getLastError().ok()) {
                    // TODO If not correct, retry?
                    return false;
                }
            }
            mongoMap.clear();
        } catch (IOException e) {
            return false;
        }
        return true;
    }
    
    @Override
    boolean buildBatchIndex(List<Variant> data) {
        for (Variant v : data) {
            String rowkey = buildRowkey(v.getChromosome(), String.valueOf(v.getPosition()));
            
            // Check that this relationship was not established yet
            BasicDBObject query = new BasicDBObject("position", rowkey);
            query.put("studies.studyId", studyName);

            if (variantCollection.count(query) == 0) {
                // Create relationship variant-study for inserting in Mongo
                BasicDBObject mongoStudy = new BasicDBObject("studyId", studyName).append("ref", v.getReference()).append("alt", v.getAltAlleles());
                
                // Add stats to study
                VariantStats stats = v.getStats();
                if (stats != null) {
                    // Generate genotype counts
                    ArrayList<BasicDBObject> genotypeCounts = new ArrayList<>();
                    for (Genotype g : stats.getGenotypes()) {
                        BasicDBObject genotype = new BasicDBObject();
                        String count = g.getAllele1() + "/" + g.getAllele2();
                        genotype.append(count, g.getCount());
                        genotypeCounts.add(genotype);
                    }
                    
                    BasicDBObject mongoStats = new BasicDBObject("maf", stats.getMaf()).append("alleleMaf", stats.getMafAllele()).append(
                            "missing", stats.getMissingGenotypes()).append("genotypeCount", genotypeCounts);
                    mongoStudy.put("stats", mongoStats);
                }
                
                // Add effects to study
                if (!v.getEffect().isEmpty()) {
                    Set<String> effectsSet = new HashSet<>();
                    for (VariantEffect effect : v.getEffect()) {
                        VariantEffectProtos.EffectInfo effectProto = buildEffectProto(effect);
                        effectsSet.add(effectProto.getConsequenceTypeObo());
                    }
                
                    mongoStudy.put("effects", effectsSet);
                }
                
                mongoMap.put(rowkey, mongoStudy);
                
            } else {
                // TODO What if there is the same position already?
            }
        }
        return true;
    }
    
    @Override
    boolean buildBatchRaw(List<Variant> data) {
        for (Variant v : data) {
            String rowkey = buildRowkey(v.getChromosome(), String.valueOf(v.getPosition()));
            
            // Check that this relationship was not established yet
            BasicDBObject query = new BasicDBObject("position", rowkey);
            query.put("studies.studyId", studyName);

            if (variantCollection.count(query) == 0) {
                // Create raw data for inserting in HBase
                Put auxPut;
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
                    sp.setSample(VariantFactory.getVcfSampleRawData(v, s));
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
            } else {
                // TODO What if there is the same position already?
            }
        }

        return true;
    }
    
    @Override
    boolean buildStatsRaw(List<Variant> data) {
        for (Variant var : data) {
            VariantStats v = var.getStats();
            if (v == null) {
                continue;
            }

            String rowkey = buildRowkey(v.getChromosome(), String.valueOf(v.getPosition()));
            VariantFieldsProtos.VariantStats stats = buildStatsProto(v);
            byte[] qualifier = (studyName + "_stats").getBytes();
            Put put2 = putMap.get(rowkey); // Modify existing Put object
            if (put2 == null) {
                put2 = new Put(Bytes.toBytes(rowkey));
            }
            put2.add(infoColumnFamily, qualifier, stats.toByteArray());
            putMap.put(rowkey, put2);
        }

        return true;
    }
    
    @Override
    boolean buildEffectRaw(List<Variant> variants) {
//        for (Variant variant : variants) {
//            for (VariantEffect v : variant.getEffect()) {
//                String rowkey = buildRowkey(v.getChromosome(), String.valueOf(v.getPosition()));
//                VariantEffectProtos.EffectInfo effectProto = buildEffectProto(v);
//                String qualifier = v.getReferenceAllele() + "_" + v.getAlternativeAllele();
//
//                // TODO Insert in the map for HBase storage
//                //            Put effectPut = new Put(Bytes.toBytes(rowkey));
//                //            effectPut.add("e".getBytes(), qualifier.getBytes(), effectProto.toByteArray());
//                //            effectPutMap.put(rowkey, effectPut);
//            }
//        }

        return true;
    }
    

///*
//      @Override
//      public boolean writeGlobalStats(VariantGlobalStats vgs) {
//          return true; //throw new UnsupportedOperationException("Not supported yet.");
//      }
//
//      @Override
//      public boolean writeSampleStats(VariantSampleStats vss) {
//          return true; //throw new UnsupportedOperationException("Not supported yet.");
//      }
//
//      @Override
//      public boolean writeSampleGroupStats(VariantSampleGroupStats vsgs) throws IOException {
//          return true;//throw new UnsupportedOperationException("Not supported yet.");
//      }
//
//      @Override
//      public boolean writeVariantGroupStats(VariantGroupStats vvgs) throws IOException {
//          return true;//throw new UnsupportedOperationException("Not supported yet.");
//      }
//      */
//
//    boolean writeVariantEffect(List<Variant> variants) {
//        Map<String, Set<String>> mongoPutMap = new HashMap<>();
//
//        for (Variant variant : variants) {
//            for (VariantEffect effect : variant.getEffect()) {
//                String rowkey = buildRowkey(effect.getChromosome(), String.valueOf(effect.getPosition()));
//                VariantEffectProtos.EffectInfo effectProto = buildEffectProto(effect);
//                String qualifier = effect.getReferenceAllele() + "_" + effect.getAlternativeAllele();
//
//                // TODO Insert in the map for HBase storage
//                //            Put effectPut = new Put(Bytes.toBytes(rowkey));
//                //            effectPut.add("e".getBytes(), qualifier.getBytes(), effectProto.toByteArray());
//                //            effectPutMap.put(rowkey, effectPut);
//
//                // Insert in the map for Mongo storage
//                Set<String> effectsSet = mongoPutMap.get(rowkey);
//                if (effectsSet == null) {
//                    effectsSet = new HashSet<>();
//                    mongoPutMap.put(rowkey, effectsSet);
//                }
//                effectsSet.add(effectProto.getConsequenceTypeObo());
//            }
//        }
//        // Insert in HBase
//        save(effectPutMap.values(), effectTable, effectPutMap);
//
//        // TODO Insert in Mongo
//        saveEffectMongo(variantCollection, mongoPutMap);
//
//        return true;
//    }
//
//    /*
//          @Override
//          public boolean writeStudy(VariantStudy study) {
//              String timeStamp = new SimpleDateFormat("dd/mm/yyyy").format(Calendar.getInstance().getTime());
//              BasicDBObject studyMongo = new BasicDBObject("name", study.getName())
//                      .append("alias", study.getAlias())
//                      .append("date", timeStamp)
//                      .append("authors", study.getAuthors())
//                      .append("samples", study.getSamples())
//                      .append("description", study.getDescription())
//                      .append("sources", study.getSources());
//
//              VariantGlobalStats global = study.getStats();
//              if (global != null) {
//                  DBObject globalStats = new BasicDBObject("samplesCount", global.getSamplesCount())
//                          .append("variantsCount", global.getVariantsCount())
//                          .append("snpCount", global.getSnpsCount())
//                          .append("indelCount", global.getIndelsCount())
//                          .append("passCount", global.getPassCount())
//                          .append("transitionsCount", global.getTransitionsCount())
//                          .append("transversionsCount", global.getTransversionsCount())
//                          .append("biallelicsCount", global.getBiallelicsCount())
//                          .append("multiallelicsCount", global.getMultiallelicsCount())
//                          .append("accumulatedQuality", global.getAccumQuality());
//                  studyMongo = studyMongo.append("globalStats", globalStats);
//              } else {
//                  // TODO Notify?
//              }
//
//              // TODO Save pedigree information
//
//              Map<String, String> meta = study.getMetadata();
//              DBObject metadataMongo = new BasicDBObjectBuilder()
//                      .add("header", meta.get("variantFileHeader"))
//                      .get();
//              studyMongo = studyMongo.append("metadata", metadataMongo);
//
//              DBObject query = new BasicDBObject("name", study.getName());
//              WriteResult wr = studyCollection.update(query, studyMongo, true, false);
//              return wr.getLastError().ok(); // TODO Is this a proper return statement?
//          }
//      */
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
    private VariantFieldsProtos.VariantInfo buildInfoProto(Variant v) {
        String[] format = parseFormat(v.getFormat());
        String[] filter = parseFilter(v.getAttribute("FILTER"));
        String[] info = parseInfo(VariantFactory.getVcfInfo(v));
        String[] alternate = parseAlternate(v.getAlternate());
        VariantFieldsProtos.VariantInfo.Builder infoBuilder = VariantFieldsProtos.VariantInfo.newBuilder();
        infoBuilder.setQuality(v.getAttribute("QUAL"));
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
        //stats.setHardyWeinberg(effect.getHw().getpValue());
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

    @Override
    public void includeStats(boolean b) {
        this.includeStats = b;
    }

    @Override
    public void includeSamples(boolean b) {
        this.includeSamples = b;
    }

    @Override
    public void includeEffect(boolean b) {
        this.includeEffect = b;
    }

}
