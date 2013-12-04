package org.opencb.opencga.storage.variant;

import com.mongodb.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.commons.bioformats.feature.Genotype;
import org.opencb.commons.bioformats.variant.VariantStudy;
import org.opencb.commons.bioformats.variant.utils.stats.*;
import org.opencb.commons.bioformats.variant.vcf4.VcfRecord;
import org.opencb.commons.bioformats.variant.utils.effect.VariantEffect;
import org.opencb.commons.bioformats.variant.vcf4.io.VariantDBWriter;


import java.io.IOException;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 * @author Jesus Rodriguez <jesusrodrc@gmail.com>
 */
public class VariantVcfMonbaseDataWriter implements VariantDBWriter<VcfRecord> {

    private String tableName;
    private String study;
    private HBaseAdmin admin;
    private HTable variantTable;
    private HTable effectTable;
    private Map<String, Put> putMap;
    private Map<String, Put> effectPutMap;
    private final byte[] info_cf = "i".getBytes();
    private final byte[] data_cf = "d".getBytes();
    private MongoClient mongoClient;
    private DB db;
    private DBCollection studyCollection;
    private DBCollection variantCollection;

    public VariantVcfMonbaseDataWriter(String tableName, String study) {
        this.tableName = tableName;
        this.study = study;
        this.putMap = new HashMap<>();
        this.effectPutMap = new HashMap<>();
    }

    @Override
    public boolean open() {
        try {
            // HBase configuration
            Configuration config = HBaseConfiguration.create();
            config.set("hbase.master", "172.24.79.30:60010");
            config.set("hbase.zookeeper.quorum", "172.24.79.30");
            config.set("hbase.zookeeper.property.clientPort", "2181");
            admin = new HBaseAdmin(config);

            // Mongo configuration
            mongoClient = new MongoClient("localhost");
            db = mongoClient.getDB(tableName);
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
            // HBase variant table creation
            if (!admin.tableExists(tableName)) {
                HTableDescriptor newTable = new HTableDescriptor(tableName.getBytes());
                // Add column family for samples
                HColumnDescriptor samplesDescriptor = new HColumnDescriptor(data_cf);
                samplesDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
                newTable.addFamily(samplesDescriptor);
                // Add column family for statistics
                HColumnDescriptor statsDescriptor = new HColumnDescriptor(info_cf);
                statsDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
                newTable.addFamily(statsDescriptor);
                // Create table
                admin.createTable(newTable);
            }
            variantTable = new HTable(admin.getConfiguration(), tableName);
            variantTable.setAutoFlush(false, true);
            // HBase effect table creation
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
            studyCollection = db.getCollection("study");
            variantCollection = db.getCollection("variants");
            
            return variantTable != null && studyCollection != null && effectTable != null;
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
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
            byte[] qualdata = (study + "_data").getBytes();
            if (putMap.get(rowkey) != null) {
                auxPut = putMap.get(rowkey);
                auxPut.add(info_cf, qualdata, info.toByteArray());
                putMap.put(rowkey, auxPut);
            } else {
                auxPut = new Put(rowkey.getBytes());
                auxPut.add(info_cf, qualdata, info.toByteArray());
                putMap.put(rowkey, auxPut);
            }

            for (String s : v.getSampleNames()) {
                VariantFieldsProtos.VariantSample.Builder sp = VariantFieldsProtos.VariantSample.newBuilder();
                sp.setSample(v.getSampleRawData(s));
                VariantFieldsProtos.VariantSample sample = sp.build();
                byte[] qual = (study + "_" + s).getBytes();
                if (putMap.get(rowkey) != null) {
                    auxPut = putMap.get(rowkey);
                    auxPut.add(data_cf, qual, sample.toByteArray());
                    putMap.put(rowkey, auxPut);
                } else {
                    auxPut = new Put(rowkey.getBytes());
                    auxPut.add(data_cf, qual, sample.toByteArray());
                    putMap.put(rowkey, auxPut);
                }
            }
        }

        // Insert into the database
        save(putMap.values(), variantTable, putMap);
        return true;
    }

    @Override
    public boolean writeVariantStats(List<VariantStats> data) {
        Put put2;
        for (VariantStats v : data) {
            String rowkey = buildRowkey(v.getChromosome(), String.valueOf(v.getPosition()));
            VariantFieldsProtos.VariantStats stats = buildStatsProto(v);
            byte[] qual = (study + "_stats").getBytes();
            put2 = new Put(Bytes.toBytes(rowkey));
            put2.add(info_cf, qual, stats.toByteArray());
            putMap.put(rowkey, put2);
            ArrayList<BasicDBObject> genotypes = new ArrayList<>();
            for(Genotype g : v.getGenotypes()){
                BasicDBObject genotype = new BasicDBObject();
                String count = g.getAllele1() + "/" + g.getAllele2();
                genotype.append(count, g.getCount());
                genotypes.add(genotype);
            }
            BasicDBObject mongoStats = new BasicDBObject("MAF", v.getMaf()).append("allele_maf",v.getMafAllele()).append("missing", v.getMissingGenotypes()).append("genotype_count", genotypes);
            BasicDBObject currentStudy = new BasicDBObject("_id", study).append("ref", v.getRefAlleles()).append("alt", v.getAltAlleles())
                    .append("stats", mongoStats);
            BasicDBObject mongoVariant = new BasicDBObject().append("$push", new BasicDBObject("studies", currentStudy));
            BasicDBObject compare = new BasicDBObject("_id",rowkey);
            WriteResult wr = variantCollection.update(compare, mongoVariant, true, false);
        }

        // Insert to Hbase
        save(putMap.values(), variantTable, putMap);
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
        for(VariantEffect v : list){
            String rowkey = buildRowkey(v.getChromosome(), String.valueOf(v.getPosition()));
            Put put2 = new Put(Bytes.toBytes(rowkey));
            VariantEffectProtos.EffectInfo effect = buildEffectProto(v);
            String qualifier = v.getReferenceAllele() + "_" + v.getAlternativeAllele();
            put2.add("e".getBytes(), qualifier.getBytes(), effect.toByteArray());
            effectPutMap.put(rowkey, put2);
        }
        //insert into database
        save(effectPutMap.values(), effectTable, effectPutMap);
        return true;
    }

    @Override
    public boolean writeStudy(VariantStudy study) {
        String timeStamp = new SimpleDateFormat("dd/mm/yyyy").format(Calendar.getInstance().getTime());
        BasicDBObjectBuilder studyMongo = new BasicDBObjectBuilder()
                .add("alias", study.getAlias())
                .add("date", timeStamp)
                .add("samples", study.getSamples())
                .add("description", study.getDescription())
                .add("sources", study.getSources());

       VariantGlobalStats global = study.getStats();
       DBObject globalStats = new BasicDBObjectBuilder()
               .add("samples_count", global.getSamplesCount())
               .add("variants_count", global.getVariantsCount())
               .add("snp_count", global.getSnpsCount())
               .add("indel_count", global.getIndelsCount())
               .add("pass_count", global.getPassCount())
               .add("transitions_count", global.getTransitionsCount())
               .add("transversions_count", global.getTransversionsCount())
               .add("biallelics_count", global.getBiallelicsCount())
               .add("multiallelics_count", global.getMultiallelicsCount())
               .add("accumulative_qualitiy", global.getAccumQuality()).get();
        studyMongo.add("global_stats", globalStats);

       Map<String,String> meta = study.getMetadata();

        DBObject metadataMongo = new BasicDBObjectBuilder()
                .add("header", meta.get("variant_file_header"))
                .get();
        studyMongo.add("metadata", metadataMongo);

        DBObject compare = new BasicDBObject("_id", study.getName());

        DBObject st = studyMongo.get();
        WriteResult wr = studyCollection.update(st, st, true, false);
        return true; // TODO Set proper return statement
    }

    @Override
    public boolean post() {
        return false;
    }

    @Override
    public boolean close() {
        try {
            admin.close();
            variantTable.close();
            effectTable.close();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            return false;
        }
    }

    /*
     * ProtocolBuffers objects construction
     */
    private VariantFieldsProtos.VariantInfo buildInfoProto(VcfRecord v) {
        String[] format = parseFormat(v.getFormat());
        String[] filter = parseFilter(v.getFilter());
        String[] infor = parseInfo(v.getInfo());
        String[] alternate = parseAlternate(v.getAlternate());
        VariantFieldsProtos.VariantInfo.Builder info = VariantFieldsProtos.VariantInfo.newBuilder();
        info.setQuality(v.getQuality());
        info.setReference(v.getReference());
        if (format != null) {
            for (String s : format) {
                info.addFormat(s);
            }
        }
        if (alternate != null) {
            for (String s : alternate) {
                info.addAlternate(s);
            }
        }
        if (filter != null) {
            for (String s : filter) {
                info.addFilters(s);
            }
        }
        if (infor != null) {
            for (String s : infor) {
                info.addInfo(s);
            }
        }
        return info.build();
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
          if(chromosome.length()>2){
            if(chromosome.substring(0,2).equals("chr")){
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

    private void save(Collection<Put> puts, HTable table, Map<String, Put> putMap) {
        try {
            table.put(new LinkedList(puts));
            putMap.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
