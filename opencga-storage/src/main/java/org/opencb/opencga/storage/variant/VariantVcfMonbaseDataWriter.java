package org.opencb.opencga.storage.variant;

import com.mongodb.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.opencb.commons.bioformats.variant.vcf4.VcfRecord;

import java.io.IOException;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.commons.bioformats.variant.VariantStudy;
import org.opencb.commons.bioformats.variant.vcf4.VariantEffect;
import org.opencb.commons.bioformats.variant.vcf4.io.VariantDBWriter;
import org.opencb.commons.bioformats.variant.vcf4.stats.*;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 * @author Jesus Rodriguez <jesusrodrc@gmail.com>
 */
public class VariantVcfMonbaseDataWriter implements VariantDBWriter<VcfRecord> {

    private String tableName;
    private String study;
    private HBaseAdmin admin;
    private HTable table;
    private Map<String, Put> putMap;
    private final byte[] info_cf = "i".getBytes();
    private final byte[] data_cf = "d".getBytes();
    private MongoClient mongoClient;
    private DB db;
    private DBCollection studyCollection;

    public VariantVcfMonbaseDataWriter(String tableName, String study) {
        this.tableName = tableName;
        this.study = study;
        this.putMap = new HashMap<>();
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
            db = mongoClient.getDB("cgonzalez");
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
            // HBase table creation
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
            table = new HTable(admin.getConfiguration(), tableName);
            table.setAutoFlush(false, true);
            
            // Mongo collection creation
            studyCollection = db.getCollection("study");
            
            return table != null && studyCollection != null;
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            return false;
        }
    }

    @Override
    public boolean writeHeader(String string) {
        throw new UnsupportedOperationException("Not supported yet.");
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
        save(putMap.values());
        return true;
    }

    @Override
    public boolean writeVariantStats(List<VcfVariantStat> data) {
        Put put2;
        for (VcfVariantStat v : data) {
            String rowkey = buildRowkey(v.getChromosome(), String.valueOf(v.getPosition()));
            VariantFieldsProtos.VariantStats stats = buildStatsProto(v);
            byte[] qual = (study + "_stats").getBytes();
            put2 = new Put(Bytes.toBytes(rowkey));
            put2.add(info_cf, qual, stats.toByteArray());
            putMap.put(rowkey, put2);
        }

        // Insert into the database
        save(putMap.values());
        return true;
    }

    @Override
    public boolean writeGlobalStats(VcfGlobalStat vgs) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean writeSampleStats(VcfSampleStat vss) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean writeSampleGroupStats(VcfSampleGroupStat vsgs) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean writeVariantGroupStats(VcfVariantGroupStat vvgs) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean writeVariantEffect(List<VariantEffect> list) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean writeStudy(VariantStudy study) {
//        "estudio_1" : {
//                "alias" : "e1",
//                "description" : "Mi primer estudio fabuloso",
//                "date" : "01/01/2014",
//                "authors" : [ "Nacho", "Pako", "Alex", "Jesús", "Cris" ],
//                "samples" : [ "NA001", "NA002", "NA003" ],
//                "variants" : [ "1:10000", "1:20000", "2:20000" ],
//                "pedigree" : "Fenotipo, sexo, y relaciones entre samples. Formato por definir :(",
//                "filters" : {
//                        "q10" : "Mínimo de calidad 10",
//                        "STD_FILTER" : "El filtro que se usa siempre"
//                },
//                "sources" : [ "file1.vcf", "file1.ped" ],
//                "meta" : {
//                        "Cabeceras" : "del VCF",
//                        "Otras" : "cosas"
//                },
//                "global_stats" : {
//                        "num_variants" : 500,
//                        "num_snps" : 300,
//                        "titv_ratio" : "0.6"
//                }
//        }
//        }

//        BasicDBObject doc = new BasicDBObject("name", "MongoDB").
//                              append("type", "database").
//                              append("count", 1).
//                              append("info", new BasicDBObject("x", 203).append("y", 102));
        BasicDBObject st = new BasicDBObject("name", study.getName()).append("alias", study.getAlias()).
                append("description", study.getDescription());
        String timeStamp = new SimpleDateFormat("dd/mm/yyyy").format(Calendar.getInstance().getTime());
        st.append("date", timeStamp);
        //st.append("authors", study); //Where?
        st.append("samples", study.getSamples());
        //st.append("variants", study);//use put list, if called last. 
        //List <BasicDBObject> filters = new ArrayList();
        //BasicDBObject aux = new BasicDBObject();
        //for(String s: study.getFilters()){
        //    aux = new BasicDBObject("", ""); 
        //}
        //from annot runner?
        //st.append("sources", study.getFiles());
        //st.append("meta", );
        //BasicDBObject stats = new BasicDBObject("")
        //st.append(global_stats)
        
        WriteResult wr = studyCollection.insert(st);
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
            table.close();
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

    private VariantFieldsProtos.VariantStats buildStatsProto(VcfVariantStat v) {
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

    private void save(Collection<Put> puts) {
        try {
            table.put(new LinkedList(puts));
            putMap.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
