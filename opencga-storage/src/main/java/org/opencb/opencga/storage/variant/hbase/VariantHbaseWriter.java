package org.opencb.opencga.storage.variant.hbase;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.formats.variant.vcf4.VcfUtils;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.ArchivedVariantFile;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.effect.VariantEffect;
import org.opencb.biodata.models.variant.protobuf.VariantProtos;
import org.opencb.biodata.models.variant.protobuf.VariantStatsProtos;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.utils.CryptoUtils;
import org.opencb.opencga.lib.auth.MonbaseCredentials;
import org.opencb.opencga.storage.variant.VariantDBWriter;
import org.opencb.opencga.storage.variant.VariantEffectProtos;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 * @author Jesus Rodriguez <jesusrodrc@gmail.com>
 */
public class VariantHbaseWriter extends VariantDBWriter {

    private final byte[] infoColumnFamily = "i".getBytes();
    private final byte[] dataColumnFamily = "d".getBytes();
    private String tableName;
    private VariantSource source;

    private HBaseAdmin admin;
    private HTable variantTable;
    private HTable effectTable;
    private Map<String, Put> putMap;
    private Map<String, Put> effectPutMap;

    private MonbaseCredentials credentials;

    private boolean includeStats;
    private boolean includeEffect;
    private boolean includeSamples;

    private VariantStatsToHbaseConverter statsConverter;
    

    public VariantHbaseWriter(VariantSource source, String species, MonbaseCredentials credentials) {
        this(source, species, credentials, false, false, false);
    }

    public VariantHbaseWriter(VariantSource source, String species, MonbaseCredentials credentials, 
            boolean includeSamples, boolean includeStats, boolean includeEffect) {
        if (credentials == null) {
            throw new IllegalArgumentException("Credentials for accessing the database must be specified");
        }
        this.source = source;
        this.tableName = species;
        this.putMap = new HashMap<>();
        this.effectPutMap = new HashMap<>();
        this.credentials = credentials;

        this.includeSamples = includeSamples;
        this.includeStats = includeStats;
        this.includeEffect = includeEffect;
        
        if (this.includeStats) {
            statsConverter = new VariantStatsToHbaseConverter();
        }
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
        } catch (MasterNotRunningException | ZooKeeperConnectionException ex) {
            Logger.getLogger(VariantHbaseWriter.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        } catch (IOException ex) {
            Logger.getLogger(VariantHbaseWriter.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }

        return admin != null;
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

            return variantTable != null && effectTable != null;
        } catch (IOException ex) {
            Logger.getLogger(VariantHbaseWriter.class.getName()).log(Level.SEVERE, null, ex);
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
        if (this.includeEffect) {
            buildEffectRaw(data);
        }
        buildBatchIndex(data);
        return writeBatch(data);
    }

    @Override
    protected boolean writeBatch(List<Variant> data) {
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
        } catch (IOException e) {
            return false;
        }
        return true;
    }
    
    @Override
    protected boolean buildBatchIndex(List<Variant> data) {
        return true;
    }
    
    @Override
    protected boolean buildBatchRaw(List<Variant> data) {
        // Query all variants in the batch instead of one by one
        List<Get> gets = new ArrayList<>(data.size());
        Result[] results;
        try {
            for (Variant v : data) {
                String rowkey = buildRowkey(v);
                gets.add(new Get(Bytes.toBytes(rowkey)));
            }
            results = variantTable.get(gets);
        } catch (IOException ex) {
            Logger.getLogger(VariantHbaseWriter.class.getName()).log(Level.SEVERE, "Could not retrieve variant rowkeys from database", ex);
            return false;
        }
       
        int i = 0;
        for (Variant v : data) {
            // Check that this variant was not stored yet
            if (results[i].isEmpty()) {
                String rowkey = buildRowkey(v);

                // Create raw data for inserting in HBase
                for (ArchivedVariantFile archiveFile : v.getFiles().values()) {
                    String prefix = source.getStudyId() + "_" + source.getFileId();

                    // Check that this variant IN THIS FILE was not stored yet
                    // (look for the column containing the file fields)
                    byte[] attrsBytes = Bytes.toBytes(prefix + "_" + "_attrs");
                    if (results[i].containsColumn(dataColumnFamily, attrsBytes)) {
                        continue;
                    }

                    Put auxPut = putMap.get(rowkey);
                    if (auxPut == null) {
                        auxPut = new Put(Bytes.toBytes(rowkey));
                        putMap.put(rowkey, auxPut);
                    }

                    // Global fields (chr, start, ref, alt...)
                    // chr, start, end, ref, alt, id, type, length, hgvs
                    auxPut.add(dataColumnFamily, Bytes.toBytes("chr"), Bytes.toBytes(v.getChromosome()));
                    auxPut.add(dataColumnFamily, Bytes.toBytes("start"), Bytes.toBytes(v.getStart()));
                    auxPut.add(dataColumnFamily, Bytes.toBytes("end"), Bytes.toBytes(v.getEnd()));
                    auxPut.add(dataColumnFamily, Bytes.toBytes("length"), Bytes.toBytes(v.getLength()));
                    auxPut.add(dataColumnFamily, Bytes.toBytes("ref"), Bytes.toBytes(v.getReference()));
                    auxPut.add(dataColumnFamily, Bytes.toBytes("alt"), Bytes.toBytes(v.getAlternate()));
                    auxPut.add(dataColumnFamily, Bytes.toBytes("id"), Bytes.toBytes(v.getId()));
                    auxPut.add(dataColumnFamily, Bytes.toBytes("type"), Bytes.toBytes(v.getType().ordinal()));
                    // TODO How are we going to store HGVS really? It is available in VEP
//                    auxPut.add(dataColumnFamily, Bytes.toBytes("hgvs"), Bytes.toBytes(v.getHgvs()));

                    // Attributes that vary depending on the input format
                    VariantProtos.VariantFileAttributes attrs = buildAttributesProto(v, archiveFile);
                    auxPut.add(dataColumnFamily, attrsBytes, attrs.toByteArray());

                    if (includeSamples) {
                        for (String s : archiveFile.getSampleNames()) {
                            VariantProtos.VariantSample.Builder sp = VariantProtos.VariantSample.newBuilder();
                            sp.setSample(VcfUtils.getJoinedSampleFields(v, archiveFile, s));
                            byte[] qualifier = Bytes.toBytes(prefix + "_" + s);
                            auxPut.add(dataColumnFamily, qualifier, sp.build().toByteArray());
                        }
                    }
                    
                    if (includeStats) {
                        VariantStatsProtos.VariantStats protoStats = statsConverter.convertToStorageType(archiveFile.getStats());
                        byte[] qualifier = Bytes.toBytes(prefix + "_stats");
                        auxPut.add(dataColumnFamily, qualifier, protoStats.toByteArray());
                    }
                }
            } else {
                Logger.getLogger(VariantHbaseWriter.class.getName()).log(Level.WARNING, 
                            "Variant already existed: {0}:{1}", new Object[]{v.getChromosome(), v.getStart()});
            }
        }

        return true;
    }
    
//    @Override
//    protected boolean buildStatsRaw(List<Variant> data) {
//        for (Variant v : data) {
//            for (ArchivedVariantFile archiveFile : v.getFiles().values()) {
//                VariantStats s = archiveFile.getStats();
//                VariantStatsProtos.VariantStats stats = buildStatsProto(s);
//                
//                String rowkey = buildRowkey(v);
//                Put put2 = putMap.get(rowkey);
//                if (put2 != null) { // This variant is not being processed
//                    String prefix = source.getStudyId() + "_" + source.getFileId();
//                    byte[] qualifier = Bytes.toBytes(prefix + "_stats");
//                    put2.add(dataColumnFamily, qualifier, stats.toByteArray());
//                }
//            }
//        }
//
//        return true;
//    }
    
    @Override
    protected boolean buildEffectRaw(List<Variant> variants) {
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
    
    @Override
    public boolean post() {
        try {
            variantTable.flushCommits();
            effectTable.flushCommits();
        } catch (IOException ex) {
            Logger.getLogger(VariantHbaseWriter.class.getName()).log(Level.SEVERE, null, ex);
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
            Logger.getLogger(VariantHbaseWriter.class.getName()).log(Level.SEVERE, null, e);
            return false;
        }

        return true;
    }

    /*
     * ProtocolBuffers objects construction
     */
    
    private VariantProtos.VariantFileAttributes buildAttributesProto(Variant v, ArchivedVariantFile file) {
        VariantProtos.VariantFileAttributes.Builder builder = VariantProtos.VariantFileAttributes.newBuilder();
        
        for (Map.Entry<String, String> attr : file.getAttributes().entrySet()) {
            VariantProtos.VariantFileAttributes.KeyValue.Builder kvBuilder = VariantProtos.VariantFileAttributes.KeyValue.newBuilder();
            kvBuilder.setKey(attr.getKey());
            kvBuilder.setValue(attr.getValue());
            builder.addAttrs(kvBuilder.build());
        }
        
        return builder.build();
    }

    private VariantStatsProtos.VariantStats buildStatsProto(VariantStats v) {
        VariantStatsProtos.VariantStats.Builder builder = VariantStatsProtos.VariantStats.newBuilder();
        
        builder.setRefAlleleCount(v.getRefAlleleCount());
        builder.setAltAlleleCount(v.getAltAlleleCount());
        for (Map.Entry<Genotype, Integer> count : v.getGenotypesCount().entrySet()) {
            VariantStatsProtos.VariantStats.Count.Builder countBuilder = VariantStatsProtos.VariantStats.Count.newBuilder();
            countBuilder.setKey(count.getKey().toString());
            countBuilder.setCount(count.getValue());
            builder.addGenotypesCount(countBuilder.build());
        }

        builder.setRefAlleleFreq(v.getRefAlleleFreq());
        builder.setAltAlleleFreq(v.getAltAlleleFreq());
        for (Map.Entry<Genotype, Float> freq : v.getGenotypesFreq().entrySet()) {
            VariantStatsProtos.VariantStats.Frequency.Builder countBuilder = VariantStatsProtos.VariantStats.Frequency.newBuilder();
            countBuilder.setKey(freq.getKey().toString());
            countBuilder.setFrequency(freq.getValue());
            builder.addGenotypesFreq(countBuilder.build());
        }

        builder.setMissingAlleles(v.getMissingAlleles());
        builder.setMissingGenotypes(v.getMissingGenotypes());
        
        builder.setMaf(v.getMaf());
        builder.setMgf(v.getMgf());
        
        builder.setPassedFilters(v.hasPassedFilters());
        
        builder.setQuality(v.getQuality());
        
        builder.setNumSamples(v.getNumSamples());
        
        builder.setTransitionsCount(v.getTransitionsCount());
        builder.setTransversionsCount(v.getTransversionsCount());

        if (v.isPedigreeStatsAvailable()) { // Optional fields, they require pedigree information
            builder.setMendelianErrors(v.getMendelianErrors());
            
            builder.setCasesPercentDominant(v.getCasesPercentDominant());
            builder.setControlsPercentDominant(v.getControlsPercentDominant());
            builder.setCasesPercentRecessive(v.getCasesPercentRecessive());
            builder.setControlsPercentRecessive(v.getControlsPercentRecessive());
            
//            builder.setHardyWeinberg(effect.getHw().getpValue());
        }
        return builder.build();
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
    
    private String buildRowkey(Variant v) {
        StringBuilder builder = new StringBuilder();
        builder.append(StringUtils.leftPad(v.getChromosome(), 4, '0'));
        builder.append("_");
        builder.append(v.getStart());
        builder.append("_");
        if (v.getReference().length() < Variant.SV_THRESHOLD) {
            builder.append(v.getReference());
        } else {
            builder.append(new String(CryptoUtils.encryptSha1(v.getReference())));
        }
        
        builder.append("_");
        
        if (v.getAlternate().length() < Variant.SV_THRESHOLD) {
            builder.append(v.getAlternate());
        } else {
            builder.append(new String(CryptoUtils.encryptSha1(v.getAlternate())));
        }
            
        return builder.toString();
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
