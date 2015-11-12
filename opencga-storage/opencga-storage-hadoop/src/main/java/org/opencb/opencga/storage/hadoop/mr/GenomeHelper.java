/**
 * 
 */
package org.opencb.opencga.storage.hadoop.mr;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
public class GenomeHelper {
    private final static Logger log = LoggerFactory.getLogger(GenomeHelper.class);

    protected static final String CONFIG_VCF_META_PROTO_FILE = "opencga.storage.hadoop.vcf.meta.proto.file";
    protected static final String CONFIG_VCF_META_PROTO_STRING = "opencga.storage.hadoop.vcf.meta.proto.string";
//    protected static final String CONFIG_VCF_META_PROTO_TABLE = "opencga.storage.hadoop.vcf.meta.proto.string";
    protected static final String CONFIG_GENOME_VARIANT_CHUNK_SIZE = "opencga.storage.hadoop.vcf.chunk_size";
    protected static final String CONFIG_GENOME_VARIANT_COLUMN_FAMILY = "opencga.storage.hadoop.vcf.column_family";
    protected static final String CONFIG_GENOME_VARIANT_ROW_KEY_SEP = "opencga.storage.hadoop.vcf.row_key_sep";
    protected static final String CONFIG_META_ROW_KEY = "opencga.storage.hadoop.vcf.meta.key";

    protected static final String DEFAULT_META_ROW_KEY = "_METADATA";
    protected static final String DEFAULT_ROWKEY_SEPARATOR = "_";
    protected static final String DEFAULT_COLUMN_FAMILY = "d";
    protected static Integer DEFAULT_CHUNK_SIZE = 100;

    private final AtomicInteger chunkSize = new AtomicInteger(DEFAULT_CHUNK_SIZE);
    private final char separator;
    private final byte[] columnFamily;
    private byte[] metaRowKey;

    private final Configuration conf;

    
    public interface MetadataAction<T>{
        T parse(InputStream is) throws IOException;
    }

    /**
     *
     */
    public GenomeHelper (Configuration conf) {
        this.conf = conf;
        this.separator = conf.get(CONFIG_GENOME_VARIANT_ROW_KEY_SEP, DEFAULT_ROWKEY_SEPARATOR).charAt(0);
        this.columnFamily = Bytes.toBytes(conf.get(CONFIG_GENOME_VARIANT_COLUMN_FAMILY,DEFAULT_COLUMN_FAMILY));
        this.metaRowKey = Bytes.toBytes(conf.get(CONFIG_META_ROW_KEY,DEFAULT_META_ROW_KEY));
        this.chunkSize.set(conf.getInt(CONFIG_GENOME_VARIANT_CHUNK_SIZE, DEFAULT_CHUNK_SIZE));
    }

    /**
     *
     */
    public GenomeHelper (GenomeHelper other) {
        this(other.getConf());
    }

    public static Logger getLog() {
        return log;
    }
    
    private Configuration getConf() {
        return conf;
    }

    public static String printClassJarPath(Class<?> clazz) {
        StringBuilder sb = new StringBuilder();
        String nl = "\n";
        sb.append(clazz.getProtectionDomain().getCodeSource().getLocation()).append(nl);
        sb.append(clazz.getResource('/' + clazz.getName().replace('.', '/') + ".class")).append(nl);
        return sb.toString();
    }

    public static String printSystemProperties() {
        StringBuilder sb = new StringBuilder();
        String nl = "\n";
        System.getProperties().forEach((a, b) -> sb.append(a + " - " + b).append(nl));
        return sb.toString();
    }

    public static String printConfig(Configuration conf) {
        StringBuilder sb = new StringBuilder();
        String nl = "\n";
        conf.iterator().forEachRemaining(e -> sb.append(e.getKey() + " - " + e.getValue()).append(nl));
        return sb.toString();
    }
    
//    public static byte[] getDefaultColumnFamily(){
//        return DEFAULT_COLUMN_FAMILY;
//    }
    
    public static void setChunkSize (Configuration conf, Integer size) {
        conf.setInt(CONFIG_GENOME_VARIANT_CHUNK_SIZE, size);
    }
    
    public static void setMetaRowKey(Configuration conf, String rowkey){
        conf.set(CONFIG_META_ROW_KEY, rowkey);
    }

    public static void setMetaProtoString (Configuration conf, String utfString) {
        conf.set(CONFIG_VCF_META_PROTO_STRING, utfString);
    }

    public char getSeparator () {
        return separator;
    }

    public byte[] getColumnFamily () {
        return columnFamily;
    }

    public <T> T loadMetaData (Configuration conf, MetadataAction<T> action) throws IOException {
        try{
            // try first from String
            String protoString = conf.get(CONFIG_VCF_META_PROTO_STRING, StringUtils.EMPTY);
            if (StringUtils.isNotEmpty(protoString)) {
                getLog().info("Load Meta from PROTO string ...");
                T obj = action.parse(new ByteArrayInputStream(ByteString.copyFromUtf8(protoString).toByteArray()));
                return obj;
            }
            // Else from file
            String filePath = conf.get(CONFIG_VCF_META_PROTO_FILE, StringUtils.EMPTY);
            if (StringUtils.isNotEmpty(filePath)) {
                getLog().info(String.format("Load Meta from file %s ...", filePath));
                Path path = new Path(URI.create(filePath));
                FileSystem fs = FileSystem.get(conf);
                try (FSDataInputStream instream = fs.open(path)) {
                    return action.parse(instream);
                }
            }
        }catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        throw new IllegalStateException("VCFMeta configuration missing");
    }

    public int getChunkSize () {
        return chunkSize.get();
    }

    public long getSlicePosition (long position) {
        return getChunkSize() > 0 ? position / (long) getChunkSize() : position;
    }
    
    public long getStartPositionFromSlice(long slice){
        return slice * (long)getChunkSize();
    }

    public byte[] getMetaRowKey() {
        return metaRowKey ;
    }

    /**
     * Generates a Row key based on Chromosome and position adjusted for the
     * Chunk size <br>
     * <ul>
     * <li>Using {@link #standardChromosome(String)} to get standard chromosome
     * name
     * <li>Using {@link #getSlicePosition(long)} to return slice position
     * <ul>
     * e.g. using chunk size 100, separator _ with chr2 and 1234 would result in
     * 2_12
     *
     * @param chrom    Chromosome name
     * @param position Genomic position
     * @return {@link String} Row key string
     */
    protected String generateBlockId(String chrom, long position) {
        long slicePosition = getSlicePosition(position);
        StringBuilder sb = new StringBuilder(standardChromosome(chrom));
        sb.append(getSeparator());
        sb.append(String.format("%012d", slicePosition));
        return sb.toString();
    }

    /**
     * Changes the String from {@link #generateBlockId(String, long)} to bytes
     *
     * @param chrom Chromosome
     * @param start Position
     * @return {@link Byte} array
     */
    public byte[] generateBlockIdAsBytes(String chrom, long start) {
        return Bytes.toBytes(generateBlockId(chrom, start));
    }

    /**
     * Generates a Row key based on Chromosome, position, ref and alt <br>
     * <ul>
     * <li>Using {@link #standardChromosome(String)} to get standard chromosome
     * name
     * <ul>
     *
     * @param chrom    Chromosome name
     * @param position Genomic position
     * @param ref      Reference name
     * @param ref      Alt name
     * @return {@link String} Row key string
     */
    protected String generateVcfRowId(String chrom, long position, String ref, String alt) {
        StringBuilder sb = new StringBuilder(standardChromosome(chrom));
        sb.append(getSeparator());
        sb.append(String.format("%012d", position));
        sb.append(getSeparator());
        sb.append(ref);
        sb.append(getSeparator());
        sb.append(alt);
        return sb.toString();
    }

    public String extractChromosomeFromBlockId (String blockId) {
        return splitBlockId(blockId)[0];
    }

    public Long extractPositionFromBlockId (String blockId) {
        return Long.valueOf(splitBlockId(blockId)[1]);
    }

    public String[] splitBlockId (String blockId) {
        char sep = getSeparator();
        String[] split = StringUtils.split(blockId, sep);
        if (split.length != 2)
            throw new IllegalStateException(String.format("Block ID is not valid - exected 2 blocks separaed by `%s`; value `%s`", sep,
                    blockId));
        return split;
    }

    /**
     * Creates a standard chromosome name from the provided string
     *
     * @param chrom Chromosome string
     * @return String chromosome name
     */
    public String standardChromosome(String chrom) {
        if (chrom.length() > 2) {
            if (chrom.substring(0, 2).equals("chr")) {
                chrom = chrom.substring(2);
            }
        } // TODO MT, X, Y, ...
        return chrom;
    }
    
    public <T extends MessageLite> Put wrapAsPut(byte[] column,byte[] row,T meta){
        byte[] data = meta.toByteArray();
        Put put = new Put(row);
        put.addColumn(getColumnFamily(), column, data);
        return put;
    }

    public <T extends MessageLite> Put wrapMetaAsPut(byte[] column, T meta){
        return wrapAsPut(column,getMetaRowKey(),meta);
    }

    @FunctionalInterface
    interface HBaseTableConsumer<T>{
        void accept(Table table) throws IOException;
    }
    @FunctionalInterface
    interface HBaseTableFunction<T>{
        T function(Table table) throws IOException;
    }
    @FunctionalInterface
    interface HBaseTableAdminFunction<T>{
        T function(Table table, Admin admin) throws IOException;
    }
    
    public void act(byte[] tablename, HBaseTableConsumer<Table> func) throws IOException{
        TableName tname = TableName.valueOf(tablename);
        try (
                Connection con = ConnectionFactory.createConnection(getConf());
                Table table = con.getTable(tname);
        ) {
            func.accept(table);
        }
    }

    public <T> T actOnTable(String tablename, HBaseTableFunction<T> func) throws IOException {
        return actOnTable(Bytes.toBytes(tablename), func);
    }

    public <T> T actOnTable(byte[] tablename, HBaseTableFunction<T> func) throws IOException {
        TableName tname = TableName.valueOf(tablename);
        try (
                Connection con = ConnectionFactory.createConnection(getConf());
                Table table = con.getTable(tname);
        ) {
            return func.function(table);
        }
    }
    public <T> T actOnTable(String tablename, HBaseTableAdminFunction<T> func) throws IOException {
        TableName tname = TableName.valueOf(tablename);
        try (
                Connection con = ConnectionFactory.createConnection(getConf());
                Table table = con.getTable(tname);
                Admin admin = con.getAdmin();
        ) {
            return func.function(table,admin);
        }
    }
}
