/**
 * 
 */
package org.opencb.opencga.storage.hadoop.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfMeta;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfRecord;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
public class GenomeVariantHelper {

    private final static Logger log = LoggerFactory.getLogger(GenomeVariantHelper.class);

    private static final String CONFIG_VCF_META_PROTO_FILE = "opencga.storage.hadoop.vcf.meta.proto.file";
    private static final String CONFIG_VCF_META_PROTO_STRING = "opencga.storage.hadoop.vcf.meta.proto.string";
    private static final String CONFIG_GENOME_VARIANT_CHUNK_SIZE = "opencga.storage.hadoop.vcf.chunk_size";
    private static final String CONFIG_META_ROW_KEY = "opencga.storage.hadoop.vcf.meta.key";

    private static final String DEFAULT_META_ROW_KEY = "META";
    private static final char DEFAULT_ROWKEY_SEPARATOR = '_';
    private static final byte[] DEFAULT_COLUMN_FAMILY = Bytes.toBytes("d");
    private static Integer DEFAULT_CHUNK_SIZE = 100;

    private final VcfMeta meta;

    private final byte[] column;
    private final AtomicInteger chunkSize = new AtomicInteger(DEFAULT_CHUNK_SIZE);
    private final char separator;
    private final byte[] columnFamily;

    private final VcfRecordComparator vcfComparator = new VcfRecordComparator();

    private byte[] metaRowKey;

    public static Logger getLog() {
        return log;
    }

    /**
     * @throws IOException
     * 
     */
    public GenomeVariantHelper (Configuration conf) throws IOException {
        meta = loadMetaData(conf);
        column = Bytes.toBytes(getMeta().getFileId());
        chunkSize.set(conf.getInt(CONFIG_GENOME_VARIANT_CHUNK_SIZE, DEFAULT_CHUNK_SIZE));
        this.separator = DEFAULT_ROWKEY_SEPARATOR;
        this.columnFamily = DEFAULT_COLUMN_FAMILY;
        this.metaRowKey = Bytes.toBytes(conf.get(CONFIG_META_ROW_KEY,DEFAULT_META_ROW_KEY));
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

    public static void setMetaProtoFile (Configuration conf, String filePath) {
        conf.set(CONFIG_VCF_META_PROTO_FILE, filePath);
    }
    
    public static void setMetaRowKey(Configuration conf, String rowkey){
        conf.set(CONFIG_META_ROW_KEY, rowkey);
    }

    public static void setMetaProtoString (Configuration conf, String utfString) {
        conf.set(CONFIG_VCF_META_PROTO_STRING, utfString);
    }

    public static void setChunkSize (Configuration conf, Integer size) {
        conf.setInt(CONFIG_GENOME_VARIANT_CHUNK_SIZE, size);
    }
    
    public static byte[] getDefaultColumnFamily(){
        return DEFAULT_COLUMN_FAMILY;
    }

    public VcfMeta getMeta () {
        return meta;
    }

    public char getSeparator () {
        return separator;
    }

    public byte[] getColumn () {
        return column;
    }

    public byte[] getColumnFamily () {
        return columnFamily;
    }

    public int getChunkSize () {
        return chunkSize.get();
    }

    public VcfMeta loadMetaData (Configuration conf) throws IOException {
        try{
            // try first from String
            String protoString = conf.get(CONFIG_VCF_META_PROTO_STRING, StringUtils.EMPTY);
            if (StringUtils.isNotEmpty(protoString)) {
                getLog().info("Load Meta from PROTO string ...");
                return VcfMeta.parseFrom(ByteString.copyFromUtf8(protoString));
            }
            // Else from file
            String filePath = conf.get(CONFIG_VCF_META_PROTO_FILE, StringUtils.EMPTY);
            if (StringUtils.isNotEmpty(filePath)) {
                getLog().info(String.format("Load Meta from file %s ...", filePath));
                Path path = new Path(filePath);
                FileSystem fs = FileSystem.get(conf);
                try (FSDataInputStream instream = fs.open(path)) {
                    return VcfMeta.parseFrom(instream);
                }
            }
        }catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        throw new IllegalStateException("VCFMeta configuration missing");
    }

    public VcfSlice join (byte[] key, Iterable<Put> input) throws InvalidProtocolBufferException {
        Builder sliceBuilder = VcfSlice.newBuilder();
        boolean isFirst = true;
        List<VcfRecord> vcfRecordLst = new ArrayList<VcfRecord>();
        for (Put p : input) {
            VcfSlice slice = extractSlice(p);

            byte[] skey = generateBlockIdAsBytes(slice.getChromosome(), slice.getPosition());
            // Consistency check
            if (!Bytes.equals(skey, key)) // Address doesn't match up -> should
                                          // never happen
                throw new IllegalStateException(String.format("Row keys don't match up!!! %s != %s", Bytes.toString(key),
                        Bytes.toString(skey)));

            if (isFirst) { // init new slice
                sliceBuilder.setChromosome(slice.getChromosome()).setPosition(slice.getPosition());
                isFirst = false;
            }
            vcfRecordLst.addAll(slice.getRecordsList());
        }

        // Sort records
        Collections.sort(vcfRecordLst, getVcfComparator());

        // Add all
        sliceBuilder.addAllRecords(vcfRecordLst);
        return sliceBuilder.build();
    }

    private VcfSlice extractSlice (Put put) throws InvalidProtocolBufferException {
        List<Cell> cList = put.get(getColumnFamily(), getColumn());
        if (cList.isEmpty()) {
            throw new IllegalStateException(String.format("No data available for row % in column %s in familiy %s!!!",
                    Bytes.toString(put.getRow()), Bytes.toString(column), Bytes.toString(columnFamily)));
        }
        if (cList.size() > 1) {
            throw new IllegalStateException(String.format("One entry instead of %s expected for row %s column %s in familiy %s!!!",
                    cList.size(), Bytes.toString(put.getRow()), Bytes.toString(column), Bytes.toString(columnFamily)));
        }
        Cell cell = cList.get(0);
        
        byte[] arr = Arrays.copyOfRange(cell.getValueArray(), cell.getValueOffset(), cell.getValueOffset() + cell.getValueLength());
        VcfSlice slice = VcfSlice.parseFrom(arr);
        return slice;
    }

    private VcfRecordComparator getVcfComparator () {
        return vcfComparator;
    }

    public long getSlicePosition (long position) {
        return getChunkSize() > 0 ? position / (long) getChunkSize() : position;
    }

    public byte[] wrap (VcfRecord record) {
        return record.toByteArray();
    }

    public Put wrap (VcfSlice slice) {
        byte[] rowId = generateBlockIdAsBytes(slice.getChromosome(), (long) slice.getPosition());
        byte[] arr = slice.toByteArray();
        Put put = new Put(rowId);
        put.addColumn(getColumnFamily(), getColumn(), arr);
        return put;
    }

    public Put getMetaAsPut(){
        byte[] rowId = getMetaRowKey();
        byte[] data = getMeta().toByteArray();
        Put put = new Put(rowId);
        put.addColumn(getColumnFamily(), getColumn(), data);
        return put;
    }

    private byte[] getMetaRowKey() {
        return metaRowKey ;
    }

    /**
     * Changes the String from {@link #generateBlockId(String, long)} to bytes
     * 
     * @param chrom
     *            Chromosome
     * @param start
     *            Position
     * @return {@link Byte} array
     */
    public byte[] generateBlockIdAsBytes (String chrom, long start) {
        return Bytes.toBytes(generateBlockId(chrom, start));
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
     * @param chrom
     *            Chromosome name
     * @param position
     *            Genomic position
     * @return {@link String} Row key string
     */
    public String generateBlockId (String chrom, long position) {
        long slicePosition = getSlicePosition(position);
        StringBuilder sb = new StringBuilder(standardChromosome(chrom));
        sb.append(getSeparator());
        sb.append(String.format("%012d", slicePosition));
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
     * @param chrom
     *            Chromosome string
     * @return String chromosome name
     */
    public String standardChromosome (String chrom) {
        if (chrom.length() > 2) {
            if (chrom.substring(0, 2).equals("chr")) {
                chrom = chrom.substring(2);
            }
        } // TODO MT, X, Y, ...
        return chrom;
    }

}
