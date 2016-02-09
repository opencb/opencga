/**
 *
 */
package org.opencb.opencga.storage.hadoop.variant;

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PVarchar;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk.
 */
public class GenomeHelper {
    private final Logger log = LoggerFactory.getLogger(GenomeHelper.class);

    @Deprecated
    public static final String CONFIG_VCF_META_PROTO_FILE = "opencga.storage.hadoop.vcf.meta.proto.file";
    @Deprecated
    public static final String CONFIG_VCF_META_PROTO_STRING = "opencga.storage.hadoop.vcf.meta.proto.string";

    public static final String CONFIG_FILE_ID = "opencga.storage.hadoop.file_id";
    public static final String OPENCGA_STORAGE_HADOOP_STUDY_ID = "opencga.storage.hadoop.study.id";
    public static final String CONFIG_ARCHIVE_TABLE = "opencga.storage.hadoop.archive_table";

    public static final String CONFIG_GENOME_VARIANT_CHUNK_SIZE = "opencga.storage.hadoop.vcf.chunk_size";
    public static final String CONFIG_GENOME_VARIANT_COLUMN_FAMILY = "opencga.storage.hadoop.vcf.column_family";
    public static final String CONFIG_GENOME_VARIANT_ROW_KEY_SEP = "opencga.storage.hadoop.vcf.row_key_sep";
    public static final String CONFIG_META_ROW_KEY = "opencga.storage.hadoop.vcf.meta.key";

    public static final String METADATA_PREFIX = "_";
    public static final String DEFAULT_META_ROW_KEY = "_METADATA";
    public static final String DEFAULT_ROWKEY_SEPARATOR = "_";
    public static final String DEFAULT_COLUMN_FAMILY = "0"; // MUST BE UPPER CASE!!!

    public static final int DEFAULT_CHUNK_SIZE = 1000;

    private final AtomicInteger chunkSize = new AtomicInteger(DEFAULT_CHUNK_SIZE);
    private final char separator;
    private final byte[] columnFamily;
    private final byte[] metaRowKey;
    private final String metaRowKeyString;

    private final Configuration conf;

    protected final HBaseManager hBaseManager;
    private int studyId;

    public interface MetadataAction<T> {
        T parse(InputStream is) throws IOException;
    }

    public GenomeHelper(Configuration conf) {
        this.conf = conf;
        this.separator = conf.get(CONFIG_GENOME_VARIANT_ROW_KEY_SEP, DEFAULT_ROWKEY_SEPARATOR).charAt(0);
        // TODO: Check if columnFamily is upper case
        // Phoenix local indexes fail if the default_column_family is lower case
        // TODO: Report this bug to phoenix JIRA
        this.columnFamily = Bytes.toBytes(conf.get(CONFIG_GENOME_VARIANT_COLUMN_FAMILY, DEFAULT_COLUMN_FAMILY));
        this.metaRowKeyString = conf.get(CONFIG_META_ROW_KEY, DEFAULT_META_ROW_KEY);
        this.metaRowKey = Bytes.toBytes(metaRowKeyString);
        this.chunkSize.set(conf.getInt(CONFIG_GENOME_VARIANT_CHUNK_SIZE, DEFAULT_CHUNK_SIZE));
        this.studyId = conf.getInt(OPENCGA_STORAGE_HADOOP_STUDY_ID, -1);
        this.hBaseManager = new HBaseManager(conf);
    }

    public GenomeHelper(GenomeHelper other) {
        this(other.getConf());
    }

    public Logger getLogger() {
        return log;
    }

    public Configuration getConf() {
        return conf;
    }

    public HBaseManager getHBaseManager() {
        return hBaseManager;
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

    public static void setChunkSize(Configuration conf, Integer size) {
        conf.setInt(CONFIG_GENOME_VARIANT_CHUNK_SIZE, size);
    }

    public static void setMetaRowKey(Configuration conf, String rowkey) {
        conf.set(CONFIG_META_ROW_KEY, rowkey);
    }

    public static void setMetaProtoString(Configuration conf, String utfString) {
        conf.set(CONFIG_VCF_META_PROTO_STRING, utfString);
    }

    public static void setStudyId(Configuration conf, Integer studyId) {
        conf.setInt(OPENCGA_STORAGE_HADOOP_STUDY_ID, studyId);
    }

    public int getStudyId() {
        return this.studyId;
    }

    public char getSeparator() {
        return separator;
    }

    public byte[] getColumnFamily() {
        return columnFamily;
    }

    public <T> T loadMetaData(Configuration conf, MetadataAction<T> action) throws IOException {
        try {
            // try first from String
            String protoString = conf.get(CONFIG_VCF_META_PROTO_STRING, StringUtils.EMPTY);
            if (StringUtils.isNotEmpty(protoString)) {
                getLogger().info("Load Meta from PROTO string ...");
                T obj = action.parse(new ByteArrayInputStream(ByteString.copyFromUtf8(protoString).toByteArray()));
                return obj;
            }
            // Else from file
            String filePath = conf.get(CONFIG_VCF_META_PROTO_FILE, StringUtils.EMPTY);
            if (StringUtils.isNotEmpty(filePath)) {
                getLogger().info(String.format("Load Meta from file %s ...", filePath));
                Path path = new Path(URI.create(filePath));
                FileSystem fs = FileSystem.get(conf);
                try (FSDataInputStream instream = fs.open(path)) {
                    return action.parse(instream);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        throw new IllegalStateException("VCFMeta configuration missing");
    }

    public int getChunkSize() {
        return chunkSize.get();
    }

    public long getSlicePosition(long position) {
        return getChunkSize() > 0 ? position / (long) getChunkSize() : position;
    }

    public long getStartPositionFromSlice(long slice) {
        return slice * (long) getChunkSize();
    }

    public byte[] getMetaRowKey() {
        return metaRowKey;
    }

    public String getMetaRowKeyString() {
        return metaRowKeyString;
    }

    /**
     * Generates a Row key based on Chromosome and position adjusted for the
     * Chunk size. <br>
     * <ul>
     * <li>Using {@link #standardChromosome(String)} to get standard chromosome
     * name
     * <li>Using {@link #getSlicePosition(long)} to return slice position
     * </ul>
     * e.g. using chunk size 100, separator _ with chr2 and 1234 would result in
     * 2_12
     *
     * @param chrom    Chromosome name
     * @param position Genomic position
     * @return {@link String} Row key string
     */
    public String generateBlockId(String chrom, long position) {
        return generateBlockIdFromSlice(chrom, getSlicePosition(position));
    }

    public String generateBlockIdFromSlice(String chrom, long slice) {
        StringBuilder sb = new StringBuilder(standardChromosome(chrom));
        sb.append(getSeparator());
        sb.append(String.format("%012d", slice));
        return sb.toString();
    }

    /**
     * Changes the String from {@link #generateBlockId(String, long)} to bytes.
     *
     * @param chrom Chromosome
     * @param start Position
     * @return {@link Byte} array
     */
    public byte[] generateBlockIdAsBytes(String chrom, int start) {
        return Bytes.toBytes(generateBlockId(chrom, start));
    }

    public String extractChromosomeFromBlockId(String blockId) {
        return extractChromosomeFromBlockId(splitBlockId(blockId));
    }

    public String extractChromosomeFromBlockId(String[] strings) {
        return strings[0];
    }

    public Long extractSliceFromBlockId(String blockId) {
        return Long.valueOf(splitBlockId(blockId)[1]);
    }

    public Long extractPositionFromBlockId(String blockId) {
        return Long.valueOf(splitBlockId(blockId)[1]) * getChunkSize();
    }

    public String[] splitBlockId(String blockId) {
        char sep = getSeparator();
        String[] split = StringUtils.splitPreserveAllTokens(blockId, sep);
        if (split.length != 2) {
            throw new IllegalStateException(String.format("Block ID is not valid - exected 2 blocks separaed by `%s`; value `%s`", sep,
                    blockId));
        }
        return split;
    }

    /* ***************
     * Variant Row Key helper methods
     *
     * Generators and extractors
     *
     */

    public byte[] generateVariantRowKey(String chrom, int position) {
//        StringBuilder sb = new StringBuilder(standardChromosome(chrom));
//        sb.append(getSeparator());
//        sb.append(String.format("%012d", position));
//        sb.append(getSeparator());
//        return sb.toString();
        return generateVariantRowKey(chrom, position, "", "");
    }

    public byte[] generateVariantRowKey(Variant var) {
        return generateVariantRowKey(var.getChromosome(), var.getStart(), var.getReference(), var.getAlternate());
    }

    /**
     * Generates a Row key based on Chromosome, position, ref and alt. <br>
     * <ul>
     * <li>Using {@link #standardChromosome(String)} to get standard chromosome
     * name
     * </ul>
     *
     * @param chrom    Chromosome name
     * @param position Genomic position
     * @param ref      Reference name
     * @param alt      Alt name
     * @return {@link String} Row key string
     */
    public byte[] generateVariantRowKey(String chrom, int position, String ref, String alt) {
//        StringBuilder sb = new StringBuilder(generateRowPositionKey(chrom, position));
//        sb.append(ref);
//        sb.append(getSeparator());
//        sb.append(alt);
//        return sb.toString();
        int size = PVarchar.INSTANCE.estimateByteSizeFromLength(chrom.length())
                + QueryConstants.SEPARATOR_BYTE_ARRAY.length
                + PUnsignedInt.INSTANCE.estimateByteSize(position)
                + PVarchar.INSTANCE.estimateByteSizeFromLength(ref.length())
                + QueryConstants.SEPARATOR_BYTE_ARRAY.length
                + PVarchar.INSTANCE.estimateByteSizeFromLength(alt.length());
        byte[] rk = new byte[size];
        int offset = 0;
        offset += PVarchar.INSTANCE.toBytes(chrom, rk, offset);
        rk[offset++] = QueryConstants.SEPARATOR_BYTE;
        offset += PUnsignedInt.INSTANCE.toBytes(position, rk, offset);
        // Separator not needed. PUnsignedInt.INSTANCE.isFixedWidth() = true
        offset += PVarchar.INSTANCE.toBytes(ref, rk, offset);
        rk[offset++] = QueryConstants.SEPARATOR_BYTE;
        offset += PVarchar.INSTANCE.toBytes(alt, rk, offset);
//        assert offset == size;
        return rk;
    }

    public byte[] generateVariantPositionPrefix(String chrom, Long position) {
        int pos = position.intValue();
        int size = PVarchar.INSTANCE.estimateByteSizeFromLength(chrom.length()) + QueryConstants.SEPARATOR_BYTE_ARRAY.length
                + PUnsignedInt.INSTANCE.estimateByteSize(pos);
        byte[] rk = new byte[size];
        int offset = 0;
        offset += PVarchar.INSTANCE.toBytes(chrom, rk, offset);
        rk[offset++] = QueryConstants.SEPARATOR_BYTE;
        offset += PUnsignedInt.INSTANCE.toBytes(pos, rk, offset);
        return rk;
    }

    public Variant extractVariantFromVariantRowKey(byte[] variantRowKey) {
//        String[] strings = splitVariantRowkey(variantRowKey);
//        return new Variant(extractChromosomeFromVariantRowKey(strings),
//                extractPositionFromVariantRowKey(strings),
//                extractReferenceFromVariantRowkey(strings),
//                extractAlternateFromVariantRowkey(strings));
        int chrPosSeparator = ArrayUtils.indexOf(variantRowKey, (byte) 0);
        String chromosome = (String) PVarchar.INSTANCE.toObject(variantRowKey, 0, chrPosSeparator, PVarchar.INSTANCE);
        Integer intSize = PUnsignedInt.INSTANCE.getByteSize();
        int position = (Integer) PUnsignedInt.INSTANCE.toObject(variantRowKey, chrPosSeparator + 1, intSize, PUnsignedInt.INSTANCE);
        int referenceOffset = chrPosSeparator + 1 + intSize;
        int refAltSeparator = ArrayUtils.indexOf(variantRowKey, (byte) 0, referenceOffset);
        String reference = (String) PVarchar.INSTANCE.toObject(variantRowKey, referenceOffset, refAltSeparator - referenceOffset,
                PVarchar.INSTANCE);
        String alternate = (String) PVarchar.INSTANCE.toObject(variantRowKey, refAltSeparator + 1,
                variantRowKey.length - (refAltSeparator + 1), PVarchar.INSTANCE);

        return new Variant(chromosome, position, reference, alternate);
    }


//
//    public String[] splitVariantRowkey (String rowkey) {
//        char sep = getSeparator();
//        String[] split = StringUtils.splitPreserveAllTokens(rowkey, sep);
//        if (split.length < 2)
//            throw new IllegalStateException(String.format("Variant rowkey is not valid - exected >2 blocks separaed by `%s`; value
// `%s`", sep,
//                    rowkey));
//        return split;
//    }

    /**
     * Creates a standard chromosome name from the provided string.
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

    public <T extends MessageLite> Put wrapAsPut(byte[] column, byte[] row, T meta) {
        byte[] data = meta.toByteArray();
        Put put = new Put(row);
        put.addColumn(getColumnFamily(), column, data);
        return put;
    }

    public <T extends MessageLite> Put wrapMetaAsPut(byte[] column, T meta) {
        return wrapAsPut(column, getMetaRowKey(), meta);
    }

}
