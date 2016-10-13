/*
 * Copyright 2015-2016 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 *
 */
package org.opencb.opencga.storage.hadoop.variant;

import com.google.protobuf.MessageLite;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PVarchar;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk.
 */
public class GenomeHelper implements AutoCloseable {
    private final Logger logger = LoggerFactory.getLogger(GenomeHelper.class);

    public static final String CONFIG_STUDY_ID = "opencga.study.id";

    //upload HBase jars and jars for any of the configured job classes via the distributed cache (tmpjars).
    public static final String CONFIG_HBASE_ADD_DEPENDENCY_JARS = "opencga.hbase.addDependencyJars";
    public static final String CONFIG_HBASE_COLUMN_FAMILY = "opencga.hbase.column_family";

    public static final String METADATA_PREFIX = "_";
    public static final String DEFAULT_METADATA_ROW_KEY = "_METADATA";
    public static final String DEFAULT_ROWKEY_SEPARATOR = "_";
    public static final String DEFAULT_COLUMN_FAMILY = "0"; // MUST BE UPPER CASE!!!

    public static final String VARIANT_COLUMN = "_V";
    public static final byte[] VARIANT_COLUMN_B = Bytes.toBytes(VARIANT_COLUMN);

    private final AtomicInteger chunkSize = new AtomicInteger(ArchiveDriver.DEFAULT_CHUNK_SIZE);
    private final char separator;
    private final byte[] columnFamily;
    private final byte[] metaRowKey;
    private final String metaRowKeyString;

    private final Configuration conf;

    protected final HBaseManager hBaseManager;
    private final int studyId;

    public GenomeHelper(Configuration conf, Connection connection) {
        this.conf = conf;
        this.separator = conf.get(ArchiveDriver.CONFIG_ARCHIVE_ROW_KEY_SEPARATOR, DEFAULT_ROWKEY_SEPARATOR).charAt(0);
        // TODO: Check if columnFamily is upper case
        // Phoenix local indexes fail if the default_column_family is lower case
        // TODO: Report this bug to phoenix JIRA
        this.columnFamily = Bytes.toBytes(conf.get(CONFIG_HBASE_COLUMN_FAMILY, DEFAULT_COLUMN_FAMILY));
        this.metaRowKeyString = DEFAULT_METADATA_ROW_KEY;
        this.metaRowKey = Bytes.toBytes(metaRowKeyString);
        this.chunkSize.set(conf.getInt(ArchiveDriver.CONFIG_ARCHIVE_CHUNK_SIZE, ArchiveDriver.DEFAULT_CHUNK_SIZE));
        this.studyId = conf.getInt(CONFIG_STUDY_ID, -1);
        this.hBaseManager = new HBaseManager(conf, connection);

    }

    public GenomeHelper(Configuration conf) {
        this(conf, null);
    }

    public GenomeHelper(GenomeHelper other) {
        this(other.getConf(), other.getHBaseManager().getCloseConnection() ? null : other.getHBaseManager()
                .getConnection());
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
        conf.setInt(ArchiveDriver.CONFIG_ARCHIVE_CHUNK_SIZE, size);
    }

    public static void setStudyId(Configuration conf, Integer studyId) {
        conf.setInt(CONFIG_STUDY_ID, studyId);
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

    public int getChunkSize() {
        return chunkSize.get();
    }

    public long getSliceId(long position) {
        return getChunkSize() > 0
                ? position / (long) getChunkSize()
                : position;
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
     * <li>Using {@link #getSliceId(long)} to return slice position
     * </ul>
     * e.g. using chunk size 100, separator _ with chr2 and 1234 would result in
     * 2_12
     *
     * @param chrom    Chromosome name
     * @param position Genomic position
     * @return {@link String} Row key string
     */
    public String generateBlockId(String chrom, long position) {
        return generateBlockIdFromSlice(chrom, getSliceId(position));
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

    /**
     * TODO: Query CellBase to get the chromosomes and sizes!
     * @param numberOfSplits    Number of splits
     * @param keyGenerator      Function to generate the rowKeys given a chromosome and a start
     * @return                  List of splits
     */
    public static List<byte[]> generateBootPreSplitsHuman(int numberOfSplits, BiFunction<String, Integer, byte[]> keyGenerator) {
        String[] chr = new String[]{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15",
                "16", "17", "18", "19", "20", "21", "22", "X", "Y", };
        long[] posarr = new long[]{249250621, 243199373, 198022430, 191154276, 180915260, 171115067, 159138663,
                146364022, 141213431, 135534747, 135006516, 133851895, 115169878, 107349540, 102531392, 90354753,
                81195210, 78077248, 59128983, 63025520, 48129895, 51304566, 155270560, 59373566, };
        return generateBootPreSplits(numberOfSplits, keyGenerator, chr, posarr);
    }

    static List<byte[]> generateBootPreSplits(int numberOfSplits, BiFunction<String, Integer, byte[]> keyGenerator,
                                              String[] chr, long[] posarr) {
        long total = Arrays.stream(posarr).sum();
        long chunkSize = total / numberOfSplits;
        List<byte[]> splitList = new ArrayList<>();
        long splitPos = chunkSize;
        while (splitPos < total) {
            long tmpPos = 0;
            int arrayPos = -1;
            for (int i = 0; i < chr.length; i++) {
                if ((tmpPos + posarr[i]) > splitPos) {
                    arrayPos = i;
                    break;
                }
                tmpPos += posarr[i];
            }
            byte[] rowKey = keyGenerator.apply(chr[arrayPos], (int) (splitPos - tmpPos));
//            String s = Bytes.toHex(rowKey);
//            System.out.println("Split " + chr[arrayPos] + " at " + (splitPos - tmpPos));
            splitList.add(rowKey);
            splitPos += chunkSize;
        }
        return splitList;
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
        if (chrom.startsWith("chr")) {
            return chrom.substring(2);
        } // TODO MT, X, Y, ...
        return chrom;
    }

    public <T extends MessageLite> Put wrapAsPut(byte[] column, byte[] row, T meta) {
        byte[] data = meta.toByteArray();
        Put put = new Put(row);
        put.addColumn(getColumnFamily(), column, data);
        return put;
    }

    @Override
    public void close() throws IOException {
        this.hBaseManager.close();
    }
}
