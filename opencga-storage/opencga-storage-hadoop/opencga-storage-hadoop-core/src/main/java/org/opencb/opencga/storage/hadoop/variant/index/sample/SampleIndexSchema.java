package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarchar;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.config.SampleIndexConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.PopulationFrequencyIndexSchema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;

import static org.apache.hadoop.hbase.util.Bytes.SIZEOF_INT;

/**
 * Define RowKey, column names, and fields. Used to build the sample index.
 *
 * Created on 11/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public final class SampleIndexSchema {

    public static final int BATCH_SIZE = 1_000_000;
    public static final Comparator<Variant> INTRA_CHROMOSOME_VARIANT_COMPARATOR =  (o1, o2) -> {
        VariantAvro v1 = o1.getImpl();
        VariantAvro v2 = o2.getImpl();
        int c = v1.getStart().compareTo(v2.getStart());
        if (c != 0) {
            return c;
        }
        c = v1.getEnd().compareTo(v2.getEnd());
        if (c != 0) {
            return c;
        }
        c = v1.getReference().compareTo(v2.getReference());
        if (c != 0) {
            return c;
        }
        c = v1.getAlternate().compareTo(v2.getAlternate());
        if (c != 0) {
            return c;
        }
        if (o1.sameGenomicVariant(o2)) {
            return 0;
        } else {
            return o1.toString().compareTo(o2.toString());
        }
    };
    public static final Comparator<Variant> INTRA_CHROMOSOME_VARIANT_COMPARATOR_AUTO =
            Comparator.comparingInt(Variant::getStart)
                    .thenComparingInt(Variant::getEnd)
                    .thenComparing(Variant::getReference)
                    .thenComparing(Variant::getAlternate)
                    .thenComparing(Variant::toString);

    static final String MENDELIAN_ERROR_COLUMN = "ME";
    static final byte[] MENDELIAN_ERROR_COLUMN_BYTES = Bytes.toBytes(MENDELIAN_ERROR_COLUMN);
    static final char META_PREFIX = '_';
    static final byte[] META_PREFIX_BYTES = Bytes.toBytes("_");
    static final String PARENTS_PREFIX = META_PREFIX + "P_";
    static final byte[] PARENTS_PREFIX_BYTES = Bytes.toBytes(PARENTS_PREFIX);
    static final String FILE_PREFIX = META_PREFIX + "F_";
    static final byte[] FILE_PREFIX_BYTES = Bytes.toBytes(FILE_PREFIX);
    static final String GENOTYPE_COUNT_PREFIX = META_PREFIX + "C_";
    static final byte[] GENOTYPE_COUNT_PREFIX_BYTES = Bytes.toBytes(GENOTYPE_COUNT_PREFIX);
    static final String GENOTYPE_DISCREPANCY_COUNT = META_PREFIX + "DC";
    static final byte[] GENOTYPE_DISCREPANCY_COUNT_BYTES = Bytes.toBytes(GENOTYPE_DISCREPANCY_COUNT);

    static final String ANNOTATION_SUMMARY_PREFIX = META_PREFIX + "A_";
    static final byte[] ANNOTATION_SUMMARY_PREFIX_BYTES = Bytes.toBytes(ANNOTATION_SUMMARY_PREFIX);
    static final String ANNOTATION_SUMMARY_COUNT_PREFIX = META_PREFIX + "AC_";
    static final byte[] ANNOTATION_SUMMARY_COUNT_PREFIX_BYTES = Bytes.toBytes(ANNOTATION_SUMMARY_COUNT_PREFIX);

    static final String ANNOTATION_CT_PREFIX = META_PREFIX + "CT_";
    static final byte[] ANNOTATION_CT_PREFIX_BYTES = Bytes.toBytes(ANNOTATION_CT_PREFIX);
    static final String ANNOTATION_BT_PREFIX = META_PREFIX + "BT_";
    static final byte[] ANNOTATION_BT_PREFIX_BYTES = Bytes.toBytes(ANNOTATION_BT_PREFIX);
    static final String ANNOTATION_CT_BT_PREFIX = META_PREFIX + "CB_";
    static final byte[] ANNOTATION_CT_BT_PREFIX_BYTES = Bytes.toBytes(ANNOTATION_CT_BT_PREFIX);
    static final String ANNOTATION_POP_FREQ_PREFIX = META_PREFIX + "PF_";
    static final byte[] ANNOTATION_POP_FREQ_PREFIX_BYTES = Bytes.toBytes(ANNOTATION_POP_FREQ_PREFIX);
    static final String ANNOTATION_CLINICAL_PREFIX = META_PREFIX + "CL_";
    static final byte[] ANNOTATION_CLINICAL_PREFIX_BYTES = Bytes.toBytes(ANNOTATION_CLINICAL_PREFIX);

    private final SampleIndexConfiguration configuration;
    private final FileIndexSchema fileIndex;
    private final PopulationFrequencyIndexSchema popFreqIndex;

    public SampleIndexSchema(SampleIndexConfiguration configuration) {
        this.configuration = configuration;
        fileIndex = new FileIndexSchema(configuration.getFileIndexConfiguration());
        popFreqIndex = new PopulationFrequencyIndexSchema(configuration.getPopulationRanges());
    }

    /**
     * Creates a default SampleIndexSchema.
     * Test purposes only!
     * @return Default schema
     */
    public static SampleIndexSchema defaultSampleIndexSchema() {
        SampleIndexConfiguration sampleIndexConfiguration = SampleIndexConfiguration.defaultConfiguration();
        return new SampleIndexSchema(sampleIndexConfiguration);
    }

    public SampleIndexConfiguration getConfiguration() {
        return configuration;
    }

    public PopulationFrequencyIndexSchema getPopFreqIndex() {
        return popFreqIndex;
    }

    public FileIndexSchema getFileIndex() {
        return fileIndex;
    }

    public static int getChunkStart(Integer start) {
        return (start / BATCH_SIZE) * BATCH_SIZE;
    }

    public static int getExpectedSize(String chromosome) {
        int expectedSize;
        if (chromosome == null) {
            expectedSize = SIZEOF_INT;
        } else {
            expectedSize = SIZEOF_INT + chromosome.length() + 1 + SIZEOF_INT;
        }
        return expectedSize;
    }

    public static byte[] toRowKey(int sample) {
        return toRowKey(sample, null, 0);
    }

    public static byte[] toRowKey(int sample, String chromosome, int position) {
        int expectedSize = getExpectedSize(chromosome);
        byte[] rk = new byte[expectedSize];

        toRowKey(sample, chromosome, position, rk);

        return rk;
    }

    private static int toRowKey(int sample, String chromosome, int position, byte[] rk) {
        int offset = 0;
        offset += PInteger.INSTANCE.toBytes(sample, rk, offset);


        if (chromosome != null) {
            offset += PVarchar.INSTANCE.toBytes(chromosome, rk, offset);
            rk[offset] = 0;
            offset++;
            offset += PInteger.INSTANCE.toBytes(position / BATCH_SIZE, rk, offset);
        }
        return offset;
    }

    public static String rowKeyToString(byte[] row) {
        if (row == null || row.length == 0) {
            return null;
        }
        Object sampleId = PInteger.INSTANCE.toObject(row, 0, 4);
        if (row.length > 5) {
            Object chr = chromosomeFromRowKey(row);
            try {
                Object pos = batchStartFromRowKey(row);
                return sampleId + "_" + chr + "_" + pos;
            } catch (RuntimeException e) {
                return sampleId + "_" + chr + "_########";
            }
        } else {
            return sampleId + "_";
        }
    }

    public static int sampleIdFromRowKey(byte[] row) {
        return ((Number) PInteger.INSTANCE.toObject(row, 0, 4)).intValue();
    }

    public static String chromosomeFromRowKey(byte[] row) {
        return (String) PVarchar.INSTANCE.toObject(row, 4, row.length - 4 - 1 - 4);
    }

    public static int batchStartFromRowKey(byte[] row) {
        return ((Integer) PInteger.INSTANCE.toObject(row, row.length - 4, 4)) * BATCH_SIZE;
    }

    public static boolean isGenotypeColumn(Cell cell) {
        byte b = cell.getQualifierArray()[cell.getQualifierOffset()];
        return b != META_PREFIX && !CellUtil.matchingQualifier(cell, MENDELIAN_ERROR_COLUMN_BYTES);
    }

    public static byte[] toGenotypeColumn(String genotype) {
        return Bytes.toBytes(genotype);
    }

    public static byte[] toGenotypeDiscrepanciesCountColumn() {
        return GENOTYPE_DISCREPANCY_COUNT_BYTES;
    }

    public static byte[] toGenotypeCountColumn(String genotype) {
        return Bytes.toBytes(GENOTYPE_COUNT_PREFIX + genotype);
    }

    public static byte[] toAnnotationIndexColumn(String genotype) {
        return Bytes.toBytes(ANNOTATION_SUMMARY_PREFIX + genotype);
    }

    public static byte[] toAnnotationIndexCountColumn(String genotype) {
        return Bytes.toBytes(ANNOTATION_SUMMARY_COUNT_PREFIX + genotype);
    }

    public static byte[] toAnnotationConsequenceTypeIndexColumn(String genotype) {
        return Bytes.toBytes(ANNOTATION_CT_PREFIX + genotype);
    }

    public static byte[] toAnnotationBiotypeIndexColumn(String genotype) {
        return Bytes.toBytes(ANNOTATION_BT_PREFIX + genotype);
    }

    public static byte[] toAnnotationCtBtIndexColumn(String genotype) {
        return Bytes.toBytes(ANNOTATION_CT_BT_PREFIX + genotype);
    }

    public static byte[] toAnnotationPopFreqIndexColumn(String genotype) {
        return Bytes.toBytes(ANNOTATION_POP_FREQ_PREFIX + genotype);
    }

    public static byte[] toAnnotationClinicalIndexColumn(String genotype) {
        return Bytes.toBytes(ANNOTATION_CLINICAL_PREFIX + genotype);
    }

    public static byte[] toFileIndexColumn(String genotype) {
        return Bytes.toBytes(FILE_PREFIX + genotype);
    }

    public static String getGt(Cell cell, byte[] prefix) {
        return Bytes.toString(
                cell.getQualifierArray(),
                cell.getQualifierOffset() + prefix.length,
                cell.getQualifierLength() - prefix.length);
    }

    public static byte[] toMendelianErrorColumn() {
        return MENDELIAN_ERROR_COLUMN_BYTES;
    }

    public static byte[] toParentsGTColumn(String genotype) {
        return Bytes.toBytes(PARENTS_PREFIX + genotype);
    }

    public static boolean createTableIfNeeded(String sampleIndexTable, HBaseManager hBaseManager, ObjectMap options) {

        int files = options.getInt(
                HadoopVariantStorageOptions.EXPECTED_FILES_NUMBER.key(),
                HadoopVariantStorageOptions.EXPECTED_FILES_NUMBER.defaultValue());
        int samples = options.getInt(
                HadoopVariantStorageOptions.EXPECTED_SAMPLES_NUMBER.key(),
                files);
        int preSplitSize = options.getInt(
                HadoopVariantStorageOptions.SAMPLE_INDEX_TABLE_PRESPLIT_SIZE.key(),
                HadoopVariantStorageOptions.SAMPLE_INDEX_TABLE_PRESPLIT_SIZE.defaultValue());

        int splits = samples / preSplitSize;
        ArrayList<byte[]> preSplits = new ArrayList<>(splits);
        for (int i = 0; i < splits; i++) {
            preSplits.add(toRowKey(i * preSplitSize));
        }

        Compression.Algorithm compression = Compression.getCompressionAlgorithmByName(
                options.getString(
                        HadoopVariantStorageOptions.SAMPLE_INDEX_TABLE_COMPRESSION.key(),
                        HadoopVariantStorageOptions.SAMPLE_INDEX_TABLE_COMPRESSION.defaultValue()));

        try {
            return hBaseManager.createTableIfNeeded(sampleIndexTable, GenomeHelper.COLUMN_FAMILY_BYTES, preSplits, compression);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static boolean isAnnotatedGenotype(String gt) {
        return GenotypeClass.MAIN_ALT.test(gt);
    }

    /**
     * Genotypes HOM_REF and MISSING are not loaded in the SampleIndexTable.
     *
     * @param gt genotype
     * @return is valid genotype
     */
    public static boolean validGenotype(String gt) {
//        return gt != null && gt.contains("1");
        if (gt != null) {
            switch (gt) {
                case "" :
                case "0" :
                case "0/0" :
                case "./0" :
                case "0|0" :
                case "0|." :
                case ".|0" :
                case "./." :
                case ".|." :
                case "." :
                    return false;
                case "1/1" :
                case "0/1" :
                case "1|1" :
                case "0|1" :
                case "1|0" :
                case GenotypeClass.NA_GT_VALUE:
                    return true;
                default:
                    return GenotypeClass.MAIN_ALT.test(gt);
            }
        }
        return false;
    }
}
