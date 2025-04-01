package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarchar;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.core.config.storage.SampleIndexConfiguration;
import org.opencb.opencga.core.models.variant.VariantAnnotationConstants;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.*;

import java.util.*;

import static org.apache.hadoop.hbase.util.Bytes.SIZEOF_INT;

/**
 * Define RowKey, column names, and individual schemas. Used to build the sample index.
 *
 * {@link SampleIndexEntry}: HBase row. Contains all the information from a sample in a specific region.
 * {@link SampleIndexEntry.SampleIndexGtEntry}: HBase columns grouped by genotype.
 * {@link SampleIndexEntryIterator}: Iterator over the variants of a {@link SampleIndexEntry}
 * {@link SampleIndexVariant}: Logical view over an entry for a specific variant and corresponding keys
 * <p>
 * - Row : {SAMPLE_ID}_{CHROMOSOME}_{BATCH_START}
 *   - Variants columns:          {GT} -> [{variant1}, {variant2}, {variant3}, ...]
 *   - Genotype columns:   _{key}_{GT} -> [{doc1}, {doc2}, {doc3}, ...]
 *                                      - doc1 = [{fieldValue1}, {fieldValue2}, {fieldValue3}, ...]
 *                                      - doc2 = [{fieldValue1}, {fieldValue2}, {fieldValue3}, ...]
 *   - Meta columns:       _{key}      -> [{doc1}, {doc2}, {doc3}, ...]
 * <p>
 * Documents from genotype columns are ordered as the variants in the variants cell.
 * Each variant is associated with a list of documents from each
 * <p>
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
    public static final Comparator<Variant> VARIANT_COMPARATOR = Comparator.comparing(Variant::getChromosome, (chr1, chr2) -> {
        return chr1.equals(chr2) ? 0 : -1;
    }).thenComparing(INTRA_CHROMOSOME_VARIANT_COMPARATOR);

    public static final Set<String> CUSTOM_LOF = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            VariantAnnotationConstants.FRAMESHIFT_VARIANT,
            VariantAnnotationConstants.INFRAME_DELETION,
            VariantAnnotationConstants.INFRAME_INSERTION,
            VariantAnnotationConstants.START_LOST,
            VariantAnnotationConstants.STOP_GAINED,
            VariantAnnotationConstants.STOP_LOST,
            VariantAnnotationConstants.SPLICE_ACCEPTOR_VARIANT,
            VariantAnnotationConstants.SPLICE_DONOR_VARIANT,
            VariantAnnotationConstants.TRANSCRIPT_ABLATION,
            VariantAnnotationConstants.TRANSCRIPT_AMPLIFICATION,
            VariantAnnotationConstants.INITIATOR_CODON_VARIANT,
            VariantAnnotationConstants.SPLICE_REGION_VARIANT,
            VariantAnnotationConstants.INCOMPLETE_TERMINAL_CODON_VARIANT
    )));
    public static final Set<String> CUSTOM_LOFE = Collections.unmodifiableSet(new HashSet<>(
            ListUtils.concat(
                    new ArrayList<>(CUSTOM_LOF),
                    Arrays.asList(VariantAnnotationConstants.MISSENSE_VARIANT))));

    static final String MENDELIAN_ERROR_COLUMN = "ME";
    static final byte[] MENDELIAN_ERROR_COLUMN_BYTES = Bytes.toBytes(MENDELIAN_ERROR_COLUMN);
    static final char META_PREFIX = '_';
    static final byte[] META_PREFIX_BYTES = Bytes.toBytes("_");
    static final String PARENTS_PREFIX = META_PREFIX + "P_";
    static final byte[] PARENTS_PREFIX_BYTES = Bytes.toBytes(PARENTS_PREFIX);
    static final String FILE_PREFIX = META_PREFIX + "F_";
    static final byte[] FILE_PREFIX_BYTES = Bytes.toBytes(FILE_PREFIX);
    static final String FILE_DATA_PREFIX = META_PREFIX + "FD_";
    static final byte[] FILE_DATA_PREFIX_BYTES = Bytes.toBytes(FILE_DATA_PREFIX);
    static final String GENOTYPE_COUNT_PREFIX = META_PREFIX + "C_";
    static final byte[] GENOTYPE_COUNT_PREFIX_BYTES = Bytes.toBytes(GENOTYPE_COUNT_PREFIX);
    static final String GENOTYPE_DISCREPANCY_COUNT = META_PREFIX + "DC";
    static final byte[] GENOTYPE_DISCREPANCY_COUNT_BYTES = Bytes.toBytes(GENOTYPE_DISCREPANCY_COUNT);

    static final String ANNOTATION_SUMMARY_PREFIX = META_PREFIX + "A_";
    static final byte[] ANNOTATION_SUMMARY_PREFIX_BYTES = Bytes.toBytes(ANNOTATION_SUMMARY_PREFIX);
    static final String ANNOTATION_SUMMARY_COUNT_PREFIX = META_PREFIX + "AC_";
    static final byte[] ANNOTATION_SUMMARY_COUNT_PREFIX_BYTES = Bytes.toBytes(ANNOTATION_SUMMARY_COUNT_PREFIX);

    // Consequence Type
    static final String ANNOTATION_CT_PREFIX = META_PREFIX + "CT_";
    static final byte[] ANNOTATION_CT_PREFIX_BYTES = Bytes.toBytes(ANNOTATION_CT_PREFIX);
    // Biotype
    static final String ANNOTATION_BT_PREFIX = META_PREFIX + "BT_";
    static final byte[] ANNOTATION_BT_PREFIX_BYTES = Bytes.toBytes(ANNOTATION_BT_PREFIX);
    // Transcript Flag
    static final String ANNOTATION_TF_PREFIX = META_PREFIX + "TF_";
    static final byte[] ANNOTATION_TF_PREFIX_BYTES = Bytes.toBytes(ANNOTATION_TF_PREFIX);
    // ConsequenceType + Biotype + TranscriptFlag combination combination
    static final String ANNOTATION_CT_BT_TF_PREFIX = META_PREFIX + "CBT_";
    static final byte[] ANNOTATION_CT_BT_TF_PREFIX_BYTES = Bytes.toBytes(ANNOTATION_CT_BT_TF_PREFIX);
    // PopFreq
    static final String ANNOTATION_POP_FREQ_PREFIX = META_PREFIX + "PF_";
    static final byte[] ANNOTATION_POP_FREQ_PREFIX_BYTES = Bytes.toBytes(ANNOTATION_POP_FREQ_PREFIX);
    // Clinical
    static final String ANNOTATION_CLINICAL_PREFIX = META_PREFIX + "CL_";
    static final byte[] ANNOTATION_CLINICAL_PREFIX_BYTES = Bytes.toBytes(ANNOTATION_CLINICAL_PREFIX);

    /**
     * The largestVariantLength might be unknown if the sample was loaded before this field was added.
     * In that case, a default value for the largestVariantLength will be used.
     * See HadoopVariantStorageOptions.SAMPLE_INDEX_QUERY_EXTENDED_REGION_FILTER
     */
    public static final String UNKNOWN_LARGEST_VARIANT_LENGTH = "unknownLargestVariantLength";
    public static final String LARGEST_VARIANT_LENGTH = "largestVariantLength";

    private final int version;
    private final SampleIndexConfiguration configuration;
    private final FileIndexSchema fileIndex;
    private final FileDataSchema fileData;
    private final PopulationFrequencyIndexSchema popFreqIndex;
    private final ConsequenceTypeIndexSchema ctIndex;
    private final BiotypeIndexSchema biotypeIndex;
    private final TranscriptFlagIndexSchema transcriptFlagIndexSchema;
    private final CtBtFtCombinationIndexSchema ctBtTfIndex;
    private final ClinicalIndexSchema clinicalIndexSchema;
//    private final AnnotationSummaryIndexSchema annotationSummaryIndexSchema;

    public SampleIndexSchema(SampleIndexConfiguration configuration, int version) {
        this.version = version;
        this.configuration = configuration;
        fileIndex = new FileIndexSchema(configuration.getFileIndexConfiguration());
        fileData = new FileDataSchema(configuration.getFileDataConfiguration());
//        annotationSummaryIndexSchema = new AnnotationSummaryIndexSchema();
        ctIndex = new ConsequenceTypeIndexSchema(configuration.getAnnotationIndexConfiguration().getConsequenceType());
        biotypeIndex = new BiotypeIndexSchema(configuration.getAnnotationIndexConfiguration().getBiotype());
        transcriptFlagIndexSchema = new TranscriptFlagIndexSchema(
                configuration.getAnnotationIndexConfiguration().getTranscriptFlagIndexConfiguration());
        ctBtTfIndex = new CtBtFtCombinationIndexSchema(ctIndex, biotypeIndex, transcriptFlagIndexSchema);
        popFreqIndex = new PopulationFrequencyIndexSchema(configuration.getAnnotationIndexConfiguration().getPopulationFrequency());
        clinicalIndexSchema = new ClinicalIndexSchema(
                configuration.getAnnotationIndexConfiguration().getClinicalSource(),
                configuration.getAnnotationIndexConfiguration().getClinicalSignificance()
        );
    }

    /**
     * Creates a default SampleIndexSchema.
     * Test purposes only!
     * @return Default schema
     */
    public static SampleIndexSchema defaultSampleIndexSchema() {
        SampleIndexConfiguration sampleIndexConfiguration = SampleIndexConfiguration.defaultConfiguration(false);
        return new SampleIndexSchema(sampleIndexConfiguration, StudyMetadata.DEFAULT_SAMPLE_INDEX_VERSION);
    }

    public int getVersion() {
        return version;
    }

    public SampleIndexConfiguration getConfiguration() {
        return configuration;
    }

//    public AnnotationSummaryIndexSchema getAnnotationSummaryIndexSchema() {
//        return annotationSummaryIndexSchema;
//    }

    public ConsequenceTypeIndexSchema getCtIndex() {
        return ctIndex;
    }

    public BiotypeIndexSchema getBiotypeIndex() {
        return biotypeIndex;
    }

    public TranscriptFlagIndexSchema getTranscriptFlagIndexSchema() {
        return transcriptFlagIndexSchema;
    }

    public CtBtFtCombinationIndexSchema getCtBtTfIndex() {
        return ctBtTfIndex;
    }

    public PopulationFrequencyIndexSchema getPopFreqIndex() {
        return popFreqIndex;
    }

    public ClinicalIndexSchema getClinicalIndexSchema() {
        return clinicalIndexSchema;
    }

    public FileIndexSchema getFileIndex() {
        return fileIndex;
    }

    public FileDataSchema getFileData() {
        return fileData;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleIndexSchema{");
        sb.append("version=").append(version);
        sb.append(", configuration=").append(configuration);
        sb.append(", fileIndex=").append(fileIndex);
        sb.append(", fileData=").append(fileData);
        sb.append(", popFreqIndex=").append(popFreqIndex);
        sb.append(", ctIndex=").append(ctIndex);
        sb.append(", biotypeIndex=").append(biotypeIndex);
        sb.append(", transcriptFlagIndexSchema=").append(transcriptFlagIndexSchema);
        sb.append(", ctBtTfIndex=").append(ctBtTfIndex);
        sb.append(", clinicalIndexSchema=").append(clinicalIndexSchema);
        sb.append('}');
        return sb.toString();
    }

    public static int getChunkStart(Integer start) {
        return (start / BATCH_SIZE) * BATCH_SIZE;
    }

    public static int getChunkStartNext(Integer start) {
        return getChunkStart(start + SampleIndexSchema.BATCH_SIZE);
    }

    public static Region getChunkRegion(Region region, int extendedFilteringRegion) {
        return getChunkRegion(region.getChromosome(), Math.max(0, region.getStart() - extendedFilteringRegion), region.getEnd());
    }

    public static Region getChunkRegion(Variant variant) {
        // We only care about the variant start for the chunk region, not the end
        return getChunkRegion(variant.getChromosome(), variant.getStart(), variant.getStart());
    }

    private static Region getChunkRegion(String chromosome, int start, int end) {
        return new Region(chromosome, SampleIndexSchema.getChunkStart(start),
                end == Integer.MAX_VALUE
                        ? Integer.MAX_VALUE
                        : SampleIndexSchema.getChunkStartNext(end));
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

    public static byte[] toAnnotationTranscriptFlagIndexColumn(String genotype) {
        return Bytes.toBytes(ANNOTATION_TF_PREFIX + genotype);
    }

    public static byte[] toAnnotationCtBtTfIndexColumn(String genotype) {
        return Bytes.toBytes(ANNOTATION_CT_BT_TF_PREFIX + genotype);
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

    public static byte[] toFileDataColumn(String genotype) {
        return Bytes.toBytes(FILE_DATA_PREFIX + genotype);
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
