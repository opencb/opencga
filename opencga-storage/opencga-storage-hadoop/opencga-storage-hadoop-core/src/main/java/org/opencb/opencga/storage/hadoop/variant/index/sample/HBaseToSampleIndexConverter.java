package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarchar;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.tools.Converter;
import org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexQuery.SingleSampleIndexQuery;

import java.util.*;

import static org.apache.hadoop.hbase.util.Bytes.SIZEOF_INT;
import static org.opencb.opencga.storage.hadoop.variant.index.IndexUtils.testIndex;

/**
 * Converts Results to collection of variants.
 * Applies some filtering based on region and annotation.
 * <p>
 * Created on 18/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseToSampleIndexConverter implements Converter<Result, Collection<Variant>> {

    public static final Comparator<Variant> INTRA_CHROMOSOME_VARIANT_COMPARATOR = Comparator.comparing(Variant::getStart)
            .thenComparing(Variant::getEnd)
            .thenComparing(Variant::getReference)
            .thenComparing(Variant::getAlternate)
            .thenComparing(Variant::toString);

    private static final char META_PREFIX = '_';
    private static final String PENDING_VARIANT_PREFIX = META_PREFIX + "V_";
    private static final byte[] PENDING_VARIANT_PREFIX_BYTES = Bytes.toBytes(PENDING_VARIANT_PREFIX);
    private static final String GENOTYPE_COUNT_PREFIX = META_PREFIX + "C_";
    private static final String ANNOTATION_PREFIX = META_PREFIX + "A_";
    private static final byte[] ANNOTATION_PREFIX_BYTES = Bytes.toBytes(ANNOTATION_PREFIX);
    private static final String FILE_PREFIX = META_PREFIX + "F_";
    private static final byte[] FILE_PREFIX_BYTES = Bytes.toBytes(FILE_PREFIX);

    public static final String SEPARATOR_STR = ",";
    public static final byte SEPARATOR = ',';
    public static final byte[] MENDELIAN_ERROR_COLUMN = Bytes.toBytes("ME");
    public static final byte MENDELIAN_ERROR_SEPARATOR = '_';

    // Region filter
    private final Region regionFilter;
    private final Set<VariantType> typesFilter;
    // Annotation mask filter
    private final byte annotationIndexMask;
    private final byte fileIndexMask;
    private final byte fileIndex;
    private final boolean mendelianErrors;
    private final byte[] family;
    private final Set<String> genotypes;

    public HBaseToSampleIndexConverter(byte[] family) {
        this.regionFilter = null;
        this.typesFilter = null;
        this.annotationIndexMask = IndexUtils.EMPTY_MASK;
        this.fileIndexMask = IndexUtils.EMPTY_MASK;
        this.fileIndex = IndexUtils.EMPTY_MASK;
        this.mendelianErrors = false;
        this.genotypes = Collections.emptySet();
        this.family = family;
    }

    public HBaseToSampleIndexConverter(SingleSampleIndexQuery query, Region region, byte[] family) {
        this.regionFilter = region;
        this.typesFilter = query.getVariantTypes();
        this.annotationIndexMask = query.getAnnotationIndexMask();
        this.fileIndexMask = query.getFileIndexMask();
        this.fileIndex = query.getFileIndex();
        this.mendelianErrors = query.getMendelianError();
        genotypes = new HashSet<>(query.getGenotypes());
        this.family = family;
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
            offset += PInteger.INSTANCE.toBytes(position / SampleIndexDBLoader.BATCH_SIZE, rk, offset);
        }
        return offset;
    }

    public static String rowKeyToString(byte[] row) {
        if (row == null || row.length == 0) {
            return null;
        }
        Object sampleId = PInteger.INSTANCE.toObject(row, 0, 4);
        if (row.length > 5) {
            Object chr = PVarchar.INSTANCE.toObject(row, 4, row.length - 4 - 1 - 4);
            try {
                Object pos = PInteger.INSTANCE.toObject(row, row.length - 4, 4);
                return sampleId + "_" + chr + "_" + pos;
            } catch (RuntimeException e) {
                return sampleId + "_" + chr + "_########";
            }
        } else {
            return sampleId + "_";
        }
    }

    public static byte[] toGenotypeColumn(String genotype) {
        return Bytes.toBytes(genotype);
    }

    public static byte[] toGenotypeCountColumn(String genotype) {
        return Bytes.toBytes(GENOTYPE_COUNT_PREFIX + genotype);
    }

    public static byte[] toPendingColumn(Variant variant, String gt) {
        return Bytes.toBytes(PENDING_VARIANT_PREFIX + variant.toString() + '_' + gt);
    }

    public static byte[] toAnnotationIndexColumn(String genotype) {
        return Bytes.toBytes(ANNOTATION_PREFIX + genotype);
    }

    public static byte[] toFileIndexColumn(String genotype) {
        return Bytes.toBytes(FILE_PREFIX + genotype);
    }

    public static Pair<String, String> parsePendingColumn(byte[] column) {
        if (Bytes.startsWith(column, PENDING_VARIANT_PREFIX_BYTES)) {
            int lastIndexOf = 0;
            for (int i = column.length - 1; i >= 0; i--) {
                if (column[i] == '_') {
                    lastIndexOf = i;
                    break;
                }
            }
            return Pair.of(Bytes.toString(column, PENDING_VARIANT_PREFIX.length(), lastIndexOf - PENDING_VARIANT_PREFIX.length()),
                    Bytes.toString(column, lastIndexOf + 1));
        } else {
            return null;
        }
    }

    public static String getGt(Cell cell, byte[] prefix) {
        return Bytes.toString(
                cell.getQualifierArray(),
                cell.getQualifierOffset() + prefix.length,
                cell.getQualifierLength() - prefix.length);
    }

    public static byte[] toMendelianErrorColumn() {
        return MENDELIAN_ERROR_COLUMN;
    }

    @Override
    public Collection<Variant> convert(Result result) {
        final Map<String, byte[]> annotationIndexGtMap;
        final Map<String, byte[]> fileIndexGtMap;
        if (annotationIndexMask != IndexUtils.EMPTY_MASK || fileIndexMask != IndexUtils.EMPTY_MASK) {
            annotationIndexGtMap = new HashMap<>();
            fileIndexGtMap = new HashMap<>();
            for (Cell cell : result.rawCells()) {
                if (columnStartsWith(cell, ANNOTATION_PREFIX_BYTES)) {
                    String gt = getGt(cell, ANNOTATION_PREFIX_BYTES);
                    annotationIndexGtMap.put(gt, CellUtil.cloneValue(cell));
                } else if (columnStartsWith(cell, FILE_PREFIX_BYTES)) {
                    String gt = getGt(cell, FILE_PREFIX_BYTES);
                    fileIndexGtMap.put(gt, CellUtil.cloneValue(cell));
                }
            }
        } else {
            annotationIndexGtMap = Collections.emptyMap();
            fileIndexGtMap = Collections.emptyMap();
        }

        if (mendelianErrors) {
            return convertFromMendelianErrors(result, annotationIndexGtMap, fileIndexGtMap);
        } else {
            return convertFromGT(result, annotationIndexGtMap, fileIndexGtMap);
        }
    }

    private Set<Variant> convertFromMendelianErrors(Result result,
                                                    Map<String, byte[]> annotationIndexGtMap, Map<String, byte[]> fileIndexGtMap) {
        Set<Variant> variants = new TreeSet<>(INTRA_CHROMOSOME_VARIANT_COMPARATOR);
        Cell cell = result.getColumnLatestCell(family, MENDELIAN_ERROR_COLUMN);
        if (cell != null && cell.getValueLength() != 0) {
            for (String s : splitValue(cell)) {
                int idx2 = s.lastIndexOf(MENDELIAN_ERROR_SEPARATOR);
                int idx1 = s.lastIndexOf(MENDELIAN_ERROR_SEPARATOR, idx2 - 1);
                String variantStr = s.substring(0, idx1);
                String gt = s.substring(idx1 + 1, idx2);
                int variantIdx = Integer.valueOf(s.substring(idx2 + 1));
                if (genotypes.isEmpty() || genotypes.contains(gt)) {
                    byte[] fileIndexGt = fileIndexGtMap.get(gt);
                    byte[] annotationIndexGt = annotationIndexGtMap.get(gt);
                    Variant variant = filterAndConvert(variantStr, variantIdx, fileIndexGt, annotationIndexGt);
                    if (variant != null) {
                        variants.add(variant);
                    }
                }
            }
        }
        return variants;
    }

    protected Set<Variant> convertFromGT(Result result, Map<String, byte[]> annotationIndexGtMap, Map<String, byte[]> fileIndexGtMap) {
        Set<Variant> variants = new TreeSet<>(INTRA_CHROMOSOME_VARIANT_COMPARATOR);
        for (Cell cell : result.rawCells()) {
            if (cell.getQualifierArray()[cell.getQualifierOffset()] != META_PREFIX) {
                String gt = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                byte[] fileIndexGt = fileIndexGtMap.get(gt);
                byte[] annotationIndexGt = annotationIndexGtMap.get(gt);

                int idx = 0;
                for (String variantStr : splitValue(cell)) {
                    Variant variant = filterAndConvert(variantStr, idx, fileIndexGt, annotationIndexGt);
                    if (variant != null) {
                        variants.add(variant);
                    }
                    idx++;
                }
            }
        }
        return variants;
    }

    private Variant filterAndConvert(String variant, int idx, byte[] fileIndexGt, byte[] annotationIndexGt) {
        // Filter using RegionFilter and AnnotationMaskFilter
        if (!variant.isEmpty()) { // Skip empty variants.
            // Test file index (if any)
            if (fileIndexGt == null || testIndex(fileIndexGt[idx], fileIndexMask, fileIndex)) {

                // Test annotation index (if any)
                if (annotationIndexGt == null || testIndex(annotationIndexGt[idx], annotationIndexMask, annotationIndexMask)) {

                    // Test region filter (if any)
                    Variant v = new Variant(variant);
                    if (regionFilter == null || regionFilter.contains(v.getChromosome(), v.getStart())) {

                        // Test type filter (if any)
                        if (typesFilter == null || typesFilter.contains(v.getType())) {
                            return v;
                        }
                    }
                }
            }
        }
        return null;
    }

    public static List<String> splitValue(Cell cell) {
        byte[] value = cell.getValueArray();
        int offset = cell.getValueOffset();
        int length = cell.getValueLength();

        List<String> values = new ArrayList<>(length / 10);
        int valueOffset = offset;
        for (int i = offset; i < length + offset; i++) {
            if (value[i] == SEPARATOR) {
                if (i != valueOffset) { // Skip empty values
                    values.add(Bytes.toString(value, valueOffset, i - valueOffset));
                }
                valueOffset = i + 1;
            }
        }
        if (length + offset != valueOffset) { // Skip empty values
            values.add(Bytes.toString(value, valueOffset, length + offset - valueOffset));
        }

        return values;
    }

    public static boolean columnStartsWith(Cell cell, byte[] prefix) {
        return AbstractPhoenixConverter.columnStartsWith(cell, prefix);
    }

    public Map<String, List<Variant>> convertToMap(Result result) {
        Map<String, List<Variant>> map = new HashMap<>();
        for (Cell cell : result.rawCells()) {
            String gt = Bytes.toString(CellUtil.cloneQualifier(cell));
            if (gt.charAt(0) != META_PREFIX) {
                List<Variant> variants = getVariants(cell);
                map.put(gt, variants);
            }
        }
        return map;
    }

    public static List<Variant> getVariants(Cell cell) {
        List<Variant> variants;
        byte[] column = CellUtil.cloneQualifier(cell);
        if (column[0] != META_PREFIX) {
            String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());

            String[] split = value.split(SEPARATOR_STR);
            variants = new ArrayList<>(split.length);

            for (String v : split) {
                if (!v.isEmpty()) { // Skip empty variants.
                    variants.add(new Variant(v));
                }
            }
        } else {
            variants = Collections.emptyList();
        }
        return variants;
    }

    public int convertToCount(Result result) {
        int count = 0;
        for (Cell cell : result.rawCells()) {
            String column = Bytes.toString(CellUtil.cloneQualifier(cell));
            if (column.startsWith(HBaseToSampleIndexConverter.GENOTYPE_COUNT_PREFIX)) {
                count += Bytes.toInt(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            }
        }
        return count;
    }
}
