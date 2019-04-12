package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.tools.Converter;
import org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.family.MendelianErrorSampleIndexConverter;
import org.opencb.opencga.storage.hadoop.variant.index.family.MendelianErrorSampleIndexConverter.MendelianErrorSampleIndexVariantIterator;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexQuery.SingleSampleIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexVariantBiConverter.SampleIndexVariantIterator;

import java.util.*;

import static org.opencb.opencga.storage.hadoop.variant.index.IndexUtils.testIndex;
import static org.opencb.opencga.storage.hadoop.variant.index.IndexUtils.testParentsGenotypeCode;
import static org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema.*;

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

    private static final byte[] ANNOTATION_PREFIX_BYTES = Bytes.toBytes(ANNOTATION_PREFIX);
    private static final byte[] FILE_PREFIX_BYTES = Bytes.toBytes(FILE_PREFIX);
    private static final byte[] PARENTS_PREFIX_BYTES = Bytes.toBytes(PARENTS_PREFIX);

    protected static final String SEPARATOR_STR = ",";

    private final SampleIndexVariantBiConverter converter;

    // Region filter
    private final Region regionFilter;
    private final Set<VariantType> typesFilter;
    // Parents filter
    private final boolean[] fatherFilter;
    private final boolean[] motherFilter;
    // Annotation mask filter
    private final byte annotationIndexMask;
    // File mask filter
    private final byte fileIndexMask;
    private final byte fileIndex;
    private final boolean mendelianErrors;
    private final byte[] family;
    private final Set<String> genotypes;

    public HBaseToSampleIndexConverter(byte[] family) {
        converter = new SampleIndexVariantBiConverter();
        this.regionFilter = null;
        this.typesFilter = null;
        this.fatherFilter = SampleIndexQuery.EMPTY_PARENT_FILTER;
        this.motherFilter = SampleIndexQuery.EMPTY_PARENT_FILTER;
        this.annotationIndexMask = IndexUtils.EMPTY_MASK;
        this.fileIndexMask = IndexUtils.EMPTY_MASK;
        this.fileIndex = IndexUtils.EMPTY_MASK;
        this.mendelianErrors = false;
        this.genotypes = Collections.emptySet();
        this.family = family;
    }

    public HBaseToSampleIndexConverter(SingleSampleIndexQuery query, Region region, byte[] family) {
        converter = new SampleIndexVariantBiConverter();
        this.regionFilter = region;
        this.typesFilter = query.getVariantTypes();
        this.fatherFilter = query.getFatherFilter();
        this.motherFilter = query.getMotherFilter();
        this.annotationIndexMask = query.getAnnotationIndexMask();
        this.fileIndexMask = query.getFileIndexMask();
        this.fileIndex = query.getFileIndex();
        this.mendelianErrors = query.getMendelianError();
        genotypes = new HashSet<>(query.getGenotypes());
        this.family = family;
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

    @Override
    public Collection<Variant> convert(Result result) {
        final Map<String, byte[]> annotationIndexGtMap;
        final Map<String, byte[]> fileIndexGtMap;
        final Map<String, byte[]> parentsGtMap;
        if (annotationIndexMask != IndexUtils.EMPTY_MASK
                || fileIndexMask != IndexUtils.EMPTY_MASK
                || fatherFilter != SampleIndexQuery.EMPTY_PARENT_FILTER
                || motherFilter != SampleIndexQuery.EMPTY_PARENT_FILTER) {
            annotationIndexGtMap = new HashMap<>();
            fileIndexGtMap = new HashMap<>();
            parentsGtMap = new HashMap<>();
            for (Cell cell : result.rawCells()) {
                if (columnStartsWith(cell, ANNOTATION_PREFIX_BYTES)) {
                    String gt = getGt(cell, ANNOTATION_PREFIX_BYTES);
                    annotationIndexGtMap.put(gt, CellUtil.cloneValue(cell));
                } else if (columnStartsWith(cell, FILE_PREFIX_BYTES)) {
                    String gt = getGt(cell, FILE_PREFIX_BYTES);
                    fileIndexGtMap.put(gt, CellUtil.cloneValue(cell));
                } else if (columnStartsWith(cell, PARENTS_PREFIX_BYTES)) {
                    String gt = getGt(cell, PARENTS_PREFIX_BYTES);
                    parentsGtMap.put(gt, CellUtil.cloneValue(cell));
                }
            }
        } else {
            annotationIndexGtMap = Collections.emptyMap();
            fileIndexGtMap = Collections.emptyMap();
            parentsGtMap = Collections.emptyMap();
        }

        if (mendelianErrors) {
            return convertFromMendelianErrors(result, annotationIndexGtMap, fileIndexGtMap);
        } else {
            return convertFromGT(result, annotationIndexGtMap, fileIndexGtMap, parentsGtMap);
        }
    }

    private Set<Variant> convertFromMendelianErrors(Result result,
                                                    Map<String, byte[]> annotationIndexGtMap, Map<String, byte[]> fileIndexGtMap) {
        Set<Variant> variants = new TreeSet<>(INTRA_CHROMOSOME_VARIANT_COMPARATOR);
        Cell cell = result.getColumnLatestCell(family, MENDELIAN_ERROR_COLUMN);
        if (cell != null && cell.getValueLength() != 0) {
            MendelianErrorSampleIndexVariantIterator iterator =
                    MendelianErrorSampleIndexConverter.toVariants(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            while (iterator.hasNext()) {
                String gt = iterator.nextGenotype();
                if (genotypes.isEmpty() || genotypes.contains(gt)) {
                    byte[] fileIndexGt = fileIndexGtMap.get(gt);
                    byte[] annotationIndexGt = annotationIndexGtMap.get(gt);
                    Variant variant = filterAndConvert(iterator, fileIndexGt, annotationIndexGt, null);
                    if (variant != null) {
                        variants.add(variant);
                    }
                } else {
                    iterator.skip();
                }
            }
        }
        return variants;
    }

    protected Set<Variant> convertFromGT(Result result, Map<String, byte[]> annotationIndexGtMap, Map<String, byte[]> fileIndexGtMap,
                                         Map<String, byte[]> parentsGtMap) {
        byte[] row = result.getRow();
        String chromosome = SampleIndexSchema.chromosomeFromRowKey(row);
        int batchStart = SampleIndexSchema.batchStartFromRowKey(row);
        Set<Variant> variants = new TreeSet<>(INTRA_CHROMOSOME_VARIANT_COMPARATOR);
        for (Cell cell : result.rawCells()) {
            if (cell.getQualifierArray()[cell.getQualifierOffset()] != META_PREFIX) {
                String gt = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                byte[] fileIndexGt = fileIndexGtMap.get(gt);
                byte[] annotationIndexGt = annotationIndexGtMap.get(gt);
                byte[] parentsGt = parentsGtMap.get(gt);

                SampleIndexVariantIterator iterator = converter.toVariantsIterator(chromosome, batchStart,
                        cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                while (iterator.hasNext()) {
                    Variant variant = filterAndConvert(iterator, fileIndexGt, annotationIndexGt, parentsGt);
                    if (variant != null) {
                        variants.add(variant);
                    }
                }
            }
        }
        return variants;
    }

    private Variant filterAndConvert(SampleIndexVariantIterator iterator, byte[] fileIndexGt, byte[] annotationIndexGt, byte[] parentsGt) {
        int idx = iterator.nextIndex();
        // Either call to next() or to skip(), but no both

        // Test file index (if any)
        if (fileIndexGt == null || testIndex(fileIndexGt[idx], fileIndexMask, fileIndex)) {

            // Test annotation index (if any)
            if (annotationIndexGt == null || testIndex(annotationIndexGt[idx], annotationIndexMask, annotationIndexMask)) {

                // Test parents filter (if any)
                if (parentsGt == null || testParentsGenotypeCode(parentsGt[idx], fatherFilter, motherFilter)) {

                    // Only at this point, get the variant.
                    Variant variant = iterator.next();

                    // Apply rest of filters
                    return filter(variant);
                }
            }
        }
        iterator.skip();
        return null;
    }

    private Variant filter(Variant variant) {
        //Test region filter (if any)
        if (regionFilter == null || regionFilter.contains(variant.getChromosome(), variant.getStart())) {

            // Test type filter (if any)
            if (typesFilter == null || typesFilter.contains(variant.getType())) {
                return variant;
            }
        }
        return null;
    }

    public static boolean columnStartsWith(Cell cell, byte[] prefix) {
        return AbstractPhoenixConverter.columnStartsWith(cell, prefix);
    }

    public Map<String, List<Variant>> convertToMap(Result result) {
        Map<String, List<Variant>> map = new HashMap<>();
        for (Cell cell : result.rawCells()) {
            String gt = Bytes.toString(CellUtil.cloneQualifier(cell));
            if (gt.charAt(0) != META_PREFIX) {
                map.put(gt, converter.toVariants(cell));
            }
        }
        return map;
    }

    public int convertToCount(Result result) {
        int count = 0;
        for (Cell cell : result.rawCells()) {
            String column = Bytes.toString(CellUtil.cloneQualifier(cell));
            if (column.startsWith(GENOTYPE_COUNT_PREFIX)) {
                count += Bytes.toInt(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            }
        }
        return count;
    }
}
