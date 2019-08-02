package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexConverter;
import org.opencb.opencga.storage.hadoop.variant.index.family.MendelianErrorSampleIndexConverter;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexEntry.SampleIndexGtEntry;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleAnnotationIndexQuery.PopulationFrequencyQuery;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleIndexQuery.SingleSampleIndexQuery;

import java.util.*;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.QueryOperation.AND;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.QueryOperation.OR;
import static org.opencb.opencga.storage.hadoop.variant.index.IndexUtils.*;
import static org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema.INTRA_CHROMOSOME_VARIANT_COMPARATOR;

/**
 * Converts SampleIndexEntry to collection of variants.
 * Applies filters based on SingleSampleIndexQuery.
 *
 * Created on 18/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexEntryFilter {

    private SingleSampleIndexQuery query;
    private final SampleIndexConfiguration configuration;
    private Region regionFilter;

    private final List<Integer> annotationIndexPositions;

    private static final boolean[] DE_NOVO_MENDELIAN_ERROR_CODES = new boolean[]{
                   /* | Code  |   Dad  | Mother | Kid  |  deNovo | */
            true,  /* |   0   |        |        |      |         | */
            false, /* |   1   |   1/1  |  1/1   | 0/1  |         | */
            true,  /* |   2   |   0/0  |  0/0   | 0/1  |   true  | */
            true,  /* |   3   |   0/0  | !0/0   | 1/1  |   true  | */
            true,  /* |   4   |  !0/0  |  0/0   | 1/1  |   true  | */
            true,  /* |   5   |   0/0  |  0/0   | 1/1  |   true  | */
            false, /* |   6   |   1/1  | !1/1   | 0/0  |         | */
            false, /* |   7   |  !1/1  |  1/1   | 0/0  |         | */
            false, /* |   8   |   1/1  |  1/1   | 0/0  |         | */
            false, /* |   9   |        |  1/1   | 0/0  |         | */
            true,  /* |  10   |        |  0/0   | 1/1  |   true  | */
            false, /* |  11   |   1/1  |        | 0/0  |         | */
            true,  /* |  12   |   0/0  |        | 1/1  |   true  | */

    };

    public SampleIndexEntryFilter(SingleSampleIndexQuery query, SampleIndexConfiguration configuration) {
        this(query, configuration, null);
    }

    public SampleIndexEntryFilter(SingleSampleIndexQuery query, SampleIndexConfiguration configuration, Region regionFilter) {
        this.query = query;
        this.configuration = configuration;
        this.regionFilter = regionFilter;

        int[] countsPerBit = IndexUtils.countPerBit(new byte[]{query.getAnnotationIndex()});

        annotationIndexPositions = new ArrayList<>(8);
        for (int i = 0; i < countsPerBit.length; i++) {
            if (countsPerBit[i] == 1) {
                annotationIndexPositions.add(i);
            }
        }
    }

    public Collection<Variant> filter(SampleIndexEntry sampleIndexEntry) {
        if (query.getMendelianError()) {
            return filterMendelian(sampleIndexEntry.getGts(), sampleIndexEntry.getMendelianVariants());
        } else {
            return filter(sampleIndexEntry.getGts(), false);
        }
    }

    public int filterAndCount(SampleIndexEntry sampleIndexEntry) {
        if (query.getMendelianError()) {
            return filterMendelian(sampleIndexEntry.getGts(), sampleIndexEntry.getMendelianVariants()).size();
        } else {
            return filter(sampleIndexEntry.getGts(), true).size();
        }
    }

    private Set<Variant> filterMendelian(Map<String, SampleIndexGtEntry> gts,
                                         MendelianErrorSampleIndexConverter.MendelianErrorSampleIndexVariantIterator iterator) {
        Set<Variant> variants = new TreeSet<>(INTRA_CHROMOSOME_VARIANT_COMPARATOR);

        if (iterator != null) {
            while (iterator.hasNext()) {
                String gt = iterator.nextGenotype();
                int mendelianErrorCode = iterator.nextCode();
                if (query.isOnlyDeNovo() && !isDeNovo(mendelianErrorCode)) {
                    iterator.skip();
                } else {
                    SampleIndexGtEntry gtEntry = gts.computeIfAbsent(gt, SampleIndexGtEntry::new);
                    Variant variant = filter(gtEntry, iterator);
                    if (variant != null) {
                        variants.add(variant);
                    }
                }
            }
        }
        return variants;
    }

    public static boolean isDeNovo(int mendelianErrorCode) {
        return DE_NOVO_MENDELIAN_ERROR_CODES[mendelianErrorCode];
    }

    private Collection<Variant> filter(Map<String, SampleIndexGtEntry> gts, boolean count) {

        List<List<Variant>> variantsByGt = new ArrayList<>(gts.size());
        int numVariants = 0;
        for (SampleIndexGtEntry gtEntry : gts.values()) {

            MutableInt expectedResultsFromAnnotation = new MutableInt(getExpectedResultsFromAnnotation(gtEntry));

            ArrayList<Variant> variants = new ArrayList<>(gtEntry.getVariants().getApproxSize());
            variantsByGt.add(variants);
            while (expectedResultsFromAnnotation.intValue() > 0 && gtEntry.getVariants().hasNext()) {
                Variant variant = filter(gtEntry, expectedResultsFromAnnotation);
                if (variant != null) {
                    variants.add(variant);
                    numVariants++;
                }
            }
        }

        if (variantsByGt.size() == 1) {
            return variantsByGt.get(0);
        }

        List<Variant> variants = new ArrayList<>(numVariants);

        for (List<Variant> variantList : variantsByGt) {
            variants.addAll(variantList);
        }

        // Only sort if not counting
        if (!count) {
            // List.sort is much faster than a TreeSet
            variants.sort(INTRA_CHROMOSOME_VARIANT_COMPARATOR);
        }

        return variants;
    }

    private int getExpectedResultsFromAnnotation(SampleIndexGtEntry gtEntry) {
        int expectedResultsFromAnnotation = Integer.MAX_VALUE;
        if (gtEntry.getAnnotationCounts() != null) {
            for (Integer idx : annotationIndexPositions) {
                expectedResultsFromAnnotation = Math.min(expectedResultsFromAnnotation, gtEntry.getAnnotationCounts()[idx]);
            }
        }
        return expectedResultsFromAnnotation;
    }

    private Variant filter(SampleIndexGtEntry gtEntry, MutableInt expectedResultsFromAnnotation) {
        return filter(gtEntry, gtEntry.getVariants(), expectedResultsFromAnnotation);
    }

    private Variant filter(SampleIndexGtEntry gtEntry, SampleIndexVariantBiConverter.SampleIndexVariantIterator variants) {
        return filter(gtEntry, variants, new MutableInt(Integer.MAX_VALUE));
    }

    private Variant filter(SampleIndexGtEntry gtEntry, SampleIndexVariantBiConverter.SampleIndexVariantIterator variants,
                           MutableInt expectedResultsFromAnnotation) {
        int idx = variants.nextIndex();
        // Either call to next() or to skip(), but no both

        // Test annotation index (if any)
        if (gtEntry.getAnnotationIndexGt() == null
                || testIndex(gtEntry.getAnnotationIndexGt()[idx], query.getAnnotationIndexMask(), query.getAnnotationIndex())) {
            expectedResultsFromAnnotation.decrement();

            if (filterOtherAnnotFields(gtEntry, variants) && filterPopFreq(gtEntry, idx)) {

                // Test file index (if any)
                if (gtEntry.getFileIndexGt() == null
                        || testIndex(gtEntry.getFileIndexGt()[idx], query.getFileIndexMask(), query.getFileIndex())) {

                    // Test parents filter (if any)
                    if (gtEntry.getParentsGt() == null
                            || testParentsGenotypeCode(gtEntry.getParentsGt()[idx], query.getFatherFilter(), query.getMotherFilter())) {

                        // Only at this point, get the variant.
                        Variant variant = variants.next();

                        // Apply rest of filters
                        return filter(variant);
                    }
                }
            }


        }
        variants.skip();
        return null;
    }

    protected static boolean isNonIntergenic(byte[] annotationIndex, int idx) {
        return IndexUtils.testIndex(annotationIndex[idx], AnnotationIndexConverter.INTERGENIC_MASK, (byte) 0);
    }

    private boolean filterPopFreq(SampleIndexGtEntry gtEntry, int idx) {
        if (query.getAnnotationIndexQuery().getPopulationFrequencyQueries().isEmpty() || gtEntry.getPopulationFrequencyIndexGt() == null) {
            return true;
        }
        for (PopulationFrequencyQuery q : query.getAnnotationIndexQuery().getPopulationFrequencyQueries()) {
            int popFreqPosition = (q.getPosition() + idx * configuration.getPopulationRanges().size())
                    * AnnotationIndexConverter.POP_FREQ_SIZE;
            int byteIdx = popFreqPosition / Byte.SIZE;
            int bitIdx = popFreqPosition % Byte.SIZE;

            int code = (gtEntry.getPopulationFrequencyIndexGt()[byteIdx] >>> bitIdx) & 0b11;
            if (q.getMinCodeInclusive() <= code && code < q.getMaxCodeExclusive()) {
                if (query.getAnnotationIndexQuery().getPopulationFrequencyQueryOperator().equals(OR)) {
                    // Require ANY match
                    // If any match, SUCCESS
                    return true;
                }
            } else {
                if (query.getAnnotationIndexQuery().getPopulationFrequencyQueryOperator().equals(AND)) {
                    // Require ALL matches.
                    // If any fail, FAIL
                    return false;
                }
            }
        }

        if (query.getAnnotationIndexQuery().getPopulationFrequencyQueryOperator().equals(AND)) {
            // Require ALL matches.
            // If no fails, SUCCESS
            return true;
        } else {
            // Require ANY match.
            // If no matches, and partial, UNKNOWN. If unknown, SUCCESS
            // If no matches, and fully covered, FAIL
            return query.getAnnotationIndexQuery().isPopulationFrequencyQueryPartial();
        }
    }

    private boolean filterOtherAnnotFields(SampleIndexGtEntry gtEntry, SampleIndexVariantBiConverter.SampleIndexVariantIterator variants) {
        int nonIntergenicIndex = variants.nextNonIntergenicIndex();
        if (nonIntergenicIndex < 0) {
            // unable to filter by this field
            return true;
        }
        if (gtEntry.getBiotypeIndexGt() == null || testIndexAny(gtEntry.getBiotypeIndexGt()[nonIntergenicIndex],
                query.getAnnotationIndexQuery().getBiotypeMask())) {

            if (gtEntry.getConsequenceTypeIndexGt() == null || testIndexAny(gtEntry.getConsequenceTypeIndexGt(), nonIntergenicIndex,
                    query.getAnnotationIndexQuery().getConsequenceTypeMask())) {

                return true;
            }
        }
        return false;
    }

    private Variant filter(Variant variant) {
        //Test region filter (if any)
        if (regionFilter == null || regionFilter.contains(variant.getChromosome(), variant.getStart())) {

            // Test type filter (if any)
            if (CollectionUtils.isEmpty(query.getVariantTypes()) || query.getVariantTypes().contains(variant.getType())) {
                return variant;
            }
        }
        return null;
    }

}
