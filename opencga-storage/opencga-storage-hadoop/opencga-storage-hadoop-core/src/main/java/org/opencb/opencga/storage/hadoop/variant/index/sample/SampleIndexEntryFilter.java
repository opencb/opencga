package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexConverter;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexEntry;
import org.opencb.opencga.storage.hadoop.variant.index.family.MendelianErrorSampleIndexEntryIterator;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleAnnotationIndexQuery.PopulationFrequencyQuery;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleFileIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.query.SingleSampleIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexEntry.SampleIndexGtEntry;

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

    public SampleIndexEntryFilter(SingleSampleIndexQuery query) {
        this(query, null);
    }

    public SampleIndexEntryFilter(SingleSampleIndexQuery query, Region regionFilter) {
        this.query = query;
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
            return filterMendelian(sampleIndexEntry.mendelianIterator());
        } else {
            return filter(sampleIndexEntry, false);
        }
    }

    public int filterAndCount(SampleIndexEntry sampleIndexEntry) {
        if (query.getMendelianError()) {
            return filterMendelian(sampleIndexEntry.mendelianIterator()).size();
        } else {
            return filter(sampleIndexEntry, true).size();
        }
    }

    private Set<Variant> filterMendelian(MendelianErrorSampleIndexEntryIterator iterator) {
        Set<Variant> variants = new TreeSet<>(INTRA_CHROMOSOME_VARIANT_COMPARATOR);

        if (iterator != null) {
            while (iterator.hasNext()) {
                int mendelianErrorCode = iterator.nextMendelianErrorCode();
                if (query.isOnlyDeNovo() && !isDeNovo(mendelianErrorCode)) {
                    iterator.skip();
                } else {
                    Variant variant = filter(iterator);
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

    private Collection<Variant> filter(SampleIndexEntry entry, boolean count) {
        Map<String, SampleIndexGtEntry> gts = entry.getGts();
        List<List<Variant>> variantsByGt = new ArrayList<>(gts.size());
        int numVariants = 0;
        // Use countIterator only if don't need to filter by region or by type
        boolean countIterator = count && regionFilter == null && CollectionUtils.isEmpty(query.getVariantTypes());
        for (SampleIndexGtEntry gtEntry : gts.values()) {

            MutableInt expectedResultsFromAnnotation = new MutableInt(getExpectedResultsFromAnnotation(gtEntry));

            SampleIndexEntryIterator variantIterator = gtEntry.iterator(countIterator);
            ArrayList<Variant> variants = new ArrayList<>(variantIterator.getApproxSize());
            variantsByGt.add(variants);
            while (expectedResultsFromAnnotation.intValue() > 0 && variantIterator.hasNext()) {
                Variant variant = filter(variantIterator, expectedResultsFromAnnotation);
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

    private Variant filter(SampleIndexEntryIterator variants) {
        return filter(variants, new MutableInt(Integer.MAX_VALUE));
    }

    private Variant filter(SampleIndexEntryIterator variants, MutableInt expectedResultsFromAnnotation) {
        // Either call to next() or to skip(), but no both

        AnnotationIndexEntry annotationIndexEntry = variants.nextAnnotationIndexEntry();

        // Test annotation index (if any)
        if (annotationIndexEntry == null
                || !annotationIndexEntry.hasSummaryIndex()
                || testIndex(annotationIndexEntry.getSummaryIndex(), query.getAnnotationIndexMask(), query.getAnnotationIndex())) {
            expectedResultsFromAnnotation.decrement();

            // Test other annotation index and popFreq (if any)
            if (annotationIndexEntry == null
                    || filterClinicalFields(annotationIndexEntry)
                    && filterBtCtFields(annotationIndexEntry)
                    && filterPopFreq(annotationIndexEntry)) {

                // Test file index (if any)
                if (filterFile(variants)) {

                    // Test parents filter (if any)
                    if (!variants.hasParentsIndex() || testParentsGenotypeCode(
                            variants.nextParentsIndexEntry(),
                            query.getFatherFilter(),
                            query.getMotherFilter())) {

                        // Only at this point, get the variant.
                        Variant variant = variants.next();

                        // Apply rest of filters
                        return filter(variant);
                    }
                }
            }
        }
        // The variant did not pass the tests. Skip
        variants.skip();
        return null;
    }

    private boolean filterFile(SampleIndexEntryIterator variants) {
        if (query.getFileIndexMask() == EMPTY_MASK || !variants.hasFileIndex()) {
            return true;
        }
        if (filterFile(variants.nextFileIndexEntry())) {
            return true;
        }
        while (variants.isMultiFileIndex()) {
            if (filterFile(variants.nextMultiFileIndexEntry())) {
                return true;
            }
        }
        return false;
    }

    private boolean filterFile(short fileIndex) {
        SampleFileIndexQuery fileQuery = query.getSampleFileIndexQuery();
        int v = fileIndex & fileQuery.getFileIndexMask();

        return (!fileQuery.hasFileIndexMask1() || fileQuery.getValidFileIndex1()[getByte1(v)])
                && (!fileQuery.hasFileIndexMask2() || fileQuery.getValidFileIndex2()[getByte2(v)]);
    }

    public static boolean isNonIntergenic(byte summaryIndex) {
        return IndexUtils.testIndex(summaryIndex, AnnotationIndexConverter.INTERGENIC_MASK, (byte) 0);
    }

    public static boolean isClinical(byte summaryIndex) {
        return IndexUtils.testIndex(summaryIndex, AnnotationIndexConverter.CLINICAL_MASK, AnnotationIndexConverter.CLINICAL_MASK);
    }

    private boolean filterPopFreq(AnnotationIndexEntry annotationIndexEntry) {
        if (query.getAnnotationIndexQuery().getPopulationFrequencyQueries().isEmpty() || annotationIndexEntry.getPopFreqIndex() == null) {
            return true;
        }
        for (PopulationFrequencyQuery q : query.getAnnotationIndexQuery().getPopulationFrequencyQueries()) {
            int code = annotationIndexEntry.getPopFreqIndex()[q.getPosition()];
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

    private boolean filterClinicalFields(AnnotationIndexEntry annotationIndexEntry) {
        if (query.getAnnotationIndexQuery().getClinicalMask() == EMPTY_MASK) {
            // No filter required
            return true;
        }
        if (annotationIndexEntry == null || !annotationIndexEntry.hasSummaryIndex()) {
            // unable to filter by this field
            return true;
        }
        if (!annotationIndexEntry.isClinical()) {
            return false;
        }
        return testIndexAny(annotationIndexEntry.getClinicalIndex(), query.getAnnotationIndexQuery().getClinicalMask());
    }

    private boolean filterBtCtFields(AnnotationIndexEntry annotationIndexEntry) {
        if (annotationIndexEntry == null || !annotationIndexEntry.hasSummaryIndex()) {
            return true;
        }
        if (annotationIndexEntry.isIntergenic()) {
            // unable to filter by this field
            return true;
        }
        if (!annotationIndexEntry.hasBtIndex()
                || testIndexAny(annotationIndexEntry.getBtIndex(), query.getAnnotationIndexQuery().getBiotypeMask())) {

            if (!annotationIndexEntry.hasCtIndex()
                    || testIndexAny(annotationIndexEntry.getCtIndex(), query.getAnnotationIndexQuery().getConsequenceTypeMask())) {

                if (annotationIndexEntry.getCtBtCombination().getCtBtMatrix() == null
                        || query.getAnnotationIndexQuery().getConsequenceTypeMask() == 0
                        || query.getAnnotationIndexQuery().getBiotypeMask() == 0) {
                    return true;
                } else {
                    AnnotationIndexEntry.CtBtCombination ctBtCombination = annotationIndexEntry.getCtBtCombination();
                    // Check ct+bt combinations
                    short ctIndex = annotationIndexEntry.getCtIndex();
                    int numCt = ctBtCombination.getNumCt();
                    int numBt = ctBtCombination.getNumBt();
                    // Pitfall!
                    // if (numCt == 1 || numBt == 1) { return true; } This may not be true.
                    // There could be missing rows/columns in the index
                    byte[] ctBtMatrix = ctBtCombination.getCtBtMatrix();
                    // Check if any combination matches que query filter.
                    // Walk through all values of CT and BT in this variant.
                    // 3 conditions must meet:
                    //  - The CT is part of the query filter
                    //  - The BT is part of the query filter
                    //  - The variant had the combination of both
                    for (int ctIndexPos = 0; ctIndexPos < numCt; ctIndexPos++) {
                        // Get the first CT value from the right.
                        short ct = (short) Integer.lowestOneBit(ctIndex);
                        // Remove the CT value from the index, so the next iteration gets the next value
                        ctIndex &= ~ct;
                        // Check if the CT is part of the query filter
                        if (IndexUtils.testIndexAny(ct, query.getAnnotationIndexQuery().getConsequenceTypeMask())) {
                            // Iterate over the Biotype values
                            byte btIndex = annotationIndexEntry.getBtIndex();
                            for (int btIndexPos = 0; btIndexPos < numBt; btIndexPos++) {
                                // As before, take the first BT value from the right.
                                byte bt = (byte) Integer.lowestOneBit(btIndex);
                                btIndex &= ~bt;
                                // Check if the BT is part of the query filter
                                if (IndexUtils.testIndexAny(bt, query.getAnnotationIndexQuery().getBiotypeMask())) {
                                    // Check if this CT was together with this BT
                                    if ((ctBtMatrix[ctIndexPos] & (1 << btIndexPos)) != 0) {
                                        return true;
                                    }
                                }
                            }
                            // assert btIndex == 0; // We should have removed all BT from the index
                        }
                    }
                    // assert ctIndex == 0; // We should have removed all CT from the index

                    // could not find any valid combination
                    return false;
                }
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
