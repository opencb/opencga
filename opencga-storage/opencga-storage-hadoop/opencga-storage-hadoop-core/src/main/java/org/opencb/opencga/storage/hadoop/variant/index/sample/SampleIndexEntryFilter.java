package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.commons.lang3.mutable.MutableInt;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.family.MendelianErrorSampleIndexConverter;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexEntry.SampleIndexGtEntry;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexQuery.SingleSampleIndexQuery;

import java.util.*;

import static org.opencb.opencga.storage.hadoop.variant.index.IndexUtils.testIndex;
import static org.opencb.opencga.storage.hadoop.variant.index.IndexUtils.testParentsGenotypeCode;
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

    public SampleIndexEntryFilter(SingleSampleIndexQuery query) {
        this(query, null);
    }

    public SampleIndexEntryFilter(SingleSampleIndexQuery query, Region regionFilter) {
        this.query = query;
        this.regionFilter = regionFilter;

        int[] countsPerBit = IndexUtils.countPerBit(new byte[]{query.getAnnotationIndexMask()});

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
            return filter(sampleIndexEntry.getGts());
        }
    }

    private Set<Variant> filterMendelian(Map<String, SampleIndexGtEntry> gts,
                                         MendelianErrorSampleIndexConverter.MendelianErrorSampleIndexVariantIterator iterator) {
        Set<Variant> variants = new TreeSet<>(INTRA_CHROMOSOME_VARIANT_COMPARATOR);

        if (iterator != null) {
            while (iterator.hasNext()) {
                String gt = iterator.nextGenotype();
                if (query.getGenotypes().isEmpty() || query.getGenotypes().contains(gt)) {
                    SampleIndexGtEntry gtEntry = gts.computeIfAbsent(gt, SampleIndexGtEntry::new);
                    Variant variant = filter(gtEntry, iterator);
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

    private Set<Variant> filter(Map<String, SampleIndexGtEntry> gts) {
        Set<Variant> variants = new TreeSet<>(INTRA_CHROMOSOME_VARIANT_COMPARATOR);
        for (SampleIndexGtEntry gtEntry : gts.values()) {
            MutableInt expectedResultsFromAnnotation = new MutableInt(getExpectedResultsFromAnnotation(gtEntry));

            while (expectedResultsFromAnnotation.intValue() > 0 && gtEntry.getVariants().hasNext()) {
                Variant variant = filter(gtEntry, expectedResultsFromAnnotation);
                if (variant != null) {
                    variants.add(variant);
                }
            }
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
                || testIndex(gtEntry.getAnnotationIndexGt()[idx], query.getAnnotationIndexMask(), query.getAnnotationIndexMask())) {
            expectedResultsFromAnnotation.decrement();

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
        variants.skip();
        return null;
    }

    private Variant filter(Variant variant) {
        //Test region filter (if any)
        if (regionFilter == null || regionFilter.contains(variant.getChromosome(), variant.getStart())) {

            // Test type filter (if any)
            if (query.getVariantTypes() == null || query.getVariantTypes().contains(variant.getType())) {
                return variant;
            }
        }
        return null;
    }

}
