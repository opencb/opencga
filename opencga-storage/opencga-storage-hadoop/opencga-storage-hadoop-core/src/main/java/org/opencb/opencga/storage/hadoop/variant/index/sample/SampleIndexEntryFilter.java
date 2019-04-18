package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.variant.index.family.MendelianErrorSampleIndexConverter;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexEntry.SampleIndexGtEntry;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexQuery.SingleSampleIndexQuery;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

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

    public SampleIndexEntryFilter(SingleSampleIndexQuery query) {
        this(query, null);
    }

    public SampleIndexEntryFilter(SingleSampleIndexQuery query, Region regionFilter) {
        this.query = query;
        this.regionFilter = regionFilter;
    }

    protected Collection<Variant> filter(SampleIndexEntry sampleIndexEntry) {
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
                    Variant variant = filterAndConvert(gtEntry, iterator);
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

    protected Set<Variant> filter(Map<String, SampleIndexGtEntry> gts) {
        Set<Variant> variants = new TreeSet<>(INTRA_CHROMOSOME_VARIANT_COMPARATOR);
        for (SampleIndexGtEntry entry : gts.values()) {
            while (entry.getVariants().hasNext()) {
                Variant variant = filterAndConvert(entry);
                if (variant != null) {
                    variants.add(variant);
                }
            }
        }
        return variants;
    }

    private Variant filterAndConvert(SampleIndexGtEntry gtEntry) {
        return filterAndConvert(gtEntry, gtEntry.getVariants());
    }

    private Variant filterAndConvert(SampleIndexGtEntry gtEntry, SampleIndexVariantBiConverter.SampleIndexVariantIterator variants) {
        int idx = variants.nextIndex();
        // Either call to next() or to skip(), but no both

        // Test annotation index (if any)
        if (gtEntry.getAnnotationIndexGt() == null
                || testIndex(gtEntry.getAnnotationIndexGt()[idx], query.getAnnotationIndexMask(), query.getAnnotationIndexMask())) {

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
