package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.SampleIndexVariantAnnotationConverter;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.SampleIndexVariantAnnotation;
import org.opencb.opencga.storage.hadoop.variant.index.core.filters.IndexFieldFilter;
import org.opencb.opencga.storage.hadoop.variant.index.family.MendelianErrorSampleIndexEntryIterator;
import org.opencb.opencga.storage.hadoop.variant.index.query.LocusQuery;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleFileIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.query.SingleSampleIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexEntry.SampleIndexGtEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.QueryOperation.OR;
import static org.opencb.opencga.storage.hadoop.variant.index.IndexUtils.testIndex;
import static org.opencb.opencga.storage.hadoop.variant.index.IndexUtils.testParentsGenotypeCode;

/**
 * Converts SampleIndexEntry to collection of variants.
 * Applies filters based on SingleSampleIndexQuery.
 *
 * Created on 18/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class AbstractSampleIndexEntryFilter<T> {

    private final SingleSampleIndexQuery query;
    private final LocusQuery locusQuery;
    private final Logger logger = LoggerFactory.getLogger(AbstractSampleIndexEntryFilter.class);
    private final List<Integer> annotationIndexPositions;
    private final SampleIndexVariantBiConverter converter;

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

    private static final boolean[] DE_NOVO_STRICT_MENDELIAN_ERROR_CODES = new boolean[]{
                    /* | Code  |   Dad  | Mother | Kid  |  deNovo | deNovoStrict | */
            false,  /* |   0   |        |        |      |         |              | */
            false,  /* |   1   |   1/1  |  1/1   | 0/1  |         |              | */
            true,   /* |   2   |   0/0  |  0/0   | 0/1  |   true  |    true      | */
            false,  /* |   3   |   0/0  | !0/0   | 1/1  |   true  |              | */
            false,  /* |   4   |  !0/0  |  0/0   | 1/1  |   true  |              | */
            true,   /* |   5   |   0/0  |  0/0   | 1/1  |   true  |    true      | */
            false,  /* |   6   |   1/1  | !1/1   | 0/0  |         |              | */
            false,  /* |   7   |  !1/1  |  1/1   | 0/0  |         |              | */
            false,  /* |   8   |   1/1  |  1/1   | 0/0  |         |              | */
            false,  /* |   9   |        |  1/1   | 0/0  |         |              | */
            false,  /* |  10   |        |  0/0   | 1/1  |   true  |              | */
            false,  /* |  11   |   1/1  |        | 0/0  |         |              | */
            false,  /* |  12   |   0/0  |        | 1/1  |   true  |              | */

    };

    public AbstractSampleIndexEntryFilter(SingleSampleIndexQuery query) {
        this(query, null);
    }

    public AbstractSampleIndexEntryFilter(SingleSampleIndexQuery query, LocusQuery locusQuery) {
        this.query = query;
        converter = new SampleIndexVariantBiConverter(query.getSchema());
        this.locusQuery = locusQuery == null || locusQuery.isEmpty() ? null : locusQuery;

        int[] countsPerBit = IndexUtils.countPerBit(new byte[]{query.getAnnotationIndex()});

        annotationIndexPositions = new ArrayList<>(8);
        for (int i = 0; i < countsPerBit.length; i++) {
            if (countsPerBit[i] == 1) {
                annotationIndexPositions.add(i);
            }
        }
    }

    protected abstract T getNext(SampleIndexEntryIterator variants);

    protected abstract Variant toVariant(T v);

    protected abstract boolean sameGenomicVariant(T v1, T v2);

    protected abstract Comparator<T> getComparator();

    public Collection<T> filter(SampleIndexEntry sampleIndexEntry) {
        if (query.getMendelianError()) {
            return filterMendelian(converter.toMendelianIterator(sampleIndexEntry));
        } else {
            return filter(sampleIndexEntry, false);
        }
    }

    public int filterAndCount(SampleIndexEntry sampleIndexEntry) {
        if (query.getMendelianError()) {
            return filterMendelian(converter.toMendelianIterator(sampleIndexEntry)).size();
        } else {
            return filter(sampleIndexEntry, true).size();
        }
    }

    private Set<T> filterMendelian(MendelianErrorSampleIndexEntryIterator iterator) {
        // Use SET to ensure order and remove duplicates
        Set<T> variants = new TreeSet<>(getComparator());

        if (iterator != null) {
            while (iterator.hasNext()) {
                int mendelianErrorCode = iterator.nextMendelianErrorCode();
                String genotype = iterator.nextGenotype();
                if (!testDeNovo(mendelianErrorCode, genotype)) {
                    iterator.skip();
                } else {
                    T variant = filter(iterator);
                    if (variant != null) {
                        variants.add(variant);
                    }
                }
            }
        }
        return variants;
    }

    private boolean testDeNovo(int mendelianErrorCode, String genotype) {
        return testDeNovo(mendelianErrorCode, genotype, query.getMendelianErrorType());
    }

    public static boolean testDeNovo(int mendelianErrorCode, String genotype,
                                     SampleIndexQuery.MendelianErrorType mendelianErrorType) {
        boolean isDeNovo = isDeNovo(mendelianErrorCode);
        if (isDeNovo) {
            // Discard those de-novo variants where the GT does not contain the main alternate
            if (!GenotypeClass.MAIN_ALT.test(genotype)) {
                return false;
            }
        }

        switch (mendelianErrorType) {
            case ALL:
                // Either mendelian-error or valid de-novo
                return true;
            case DE_NOVO:
                // accept any valid de-novo variant
                return isDeNovo;
            case DE_NOVO_STRICT:
                return isDeNovoStrict(mendelianErrorCode);
            default:
                throw new IllegalArgumentException("Unknown MendelianErrorType " + mendelianErrorType);
        }
    }

    public static boolean isDeNovo(int mendelianErrorCode) {
        return DE_NOVO_MENDELIAN_ERROR_CODES[mendelianErrorCode];
    }

    public static boolean isDeNovoStrict(int mendelianErrorCode) {
        return DE_NOVO_STRICT_MENDELIAN_ERROR_CODES[mendelianErrorCode];
    }

    private Collection<T> filter(SampleIndexEntry entry, boolean count) {
        Map<String, SampleIndexGtEntry> gts = entry.getGts();
        List<List<T>> variantsByGt = new ArrayList<>(gts.size());
        int numVariants = 0;
        // Use countIterator only if don't need to filter by locus or by type
        boolean countIterator = count
                && locusQuery == null
                && CollectionUtils.isEmpty(query.getVariantTypes())
                && !query.isMultiFileSample();
        for (SampleIndexGtEntry gtEntry : gts.values()) {
            MutableInt expectedResultsFromAnnotation = new MutableInt(getExpectedResultsFromAnnotation(gtEntry));

            SampleIndexEntryIterator variantIterator = converter.toVariantsIterator(gtEntry, countIterator);
            ArrayList<T> variants = new ArrayList<>(variantIterator.getApproxSize());
            try {
                while (expectedResultsFromAnnotation.intValue() > 0 && variantIterator.hasNext()) {
                    T variant = filter(variantIterator, expectedResultsFromAnnotation);
                    if (variant != null) {
                        variants.add(variant);
                        numVariants++;
                    }
                }
            } catch (Exception e) {
                logger.error("Error '{}' filtering SampleIndexGtEntry. sample={}, region={}:{} gt={}",
                        e.getClass().getName(),
                        entry.getSampleId(),
                        entry.getChromosome(), entry.getBatchStart(),
                        gtEntry.getGt());
                try {
                    logger.warn(gtEntry.toStringSummary());
                    logger.warn(converter.getSchema().toString());
                } catch (Exception exception) {
                    e.addSuppressed(exception);
                }
                throw e;
            }
            if (!variants.isEmpty()) {
                variantsByGt.add(variants);
            }
        }

        // Shortcut. Do not sort or remove duplicates if empty of there are only variants from one genotype
        if (variantsByGt.isEmpty()) {
            return Collections.emptyList();
        } else if (variantsByGt.size() == 1) {
            return variantsByGt.get(0);
        }

        List<T> variants = new ArrayList<>(numVariants);

        for (List<T> variantList : variantsByGt) {
            variants.addAll(variantList);
        }

        boolean mayHaveDiscrepancies = query.isMultiFileSample() && entry.getDiscrepancies() > 0;

        // Only sort not counting or the sample may have discrepancies
        if (!count || mayHaveDiscrepancies) {
            // List.sort is much faster than a TreeSet
            variants.sort(getComparator());

            if (mayHaveDiscrepancies) {
                // Remove possible duplicated elements
                Iterator<T> iterator = variants.iterator();
                T variant = iterator.next();
                while (iterator.hasNext()) {
                    T next = iterator.next();
                    if (sameGenomicVariant(variant, next)) {
                        iterator.remove();
                    } else {
                        variant = next;
                    }
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

    private T filter(SampleIndexEntryIterator variants) {
        return filter(variants, new MutableInt(Integer.MAX_VALUE));
    }

    private T filter(SampleIndexEntryIterator variants, MutableInt expectedResultsFromAnnotation) {
        // Either call to next() or to skip(), but no both

        SampleIndexVariantAnnotation annotationIndex;
        try {
            annotationIndex = variants.nextAnnotationIndexEntry();
        } catch (RuntimeException e) {
            logger.error("Error reading AnnotationIndexEntry at " + variants.toString());
            throw VariantQueryException.internalException(e);
        }

        // Test annotation index (if any)
        if (annotationIndex == null
                || !annotationIndex.hasSummaryIndex()
                || testIndex(annotationIndex.getSummaryIndex(), query.getAnnotationIndexMask(), query.getAnnotationIndex())) {
            expectedResultsFromAnnotation.decrement();

            // Test other annotation index and popFreq (if any)
            if (annotationIndex == null
                    || filterClinicalFields(annotationIndex)
                    && filterBtCtTfFields(annotationIndex)
                    && filterPopFreq(annotationIndex)) {

                // Test file index (if any)
                if (filterFile(variants)) {

                    // Test parents filter (if any)
                    if (!variants.hasParentsIndex() || testParentsGenotypeCode(
                            variants.nextParentsIndexEntry(),
                            query.getFatherFilter(),
                            query.getMotherFilter())) {

                        // Only at this point, get the variant.
                        T variant = getNext(variants);

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
        if (query.emptyFileIndex() || !variants.hasFileIndex()) {
            return true;
        }
        if (query.getSampleFileIndexQuery().getOperation() == null || query.getSampleFileIndexQuery().getOperation() == OR) {
            if (filterFileAnyMatch(variants.nextFileIndexEntry())) {
                return true;
            }
            while (variants.isMultiFileIndex()) {
                if (filterFileAnyMatch(variants.nextMultiFileIndexEntry())) {
                    return true;
                }
            }
        } else {
            boolean[] passedFilters = new boolean[query.getSampleFileIndexQuery().size()];
            if (filterFileAllMatch(passedFilters, variants.nextFileIndexEntry())) {
                return true;
            }
            while (variants.isMultiFileIndex()) {
                if (filterFileAllMatch(passedFilters, variants.nextMultiFileIndexEntry())) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean filterFileAllMatch(boolean[] passedFilters, BitBuffer fileIndexEntry) {
        int numPass = 0;
        for (int i = 0; i < query.getSampleFileIndexQuery().size(); i++) {
            if (!passedFilters[i]) {
                SampleFileIndexQuery fileIndexQuery = query.getSampleFileIndexQuery().get(i);
                passedFilters[i] = filterFile(fileIndexEntry, fileIndexQuery);
                if (passedFilters[i]) {
                    numPass++;
                }
            } else {
                numPass++;
            }
        }
        return numPass == passedFilters.length;
    }

    private boolean filterFileAnyMatch(BitBuffer fileIndex) {
        // Return true if any file filter matches
        // return query.getSampleFileIndexQuery().stream().anyMatch(sampleFileIndexQuery -> filterFile(fileIndex, sampleFileIndexQuery));
        for (SampleFileIndexQuery sampleFileIndexQuery : query.getSampleFileIndexQuery()) {
            if (filterFile(fileIndex, sampleFileIndexQuery)) {
                // Any match
                return true;
            }
        }
        // No match
        return false;
    }

    private boolean filterFile(BitBuffer fileIndex, SampleFileIndexQuery fileQuery) {
        for (IndexFieldFilter filter : fileQuery.getFilters()) {
            if (!filter.readAndTest(fileIndex)) {
                return false;
            }
        }
        return true;
    }

    public static boolean isNonIntergenic(byte summaryIndex) {
        return IndexUtils.testIndex(summaryIndex, SampleIndexVariantAnnotationConverter.INTERGENIC_MASK, (byte) 0);
    }

    public static boolean isClinical(byte summaryIndex) {
        return IndexUtils.testIndex(summaryIndex,
                SampleIndexVariantAnnotationConverter.CLINICAL_MASK, SampleIndexVariantAnnotationConverter.CLINICAL_MASK);
    }

    private boolean filterPopFreq(SampleIndexVariantAnnotation annotationIndex) {
        return query.getAnnotationIndexQuery().getPopulationFrequencyFilter().test(annotationIndex.getPopFreqIndex());
    }

    private boolean filterClinicalFields(SampleIndexVariantAnnotation annotationIndex) {
        if (query.getAnnotationIndexQuery().getClinicalFilter().isNoOp()) {
            // No filter required
            return true;
        }
        if (annotationIndex == null || !annotationIndex.hasSummaryIndex()) {
            // unable to filter by this field
            return true;
        }
        if (!annotationIndex.hasClinical()) {
            return false;
        }
        // FIXME
        return query.getAnnotationIndexQuery().getClinicalFilter().test(annotationIndex.getClinicalIndex());
    }

    private boolean filterBtCtTfFields(SampleIndexVariantAnnotation annotationIndex) {
        if (annotationIndex == null || !annotationIndex.hasSummaryIndex()) {
            // Missing annotation. Unable to filter
            return true;
        }
        if (annotationIndex.isIntergenic()) {
            // unable to filter by this field
            return true;
        }
        if (annotationIndex.hasBtIndex()
                && !query.getAnnotationIndexQuery().getBiotypeFilter().test(annotationIndex.getBtIndex())) {
            return false;
        }

        if (annotationIndex.hasCtIndex()
                && !query.getAnnotationIndexQuery().getConsequenceTypeFilter().test(annotationIndex.getCtIndex())) {
            return false;
        }

        if (annotationIndex.hasTfIndex()
                && !query.getAnnotationIndexQuery().getTranscriptFlagFilter().test(annotationIndex.getTfIndex())) {
            return false;
        }

        if (annotationIndex.getCtBtTfCombination().getMatrix() != null
                && !query.getAnnotationIndexQuery().getCtBtTfFilter().test(annotationIndex.getCtBtTfCombination(),
                annotationIndex.getCtIndex(),
                annotationIndex.getBtIndex(),
                annotationIndex.getTfIndex())) {
            return false;
        }

        return true;
    }

    private T filter(T v) {
        Variant variant = toVariant(v);
        //Test region filter (if any)
        if (filterLocus(variant)) {

            // Test type filter (if any)
            if (CollectionUtils.isEmpty(query.getVariantTypes()) || query.getVariantTypes().contains(variant.getType())) {
                return v;
            }
        }
        return null;
    }

    private boolean filterLocus(Variant variant) {
        if (locusQuery == null) {
            // No locus filter defined. Skip filter.
            return true;
        }
        for (Region region : locusQuery.getRegions()) {
            if (region.overlaps(variant.getChromosome(), variant.getStart(), variant.getEnd())) {
                return true;
            }
        }
        for (Variant queryVariant : locusQuery.getVariants()) {
            if (variant.sameGenomicVariant(queryVariant)) {
                return true;
            }
        }
        return false;
    }

}
