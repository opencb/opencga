package org.opencb.opencga.storage.hadoop.variant.index.family;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexEntry;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexEntry;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexEntryIterator;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleVariantIndexEntry;

import java.util.*;

import static org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexVariantBiConverter.split;

/**
 * Iterate through the mendelian error variants.
 *
 * Adds a method to indicate, for each variant, which MendelianErrorCode has.
 * It has a CountSampleIndexGtEntryIterator for each genotype to keep an updated pointer of to its annotation, as
 * variants from the MendelianError list are not sequential.
 */
public class MendelianErrorSampleIndexEntryIterator implements SampleIndexEntryIterator {
    private final ListIterator<String> variants;
    private final int size;
    private Variant next;
    private int nextIndex;
    private int nextCode;
    private String nextGt;
    private Map<String, SampleIndexEntryIterator> gtIterators = Collections.emptyMap();

    public MendelianErrorSampleIndexEntryIterator(SampleIndexEntry entry) {
        List<String> values = split(
                entry.getMendelianVariantsValue(),
                entry.getMendelianVariantsOffset(),
                entry.getMendelianVariantsLength());
        size = values.size();
        variants = values.listIterator();
        gtIterators = new HashMap<>(entry.getGts().size());
        for (SampleIndexEntry.SampleIndexGtEntry gtEntry : entry.getGts().values()) {
            gtIterators.put(gtEntry.getGt(), gtEntry.iterator(true));
        }
    }

    public MendelianErrorSampleIndexEntryIterator(byte[] value, int offset, int length) {
        List<String> values = split(value, offset, length);
        size = values.size();
        variants = values.listIterator();
    }

    @Override
    public boolean hasFileIndex() {
        SampleIndexEntryIterator it = getGtIterator();
        return it != null && it.hasFileIndex();
    }

    @Override
    public BitBuffer nextFileIndexEntry() {
        return getGtIterator().nextFileIndexEntry();
    }

    @Override
    public boolean isMultiFileIndex() {
        return getGtIterator().isMultiFileIndex();
    }

    @Override
    public BitBuffer nextMultiFileIndexEntry() {
        return getGtIterator().nextMultiFileIndexEntry();
    }

    @Override
    public boolean hasParentsIndex() {
        SampleIndexEntryIterator it = getGtIterator();
        return it != null && it.hasParentsIndex();
    }

    @Override
    public byte nextParentsIndexEntry() {
        return getGtIterator().nextParentsIndexEntry();
    }

    @Override
    public AnnotationIndexEntry nextAnnotationIndexEntry() {
        SampleIndexEntryIterator it = getGtIterator();
        return it == null ? null : it.nextAnnotationIndexEntry();
    }

    @Override
    public int nextIndex() {
        fetchNextIfNeeded();
        return nextIndex;
    }

    @Override
    public String nextGenotype() {
        fetchNextIfNeeded();
        return nextGt;
    }

    /**
     * @return Gets the mendelian error code of the next variant.
     */
    public int nextMendelianErrorCode() {
        fetchNextIfNeeded();
        return nextCode;
    }

    @Override
    public boolean hasNext() {
        return variants.hasNext();
    }

    @Override
    public void skip() {
        next();
    }

    @Override
    public Variant next() {
        fetchNextIfNeeded();
        Variant variant = next;
        next = null; // Clean next variant
        return variant;
    }

    @Override
    public SampleVariantIndexEntry nextSampleVariantIndexEntry() {
        AnnotationIndexEntry annotationIndexEntry = nextAnnotationIndexEntry();
        BitBuffer fileIndex = null;
        if (hasFileIndex()) {
            fileIndex = nextFileIndexEntry();
        }
        String genotype = nextGenotype();
        int meCode = nextMendelianErrorCode();
        Variant variant = next();
        return new SampleVariantIndexEntry(variant, fileIndex, genotype, annotationIndexEntry, meCode);
    }

    @Override
    public int nextNonIntergenicIndex() {
        SampleIndexEntryIterator gtIterator = getGtIterator();
        return gtIterator == null ? -1 : gtIterator.nextNonIntergenicIndex();
    }

    @Override
    public int nextClinicalIndex() {
        SampleIndexEntryIterator gtIterator = getGtIterator();
        return gtIterator == null ? -1 : gtIterator.nextClinicalIndex();
    }

    @Override
    public int getApproxSize() {
        return size;
    }

    private SampleIndexEntryIterator getGtIterator() {
        return gtIterators.get(nextGenotype());
    }

    private void fetchNextIfNeeded() {
        if (next == null) {
            fetchNext();
        }
        if (next == null) {
            throw new NoSuchElementException();
        }
    }

    private void fetchNext() {
        if (variants.hasNext()) {
            String s = variants.next();
            int idx2 = s.lastIndexOf(MendelianErrorSampleIndexConverter.MENDELIAN_ERROR_SEPARATOR);
            int idx1 = s.lastIndexOf(MendelianErrorSampleIndexConverter.MENDELIAN_ERROR_SEPARATOR, idx2 - 1);
            String variantStr = s.substring(0, idx1);
            nextGt = s.substring(idx1 + 1, idx2);
            String idxCode = s.substring(idx2 + 1);
            int i = idxCode.lastIndexOf(MendelianErrorSampleIndexConverter.MENDELIAN_ERROR_CODE_SEPARATOR);
            nextIndex = i == StringUtils.INDEX_NOT_FOUND
                    ? Integer.valueOf(idxCode)
                    : Integer.valueOf(idxCode.substring(0, i));
            nextCode = i == StringUtils.INDEX_NOT_FOUND
                    ? 0
                    : Integer.valueOf(idxCode.substring(i + 1));
            next = new Variant(variantStr);

            // Move pointer on underlying iterators to point to the correct annotation position
            SampleIndexEntryIterator it = getGtIterator();
            if (it != null) {
                while (it.nextIndex() != nextIndex) {
                    it.skip();
                }
            }

        } else {
            next = null;
            nextGt = null;
            nextIndex = -1;
            nextCode = -1;
        }
    }

}
