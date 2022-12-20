package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexEntry;

import java.util.Iterator;

/**
 * Iterate through the variants of a SampleIndexEntry.
 *
 * Multiple implementations to allow different ways to walk through the sample index entry.
 */
public interface SampleIndexEntryIterator extends Iterator<Variant> {

    /**
     * @return {@code true} if the iteration has more elements
     */
    boolean hasNext();

    /**
     * Skip next element. Avoid conversion.
     */
    void skip();

    /**
     * Move cursor to next variant.
     * @return next variant
     */
    Variant next();

    default SampleVariantIndexEntry nextSampleVariantIndexEntry() {
        AnnotationIndexEntry annotationIndexEntry = nextAnnotationIndexEntry();
        if (annotationIndexEntry != null) {
            // Make a copy of the AnnotationIndexEntry!
            // This object could be reused
            annotationIndexEntry = new AnnotationIndexEntry(annotationIndexEntry);
        }
        BitBuffer fileIndex = null;
        if (hasFileIndex()) {
            fileIndex = nextFileIndexEntry();
        }
        Byte parentsCode = null;
        if (hasParentsIndex()) {
            parentsCode = nextParentsIndexEntry();
        }
        String genotype = nextGenotype();
        Variant variant = next();
        return new SampleVariantIndexEntry(variant, fileIndex, genotype, annotationIndexEntry, parentsCode);
    }

    /**
     * @return the index of the element that would be returned by a
     * subsequent call to {@code next}.
     */
    int nextIndex();

    /**
     * @return the non intergenic index of the element that would be returned by a
     * subsequent call to {@code next}.
     */
    int nextNonIntergenicIndex();

    int nextClinicalIndex();

    /**
     * @return the genotype of the next element.
     */
    String nextGenotype();

    boolean hasFileIndex();

    boolean isMultiFileIndex();

    /**
     * @return the file index value of the next element.
     */
    BitBuffer nextFileIndexEntry();

    BitBuffer nextMultiFileIndexEntry();

    boolean hasParentsIndex();

    /**
     * @return the parents index value of the next element.
     */
    byte nextParentsIndexEntry();

    /**
     * @return the AnnotationIndexEntry of the next element.
     */
    AnnotationIndexEntry nextAnnotationIndexEntry();

    int getApproxSize();
}
