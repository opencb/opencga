package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexEntry;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

    /**
     * Get next variant without moving the cursor.
     * @return next variant
     */
    Variant nextVariant();

    default SampleVariantIndexEntry nextSampleVariantIndexEntry() {
        AnnotationIndexEntry annotationIndexEntry = nextAnnotationIndexEntry();
        if (annotationIndexEntry != null) {
            // Make a copy of the AnnotationIndexEntry!
            // This object could be reused
            annotationIndexEntry = new AnnotationIndexEntry(annotationIndexEntry);
        }
        List<BitBuffer> filesIndex = new ArrayList<>();
        List<ByteBuffer> filesData = new ArrayList<>();
        if (hasFileIndex()) {
            filesIndex.add(nextFileIndexEntry());
            if (hasFileDataIndex()) {
                filesData.add(getFileDataEntry());
            }
            while (isMultiFileIndex()) {
                filesIndex.add(nextMultiFileIndexEntry());
                if (hasFileDataIndex()) {
                    filesData.add(getFileDataEntry());
                }
            }
        }
        Byte parentsCode = null;
        if (hasParentsIndex()) {
            parentsCode = nextParentsIndexEntry();
        }
        String genotype = nextGenotype();
        Variant variant = next();
        return new SampleVariantIndexEntry(variant, filesIndex, filesData, genotype, annotationIndexEntry, parentsCode, null);
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

    boolean hasFileDataIndex();

    boolean isMultiFileIndex();

    /**
     * @return the file index value of the next element.
     */
    BitBuffer nextFileIndexEntry();

    ByteBuffer getFileDataEntry();

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
