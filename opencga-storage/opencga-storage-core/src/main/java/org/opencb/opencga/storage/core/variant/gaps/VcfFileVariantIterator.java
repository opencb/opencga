package org.opencb.opencga.storage.core.variant.gaps;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.opencga.storage.core.io.plain.StringDataReader;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;

import java.io.File;
import java.util.*;

/**
 * A buffered variant iterator that reads from a VCF-derived variant stream, maintaining per-chromosome buffers.
 * Extracted from HBase's FillGapsFromFile.VariantIterator to be reusable across storage backends.
 */
public class VcfFileVariantIterator implements ListIterator<Variant> {

    private final File file;
    private final String fileName;
    private final int fileId;
    private final LinkedHashSet<Integer> sampleIds;
    private final Iterator<Variant> variantIterator;
    private final StringDataReader.SizeInputStream sizeInputStream;
    private long lastAvailable;
    private String chromosome;

    // Variants buffer from the current chromosome
    private RandomAccessDequeue<Variant> buffer;
    private ListIterator<Variant> bufferIterator;

    private final Set<String> prevChromosomes = new HashSet<>();
    // Buffer for other chromosomes
    private final Map<String, RandomAccessDequeue<Variant>> bufferByChr = new LinkedHashMap<>();
    private final int maxBufferSize;

    public VcfFileVariantIterator(File file, String fileName, int fileId, LinkedHashSet<Integer> sampleIds,
                                  Iterator<Variant> variantIterator, StringDataReader.SizeInputStream sizeInputStream,
                                  int maxBufferSize) {
        this.file = file;
        this.fileName = fileName;
        this.fileId = fileId;
        this.sampleIds = sampleIds;
        this.variantIterator = variantIterator;
        this.sizeInputStream = sizeInputStream;
        this.maxBufferSize = maxBufferSize;
        buffer = newBuffer();
        bufferIterator = buffer.listIterator();
        this.lastAvailable = sizeInputStream.availableLong();
    }

    public String getFileName() {
        return fileName;
    }

    public int getFileId() {
        return fileId;
    }

    public LinkedHashSet<Integer> getSampleIds() {
        return sampleIds;
    }

    private RandomAccessDequeue<Variant> newBuffer() {
        return new RandomAccessDequeue<>(this.maxBufferSize * 2);
    }

    public long getReadBytes() {
        long newAvailable = sizeInputStream.availableLong();
        long readBytes = lastAvailable - newAvailable;
        lastAvailable = newAvailable;
        return readBytes;
    }

    @Override
    public boolean hasNext() {
        if (bufferIterator.hasNext()) {
            return true;
        } else {
            return addToBuffer() != null;
        }
    }

    @Override
    public Variant next() {
        if (!bufferIterator.hasNext()) {
            if (addToBuffer() == null) {
                throw new NoSuchElementException();
            }
        }
        return bufferIterator.next();
    }

    private int getBufferSize() {
        return buffer.size() + bufferByChr.values().stream().mapToInt(RandomAccessDequeue::size).sum();
    }

    @Override
    public boolean hasPrevious() {
        return bufferIterator.hasPrevious();
    }

    @Override
    public Variant previous() {
        return bufferIterator.previous();
    }

    @Override
    public int nextIndex() {
        return bufferIterator.nextIndex();
    }

    @Override
    public int previousIndex() {
        return bufferIterator.previousIndex();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void set(Variant variant) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void add(Variant variant) {
        throw new UnsupportedOperationException();
    }

    private Variant addToBuffer() {
        if (variantIterator.hasNext()) {
            if (buffer.isEmpty() && getBufferSize() > maxBufferSize) {
                // This chromosome is empty, and we're already reading from another chromosome
                // Do not overflow the buffer
                return null;
            }
            Variant variant = variantIterator.next();
            if (chromosome == null) {
                chromosome = variant.getChromosome();
            }
            if (variant.getChromosome().equals(chromosome)) {
                buffer.add(variant);
                return variant;
            } else {
                bufferByChr.computeIfAbsent(variant.getChromosome(), k -> newBuffer()).add(variant);
                if (prevChromosomes.contains(variant.getChromosome())) {
                    throw new IllegalStateException("Chromosome \"" + variant.getChromosome() + "\" already processed!"
                            + " Unordered chromosomes in file \"" + file + "\"");
                }
                return null;
            }
        }
        return null;
    }

    public Variant getNextVariant(Variant prevVariant) {
        // Look for an actual variant in the buffer
        int i = nextIndex();
        Variant variant = null;
        while (i < buffer.size() && !isNextVariant(prevVariant, variant)) {
            variant = buffer.get(i);
            i++;
        }
        if (!isNextVariant(prevVariant, variant)) {
            // Look for an actual variant in the iterator
            variant = addToBuffer();
            while (variant != null && !isNextVariant(prevVariant, variant)) {
                variant = addToBuffer();
            }
        }
        return variant;
    }

    protected static boolean isNextVariant(Variant prevVariant, Variant variant) {
        if (variant == null) {
            return false;
        } else if (isVariant(variant)) {
            if (prevVariant == null) {
                return true;
            } else {
                if (variant.getChromosome().equals(prevVariant.getChromosome())) {
                    int compare = SampleIndexSchema.INTRA_CHROMOSOME_VARIANT_COMPARATOR.compare(prevVariant, variant);
                    return compare < 0;
                } else {
                    return true;
                }
            }
        } else {
            return false;
        }
    }

    private static boolean isVariant(Variant variant) {
        return variant.getType() != VariantType.NO_VARIATION;
    }

    public void trim() {
        // Remove all variants before the current variant
        while (bufferIterator.nextIndex() > 0) {
            buffer.removeHead();
        }
    }

    public void setChromosome(String newChromosome) {
        if (this.chromosome != null && !this.chromosome.equals(newChromosome)) {
            // When changing chromosome, change the buffer
            prevChromosomes.add(this.chromosome);
            RandomAccessDequeue<Variant> chrBuffer = bufferByChr.remove(newChromosome);
            buffer = chrBuffer == null ? newBuffer() : chrBuffer;
            bufferIterator = buffer.listIterator();
        }
        this.chromosome = newChromosome;
    }

    public String getNextChromosome() {
        if (chromosome == null) {
            Variant variant = getNextVariant(null);
            if (variant == null) {
                return null;
            } else {
                return variant.getChromosome(); // First chromosome
            }
        } else if (bufferByChr.isEmpty()) {
            return null;
        } else {
            return bufferByChr.keySet().iterator().next();
        }
    }
}
