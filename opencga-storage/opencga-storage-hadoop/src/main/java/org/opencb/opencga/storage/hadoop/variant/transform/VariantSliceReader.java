package org.opencb.opencga.storage.hadoop.variant.transform;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.io.DataReader;
import org.opencb.opencga.storage.hadoop.variant.archive.VariantHbaseTransformTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created on 18/08/16.
 *
 * Group variants from the input VariantReader and make groups of variants contained in regions of size "chunkSize".
 * If a variant is in two or more regions, will be emitted in all of them.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantSliceReader implements DataReader<ImmutablePair<Long, List<Variant>>> {

    private int numSlices;
    protected final Logger logger = LoggerFactory.getLogger(VariantSliceReader.class);
    private final int chunkSize;
    private final VariantReader reader;
    // chromosome -> slice -> variants
    // LinkedHashMap will preserve the reading order for the chromosomes
    private final LinkedHashMap<String, TreeMap<Long, List<Variant>>> bufferTree;
    private String currentChromosome = null;

    public VariantSliceReader(int chunkSize, VariantReader reader) {
        this.chunkSize = chunkSize;
        this.reader = reader;
        bufferTree = new LinkedHashMap<>();
    }

    @Override
    public boolean open() {
        return reader.open();
    }

    @Override
    public boolean pre() {
        return reader.pre();
    }

    @Override
    public boolean post() {
        return reader.post();
    }

    @Override
    public boolean close() {
        return reader.close();
    }


    @Override
    public List<ImmutablePair<Long, List<Variant>>> read() {
        return read(1);
    }

    @Override
    public List<ImmutablePair<Long, List<Variant>>> read(int batchSize) {

        List<ImmutablePair<Long, List<Variant>>> slices = new ArrayList<>(batchSize);

        while (slices.size() < batchSize) {

            List<Variant> read;
            do {
                read = reader.read(10);
                for (Variant variant : read) {
                    addVariant(variant);
                }
            } while (!read.isEmpty() && numSlices < 100);

            // Nothing to read! Empty reader.
            if (numSlices == 0) {
                return slices;
            }

            // Get current chromosome
            if (bufferTree.get(currentChromosome).isEmpty()) {
                for (Map.Entry<String, TreeMap<Long, List<Variant>>> entry : bufferTree.entrySet()) {
                    if (!entry.getValue().isEmpty()) {
                        currentChromosome = entry.getKey();
                        break;
                    }
                }
            }

            TreeMap<Long, List<Variant>> map = bufferTree.get(currentChromosome);

            Long slicePosition = map.firstKey();
            List<Variant> variants = map.remove(slicePosition);
            numSlices--;

            slices.add(new ImmutablePair<>(slicePosition, variants));
        }

        return slices;
    }

    private void addVariant(Variant variant) {
        String chromosome = variant.getChromosome();
        long[] coveredSlicePositions = getCoveredSlicePositions(variant);
        for (long slicePos : coveredSlicePositions) {
            addVariant(variant, chromosome, slicePos);
        }
    }

    private void addVariant(Variant variant, String chromosome, long slicePos) {
        List<Variant> list;
        TreeMap<Long, List<Variant>> positionMap = bufferTree.compute(chromosome,
                (s, map) -> map == null ? new TreeMap<>(Long::compareTo) : map);
        list = positionMap.compute(slicePos, (pos, variants) -> variants == null ? new LinkedList<>() : variants);
        if (list.isEmpty()) {
            // New list, new slice
            numSlices++;
        }
        list.add(variant);

        if (currentChromosome == null) {
            // Set first chromosome
            currentChromosome = chromosome;
        }
    }


    private long[] getCoveredSlicePositions(Variant var) {
        return VariantHbaseTransformTask.getCoveredSlicePositions(var.getChromosome(), var.getStart(), var.getEnd(), chunkSize);
    }

}
