/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.hadoop.variant.transform;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.ProgressLogger;
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

    private final Logger logger = LoggerFactory.getLogger(VariantSliceReader.class);
    private final ProgressLogger progressLogger;
    private final int chunkSize;
    private final String studyId;
    private final String fileId;
    private final DataReader<Variant> reader;
    private final int sliceOffset;

    // chromosome -> slice -> variants
    // LinkedHashMap will preserve the reading order for the chromosomes
    private final LinkedHashMap<String, TreeMap<Long, List<Variant>>> bufferTree;
    private Variant lastVariant = null;
    private long firstSlicePosition = 0;
    private String currentChromosome = null;
    private int numSlices;

    public VariantSliceReader(int chunkSize, DataReader<Variant> reader, int studyId, int fileId, int sliceBufferSize) {
        this(chunkSize, reader, studyId, fileId, sliceBufferSize, null);
    }

    public VariantSliceReader(int chunkSize, DataReader<Variant> reader, int studyId, int fileId, int sliceBufferSize,
                              ProgressLogger progressLogger) {
        this.sliceOffset = sliceBufferSize * chunkSize;
        this.chunkSize = chunkSize;
        this.studyId = String.valueOf(studyId);
        this.fileId = String.valueOf(fileId);
        this.reader = reader;
        this.progressLogger = progressLogger;
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
            // Keep reading while slice is not ready
            while (!isSliceReady()) {
                List<Variant> read = reader.read(10);
                if (read == null || read.isEmpty()) {
                    // Stop looping when the reader is drained.
                    break;
                }
                if (progressLogger != null) {
                    progressLogger.increment(read.size());
                }
                // Add batch of variants to the bufferTree
                addVariants(read);
            }

            // Nothing to read! Empty reader.
            if (numSlices == 0) {
                return slices;
            }

            // Take one slice from the bufferTree
            slices.add(takeSlice());
        }

        return slices;
    }

    private boolean isSliceReady() {
        boolean initialRead = lastVariant == null || currentChromosome == null;
        if (initialRead) {
            // Nothing read yet. Slice not ready
            return false;
        }
        boolean sameChromosome = lastVariant.getChromosome().equals(currentChromosome);
        if (sameChromosome) {
            long firstSliceOffset = lastVariant.getStart() - firstSlicePosition;
            if (firstSliceOffset < sliceOffset) {
                // Same chromosome, and sliceOffset not met. Slice not ready.
                return false;
            }
        }
        return true;
    }

    private ImmutablePair<Long, List<Variant>> takeSlice() {
        TreeMap<Long, List<Variant>> map = bufferTree.get(currentChromosome);

        Long slicePosition = map.firstKey();
        List<Variant> variants = map.remove(slicePosition);
        numSlices--;

        // update current chromosome
        if (bufferTree.get(currentChromosome).isEmpty()) {
            for (Map.Entry<String, TreeMap<Long, List<Variant>>> entry : bufferTree.entrySet()) {
                if (!entry.getValue().isEmpty()) {
                    currentChromosome = entry.getKey();
                    break;
                }
            }
        }

        updateFirstSlicePosition();
        return ImmutablePair.of(slicePosition, variants);
    }

    private void updateFirstSlicePosition() {
        if (lastVariant != null && numSlices > 0) {
            TreeMap<Long, List<Variant>> map = bufferTree.get(currentChromosome);
            Map.Entry<Long, List<Variant>> entry = map.firstEntry();
            firstSlicePosition = entry.getKey();
        }
    }

    private void addVariants(List<Variant> read) {
        for (Variant variant : read) {
            addVariant(variant);
        }
        updateFirstSlicePosition();
    }

    private void addVariant(Variant variant) {
        String chromosome = variant.getChromosome();

        // Remap studyId and fileId
        StudyEntry studyEntry = variant.getStudies().get(0);
        studyEntry.setStudyId(studyId);
        studyEntry.getFiles().get(0).setFileId(fileId);

        long[] coveredSlicePositions = getCoveredSlicePositions(variant);
        for (long slicePos : coveredSlicePositions) {
            addVariant(variant, chromosome, slicePos);
        }
        lastVariant = variant;
    }

    private void addVariant(Variant variant, String chromosome, long slicePos) {
        TreeMap<Long, List<Variant>> positionMap = bufferTree.computeIfAbsent(chromosome, (key) -> new TreeMap<>(Long::compareTo));
        List<Variant> list = positionMap.computeIfAbsent(slicePos, (key) -> new LinkedList<>());
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
        return VariantHbaseTransformTask.getCoveredSlicePositions(var.getStart(), var.getEnd(), chunkSize);
    }

}
