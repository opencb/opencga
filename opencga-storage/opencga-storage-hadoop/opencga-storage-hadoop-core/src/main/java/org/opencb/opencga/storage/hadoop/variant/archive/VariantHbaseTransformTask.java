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

package org.opencb.opencga.storage.hadoop.variant.archive;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;
import org.opencb.biodata.tools.variant.converters.proto.VariantToProtoVcfRecord;
import org.opencb.biodata.tools.variant.converters.proto.VariantToVcfSliceConverter;
import org.opencb.commons.run.ParallelTaskRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 * @deprecated use {@link org.opencb.opencga.storage.hadoop.variant.transform.VariantSliceReader} and {@link VariantToVcfSliceConverter}
 */
@Deprecated
public class VariantHbaseTransformTask implements ParallelTaskRunner.Task<Variant, VcfSlice> {

    protected final Logger logger = LoggerFactory.getLogger(VariantHbaseTransformTask.class);
    private static final List<VcfSlice> EMPTY_LIST = Collections.emptyList();
    private final VariantToVcfSliceConverter converter;
    private final ArchiveRowKeyFactory keyFactory;
    private final ArchiveTableHelper helper;

    private final Set<String> storedChr;
    private final Set<String> lookup;
    private final Map<String, List<Variant>> buffer;
    private final LinkedList<String> lookupOrder;
    private final AtomicLong timeProto = new AtomicLong(0);
    private final AtomicLong timeIndex = new AtomicLong(0);
    private final AtomicLong timePut = new AtomicLong(0);

    private final AtomicInteger bufferSize = new AtomicInteger(200);

    /**
     * @param helper {@link ArchiveTableHelper}
     */
    public VariantHbaseTransformTask(ArchiveTableHelper helper) {
        converter = new VariantToVcfSliceConverter();
        this.helper = helper;
        storedChr = new HashSet<>();
        lookup = new HashSet<>();
        buffer = new HashMap<>();
        lookupOrder = new LinkedList<>();
        keyFactory = helper.getKeyFactory();
    }

    public void setBufferSize(Integer size) {
        this.bufferSize.set(size);
    }

    public int getBufferSize() {
        return bufferSize.get();
    }

    @Override
    public List<VcfSlice> apply(List<Variant> batch) {
        return encodeVariants(batch);
    }

    protected List<VcfSlice> encodeVariants(List<Variant> variants) {
        long curr = System.currentTimeMillis();
        variants.forEach(var -> addVariant(var));
        this.timeIndex.addAndGet(System.currentTimeMillis() - curr);
        List<VcfSlice> data = checkSlices(getBufferSize());
        curr = System.currentTimeMillis();
        this.timePut.addAndGet(System.currentTimeMillis() - curr);
        return data;
    }

    @Override
    public List<VcfSlice> drain() {
        List<VcfSlice> data = checkSlices(0);
        return data;
    }

    private List<VcfSlice> checkSlices(int limit) {
        if (lookupOrder.size() < limit) {
            return EMPTY_LIST;
        }

        SortedMap<Long, String> keys = orderKeys(lookupOrder, storedChr, getHelper().getChunkSize());
        List<VcfSlice> retSlice = new ArrayList<>();
        while (lookupOrder.size() > limit) { // key buffer size
            if (keys.isEmpty()) {
                keys = orderKeys(lookupOrder, storedChr, getHelper().getChunkSize()); // next Chromosome starts
            }
            Long firstKey = keys.firstKey();
            String key = keys.remove(firstKey);
            lookupOrder.remove(key);
            List<Variant> data = buffer.remove(key);
            if (data.size() > 0) {
                String chr = data.get(0).getChromosome();
                if (storedChr.add(chr)) {
                    logger.debug("Flush for {}: {}", chr, lookupOrder);
                    lookup.clear(); // clear for each Chromosome
                    lookup.addAll(lookupOrder);
                }
                long sliceStart = keyFactory.extractPositionFromBlockId(key);
                // List<Variant> varLst = data.stream().map(v -> new
                // Variant(v)).collect(Collectors.toList());

                long curr = System.currentTimeMillis();
                VcfSlice slice = converter.convert(data, (int) sliceStart);
                this.timeProto.addAndGet(System.currentTimeMillis() - curr);
                retSlice.add(slice);
            }
        }
        return retSlice;
    }

    private SortedMap<Long, String> orderKeys(LinkedList<String> lookupOrder, Set<String> storedChr, int chunkSize) {
        List<String> currentChr = lookupOrder.stream().filter(s -> storedChr.contains(keyFactory.splitBlockId(s)[0]))
                .collect(Collectors.toList());
        // first finish off current chromosome
        if (currentChr.isEmpty()) {
            currentChr = new ArrayList<String>(lookupOrder);
        }

        // find min position
        TreeMap<Long, String> idx = new TreeMap<>();
        for (String slice : currentChr) {
            Long extr = keyFactory.extractPositionFromBlockId(slice);
            idx.put(extr, slice);
        }
        return idx;
    }

    private void addVariant(Variant var) {
        String chromosome = var.getChromosome();
        long[] coveredSlicePositions = getCoveredSlicePositions(var);
        for (long slicePos : coveredSlicePositions) {
            String blockKey = keyFactory.generateBlockId(helper.getFileId(), chromosome, slicePos);
            addVariant(blockKey, var);
        }
    }

    private void addVariant(String blockKey, Variant var) {
        List<Variant> list = null;
        if (!lookup.contains(blockKey)) {
            lookup.add(blockKey);
            lookupOrder.add(blockKey);
            list = new ArrayList<Variant>();
            buffer.put(blockKey, list);
        } else {
            list = buffer.get(blockKey);
        }
        if (list == null) {
            logger.error(" Current lookup queue: " + StringUtils.join(lookupOrder, ','));
            logger.error(String.format("Current Variant: %s", var.getImpl()));
            throw new IllegalStateException("Input file not sorted!!!: " + blockKey);
        }
        list.add(var);
    }

    private long[] getCoveredSlicePositions(Variant var) {
        return getCoveredSlicePositions(var.getChromosome(), var.getStart(), var.getEnd(), getHelper().getChunkSize());
    }

    public static long[] getCoveredSlicePositions(String chromosome, long start, long end, int chunkSize) {
        long startChunk = VariantToProtoVcfRecord.getSlicePosition((int) start, chunkSize);
        long endChunk = VariantToProtoVcfRecord.getSlicePosition((int) end, chunkSize);
        if (endChunk == startChunk) {
            return new long[]{startChunk};
        }
        int len = (int) ((endChunk - startChunk) / chunkSize) + 1;
        long[] ret = new long[len];
        for (int i = 0; i < len; ++i) {
            ret[i] = startChunk + (((long) i) * chunkSize);
        }
        return ret;
    }

    @Override
    public void post() {
        logger.info(String.format("Time norm2proto: %s", this.timeProto.get()));
        logger.info(String.format("Time idx: %s", this.timeIndex.get()));
    }

    private ArchiveTableHelper getHelper() {
        return this.helper;
    }
}
