package org.opencb.opencga.storage.hadoop.variant.archive;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;
import org.opencb.biodata.tools.variant.converter.VariantToProtoVcfRecord;
import org.opencb.biodata.tools.variant.converter.VariantToVcfSliceConverter;
import org.opencb.commons.run.ParallelTaskRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 */
public class VariantHbaseTransformTask implements ParallelTaskRunner.Task<Variant, VcfSlice> {

    protected final Logger logger = LoggerFactory.getLogger(VariantHbaseTransformTask.class);
    private static final List<VcfSlice> EMPTY_LIST = Collections.emptyList();
    private final VariantToVcfSliceConverter converter;
    private final ArchiveHelper helper;

    private final Set<String> storedChr;
    private final Set<String> lookup;
    private final Map<String, List<Variant>> buffer;
    private final LinkedList<String> lookupOrder;
    private final AtomicLong timeProto = new AtomicLong(0);
    private final AtomicLong timeIndex = new AtomicLong(0);
    private final AtomicLong timePut = new AtomicLong(0);
    private final AtomicInteger bufferSize = new AtomicInteger(200);
    private final TableName tableName;
    private Connection connection;
    private BufferedMutator tableMutator;

    /**
     * @param helper {@link ArchiveHelper}
     * @param table  {@link String} HBase table name
     */
    public VariantHbaseTransformTask(ArchiveHelper helper, String table) {
        converter = new VariantToVcfSliceConverter();
        this.helper = helper;
        storedChr = new HashSet<>();
        lookup = new HashSet<>();
        buffer = new HashMap<>();
        lookupOrder = new LinkedList<>();
        this.tableName = table == null ? null : TableName.valueOf(table);
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
        submit(data);
        this.timePut.addAndGet(System.currentTimeMillis() - curr);
        return data;
    }

    @Override
    public List<VcfSlice> drain() {
        List<VcfSlice> data = checkSlices(0);
        submit(data);
        return data;
    }

    private void submit(List<VcfSlice> data) {
        if (null != this.tableName) {
            List<Put> putList = data.stream().map(s -> this.getHelper().wrap(s)).collect(Collectors.toList());
            try {
                this.tableMutator.mutate(putList);
            } catch (IOException e) {
                throw new RuntimeException(String.format("Problems submitting %s data to hbase %s ", putList.size(),
                        this.tableName.getNameAsString()), e);
            }
        }
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
                long sliceStart = getHelper().extractPositionFromBlockId(key);
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
        List<String> currentChr = lookupOrder.stream().filter(s -> storedChr.contains(getHelper().splitBlockId(s)[0]))
                .collect(Collectors.toList());
        // first finish off current chromosome
        if (currentChr.isEmpty()) {
            currentChr = new ArrayList<String>(lookupOrder);
        }

        // find min position
        TreeMap<Long, String> idx = new TreeMap<>();
        for (String slice : currentChr) {
            Long extr = getHelper().extractPositionFromBlockId(slice);
            idx.put(extr, slice);
        }
        return idx;
    }

    private void addVariant(Variant var) {
        String chromosome = var.getChromosome();
        long[] coveredSlicePositions = getCoveredSlicePositions(var);
        for (long slicePos : coveredSlicePositions) {
            String blockKey = getHelper().generateBlockId(chromosome, slicePos);
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
    public void pre() {
        if (null != this.tableName) {
            try {
                logger.info("Open connection using " + getHelper().getConf());
                connection = ConnectionFactory.createConnection(getHelper().getConf());
                tableMutator = connection.getBufferedMutator(this.tableName);
            } catch (IOException e) {
                throw new RuntimeException("Failed to connect to Hbase", e);
            }
        }
    }

    @Override
    public void post() {
        logger.info(String.format("Time norm2proto: %s", this.timeProto.get()));
        logger.info(String.format("Time idx: %s", this.timeIndex.get()));
        if (null != this.tableName) {
            if (null != this.tableMutator) {
                try {
                    this.tableMutator.close();
                } catch (IOException e) {
                    logger.error("Problem closing Table mutator from HBase", e);
                } finally {
                    this.tableMutator = null;
                }

            }
            if (null != connection) {
                try {
                    connection.close();
                } catch (IOException e) {
                    logger.error("Issue with closing DB connection", e);
                } finally {
                    connection = null;
                }
                logger.info(String.format("Time put: %s", this.timeIndex.get()));
            }
        }
    }

    private ArchiveHelper getHelper() {
        return this.helper;
    }
}
