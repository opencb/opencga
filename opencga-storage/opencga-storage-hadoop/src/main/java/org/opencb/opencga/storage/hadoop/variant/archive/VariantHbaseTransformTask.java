package org.opencb.opencga.storage.hadoop.variant.archive;

import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderVersion;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;
import org.opencb.biodata.tools.variant.converter.VariantToProtoVcfRecord;
import org.opencb.biodata.tools.variant.converter.VariantToVcfSliceConverter;
import org.opencb.biodata.tools.variant.stats.VariantGlobalStatsCalculator;
import org.opencb.opencga.storage.core.variant.transform.VariantTransformTask;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
public class VariantHbaseTransformTask extends VariantTransformTask<VcfSlice> {
    private static final List<VcfSlice> EMPTY_LIST = Collections.emptyList();
    private final VariantToVcfSliceConverter converter;
    private final ArchiveHelper helper;

    private final Set<String> storedChr;
    private final Set<String> lookup;
    private final Map<String, List<Variant>> buffer;
    private final LinkedList<String> lookupOrder;
//    private Connection connection;
//    private final TableName tableName;

    /**
     * @param header {@link VCFHeader}
     * @param version {@link VCFHeaderVersion}
     * @param source {@link VariantSource}
     * @param variantStatsTask {@link VariantGlobalStatsCalculator}
     * @param includeSrc boolean
     * @param generateReferenceBlocks boolean
     * @param helper {@link ArchiveHelper}
     */
    public VariantHbaseTransformTask(VCFHeader header, VCFHeaderVersion version, VariantSource source,
            VariantGlobalStatsCalculator variantStatsTask, boolean includeSrc, boolean generateReferenceBlocks, ArchiveHelper helper) {
        super(header, version, source, null, variantStatsTask, includeSrc, generateReferenceBlocks);
        converter = new VariantToVcfSliceConverter();
        this.helper = helper;
        storedChr = new HashSet<>();
        lookup = new HashSet<>();
        buffer = new HashMap<>();
        lookupOrder = new LinkedList<>();
//        this.tableName = TableName.valueOf(tableName);
    }

    @Override
    protected List<VcfSlice> encodeVariants(List<Variant> variants) {
        for (Variant var : variants) {
            addVariant(var);
        }
        List<VcfSlice> data = checkSlices(1000);
//        submit(data);
        return data;
    }

    @Override
    public List<VcfSlice> drain() {
        List<VcfSlice> data = checkSlices(0);
//        submit(data);
        return data;
    }

    private void submit(List<Put> data) {
        if (data.isEmpty()) {
            return; // Nothing to do
        }
////        logger.info("Open to table " + this.tableName.getNameAsString());
//        try (Table table = connection.getTable(this.tableName);) { // TODO enable
//            table.put(data);
//        } catch (IOException e) {
//            throw new RuntimeException(String.format("Problems submitting %s data to hbase %s ", data.size(),
//                    this.tableName.getNameAsString()), e);
//        }
    }

    private List<VcfSlice> checkSlices(int limit) {
        if (lookupOrder.size() < limit) {
            return EMPTY_LIST;
        }
        List<VcfSlice> retPut = new ArrayList<>();
        while (lookupOrder.size() > limit) { // 1000 key buffer
            String key = findSmallest(lookupOrder, storedChr, getHelper().getChunkSize());
            lookupOrder.remove(key);
            List<Variant> data = buffer.remove(key);
            if (data.size() > 0) {
                String chr = data.get(0).getChromosome();
                if (storedChr.add(chr)) {
                    logger.info(String.format("Flush for %s: %s", chr, StringUtils.join(lookupOrder, ',')));
                    lookup.clear(); // clear for each Chromosome
                    lookup.addAll(lookupOrder);
                }
                long sliceStart = getHelper().extractPositionFromBlockId(key);
                // List<Variant> varLst = data.stream().map(v -> new
                // Variant(v)).collect(Collectors.toList());
                 VcfSlice slice = converter.convert(data, (int) sliceStart);
                 retPut.add(slice);
//                 Put put = getHelper().wrap(slice);
//                 retPut.add(put);
            }
        }
        return retPut;
    }

    private String findSmallest(LinkedList<String> lookupOrder, Set<String> storedChr, int chunkSize) {
        List<String> currentChr = lookupOrder.stream().filter(s -> storedChr.contains(getHelper().splitBlockId(s)[0]))
                .collect(Collectors.toList());
        // first finish off current chromosome
        if (currentChr.isEmpty()) {
            currentChr = new ArrayList<String>(lookupOrder);
        }
        // find min position
        long minPos = -1;
        String key = null;
        for (String slice : currentChr) {
            Long extr = getHelper().extractPositionFromBlockId(slice);
            if (minPos < 0 || minPos > extr.longValue()) {
                key = slice;
                minPos = extr.longValue();
            }
        }
        return key;
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
            return new long[] { startChunk };
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
//        try { // TODO enable
//            logger.info("Open connection using " + getHelper().getConf());
//            connection = ConnectionFactory.createConnection(getHelper().getConf());
//        } catch (IOException e) {
//           throw new RuntimeException("Failed to connect to Hbase", e);
//        }
        super.pre();
    }

    @Override
    public void post() {
        synchronized (variantStatsTask) {
            variantStatsTask.post();
        }
//        try (ArchiveFileMetadataManager manager = new ArchiveFileMetadataManager(
//                tableName.getNameAsString(), getHelper().getConf(), null);) {
//            manager.updateVcfMetaData(source);
//        } catch (IOException e1) {
//            throw new RuntimeException(e1);
//        } finally {
//            if (null != connection) {
//                try {
//                    connection.close();
//                } catch (IOException e) {
//                    logger.error("Issue with closing DB connection", e);
//                } finally {
//                    connection = null;
//                }
//            }
//        }
    }

    private ArchiveHelper getHelper() {
        return this.helper;
    }
}
