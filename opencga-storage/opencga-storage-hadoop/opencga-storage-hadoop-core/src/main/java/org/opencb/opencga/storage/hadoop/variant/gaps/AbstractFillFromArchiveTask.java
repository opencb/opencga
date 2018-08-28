package org.opencb.opencga.storage.hadoop.variant.gaps;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StopWatch;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;
import org.opencb.biodata.tools.variant.converters.proto.VcfRecordProtoToVariantConverter;
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveRowKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexConsolidationDrive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine.TARGET_VARIANT_TYPE_SET;

/**
 * Created on 31/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class AbstractFillFromArchiveTask implements Task<Result, AbstractFillFromArchiveTask.FillResult> {

    protected static final Comparator<Variant> VARIANT_COMPARATOR = Comparator
            .comparing(Variant::getStart)
            .thenComparing(Variant::getEnd)
            .thenComparing(Variant::getReference)
            .thenComparing(Variant::getAlternate)
            .thenComparing(Variant::compareTo);
//    private static final int ARCHIVE_FILES_READ_BATCH_SIZE = 1000;

    protected final StudyConfiguration studyConfiguration;
    protected final GenomeHelper helper;
    protected final FillGapsTask fillGapsTask;
    protected final SortedSet<Integer> fileIds;
    protected final Map<Integer, byte[]> fileToNonRefColumnMap;
    protected final Logger logger = LoggerFactory.getLogger(AbstractFillFromArchiveTask.class);
    protected final ArchiveRowKeyFactory rowKeyFactory;
    protected long timestamp = HConstants.LATEST_TIMESTAMP;

    private final Map<String, Long> stats = new HashMap<>();

    public static final class FillResult {
        private List<Put> variantPuts;
        private List<Put> samplesIndexPuts;

        private FillResult(List<Put> variantPuts, List<Put> samplesIndexPuts) {
            this.variantPuts = variantPuts;
            this.samplesIndexPuts = samplesIndexPuts;
        }

        /**
         * Get the list of PUT mutations over the Variants table.
         * This mutations will contain values for new FILE and SAMPLE columns.
         *
         * @return List of PUT mutations
         */
        public List<Put> getVariantPuts() {
            return variantPuts;
        }

        /**
         * Get the list of PUT mutations over the SampleIndex table.
         * This mutations will contain values of pending variants that should be added to the table.
         *
         * @see SampleIndexConsolidationDrive
         *
         * @return List of PUT mutations
         */
        public List<Put> getSamplesIndexPuts() {
            return samplesIndexPuts;
        }
    }

    protected AbstractFillFromArchiveTask(StudyConfiguration studyConfiguration,
                                          GenomeHelper helper,
                                          Collection<Integer> samples,
                                          boolean skipReferenceVariants) {
        this.studyConfiguration = studyConfiguration;
        this.helper = helper;

        fileIds = new TreeSet<>();
        fileToNonRefColumnMap = new HashMap<>();
        if (samples == null || samples.isEmpty()) {
            fileIds.addAll(studyConfiguration.getIndexedFiles());
            for (Integer fileId : fileIds) {
                fileToNonRefColumnMap.put(fileId, Bytes.toBytes(ArchiveTableHelper.getNonRefColumnName(fileId)));
            }
        } else {
            for (Map.Entry<Integer, LinkedHashSet<Integer>> entry : studyConfiguration.getSamplesInFiles().entrySet()) {
                Integer fileId = entry.getKey();
                if (studyConfiguration.getIndexedFiles().contains(fileId) && !Collections.disjoint(entry.getValue(), samples)) {
                    fileToNonRefColumnMap.put(fileId, Bytes.toBytes(ArchiveTableHelper.getNonRefColumnName(fileId)));
                    fileIds.add(fileId);
                }
            }
        }

        fillGapsTask = new FillGapsTask(studyConfiguration, helper, skipReferenceVariants);
        rowKeyFactory = new ArchiveRowKeyFactory(helper.getConf());
    }

    public void setQuiet(boolean quiet) {
        fillGapsTask.setQuiet(quiet);
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public void pre() throws IOException { }

    @Override
    public void post() throws IOException { }

    @Override
    public List<FillResult> apply(List<Result> list) throws IOException {
        List<FillResult> results = new ArrayList<>(list.size());
        for (Result result : list) {
            results.add(apply(result));
        }
        return results;
    }

    public FillResult apply(Result result) throws IOException {
        StopWatch stopWatch = new StopWatch().start();
        Context context = buildContext(result);
        increment("BUILD_CONTEXT", context.fileBatch, stopWatch);

        return fillGaps(context);
    }

    public FillResult fillGaps(Context context) throws IOException {
        Map<Variant, Set<Integer>> variantsToFill = context.getVariantsToFill();

        // Use a sorted map to fetch VcfSlice orderly
        SortedMap<Integer, List<Variant>> fileToVariantsMap = new TreeMap<>();
        // Invert map variantsToFill -> filesToVariants
        for (Map.Entry<Variant, Set<Integer>> entry : variantsToFill.entrySet()) {
            Variant variant = entry.getKey();
            for (Integer fileId : entry.getValue()) {
                fileToVariantsMap.computeIfAbsent(fileId, ArrayList::new).add(variant);
            }
        }

        // Store all PUT operations, one for each variant
        Map<Variant, Put> putsMap = new TreeMap<>(VARIANT_COMPARATOR);
        List<Put> sampleIndexPuts = new ArrayList<>();
        for (Map.Entry<Integer, List<Variant>> entry : fileToVariantsMap.entrySet()) {
            Integer fileId = entry.getKey();
            List<Variant> variants = entry.getValue();

            VcfSlicePair vcfSlicePair = context.getVcfSlice(fileId);
            if (vcfSlicePair == null) {
                continue;
            }
            VcfSlice nonRefVcfSlice = vcfSlicePair.getNonRefVcfSlice();
            ListIterator<VcfSliceProtos.VcfRecord> nonRefIterator = nonRefVcfSlice == null
                    ? Collections.<VcfSliceProtos.VcfRecord>emptyList().listIterator()
                    : nonRefVcfSlice.getRecordsList().listIterator();
            VcfSlice refVcfSlice = vcfSlicePair.getRefVcfSlice();
            ListIterator<VcfSliceProtos.VcfRecord> refIterator = refVcfSlice == null
                    ? Collections.<VcfSliceProtos.VcfRecord>emptyList().listIterator()
                    : refVcfSlice.getRecordsList().listIterator();


            Set<Integer> sampleIds = studyConfiguration.getSamplesInFiles().get(fileId);
            for (Variant variant : variants) {
                Put put = putsMap.computeIfAbsent(variant, this::createPut);

                StopWatch stopWatch = new StopWatch().start();
                VariantOverlappingStatus overlappingStatus = fillGapsTask.fillGaps(variant, sampleIds, put, sampleIndexPuts, fileId,
                        nonRefVcfSlice, nonRefIterator, refVcfSlice, refIterator);
                increment("OVERLAPPING_STATUS_" + String.valueOf(overlappingStatus), context.fileBatch, 1);
                increment("OVERLAPPING_STATUS_" + String.valueOf(overlappingStatus), context.fileBatch, stopWatch);
            }
            context.clearVcfSlice(fileId);
        }

        List<Put> variantPuts = new ArrayList<>(variantsToFill.size());
        for (Put put : putsMap.values()) {
            if (!put.isEmpty()) {
                variantPuts.add(put);
            } else {
                increment("PUTS_EMPTY", context.fileBatch, 1);
            }
        }
        if (variantPuts.isEmpty()) {
            increment("PUTS_NONE", context.fileBatch, 1);
        } else {
            increment("PUTS", context.fileBatch, variantPuts.size());
        }
        return new FillResult(variantPuts, sampleIndexPuts);
    }

    protected Put createPut(Variant v) {
        return new Put(VariantPhoenixKeyFactory.generateVariantRowKey(v), timestamp);
    }

    protected abstract Context buildContext(Result result) throws IOException;

    public abstract class Context {
        /** Current rowkey from archive table. */
        protected final byte[] rowKey;

        /** Look up map of VcfSlice objects. */
        protected final Map<Integer, VcfSlicePair> filesMap;
        protected final SortedSet<Integer> fileIdsInBatch;
        protected final Result result;
        protected final int fileBatch;

        protected final Region region;

        protected Context(Result result) throws IOException {
            this.rowKey = result.getRow();
            this.result = result;
            String blockId = Bytes.toString(rowKey);
            region = rowKeyFactory.extractRegionFromBlockId(blockId);

            fileBatch = rowKeyFactory.extractFileBatchFromBlockId(blockId);
            this.fileIdsInBatch = new TreeSet<>();
            for (Integer fileId : AbstractFillFromArchiveTask.this.fileIds) {
                if (rowKeyFactory.getFileBatch(fileId) == fileBatch) {
                    fileIdsInBatch.add(fileId);
                }
            }
            if (fileIdsInBatch.isEmpty()) {
                throw new IllegalStateException("Read data from RK " + blockId + " from file batch " + fileBatch
                        + " without any file from " + AbstractFillFromArchiveTask.this.fileIds);
            }

            filesMap = new HashMap<>();

//            StopWatch stopWatch = new StopWatch().start();
//            variants = extractVariantsToFill();
//            increment("EXTRACT_VARIANTS_TO_FILL", fileBatch, stopWatch);

            increment("RESULTS", 1);
            increment("RESULTS", fileBatch, 1);
//            increment("VARIANTS_TO_FILL", fileBatch, variants.size());
        }

        public VcfSlicePair getVcfSlice(int fileId) throws IOException {
            if (!filesMap.containsKey(fileId)) {
                VcfSlicePair pair = getVcfSlicePairFromResult(fileId);
                // Check if fileBatch matches with current fileBatch
                if (pair == null) {
                    if (fileBatch != rowKeyFactory.getFileBatch(fileId)) {
                        // This should never happen
                        logger.warn("Skip VcfSlice for file " + fileId + " in RK " + Bytes.toString(rowKey));
                    }
                    vcfSliceNotFound(fileId);
                }
                filesMap.put(fileId, pair);
            }
            return filesMap.get(fileId);
        }

        public VcfSlice parseVcfSlice(byte[] data) throws IOException {
            VcfSlice vcfSlice;
            if (data != null && data.length != 0) {
                try {
                    StopWatch stopWatch = new StopWatch().start();
                    vcfSlice = VcfSlice.parseFrom(data);
                    increment("PARSE_VCF_SLICE", fileBatch, stopWatch);
                    increment("PARSE_VCF_SLICE", fileBatch, 1);
                } catch (InvalidProtocolBufferException | RuntimeException e) {
                    throw new IOException("Error parsing data from row " + Bytes.toString(rowKey), e);
                }
            } else {
                vcfSlice = null;
            }
            return vcfSlice;
        }

        public void clearVcfSlice(Integer fileId) {
            filesMap.put(fileId, null);
        }

        protected abstract void vcfSliceNotFound(int fileId);

        protected abstract VcfSlicePair getVcfSlicePairFromResult(Integer fileId) throws IOException;

        public abstract TreeMap<Variant, Set<Integer>> getVariantsToFill() throws IOException;
    }

    public static class VcfSlicePair {
        private final VcfSlice nonRefVcfSlice;
        private final VcfSlice refVcfSlice;

        public VcfSlicePair(VcfSlice nonRefVcfSlice, VcfSlice refVcfSlice) {
            this.nonRefVcfSlice = nonRefVcfSlice;
            this.refVcfSlice = refVcfSlice;
        }

        public VcfSlice getNonRefVcfSlice() {
            return nonRefVcfSlice;
        }

        public VcfSlice getRefVcfSlice() {
            return refVcfSlice;
        }
    }

    protected static Scan buildScan(Configuration conf) {
        return buildScan(null, -1, conf);
    }

    protected static Scan buildScan(String regionStr, int fileId, Configuration conf) {
        Scan scan = new Scan();
        Region region;
        if (StringUtils.isNotEmpty(regionStr)) {
            region = Region.parseRegion(regionStr);
        } else {
            region = null;
        }

        scan.setCacheBlocks(false);
        ArchiveRowKeyFactory archiveRowKeyFactory = new ArchiveRowKeyFactory(conf);
        VariantHBaseQueryParser.addArchiveRegionFilter(scan, region, fileId, archiveRowKeyFactory);
        return scan;
    }

    protected static boolean isVariantAlreadyLoaded(VcfSliceProtos.VcfSlice slice, VcfSliceProtos.VcfRecord vcfRecord) {
        VariantType variantType = VcfRecordProtoToVariantConverter.getVariantType(vcfRecord.getType());
        // The variant is not loaded if is a NO_VARIATION (fast check first)
        if (!TARGET_VARIANT_TYPE_SET.contains(variantType)) {
            return false;
        }

        // If any of the genotypes is HOM_REF, the variant won't be completely loaded, so there may be a gap.
        return !FillGapsTask.hasAnyReferenceGenotype(slice, vcfRecord);
    }

    public Map<String, Long> takeStats() {
        HashMap<String, Long> copy = new HashMap<>(stats);
        stats.clear();
        return copy;
    }

    protected long increment(String name, long delta) {
        return stats.compute(name, (key, value) -> value == null ? delta : value + delta);
    }

    protected long increment(String name, int fileBatch, long delta) {
        if (fileBatch <= 1) {
            return stats.compute(name + "_(fb=" + fileBatch + ')', (key, value) -> value == null ? delta : value + delta);
        } else {
            return -1;
        }
    }

    protected long increment(String name, int fileBatch, StopWatch stopWatch) {
        if (fileBatch <= 1) {
            long delta = stopWatch.now(TimeUnit.NANOSECONDS);
            return stats.compute(name + "_TIME_NS_(fb=" + fileBatch + ')', (key, value) -> value == null ? delta : value + delta);
        } else {
            return -1;
        }
    }

}
