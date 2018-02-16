package org.opencb.opencga.storage.hadoop.variant.gaps;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
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
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveRowKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixKeyFactory;
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
public abstract class AbstractFillFromArchiveTask implements ParallelTaskRunner.TaskWithException<Result, Put, IOException> {

    private static final Comparator<Variant> VARIANT_COMPARATOR = Comparator
            .comparing(Variant::getStart)
            .thenComparing(Variant::getEnd)
            .thenComparing(Variant::getReference)
            .thenComparing(Variant::getAlternate)
            .thenComparing(Variant::compareTo);
//    private static final int ARCHIVE_FILES_READ_BATCH_SIZE = 1000;

    protected final HBaseManager hBaseManager;
    protected final StudyConfiguration studyConfiguration;
    protected final GenomeHelper helper;
    protected final FillGapsTask fillGapsTask;
    protected final SortedSet<Integer> fileIds;
    protected final Map<Integer, byte[]> fileToNonRefColumnMap;
    protected final Logger logger = LoggerFactory.getLogger(AbstractFillFromArchiveTask.class);
    protected final ArchiveRowKeyFactory rowKeyFactory;


    private final Map<String, Long> stats = new HashMap<>();

    protected AbstractFillFromArchiveTask(HBaseManager hBaseManager,
                                          StudyConfiguration studyConfiguration,
                                          GenomeHelper helper,
                                          Collection<Integer> samples,
                                          boolean skipReferenceVariants) {
        this.hBaseManager = hBaseManager;
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

    @Override
    public void pre() throws IOException { }

    @Override
    public void post() throws IOException { }

    @Override
    public List<Put> apply(List<Result> list) throws IOException {
        List<Put> puts = new ArrayList<>(list.size());
        for (Result result : list) {
            StopWatch stopWatch = new StopWatch().start();
            Context context = buildContext(result);
            increment("BUILD_CONTEXT", context.fileBatch, stopWatch);
            puts.addAll(fillGaps(context));
        }
        return puts;
    }

    public List<Put> fillGaps(Context context) throws IOException {
        Map<Variant, Set<Integer>> variantsToFill = new TreeMap<>(VARIANT_COMPARATOR);

        // If filling all the files, (i.e. fill missing) we can use the list of already processed variants and files
        if (context.getNewFiles().isEmpty()) {
            increment("NO_NEW_FILES", context.fileBatch, 1);
            // No files to process. Nothing to do!
            return Collections.emptyList();
        } else { // New files
            for (Variant variant : context.getProcessedVariants()) {
                // Already processed variants has to be filled with new files only
                variantsToFill.put(variant, new HashSet<>(context.getNewFiles()));
            }
            // New variants?
            for (Variant variant : context.getNewVariants()) {
                // New variants has to be filled with all files
                variantsToFill.put(variant, new HashSet<>(context.getAllFiles()));
            }
        }

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
                Put put = putsMap.computeIfAbsent(variant, v -> new Put(VariantPhoenixKeyFactory.generateVariantRowKey(v)));

                StopWatch stopWatch = new StopWatch().start();
                VariantOverlappingStatus overlappingStatus = fillGapsTask.fillGaps(variant, sampleIds, put, fileId,
                        nonRefVcfSlice, nonRefIterator, refVcfSlice, refIterator);
                increment("OVERLAPPING_STATUS_" + String.valueOf(overlappingStatus), context.fileBatch, 1);
                increment("OVERLAPPING_STATUS_" + String.valueOf(overlappingStatus), context.fileBatch, stopWatch);
            }
            context.clearVcfSlice(fileId);
        }

        List<Put> puts = new ArrayList<>(variantsToFill.size());
        for (Put put : putsMap.values()) {
            if (!put.isEmpty()) {
                puts.add(put);
            }
        }
        if (puts.isEmpty()) {
            increment("PUTS_NONE", context.fileBatch, 1);
        } else {
            increment("PUTS", context.fileBatch, puts.size());
        }
        return puts;
    }

    protected abstract Context buildContext(Result result) throws IOException;

    public abstract class Context {
        /** Empty variants (just chr:pos:ref:alt) for the current region. */
        protected final List<Variant> variants;
        /** List of already processed files in this region. */
        protected final List<Integer> processedFiles;
        /** List of already processed variants in this region. */
        protected final List<Variant> processedVariants;
        /** Current rowkey from archive table. */
        protected final byte[] rowKey;

        /** Look up map of VcfSlice objects. */
        protected final Map<Integer, VcfSlicePair> filesMap;
        protected final SortedSet<Integer> fileIdsInBatch;
        protected final SortedSet<Integer> newFiles;
        protected final SortedSet<Variant> newVariants;
        protected final Result result;
        protected final int fileBatch;

        protected Context(Result result) throws IOException {
            this.rowKey = result.getRow();
            this.result = result;
            fileBatch = rowKeyFactory.extractFileBatchFromBlockId(Bytes.toString(rowKey));
            this.fileIdsInBatch = new TreeSet<>();
            for (Integer fileId : AbstractFillFromArchiveTask.this.fileIds) {
                if (rowKeyFactory.getFileBatch(fileId) == fileBatch) {
                    fileIdsInBatch.add(fileId);
                }
            }
            if (fileIdsInBatch.isEmpty()) {
                throw new IllegalStateException("Read data from RK " + Bytes.toString(rowKey) + " from file batch " + fileBatch
                        + " without any file from " + AbstractFillFromArchiveTask.this.fileIds);
            }

            filesMap = new HashMap<>();

            // TODO: Find list of processed files.
            // TODO: If fillAllFiles == true, (global fill operation), this can be stored in the StudyConfiguration
            // if fillAllFiles == true
            //     processedFiles = studyConfiguration.getXXXX()
            // TODO: Should we store this for each chunk? Or should we store a timestamp to indicate how updated is this chunk?
            this.processedFiles = Collections.emptyList();
            // TODO: Find list of processed variants
            this.processedVariants = Collections.emptyList();

            newFiles = new TreeSet<>(fileIdsInBatch);
            newFiles.removeAll(processedFiles);

            StopWatch stopWatch = new StopWatch().start();
            variants = extractVariantsToFill();
            increment("EXTRACT_VARIANTS_TO_FILL", fileBatch, stopWatch);

            // Do not compute Variant::hashCode
            newVariants = new TreeSet<>(VARIANT_COMPARATOR);
            newVariants.addAll(variants);
            newVariants.removeAll(processedVariants);

            increment("RESULTS", 1);
            increment("RESULTS", fileBatch, 1);
            increment("VARIANTS_TO_FILL", fileBatch, variants.size());
        }

        protected abstract List<Variant> extractVariantsToFill() throws IOException;

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

        protected abstract void vcfSliceNotFound(int fileId);

        protected abstract VcfSlicePair getVcfSlicePairFromResult(Integer fileId) throws IOException;

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

        public Set<Variant> getNewVariants() {
//            return variants - processedVariants;
            return newVariants;
        }
        public List<Variant> getVariants() {
            return variants;
        }

        public SortedSet<Integer> getNewFiles() {
//            return files - processedFiles;
            return newFiles;
        }

        public abstract Set<Integer> getAllFiles();

        public List<Integer> getProcessedFiles() {
            return processedFiles;
        }

        public List<Variant> getProcessedVariants() {
            return processedVariants;
        }

        public void clearVcfSlice(Integer fileId) {
            filesMap.put(fileId, null);
        }
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

    protected static Scan buildScan(String regionStr, Configuration conf) {
        Scan scan = new Scan();
        Region region;
        if (StringUtils.isNotEmpty(regionStr)) {
            region = Region.parseRegion(regionStr);
        } else {
            region = null;
        }

        scan.setCacheBlocks(false);
        ArchiveRowKeyFactory archiveRowKeyFactory = new ArchiveRowKeyFactory(conf);
        VariantHBaseQueryParser.addArchiveRegionFilter(scan, region, 0, archiveRowKeyFactory);
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
