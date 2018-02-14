package org.opencb.opencga.storage.hadoop.variant.gaps;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
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
import java.io.UncheckedIOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.hadoop.variant.index.VariantMergerTableMapper.TARGET_VARIANT_TYPE_SET;

/**
 * Created on 31/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FillGapsFromArchiveTask implements ParallelTaskRunner.TaskWithException<Result, Put, IOException> {

    private static final Comparator<Variant> VARIANT_COMPARATOR = Comparator
            .comparing(Variant::getStart)
            .thenComparing(Variant::getEnd)
            .thenComparing(Variant::compareTo);
    private static final int ARCHIVE_FILES_READ_BATCH_SIZE = 1000;

    private final HBaseManager hBaseManager;
    private final String archiveTableName;
    private final StudyConfiguration studyConfiguration;
    private final GenomeHelper helper;
    private final FillGapsTask fillGapsTask;
    private final SortedSet<Integer> fileIds;
    private final boolean fillAllVariants;
    private final Map<Integer, byte[]> fileToColumnMap;
    private final Logger logger = LoggerFactory.getLogger(FillGapsFromArchiveTask.class);
    private final ArchiveRowKeyFactory rowKeyFactory;

    private Table archiveTable;

    public FillGapsFromArchiveTask(HBaseManager hBaseManager,
                                   String archiveTableName,
                                   StudyConfiguration studyConfiguration,
                                   GenomeHelper helper,
                                   boolean skipReferenceVariants) {
        this(hBaseManager, archiveTableName, studyConfiguration, helper, null, skipReferenceVariants);
    }

    public FillGapsFromArchiveTask(HBaseManager hBaseManager,
                                   String archiveTableName,
                                   StudyConfiguration studyConfiguration,
                                   GenomeHelper helper,
                                   Collection<Integer> samples,
                                   boolean skipReferenceVariants) {
        this.hBaseManager = hBaseManager;
        this.archiveTableName = archiveTableName;
        this.studyConfiguration = studyConfiguration;
        this.helper = helper;

        fileIds = new TreeSet<>();
        fileToColumnMap = new HashMap<>();
//            Map<Integer, Integer> samplesFileMap = new HashMap<>();
        if (samples == null || samples.isEmpty()) {
            fileIds.addAll(studyConfiguration.getIndexedFiles());
            for (Integer fileId : fileIds) {
                fileToColumnMap.put(fileId, Bytes.toBytes(ArchiveTableHelper.getColumnName(fileId)));
            }
        } else {
            for (Integer sample : samples) {
                for (Map.Entry<Integer, LinkedHashSet<Integer>> entry : studyConfiguration.getSamplesInFiles().entrySet()) {
                    if (entry.getValue().contains(sample)) {
                        Integer fileId = entry.getKey();
//                        samplesFileMap.put(sample, fileId);
                        fileToColumnMap.put(fileId, Bytes.toBytes(ArchiveTableHelper.getColumnName(fileId)));
                        fileIds.add(fileId);
                        break;
                    }
                }
            }
        }
        fillAllVariants = fileIds.size() == studyConfiguration.getIndexedFiles().size();

        fillGapsTask = new FillGapsTask(studyConfiguration, helper, skipReferenceVariants);
        rowKeyFactory = new ArchiveRowKeyFactory(helper.getConf());
    }

    @Override
    public void pre() {
        try {
            archiveTable = hBaseManager.getConnection().getTable(TableName.valueOf(archiveTableName));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void post() {
        try {
            archiveTable.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public List<Put> apply(List<Result> list) throws IOException {
        List<Put> puts = new ArrayList<>(list.size());
        for (Result result : list) {
            puts.addAll(fillGaps(buildContext(result)));
        }
        return puts;
    }

    public List<Put> fillGaps(Context context) throws IOException {
        Map<Variant, Set<Integer>> variantsToFill = new TreeMap<>(VARIANT_COMPARATOR);
        // If we are filling all variants, read variantsToFill from "_V" , i.e. context.getVariants()
        if (fillAllVariants) {
            // If filling all the files, (i.e. fill missing) we can use the list of already processed variants and files
            if (context.getNewFiles().isEmpty()) {
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
                    variantsToFill.put(variant, new HashSet<>(context.getFileIds()));
                }
            }
        } else {
            // Otherwise, we should fill only the variants from any of the files to fill
            for (Integer fileId : fileIds) {
                VcfSlice vcfSlice = context.getVcfSlice(fileId);
                if (vcfSlice != null) {
                    for (VcfSliceProtos.VcfRecord vcfRecord : vcfSlice.getRecordsList()) {
                        VariantType variantType = VcfRecordProtoToVariantConverter.getVariantType(vcfRecord.getType());
                        // Get loaded variants from this VcfSlice
                        if (isVariantAlreadyLoaded(vcfSlice, vcfRecord)) {

                            int position = vcfSlice.getPosition();
                            int start = VcfRecordProtoToVariantConverter.getStart(vcfRecord, position);
                            int end = VcfRecordProtoToVariantConverter.getEnd(vcfRecord, position);

                            Variant variant = new Variant(vcfSlice.getChromosome(), start, end, vcfRecord.getReference(),
                                    vcfRecord.getAlternate()).setType(variantType);
                            variantsToFill.computeIfAbsent(variant, v -> new HashSet<>(fileIds)).remove(fileId);
                        }
                    }
                }
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

            VcfSlice vcfSlice = context.getVcfSlice(fileId);
            if (vcfSlice == null) {
                logger.warn("Vcf slice null for file " + fileId + " in RK " + Bytes.toString(context.rowKey));
                continue;
            }

            for (Variant variant : variants) {
                Put put = putsMap.computeIfAbsent(variant, v -> new Put(VariantPhoenixKeyFactory.generateVariantRowKey(v)));

                Set<Integer> sampleIds = studyConfiguration.getSamplesInFiles().get(fileId);
                fillGapsTask.fillGaps(variant, sampleIds, put, fileId, vcfSlice);
            }
            context.clearVcfSlice(fileId);
        }

        List<Put> puts = new ArrayList<>(variantsToFill.size());
        for (Put put : putsMap.values()) {
            if (!put.isEmpty()) {
                puts.add(put);
            }
        }
        return puts;
    }

    public Context buildContext(Result result) {
        String chromosome = rowKeyFactory.extractChromosomeFromBlockId(Bytes.toString(result.getRow()));

        List<Variant> variants = Arrays.stream(result.rawCells())
                .filter(c -> Bytes.startsWith(CellUtil.cloneQualifier(c), GenomeHelper.VARIANT_COLUMN_B_PREFIX))
                .map(c -> GenomeHelper.getVariantFromArchiveVariantColumn(chromosome, CellUtil.cloneQualifier(c)))
                .collect(Collectors.toList());

        // TODO: Find list of processed files.
        // TODO: If fillAllFiles == true, (global fill operation), this can be stored in the StudyConfiguration
        // if fillAllFiles == true
        //     processedFiles = studyConfiguration.getXXXX()
        // TODO: Should we store this for each chunk? Or should we store a timestamp to indicate how updated is this chunk?
        List<Integer> processedFiles = Collections.emptyList();
        // TODO: Find list of processed variants
        List<Variant> processedVariants = Collections.emptyList();
        return new Context(variants, processedFiles, processedVariants, result.getRow());
    }

    public final class Context {
        /** Empty variants (just chr:pos:ref:alt) for the current region. Read from _V. */
        private final List<Variant> variants;
        /** List of already processed files in this region. */
        private final List<Integer> processedFiles;
        /** List of already processed variants in this region. */
        private final List<Variant> processedVariants;
        /** Current rowkey from archive table. */
        private final byte[] rowKey;

        /** Look up map of VcfSlice objects. */
        private final Map<Integer, VcfSlice> filesMap;
        private final SortedSet<Integer> newFiles;
        private final SortedSet<Variant> newVariants;

        private Context(List<Variant> variants, List<Integer> processedFiles,
                        List<Variant> processedVariants, byte[] rowKey) {
            this.variants = variants;
            this.processedFiles = processedFiles;
            this.processedVariants = processedVariants;
            this.rowKey = rowKey;

            filesMap = new HashMap<>();

            newFiles = new TreeSet<>(fileIds);
            newFiles.removeAll(processedFiles);

            // Do not compute Variant::hashCode
            newVariants = new TreeSet<>(VARIANT_COMPARATOR);
            newVariants.addAll(variants);
            newVariants.removeAll(processedVariants);
        }

        public VcfSlice getVcfSlice(int fileId) throws IOException {
            if (!filesMap.containsKey(fileId)) {
                // Read files in order, so we don't have to keep much objects in memory
                SortedSet<Integer> allFilesToRead;
                if (variants != processedVariants) {
                    allFilesToRead = fileIds;
                } else {
                    allFilesToRead = getNewFiles();
                }

                List<Integer> filesToRead = new ArrayList<>(ARCHIVE_FILES_READ_BATCH_SIZE);
                filesToRead.add(fileId);

                for (Integer fileToRead : allFilesToRead) {
                    if (!filesMap.containsKey(fileToRead) && fileId != fileToRead) {
                        filesToRead.add(fileToRead);
                        if (filesToRead.size() >= ARCHIVE_FILES_READ_BATCH_SIZE) {
                            break;
                        }
                    }
                }

                logger.debug("Fetch files {}", filesToRead);

                Get get = new Get(rowKey);
                for (Integer fileToRead : filesToRead) {
                    get.addColumn(helper.getColumnFamily(), fileToColumnMap.get(fileToRead));
                }

                Result result = archiveTable.get(get);

                for (Cell cell : result.rawCells()) {
                    byte[] data = CellUtil.cloneValue(cell);
                    VcfSlice vcfSlice;
                    if (data.length != 0) {
                        try {
                            vcfSlice = VcfSlice.parseFrom(data);
                        } catch (Exception e) {
                            System.out.println("Error parsing data from row " + Bytes.toString(rowKey));
                            throw e;
                        }
                    } else {
                        vcfSlice = null;
                    }
                    filesMap.put(ArchiveTableHelper.getFileIdFromColumnName(CellUtil.cloneQualifier(cell)), vcfSlice);
                }
            }
            return filesMap.get(fileId);
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

        public Set<Integer> getFileIds() {
            return fileIds;
        }

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

    public static Scan buildScan() {
        return buildScan(null, null);
    }

    public static Scan buildScan(String regionStr, Configuration conf) {
        Scan scan = new Scan();
        Region region;
        if (StringUtils.isNotEmpty(regionStr)) {
            region = Region.parseRegion(regionStr);
        } else {
            region = null;
        }
        ArchiveRowKeyFactory archiveRowKeyFactory = new ArchiveRowKeyFactory(conf);
        VariantHBaseQueryParser.addArchiveRegionFilter(scan, region, archiveRowKeyFactory);
        return scan;
    }

    private static boolean isVariantAlreadyLoaded(VcfSliceProtos.VcfSlice slice, VcfSliceProtos.VcfRecord vcfRecord) {
        VariantType variantType = VcfRecordProtoToVariantConverter.getVariantType(vcfRecord.getType());
        // The variant is not loaded if is a NO_VARIATION (fast check first)
        if (!TARGET_VARIANT_TYPE_SET.contains(variantType)) {
            return false;
        }

        // If any of the genotypes is HOM_REF, the variant won't be completely loaded, so there may be a gap.
        return !FillGapsTask.hasAnyReferenceGenotype(slice, vcfRecord);
    }


}
