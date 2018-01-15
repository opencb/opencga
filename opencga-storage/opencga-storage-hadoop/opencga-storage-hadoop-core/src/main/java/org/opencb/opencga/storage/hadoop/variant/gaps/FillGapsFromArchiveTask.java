package org.opencb.opencga.storage.hadoop.variant.gaps;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;
import org.opencb.biodata.tools.variant.converters.proto.VcfRecordProtoToVariantConverter;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
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
    private static final int ARCHIVE_FILES_READ_BATCH_SIZE = 2;

    private final HBaseManager hBaseManager;
    private final String archiveTableName;
    private final StudyConfiguration studyConfiguration;
    private final GenomeHelper helper;
    private final FillGapsTask fillGapsTask;
    private final Set<Integer> fileIds;
    private final boolean fillAllFiles;
    private final Map<Integer, byte[]> fileToColumnMap;
    private final Logger logger = LoggerFactory.getLogger(FillGapsFromArchiveTask.class);
    private final ArchiveRowKeyFactory rowKeyFactory;

    private Table archiveTable;

    public FillGapsFromArchiveTask(HBaseManager hBaseManager,
                                   String archiveTableName,
                                   StudyConfiguration studyConfiguration,
                                   GenomeHelper helper,
                                   boolean fillOnlyMissingGenotypes) throws IOException {
        this(hBaseManager, archiveTableName, studyConfiguration, helper, null, fillOnlyMissingGenotypes);
    }

    public FillGapsFromArchiveTask(HBaseManager hBaseManager,
                                   String archiveTableName,
                                   StudyConfiguration studyConfiguration,
                                   GenomeHelper helper,
                                   Collection<Integer> samples,
                                   boolean fillOnlyMissingGenotypes) throws IOException {
        this.hBaseManager = hBaseManager;
        this.archiveTableName = archiveTableName;
        this.studyConfiguration = studyConfiguration;
        this.helper = helper;

        fileIds = new HashSet<>();
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
        fillAllFiles = fileIds.size() == studyConfiguration.getIndexedFiles().size();

        fillGapsTask = new FillGapsTask(studyConfiguration, helper, fillOnlyMissingGenotypes);
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
        // If we are filling all the files, is not
        if (fillAllFiles) {
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
                        // Get valid variants from this VcfSlice
                        if (TARGET_VARIANT_TYPE_SET.contains(variantType)) {

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

        // TODO: Should we flip this loop, so we don't need to retain in memory the VCFSlices from all files?
        //
        //  NOW
        // for variant in variantsToFill
        //   for file in filesToFill
        //     XXX
        //
        //  DESIRED
        // for file in filesToFill
        //   for variant in variantsToFill
        //     XXX


        List<Put> puts = new ArrayList<>(variantsToFill.size());
        for (Map.Entry<Variant, Set<Integer>> entry : variantsToFill.entrySet()) {
            Variant variant = entry.getKey();
            Set<Integer> filesToFill = entry.getValue();

/*DEBUG*/       List<Integer> filesInVariant = new ArrayList<>(fileIds);
/*DEBUG*/       filesInVariant.removeAll(filesToFill);
/*DEBUG*/       logger.debug("Variant " + variant + " with files " + filesInVariant);

            if (filesToFill.isEmpty()) {
                // Nothing to do!
                continue;
            }
            Put put = new Put(VariantPhoenixKeyFactory.generateVariantRowKey(variant));
            for (Integer fileId : filesToFill) {
                VcfSlice vcfSlice = context.getVcfSlice(fileId);

                if (vcfSlice != null) {
                    Set<Integer> sampleIds = studyConfiguration.getSamplesInFiles().get(fileId);
                    fillGapsTask.fillGaps(variant, sampleIds, put, fileId, vcfSlice);
                } else {
                    System.out.println("Vcf slice null for file " + fileId + " in RK " + Bytes.toString(context.rowKey));
                }
            }
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
        return new Context(new HashMap<>(), variants, processedFiles, processedVariants, result.getRow());
    }

    public final class Context {
        private final Map<Integer, VcfSlice> filesMap;
        private final List<Variant> variants;

        private final List<Integer> processedFiles;
        private final List<Variant> processedVariants;

        private final byte[] rowKey;

        private final Set<Integer> newFiles;
        private final Set<Variant> newVariants;

        private Context(Map<Integer, VcfSlice> filesMap, List<Variant> variants, List<Integer> processedFiles,
                        List<Variant> processedVariants, byte[] rowKey) {
            this.filesMap = filesMap;
            this.variants = variants;
            this.processedFiles = processedFiles;
            this.processedVariants = processedVariants;

            this.rowKey = rowKey;

            newFiles = new HashSet<>(fileIds);
            newFiles.removeAll(processedFiles);

            // Do not compute Variant::hashCode
            newVariants = new TreeSet<>(VARIANT_COMPARATOR);
            newVariants.addAll(variants);
            newVariants.removeAll(processedVariants);
        }

        public VcfSlice getVcfSlice(int fileId) throws IOException {
            if (!filesMap.containsKey(fileId)) {
                Set<Integer> allFilesToRead;
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

                // TODO: Should have debug log level
                logger.info("Fetch files {}", filesToRead);

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
            // TODO
//            return variants - processedVariants;
            return newVariants;
        }
        public List<Variant> getVariants() {
            return variants;
        }

        public Set<Integer> getNewFiles() {
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
    }

}
