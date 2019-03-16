package org.opencb.opencga.storage.hadoop.variant.gaps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.biodata.tools.variant.converters.proto.VcfRecordProtoToVariantConverter;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveRowKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created on 06/02/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FillGapsFromArchiveTask extends AbstractFillFromArchiveTask {

    protected final Map<Integer, byte[]> fileToRefColumnMap;
    private final Integer mainFileBatch;
    private final Map<Integer, List<Integer>> otherFilesGroupByFilesBatch;

    protected final String archiveTableName;
    protected Table archiveTable;
    protected final HBaseManager hBaseManager;

    public FillGapsFromArchiveTask(HBaseManager hBaseManager,
                                   String archiveTableName,
                                   StudyMetadata studyMetadata,
                                   GenomeHelper helper,
                                   Collection<Integer> samples, VariantStorageMetadataManager metadataManager) {
        super(studyMetadata, metadataManager, helper, samples, false);
        this.archiveTableName = archiveTableName;
        this.hBaseManager = hBaseManager;

        fileToRefColumnMap = new HashMap<>();
        for (Integer fileId : fileIds) {
            fileToRefColumnMap.put(fileId, Bytes.toBytes(ArchiveTableHelper.getRefColumnName(fileId)));
        }

        mainFileBatch = getMainFileBatch(fileIds, rowKeyFactory);

        otherFilesGroupByFilesBatch = groupFilesByBatch(fileIds, rowKeyFactory);
        otherFilesGroupByFilesBatch.remove(mainFileBatch);
    }

    @Override
    public void pre() throws IOException {
        archiveTable = hBaseManager.getConnection().getTable(TableName.valueOf(archiveTableName));
    }

    @Override
    public void post() throws IOException {
        archiveTable.close();
    }

    private static Integer getMainFileBatch(Collection<Integer> fileIds, ArchiveRowKeyFactory rowKeyFactory) {
        Map<Integer, List<Integer>> groupFilesByBatch = groupFilesByBatch(fileIds, rowKeyFactory);

        Integer maxNumFilesInBatch = groupFilesByBatch.values().stream().map(List::size).max(Integer::compareTo).get();
        // In case of having more than one batch with the same num of files, get the smallest batchFile
        return groupFilesByBatch.entrySet().stream().filter(entry -> entry.getValue().size() == maxNumFilesInBatch)
                .min(Comparator.comparingInt(Map.Entry::getKey)).get().getKey();
    }

    private static Map<Integer, List<Integer>> groupFilesByBatch(Collection<Integer> fileIds, ArchiveRowKeyFactory rowKeyFactory) {
        return fileIds.stream().collect(Collectors.groupingBy(rowKeyFactory::getFileBatch));
    }

    @Override
    protected Context buildContext(Result result) throws IOException {
        return new FillGapsContext(result);
    }

    private class FillGapsContext extends Context {

        private Map<Integer, Result> results;

        protected FillGapsContext(Result result) throws IOException {
            super(result);
        }

        @Override
        public TreeMap<Variant, Set<Integer>> getVariantsToFill() throws IOException {
            TreeMap<Variant, Set<Integer>> variantsToFill = new TreeMap<>(VARIANT_COMPARATOR);

            List<Variant> variants = extractVariantsToFill();
            for (Variant variant : variants) {
                variantsToFill.put(variant, new HashSet<>(fileIds));
            }
            return variantsToFill;
        }

        protected List<Variant> extractVariantsToFill() throws IOException {
            // If there are files not in the main batch, make an specific get to that batch
            if (!otherFilesGroupByFilesBatch.isEmpty()) {
                List<Get> gets = new ArrayList<>(otherFilesGroupByFilesBatch.size());
                String chromosome = rowKeyFactory.extractChromosomeFromBlockId(Bytes.toString(rowKey));
                long slice = rowKeyFactory.extractSliceFromBlockId(Bytes.toString(rowKey));
                for (Map.Entry<Integer, List<Integer>> entry : otherFilesGroupByFilesBatch.entrySet()) {
                    Integer fileBatch = entry.getKey();
                    String otherRowKey = rowKeyFactory.generateBlockIdFromSliceAndBatch(fileBatch, chromosome, slice);
                    Get get = new Get(Bytes.toBytes(otherRowKey));
                    for (Integer fileId : entry.getValue()) {
                        get.addColumn(helper.getColumnFamily(), fileToNonRefColumnMap.get(fileId));
                        get.addColumn(helper.getColumnFamily(), fileToRefColumnMap.get(fileId));
                    }
                    gets.add(get);
                }
                results = new HashMap<>();
                for (Result result : archiveTable.get(gets)) {
                    results.put(rowKeyFactory.extractFileBatchFromBlockId(Bytes.toString(result.getRow())), result);
                }
                results.put(mainFileBatch, result);
            } else {
                results = Collections.singletonMap(mainFileBatch, result);
            }

            // We should fill only the variants from any of the files to fill
            List<Variant> variants = new ArrayList<>();
            for (Integer fileId : fileIds) {
                VcfSlicePair vcfSlicePair = getVcfSlice(fileId);

                if (vcfSlicePair != null && vcfSlicePair.getNonRefVcfSlice() != null) {
                    VcfSliceProtos.VcfSlice vcfSlice = vcfSlicePair.getNonRefVcfSlice();
                    for (VcfSliceProtos.VcfRecord vcfRecord : vcfSlice.getRecordsList()) {
                        VariantType variantType = VcfRecordProtoToVariantConverter.getVariantType(vcfRecord.getType());
                        // Get loaded variants from this VcfSlice
                        if (isVariantAlreadyLoaded(vcfSlice, vcfRecord)) {
                            int position = vcfSlice.getPosition();
                            int start = VcfRecordProtoToVariantConverter.getStart(vcfRecord, position);
                            int end = VcfRecordProtoToVariantConverter.getEnd(vcfRecord, position);

                            Variant variant = new Variant(vcfSlice.getChromosome(), start, end, vcfRecord.getReference(),
                                    vcfRecord.getAlternate()).setType(variantType);
                            variants.add(variant);
//                                variantsToFill.computeIfAbsent(variant, v -> new HashSet<>(fileIds)).remove(fileId);
                        }
                    }
                }
            }
            return variants;
        }

        @Override
        protected void vcfSliceNotFound(int fileId) {
            // We are trying to read Ref and NonRef from this file. There was a gap?
            logger.warn("Nothing found for fileId " + fileId + " in RK " + Bytes.toString(rowKey));
        }

        @Override
        protected VcfSlicePair getVcfSlicePairFromResult(Integer fileId) throws IOException {
            int thisFileBatch = rowKeyFactory.getFileBatch(fileId);
            return getVcfSlicePairFromResult(fileId, results.get(thisFileBatch));
        }

        protected VcfSlicePair getVcfSlicePairFromResult(Integer fileId, Result result) throws IOException {
            VcfSliceProtos.VcfSlice nonRefVcfSlice = parseVcfSlice(
                    result.getValue(helper.getColumnFamily(), fileToNonRefColumnMap.get(fileId)));
            VcfSliceProtos.VcfSlice refVcfSlice = parseVcfSlice(
                    result.getValue(helper.getColumnFamily(), fileToRefColumnMap.get(fileId)));

            if (nonRefVcfSlice == null && refVcfSlice == null) {
                return null;
            } else {
                return new VcfSlicePair(nonRefVcfSlice, refVcfSlice);
            }
        }

    }


    public static Scan buildScan(Collection<Integer> fileIds, String regionStr, Configuration conf) {
        ArchiveRowKeyFactory archiveRowKeyFactory = new ArchiveRowKeyFactory(conf);
        Integer mainFileBatch = getMainFileBatch(fileIds, archiveRowKeyFactory);

        Scan scan = AbstractFillFromArchiveTask.buildScan(regionStr, archiveRowKeyFactory.getFirstFileFromBatch(mainFileBatch), conf);

        GenomeHelper helper = new GenomeHelper(conf);
        for (Integer fileId : fileIds) {
            // Scan files only from the main file batch
            if (mainFileBatch == archiveRowKeyFactory.getFileBatch(fileId)) {
                scan.addColumn(helper.getColumnFamily(), Bytes.toBytes(ArchiveTableHelper.getNonRefColumnName(fileId)));
                scan.addColumn(helper.getColumnFamily(), Bytes.toBytes(ArchiveTableHelper.getRefColumnName(fileId)));
            }
        }
        return scan;
    }

}
