package org.opencb.opencga.storage.hadoop.variant.gaps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.biodata.tools.variant.converters.proto.VcfRecordProtoToVariantConverter;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created on 06/02/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FillGapsFromArchiveTask2 extends AbstractFillFromArchiveTask {

    protected final Map<Integer, byte[]> fileToRefColumnMap;

    public FillGapsFromArchiveTask2(HBaseManager hBaseManager,
                                    String variantsTableName,
                                    String archiveTableName,
                                    StudyConfiguration studyConfiguration,
                                    GenomeHelper helper,
                                    Collection<Integer> samples) {
        super(hBaseManager, variantsTableName, archiveTableName, studyConfiguration, helper, samples, false);


        fileToRefColumnMap = new HashMap<>();
        for (Integer fileId : fileIds) {
            fileToRefColumnMap.put(fileId, Bytes.toBytes(ArchiveTableHelper.getRefColumnName(fileId)));
        }

        Map<Integer, List<Integer>> groupFilesByBatch = fileIds.stream().collect(Collectors.groupingBy(rowKeyFactory::getFileBatch));
        Integer fileBatch = groupFilesByBatch.entrySet()
                .stream()
                .max(Comparator.comparingInt(entry -> entry.getValue().size()))
                .map(Map.Entry::getKey)
                .get();
    }

    @Override
    protected Context buildContext(Result result) throws IOException {
        return new FillGapsContext(result);
    }

    private class FillGapsContext extends Context {

        protected FillGapsContext(Result result) throws IOException {
            super(result);
        }

        @Override
        protected List<Variant> getVariantsToFill() throws IOException {
            // We should fill only the variants from any of the files to fill
            List<Variant> variants = new ArrayList<>();
            for (Integer fileId : fileIds) {
                VcfSlicePair vcfSlicePair = getVcfSlice(fileId);
                if (vcfSlicePair == null && rowKeyFactory.getFileBatch(fileId) != fileBatch) {
                    throw new UnsupportedOperationException("TODO: Read file from a different batch!");
                }
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
        protected VcfSlicePair getVcfSlicePairFromResult(Result result, Integer fileId) throws IOException {
            VcfSliceProtos.VcfSlice nonRefVcfSlice = parseVcfSlice(result.getValue(helper.getColumnFamily(),
                    fileToNonRefColumnMap.get(fileId)));
            VcfSliceProtos.VcfSlice refVcfSlice = parseVcfSlice(result.getValue(helper.getColumnFamily(),
                    fileToRefColumnMap.get(fileId)));

            if (nonRefVcfSlice == null && refVcfSlice == null) {
                return null;
            } else {
                return new VcfSlicePair(nonRefVcfSlice, refVcfSlice);
            }
        }
    }


    public static Scan buildScan(Collection<Integer> fileIds, String regionStr, Configuration conf) {
        Scan scan = AbstractFillFromArchiveTask.buildScan(regionStr, conf);

        GenomeHelper helper = new GenomeHelper(conf);
        for (Integer fileId : fileIds) {
            scan.addColumn(helper.getColumnFamily(), Bytes.toBytes(ArchiveTableHelper.getNonRefColumnName(fileId)));
            scan.addColumn(helper.getColumnFamily(), Bytes.toBytes(ArchiveTableHelper.getRefColumnName(fileId)));
        }
        return scan;
    }

}
