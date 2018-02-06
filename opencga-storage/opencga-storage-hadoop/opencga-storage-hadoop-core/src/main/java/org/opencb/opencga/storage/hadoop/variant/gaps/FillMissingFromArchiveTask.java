package org.opencb.opencga.storage.hadoop.variant.gaps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixKeyFactory;

import java.io.IOException;
import java.util.*;

import static org.opencb.opencga.storage.hadoop.variant.index.VariantTableStudyRow.HOM_REF;
import static org.opencb.opencga.storage.hadoop.variant.index.VariantTableStudyRow.buildColumnKey;

/**
 * Created on 06/02/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FillMissingFromArchiveTask extends AbstractFillFromArchiveTask {

    public FillMissingFromArchiveTask(HBaseManager hBaseManager, String variantsTableName, String archiveTableName,
                                      StudyConfiguration studyConfiguration, GenomeHelper helper) {
        super(hBaseManager, variantsTableName, archiveTableName, studyConfiguration, helper, Collections.emptyList(), true);
    }

    @Override
    protected Context buildContext(Result result) throws IOException {
        return new FillMissingContext(result);
    }


    private class FillMissingContext extends Context {

        protected FillMissingContext(Result result) throws IOException {
            super(result);
        }

        @Override
        protected List<Variant> extractVariantsToFill() throws IOException {
            // Fill all variants from the study. Get variant ids from variants table

            List<Variant> variants = new ArrayList<>();
            Region region = rowKeyFactory.extractRegionFromBlockId(Bytes.toString(result.getRow()));

            // Build scan. Only get variants from the needed region, from this study
            Scan scan = new Scan();
            VariantHBaseQueryParser.addRegionFilter(scan, region);
            scan.addColumn(helper.getColumnFamily(), Bytes.toBytes(buildColumnKey(studyConfiguration.getStudyId(), HOM_REF)));

            try (ResultScanner scanner = variantsTable.getScanner(scan)) {
                for (Result variantResult : scanner) {
                    variants.add(VariantPhoenixKeyFactory.extractVariantFromVariantRowKey(variantResult.getRow()));
                }
            }

            return variants;
        }

        @Override
        protected void vcfSliceNotFound(int fileId) {
            // Not reading Ref column. It may have information, but only reference information
            logger.debug("Nothing to read for fileId " + fileId + " in RK " + Bytes.toString(rowKey));
        }

        @Override
        protected VcfSlicePair getVcfSlicePairFromResult(Integer fileId) throws IOException {
            VcfSliceProtos.VcfSlice nonRefVcfSlice = parseVcfSlice(result.getValue(helper.getColumnFamily(),
                    fileToNonRefColumnMap.get(fileId)));

            if (nonRefVcfSlice == null) {
                return null;
            } else {
                return new VcfSlicePair(nonRefVcfSlice, null);
            }
        }

        @Override
        public Set<Integer> getAllFiles() {
            return fileIdsInBatch;
        }
    }

    public static Scan buildScan(Collection<Integer> fileIds, String regionStr, Configuration conf) {
        Scan scan = AbstractFillFromArchiveTask.buildScan(regionStr, conf);


        GenomeHelper helper = new GenomeHelper(conf);
        for (Integer fileId : fileIds) {
            scan.addColumn(helper.getColumnFamily(), Bytes.toBytes(ArchiveTableHelper.getNonRefColumnName(fileId)));
        }

        return scan;
    }

}

