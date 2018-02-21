package org.opencb.opencga.storage.hadoop.variant.gaps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;

import java.io.IOException;
import java.util.*;

/**
 * Created on 06/02/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FillMissingFromArchiveTask extends AbstractFillFromArchiveTask {

    public static final String VARIANT_COLUMN_PREFIX = "_V";
    public static final byte[] VARIANT_COLUMN_B_PREFIX = Bytes.toBytes(VARIANT_COLUMN_PREFIX);

    public FillMissingFromArchiveTask(HBaseManager hBaseManager, StudyConfiguration studyConfiguration, GenomeHelper helper) {
        super(hBaseManager, studyConfiguration, helper, Collections.emptyList(), true);
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
            // Fill all variants from the study. Read variants from _V

            List<Variant> variants = new ArrayList<>();
            Region region = rowKeyFactory.extractRegionFromBlockId(Bytes.toString(result.getRow()));

            for (Cell cell : result.rawCells()) {
                if (Bytes.startsWith(CellUtil.cloneQualifier(cell), VARIANT_COLUMN_B_PREFIX)) {
                    variants.add(getVariantFromArchiveVariantColumn(region.getChromosome(), CellUtil.cloneQualifier(cell)));
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

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        for (Integer fileId : fileIds) {
            byte[] value = Bytes.toBytes(ArchiveTableHelper.getNonRefColumnName(fileId));
            filterList.addFilter(new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(value)));
        }
        filterList.addFilter(new ColumnPrefixFilter(VARIANT_COLUMN_B_PREFIX));
        if (scan.getFilter() == null) {
            scan.setFilter(filterList);
        } else {
            scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL, filterList, scan.getFilter()));
        }

        return scan;
    }

    public static byte[] getArchiveVariantColumn(Variant variant) {
        return Bytes.toBytes(VARIANT_COLUMN_PREFIX + '_' + variant.getStart() + '_'
                + variant.getReference() + '_' + variant.getAlternate());
    }

    public static Variant getVariantFromArchiveVariantColumn(String chromosome, byte[] column) {
        String[] split = Bytes.toString(column).split("_", -1);
        if (split.length != 5) {
            return null;
        } else {
            return new Variant(chromosome, Integer.valueOf(split[2]), split[3], split[4]);
        }
    }
}

