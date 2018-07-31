package org.opencb.opencga.storage.hadoop.variant.gaps;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PInteger;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveRowKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;

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
    private final PhoenixHelper.Column fillMissingColumn;
    private final byte[] lastFileBytes;
    private final List<Integer> indexedFiles;
    private Map<Integer, Set<Integer>> filesToProcessMap = new HashMap<>();
    private boolean overwrite;

    public FillMissingFromArchiveTask(StudyConfiguration studyConfiguration, GenomeHelper helper, boolean overwrite) {
        super(studyConfiguration, helper, Collections.emptyList(), true);
        fillMissingColumn = VariantPhoenixHelper.getFillMissingColumn(studyConfiguration.getStudyId());
        this.overwrite = overwrite;
        Integer lastFile = new ArrayList<>(studyConfiguration.getIndexedFiles()).get(studyConfiguration.getIndexedFiles().size() - 1);
        lastFileBytes = PInteger.INSTANCE.toBytes(lastFile);
        indexedFiles = new ArrayList<>(studyConfiguration.getIndexedFiles());
    }

    @Override
    protected Context buildContext(Result result) throws IOException {
        return new FillMissingContext(result);
    }

    @Override
    protected Put createPut(Variant v) {
        return super.createPut(v).addColumn(helper.getColumnFamily(), fillMissingColumn.bytes(), lastFileBytes);
    }

    private class FillMissingContext extends Context {

        protected FillMissingContext(Result result) throws IOException {
            super(result);
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
        public TreeMap<Variant, Set<Integer>> getVariantsToFill() {
            TreeMap<Variant, Set<Integer>> variantsToFill = new TreeMap<>(VARIANT_COMPARATOR);

            for (Cell cell : result.rawCells()) {
                if (Bytes.startsWith(CellUtil.cloneQualifier(cell), VARIANT_COLUMN_B_PREFIX)) {
                    Variant variant = getVariantFromArchiveVariantColumn(region.getChromosome(), CellUtil.cloneQualifier(cell));
                    if (cell.getValueLength() > 0 && !overwrite) {
                        byte[] bytes = CellUtil.cloneValue(cell);
                        Integer lastFile = (Integer) PInteger.INSTANCE.toObject(bytes);
                        Set<Integer> filesToProcess = filesToProcessMap.computeIfAbsent(lastFile,
                                (key) -> getFilesToProcess(lastFile, variant));

                        variantsToFill.put(variant, filesToProcess);
                    } else {
                        variantsToFill.put(variant, fileIdsInBatch);
                    }
                }
            }

//            System.out.println(Bytes.toString(result.getRow()) + " variantsToFill = " + variantsToFill);
            return variantsToFill;
        }

        public Set<Integer> getFilesToProcess(Integer lastFile, Variant variant) {
            List<Integer> allfilesToProcess = indexedFiles.subList(indexedFiles.indexOf(lastFile) + 1, indexedFiles.size());

            Set<Integer> filesToProcess = new HashSet<>();

            for (Integer fileToProcess : allfilesToProcess) {
                if (fileIdsInBatch.contains(fileToProcess)) {
                    filesToProcess.add(fileToProcess);
                }
            }

            if (filesToProcess.isEmpty()) {
                throw new IllegalStateException("No files found for variant " + variant
                        + " in row " + Bytes.toString(result.getRow()));
            }
            return filesToProcess;
        }
    }

    public static Scan buildScan(Collection<Integer> fileIds, Configuration conf) {
        return buildScan(fileIds, null, null, conf);
    }

    public static List<Scan> buildScan(Collection<Integer> fileIds, String regionStr, Configuration conf) {
        if (StringUtils.isEmpty(regionStr)) {
            return Collections.singletonList(buildScan(fileIds, conf));
        }

        Set<Integer> fileBatches = new HashSet<>();
        ArchiveRowKeyFactory archiveRowKeyFactory = new ArchiveRowKeyFactory(conf);

        for (Integer fileId : fileIds) {
            fileBatches.add(archiveRowKeyFactory.getFileBatch(fileId));
        }

        List<Scan> scans = new ArrayList<>(fileBatches.size());
        for (Integer fileBatch : fileBatches) {
            scans.add(buildScan(fileIds, fileBatch, regionStr, conf));
        }
        return scans;
    }

    private static Scan buildScan(Collection<Integer> fileIds, Integer fileBatch, String regionStr, Configuration conf) {
        ArchiveRowKeyFactory archiveRowKeyFactory = new ArchiveRowKeyFactory(conf);

        Scan scan;
        if (StringUtils.isEmpty(regionStr)) {
            scan = AbstractFillFromArchiveTask.buildScan(conf);
        } else {
            scan = AbstractFillFromArchiveTask.buildScan(regionStr, archiveRowKeyFactory.getFirstFileFromBatch(fileBatch), conf);
        }

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        filterList.addFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL,
                new ColumnPrefixFilter(VARIANT_COLUMN_B_PREFIX),
                new TimestampsFilter(Arrays.asList(conf.getLong(AbstractVariantsTableDriver.TIMESTAMP, 0)))
        ));
        for (Integer fileId : fileIds) {
            if (fileBatch == null || archiveRowKeyFactory.getFileBatch(fileId) == fileBatch) {
                byte[] value = Bytes.toBytes(ArchiveTableHelper.getNonRefColumnName(fileId));
                filterList.addFilter(new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(value)));
            }
        }
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

