package org.opencb.opencga.storage.hadoop.variant.gaps;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.run.ParallelTaskRunner.TaskWithException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
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

/**
 * Created on 26/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FillGapsFromVariantTask implements TaskWithException<Variant, Put, IOException> {

    private final HBaseManager hBaseManager;
    private final String archiveTableName;
    private final StudyConfiguration studyConfiguration;
    private final GenomeHelper helper;
    private final Integer anyFileId;
    private Table archiveTable;
    private final ArchiveRowKeyFactory archiveRowKeyFactory;
    private final Collection<Integer> samples;
    private final Map<Integer, Integer> samplesFileMap;
    private final Map<Integer, byte[]> fileToNonRefColumnMap = new HashMap<>();
    private final Logger logger = LoggerFactory.getLogger(FillGapsFromVariantTask.class);
    private FillGapsTask fillGapsTask;

    public FillGapsFromVariantTask(HBaseManager hBaseManager,
                                   String archiveTableName,
                                   StudyConfiguration studyConfiguration,
                                   GenomeHelper helper,
                                   Collection<Integer> samples) {
        this.hBaseManager = hBaseManager;
        this.archiveTableName = archiveTableName;
        this.studyConfiguration = studyConfiguration;
        this.helper = helper;
        archiveRowKeyFactory = new ArchiveRowKeyFactory(helper.getConf());
        this.samples = samples;
        samplesFileMap = new HashMap<>();
        for (Integer sample : samples) {
            for (Map.Entry<Integer, LinkedHashSet<Integer>> entry : studyConfiguration.getSamplesInFiles().entrySet()) {
                if (entry.getValue().contains(sample)) {
                    Integer fileId = entry.getKey();
                    samplesFileMap.put(sample, fileId);
                    fileToNonRefColumnMap.put(fileId, Bytes.toBytes(ArchiveTableHelper.getNonRefColumnName(fileId)));
                    break;
                }
            }
        }
        anyFileId = fileToNonRefColumnMap.keySet().iterator().next();
        for (Integer fileId : fileToNonRefColumnMap.keySet()) {
            // FIXME !!
            if (archiveRowKeyFactory.getFileBatch(anyFileId) != archiveRowKeyFactory.getFileBatch(fileId)) {
                throw new IllegalStateException("Unable to fill gaps for files from different batches in archive!");
            }
        }
        fillGapsTask = new FillGapsTask(studyConfiguration, helper, false);
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
    public List<Put> apply(List<Variant> list) throws IOException {
        List<Put> puts = new ArrayList<>(list.size());
        for (Variant variant : list) {
            Put put = fillGaps(variant);
            if (put != null && !put.isEmpty()) {
                puts.add(put);
            }
        }
        return puts;
    }

    @Override
    public void post() {
        try {
            archiveTable.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * @param variant Variant to fill
     * @return Put with required changes
     * @throws IOException if fails reading from HBAse
     */
    public Put fillGaps(Variant variant) throws IOException {
        HashSet<Integer> missingSamples = new HashSet<>();
        for (Integer sampleId : samples) {
            if (variant.getStudies().get(0).getSampleData(studyConfiguration.getSampleIds().inverse().get(sampleId)).get(0).equals("?/?")) {
                missingSamples.add(sampleId);
            }
        }
        return fillGaps(variant, missingSamples);
    }
    /**
     * @param variant        Variant to fill
     * @param missingSamples Missing samples in this variant
     * @return Put with required changes
     * @throws IOException if fails reading from HBAse
     */
    public Put fillGaps(Variant variant, Set<Integer> missingSamples) throws IOException {
        if (samples.size() == missingSamples.size() || missingSamples.isEmpty()) {
            // Nothing to do!
            return null;
        }

        Set<Integer> fileIds = new HashSet<>();
        for (Integer missingSample : missingSamples) {
            fileIds.add(samplesFileMap.get(missingSample));
        }
        Get get = new Get(Bytes.toBytes(archiveRowKeyFactory.generateBlockId(variant, anyFileId)));
        for (Integer fileId : fileIds) {
            get.addColumn(helper.getColumnFamily(), fileToNonRefColumnMap.get(fileId));
        }

        Put put = new Put(VariantPhoenixKeyFactory.generateVariantRowKey(variant));
        Result result = archiveTable.get(get);
        for (Integer fileId : fileIds) {
            byte[] bytes = result.getValue(helper.getColumnFamily(), fileToNonRefColumnMap.get(fileId));
            if (bytes != null) {
                VcfSliceProtos.VcfSlice refVcfSlice = null; // FIXME !!
                VcfSliceProtos.VcfSlice nonRefVcfSlice = VcfSliceProtos.VcfSlice.parseFrom(bytes);
                ArrayList<Put> sampleIndexPuts = null; // FIXME !!
                fillGapsTask.fillGaps(variant, missingSamples, put, sampleIndexPuts, fileId, nonRefVcfSlice, refVcfSlice);
            } else {
                logger.debug("Missing fileId " + fileId + " in variant " + variant);
            }
        }
        return put;
    }

    public static Query buildQuery(Object study, Collection<?> sampleIds, Collection<?> fileIds) {
        return new Query()
                .append(VariantQueryParam.STUDY.key(), study)
                .append(VariantQueryParam.FILE.key(), fileIds)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), sampleIds);
    }

    public static QueryOptions buildQueryOptions() {
        return new QueryOptions(QueryOptions.EXCLUDE, Arrays.asList(VariantField.ANNOTATION, VariantField.STUDIES_STATS));
    }

}
