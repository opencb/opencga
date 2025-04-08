package org.opencb.opencga.storage.hadoop.variant.gaps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveRowKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions.FILL_GAPS_GAP_GENOTYPE;

/**
 * Created on 26/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FillGapsFromVariantTask implements Task<Variant, Put> {

    private final HBaseManager hBaseManager;
    private final String archiveTableName;
    private final StudyMetadata studyMetadata;
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
                                   StudyMetadata studyMetadata,
                                   VariantStorageMetadataManager metadataManager,
                                   Configuration configuration,
                                   Collection<Integer> samples) {
        this.hBaseManager = hBaseManager;
        this.archiveTableName = archiveTableName;
        this.studyMetadata = studyMetadata;
        archiveRowKeyFactory = new ArchiveRowKeyFactory(configuration);
        this.samples = samples;
        samplesFileMap = new HashMap<>();
        for (Integer sample : samples) {
            SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(studyMetadata.getId(), sample);
            for (Integer fileId : sampleMetadata.getFiles()) {
                samplesFileMap.put(sample, fileId);
                fileToNonRefColumnMap.put(fileId, Bytes.toBytes(ArchiveTableHelper.getNonRefColumnName(fileId)));
            }
        }
        anyFileId = fileToNonRefColumnMap.keySet().iterator().next();
        for (Integer fileId : fileToNonRefColumnMap.keySet()) {
            // FIXME !!
            if (archiveRowKeyFactory.getFileBatch(anyFileId) != archiveRowKeyFactory.getFileBatch(fileId)) {
                throw new IllegalStateException("Unable to fill gaps for files from different batches in archive!");
            }
        }
        String gapsGenotype = configuration.get(
                FILL_GAPS_GAP_GENOTYPE.key(),
                FILL_GAPS_GAP_GENOTYPE.defaultValue());
        fillGapsTask = new FillGapsTask(metadataManager, studyMetadata, false, false, gapsGenotype);
    }

    @Override
    public void pre() throws IOException {
        archiveTable = hBaseManager.getConnection().getTable(TableName.valueOf(archiveTableName));
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
    public void post() throws IOException, StorageEngineException {
        fillGapsTask.updateLoadedGenotypes();
        archiveTable.close();
    }

    /**
     * @param variant Variant to fill
     * @return Put with required changes
     * @throws IOException if fails reading from HBAse
     */
    public Put fillGaps(Variant variant) throws IOException {
        HashSet<Integer> missingSamples = new HashSet<>();
        StudyEntry studyEntry = variant.getStudies().get(0);
        for (Map.Entry<String, Integer> entry : studyEntry.getSamplesPosition().entrySet()) {
            Integer sampleId = entry.getValue();
            String sampleName = entry.getKey();
            if (studyEntry.getSamples().get(sampleId).getData().get(0).equals(GenotypeClass.UNKNOWN_GENOTYPE)) {
                missingSamples.add(fillGapsTask.getSampleId(sampleName));
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
            get.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, fileToNonRefColumnMap.get(fileId));
        }

        Put put = new Put(VariantPhoenixKeyFactory.generateVariantRowKey(variant));
        Result result = archiveTable.get(get);
        for (Integer fileId : fileIds) {
            byte[] bytes = result.getValue(GenomeHelper.COLUMN_FAMILY_BYTES, fileToNonRefColumnMap.get(fileId));
            if (bytes != null) {
                VcfSliceProtos.VcfSlice refVcfSlice = null; // FIXME !!
                VcfSliceProtos.VcfSlice nonRefVcfSlice = VcfSliceProtos.VcfSlice.parseFrom(bytes);
                fillGapsTask.fillGaps(variant, missingSamples, put, fileId, nonRefVcfSlice, refVcfSlice);
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
