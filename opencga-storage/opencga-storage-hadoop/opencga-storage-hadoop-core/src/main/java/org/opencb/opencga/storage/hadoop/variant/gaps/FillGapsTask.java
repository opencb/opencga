package org.opencb.opencga.storage.hadoop.variant.gaps;

import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.biodata.tools.variant.converters.proto.VcfRecordProtoToVariantConverter;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
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
import org.opencb.opencga.storage.hadoop.variant.converters.study.StudyEntryToHBaseConverter;
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
public class FillGapsTask implements TaskWithException<Variant, Put, IOException> {

    private final HBaseManager hBaseManager;
    private final String archiveTableName;
    private final StudyConfiguration studyConfiguration;
    private final GenomeHelper helper;
    private Table archiveTable;
    private final ArchiveRowKeyFactory archiveRowKeyFactory;
    private final Collection<Integer> samples;
    private final Map<Integer, Integer> samplesFileMap;
    private final Map<Integer, byte[]> fileToColumnMap = new HashMap<>();
    private final Map<Integer, LinkedHashMap<String, Integer>> fileToSamplePositions = new HashMap<>();
    private final StudyEntryToHBaseConverter studyConverter;
    private final Logger logger = LoggerFactory.getLogger(FillGapsTask.class);
    private final VariantMerger variantMerger;

    public FillGapsTask(HBaseManager hBaseManager,
                        String archiveTableName,
                        StudyConfiguration studyConfiguration,
                        GenomeHelper helper,
                        Collection<Integer> samples) {
        this.hBaseManager = hBaseManager;
        this.archiveTableName = archiveTableName;
        this.studyConfiguration = studyConfiguration;
        this.helper = helper;
        archiveRowKeyFactory = new ArchiveRowKeyFactory(helper.getChunkSize(), helper.getSeparator());
        this.samples = samples;
        samplesFileMap = new HashMap<>();
        for (Integer sample : samples) {
            for (Map.Entry<Integer, LinkedHashSet<Integer>> entry : studyConfiguration.getSamplesInFiles().entrySet()) {
                if (entry.getValue().contains(sample)) {
                    Integer fileId = entry.getKey();
                    samplesFileMap.put(sample, fileId);
                    fileToColumnMap.put(fileId, Bytes.toBytes(ArchiveTableHelper.getColumnName(fileId)));
                    break;
                }
            }
        }
        for (Integer fileId : fileToColumnMap.keySet()) {
            LinkedHashMap<String, Integer> map = new LinkedHashMap<>();
            for (Integer sampleId : studyConfiguration.getSamplesInFiles().get(fileId)) {
                map.put(studyConfiguration.getSampleIds().inverse().get(sampleId), map.size());
            }
            fileToSamplePositions.put(fileId, map);
        }
        studyConverter = new StudyEntryToHBaseConverter(helper.getColumnFamily(), studyConfiguration, true, Collections.singleton("?/?"));
        variantMerger = new VariantMerger(false).configure(studyConfiguration.getVariantHeader());
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
        Get get = new Get(Bytes.toBytes(archiveRowKeyFactory.generateBlockId(variant)));
        for (Integer fileId : fileIds) {
            get.addColumn(helper.getColumnFamily(), fileToColumnMap.get(fileId));
        }

        Put put = new Put(VariantPhoenixKeyFactory.generateVariantRowKey(variant));
        Result result = archiveTable.get(get);
        for (Integer fileId : fileIds) {
            byte[] bytes = result.getValue(helper.getColumnFamily(), fileToColumnMap.get(fileId));
            if (bytes != null) {
                VcfSliceProtos.VcfSlice vcfSlice = VcfSliceProtos.VcfSlice.parseFrom(bytes);
                String chromosome = vcfSlice.getChromosome();
                int position = vcfSlice.getPosition();

                // Three scenarios:
                //  Overlap with NO_VARIATION,
                //  Overlap with another variant
                //  No overlap
                // TODO: What if multiple overlapps?
                boolean found = false;
                for (VcfSliceProtos.VcfRecord vcfRecord : vcfSlice.getRecordsList()) {
                    int start = VcfRecordProtoToVariantConverter.getStart(vcfRecord, position);
                    int end = VcfRecordProtoToVariantConverter.getEnd(vcfRecord, position);
                    if (overlapsWith(variant, chromosome, start, end)) {
                        VcfRecordProtoToVariantConverter converter = new VcfRecordProtoToVariantConverter(vcfSlice.getFields(),
                                fileToSamplePositions.get(fileId), fileId.toString(), studyConfiguration.getStudyName());
                        Variant archiveVariant = converter.convert(vcfRecord, chromosome, position);
                        if (archiveVariant.getType().equals(VariantType.NO_VARIATION)) {
                            FileEntry fileEntry = archiveVariant.getStudies().get(0).getFiles().get(0);
                            fileEntry.getAttributes().remove(VCFConstants.END_KEY);
                            if (StringUtils.isEmpty(fileEntry.getCall())) {
                                fileEntry.setCall(archiveVariant.getStart() + ":" + archiveVariant.getReference() + ":.:0");
                            }
                            studyConverter.convert(archiveVariant, put, missingSamples);
                            found = true;
                        } else {
                            Variant mergedVariant = new Variant(
                                    variant.getChromosome(),
                                    variant.getStart(),
                                    variant.getEnd(),
                                    variant.getReference(),
                                    variant.getAlternate());
                            StudyEntry studyEntry = new StudyEntry();
                            studyEntry.setFormat(archiveVariant.getStudies().get(0).getFormat());
                            studyEntry.setSortedSamplesPosition(new LinkedHashMap<>());
                            studyEntry.setSamplesData(new ArrayList<>());

                            mergedVariant.addStudyEntry(studyEntry);
                            mergedVariant.setType(variant.getType());

                            mergedVariant = variantMerger.merge(mergedVariant, archiveVariant);
                            studyConverter.convert(mergedVariant, put, missingSamples);
                            found = true;
                        }
                    }
                }
                if (!found) {
                    logger.debug("Not overlap for fileId " + fileId + " in variant " + variant);
                }
            } else {
                logger.debug("Missing fileId " + fileId + " in variant " + variant);
            }
        }
        return put;
    }

    public static boolean overlapsWith(Variant variant, String chromosome, int start, int end) {
//        return variant.overlapWith(chromosome, start, end, true);
        if (!StringUtils.equals(variant.getChromosome(), chromosome)) {
            return false; // Different Chromosome
        } else {
            return variant.getStart() <= end && variant.getEnd() >= start
                    // Insertions in the same position won't match previous statement.
                    || variant.getStart() == start && variant.getEnd() == end;
        }
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
