package org.opencb.opencga.storage.hadoop.variant.index.family;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.schema.types.PVarchar;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.pedigree.MendelianError;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.mr.VariantTableSampleIndexOrderMapper;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBLoader;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexToHBaseConverter;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantAlignedInputFormat;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created on 04/03/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FamilyIndexDriver extends AbstractVariantsTableDriver {

    private static final Logger LOGGER = LoggerFactory.getLogger(FamilyIndexDriver.class);

    public static final String TRIOS = "trios";
    public static final String TRIOS_FILE = "triosFile";
    public static final String TRIOS_FILE_DELETE = "triosFileDelete";
    public static final String OVERWRITE = "overwrite";

    private static final String TRIOS_LIST = "FamilyIndexDriver.trios_list";
    private List<Integer> sampleIds;
    private boolean partial;
    private String region;

    @Override
    protected Class<FamilyIndexMapper> getMapperClass() {
        return FamilyIndexMapper.class;
    }

    @Override
    protected Map<String, String> getParams() {
        Map<String, String> params = new HashMap<>();
        params.put("--" + TRIOS, "(father,mother,child;)**");
        params.put("--" + VariantQueryParam.REGION.key(), "<region>");
        params.put("--" + OVERWRITE, "<true|false>");
        return params;
    }

    @Override
    protected void parseAndValidateParameters() throws IOException {
        super.parseAndValidateParameters();

        VariantStorageMetadataManager metadataManager = getMetadataManager();

        String triosFile = getParam(TRIOS_FILE);
        boolean triosFileDelete = Boolean.valueOf(getParam(TRIOS_FILE_DELETE));
        boolean overwrite = Boolean.valueOf(getParam(OVERWRITE));
        List<String> trios;
        if (StringUtils.isNotEmpty(triosFile)) {
            File file = new File(triosFile);
            trios = FileUtils.readLines(file);
            if (triosFileDelete) {
                Files.delete(file.toPath());
            }
        } else {
            trios = Arrays.asList(getParam(TRIOS).split(";"));
        }

        if (CollectionUtils.isNotEmpty(trios)) {
            sampleIds = new LinkedList<>();
            List<Integer> trioList = new ArrayList<>(3);
            for (String trio : trios) {
                for (String sample : trio.split(",")) {
                    Integer sampleId;
                    if (sample.equals("-")) {
                        sampleId = -1;
                    } else {
                        sampleId = metadataManager.getSampleId(getStudyId(), sample);
                        if (sampleId == null) {
                            throw new IllegalArgumentException("Sample '" + sample + "' not found.");
                        }
                    }
                    trioList.add(sampleId);
                }
                if (trioList.size() != 3) {
                    throw new IllegalArgumentException("Found trio with " + trioList.size() + " members, instead of 3: " + trioList);
                }
                SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(getStudyId(), trioList.get(2));
                if (!overwrite && sampleMetadata.getMendelianErrorStatus().equals(TaskMetadata.Status.READY)) {
                    LOGGER.info("Skip sample " + sampleMetadata.getName() + ". Already precomputed!");
                } else {
                    sampleIds.addAll(trioList);
                    LOGGER.info("Trio: " + trio + " -> " + trioList);
                }
                trioList.clear();
            }
        } else {
            throw new IllegalArgumentException("Missing list of trios!");
        }

        if (sampleIds.isEmpty()) {
            throw new IllegalArgumentException("Missing list of trios!");
        }

        if (sampleIds.size() % 3 != 0) {
            throw new IllegalArgumentException("Wrong number of samples in trios!");
        }

        region = getParam(VariantQueryParam.REGION.key(), "");

        partial = StringUtils.isNotEmpty(region);
    }

    @Override
    protected Job setupJob(Job job, String archiveTable, String variantTable) throws IOException {

        Scan scan = new Scan();

        if (StringUtils.isNotEmpty(region)) {
            LOGGER.info("Calculate Mendelian Errors from region " + region);
            VariantHBaseQueryParser.addRegionFilter(scan, new Region(region));
        }
        LOGGER.info("Calculate Mendelian Errors for " + (sampleIds.size() / 3) + " trios");


        job.getConfiguration().set(TRIOS_LIST, sampleIds.stream().map(Objects::toString).collect(Collectors.joining(",")));

        for (Integer sampleId : sampleIds) {
            scan.addColumn(getHelper().getColumnFamily(), VariantPhoenixHelper.buildSampleColumnKey(getStudyId(), sampleId));
        }
//        scan.addColumn(getHelper().getColumnFamily(), VariantPhoenixHelper.VariantColumn.FULL_ANNOTATION.bytes());


        VariantMapReduceUtil.configureMapReduceScan(scan, getConf());

        VariantMapReduceUtil.initTableMapperJob(job, variantTable,
                scan, getMapperClass(), VariantAlignedInputFormat.class);
        VariantAlignedInputFormat.setDelegatedInputFormat(job, TableInputFormat.class);
        VariantAlignedInputFormat.setBatchSize(job, SampleIndexSchema.BATCH_SIZE);

        VariantMapReduceUtil.setOutputHBaseTable(job, getTableNameGenerator().getSampleIndexTableName(getStudyId()));

        VariantMapReduceUtil.setNoneReduce(job);
        return job;
    }

    @Override
    protected void postExecution(boolean succeed) throws IOException, StorageEngineException {
        super.postExecution(succeed);
        if (succeed && !partial) {
            VariantStorageMetadataManager metadataManager = getMetadataManager();
            for (int i = 0; i < sampleIds.size(); i += 3) {
                Integer father = sampleIds.get(i);
                Integer mother = sampleIds.get(i + 1);
                Integer child = sampleIds.get(i + 2);
                metadataManager.updateSampleMetadata(getStudyId(), child, sampleMetadata -> {
                    sampleMetadata.setMendelianErrorStatus(TaskMetadata.Status.READY);
                    sampleMetadata.setFamilyIndexStatus(TaskMetadata.Status.READY);
                    if (father > 0) {
                        sampleMetadata.setFather(father);
                    }
                    if (mother > 0) {
                        sampleMetadata.setMother(mother);
                    }
                    return sampleMetadata;
                });
            }
        }
    }

    @Override
    protected String getJobOperationName() {
        return "generate_family_index";
    }

    public static class FamilyIndexMapper extends VariantTableSampleIndexOrderMapper<ImmutableBytesWritable, Put> {

        private Map<Integer, ByteArrayOutputStream> mendelianErrorsMap = new HashMap<>();
        private Map<Integer, Map<String, ByteArrayOutputStream>> parentsGTMap = new HashMap<>();
        private Map<Integer, Map<String, Integer>> genotypeCount = new HashMap<>();
        private List<List<Integer>> trios;
        private byte[] family;
        private SampleIndexToHBaseConverter converter;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            family = new GenomeHelper(context.getConfiguration()).getColumnFamily();
            converter = new SampleIndexToHBaseConverter(family);

            int[] sampleIds = context.getConfiguration().getInts(TRIOS_LIST);
            trios = new ArrayList<>(sampleIds.length / 3);

            for (int i = 0; i < sampleIds.length; i += 3) {
                trios.add(Arrays.asList(
                        sampleIds[i],
                        sampleIds[i + 1],
                        sampleIds[i + 2]));
            }

            for (List<Integer> trio : trios) {
                Integer child = trio.get(2);
                mendelianErrorsMap.put(child, new ByteArrayOutputStream());
                parentsGTMap.put(child, new HashMap<>());
                genotypeCount.put(child, new HashMap<>());
            }

        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            Variant variant = VariantPhoenixKeyFactory.extractVariantFromVariantRowKey(value.getRow());
            String chromosome = variant.getChromosome();

            Map<Integer, String> gtMap = new HashMap<>();
            for (Cell cell : value.rawCells()) {
                Integer sampleId = VariantPhoenixHelper
                        .extractSampleId(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                if (sampleId != null) {
                    ImmutableBytesWritable ptr = new ImmutableBytesWritable(
                            cell.getValueArray(),
                            cell.getValueOffset(),
                            cell.getValueLength());
                    PhoenixHelper.positionAtArrayElement(ptr, 0, PVarchar.INSTANCE, null);
                    String gt = Bytes.toString(ptr.get(), ptr.getOffset(), ptr.getLength());
                    gtMap.put(sampleId, gt);
                }
            }

            for (List<Integer> trio : trios) {
                Integer father = trio.get(0);
                Integer mother = trio.get(1);
                Integer child = trio.get(2);

                String fatherGtStr = gtMap.get(father);
                String motherGtStr = gtMap.get(mother);
                String childGtStr = gtMap.get(child);

                int idx = genotypeCount.get(child).merge(childGtStr, 1, Integer::sum) - 1;

                if (SampleIndexSchema.validGenotype(childGtStr)) {
                    parentsGTMap.get(child)
                            .computeIfAbsent(childGtStr, gt -> new ByteArrayOutputStream())
                            .write(GenotypeCodec.encode(fatherGtStr, motherGtStr));
                }

                if ((fatherGtStr != null || father == -1) && (motherGtStr != null || mother == -1) && childGtStr != null) {
                    Genotype fatherGt;
                    Genotype motherGt;
                    Genotype childGt;
                    try {
                        fatherGt = fatherGtStr == null ? null : new Genotype(fatherGtStr);
                        motherGt = motherGtStr == null ? null : new Genotype(motherGtStr);
                        childGt = new Genotype(childGtStr);
                    } catch (IllegalArgumentException e) {
                        // Skip malformed GT
                        context.getCounter(COUNTER_GROUP_NAME, "wrong_gt").increment(1);
                        continue;
                    }
                    Integer me = MendelianError.compute(fatherGt, motherGt, childGt, chromosome);
                    context.getCounter(COUNTER_GROUP_NAME, "me_" + me).increment(1);
                    if (me > 0) {
                        ByteArrayOutputStream stream = mendelianErrorsMap.get(child);
                        MendelianErrorSampleIndexConverter.toBytes(stream, variant, childGtStr, idx, me);
                    }
                }
            }

        }

        @Override
        public void flush(Context context, String chromosome, int position) throws IOException, InterruptedException {

            for (Map.Entry<Integer, ByteArrayOutputStream> entry : mendelianErrorsMap.entrySet()) {
                Integer sampleId = entry.getKey();
                ByteArrayOutputStream meValue = entry.getValue();

                byte[] row = SampleIndexSchema.toRowKey(sampleId, chromosome, position);
                Put put = new Put(row);

                for (Map.Entry<String, ByteArrayOutputStream> gtEntry : parentsGTMap.get(sampleId).entrySet()) {
                    String gt = gtEntry.getKey();
                    ByteArrayOutputStream gtValue = gtEntry.getValue();

                    if (gtValue.size() > 0) {
                        // Copy value, as the ByteArrayOutputStream is erased
                        byte[] value = gtValue.toByteArray();
                        put.addColumn(family, SampleIndexSchema.toParentsGTColumn(gt), value);
                        gtValue.reset();
                    }
                }

                if (meValue.size() > 0) {
                    // Copy value, as the ByteArrayOutputStream is erased
                    byte[] value = meValue.toByteArray();
                    put.addColumn(family, SampleIndexSchema.toMendelianErrorColumn(), value);
                    meValue.reset();
                }

                if (!put.isEmpty()) {
                    context.write(new ImmutableBytesWritable(row), put);
                }
            }

            genotypeCount.values().forEach(Map::clear);
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(new FamilyIndexDriver().privateMain(args, null));
        } catch (Exception e) {
            LOGGER.error("Error executing " + FamilyIndexDriver.class, e);
            System.exit(1);
        }
    }
}
