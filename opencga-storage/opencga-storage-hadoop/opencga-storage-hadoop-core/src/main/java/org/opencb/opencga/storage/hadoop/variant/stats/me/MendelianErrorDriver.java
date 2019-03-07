package org.opencb.opencga.storage.hadoop.variant.stats.me;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.pedigree.MendelianError;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.mr.VariantTableSampleIndexOrderMapper;
import org.opencb.opencga.storage.hadoop.variant.index.sample.HBaseToSampleIndexConverter;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBLoader;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantAlignedInputFormat;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created on 04/03/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MendelianErrorDriver extends AbstractVariantsTableDriver {

    private static final Logger LOGGER = LoggerFactory.getLogger(MendelianErrorDriver.class);

    public static final String TRIOS = "trios";
    public static final String TRIOS_LIST = "MendelianErrorDriver.trios_list";
    private List<Integer> sampleIds;

    @Override
    protected Class<MendelianErrorMapper> getMapperClass() {
        return MendelianErrorMapper.class;
    }

    @Override
    protected Map<String, String> getParams() {
        Map<String, String> params = new HashMap<>();
        params.put("--" + TRIOS, "(father,mother,child;)**");
        params.put("--" + VariantQueryParam.REGION.key(), "[region]");
        return params;
    }

    @Override
    protected void parseAndValidateParameters() throws IOException {
        super.parseAndValidateParameters();


        VariantStorageMetadataManager metadataManager = getMetadataManager();
        String trios = getParam(TRIOS);
        if (StringUtils.isNotEmpty(trios)) {
            sampleIds = new LinkedList<>();
            List<Integer> trioList = new ArrayList<>(3);
            for (String trio : trios.split(";")) {
                for (String sample : trio.split(",")) {
                    Integer sampleId = metadataManager.getSampleId(getStudyId(), sample);
                    if (sampleId == null) {
                        throw new IllegalArgumentException("Sample '" + sample + "' not found.");
                    }
                    trioList.add(sampleId);
                    sampleIds.add(sampleId);
                }
                if (trioList.size() != 3) {
                    throw new IllegalArgumentException("Found trio with " + trioList.size() + " members, instead of 3: " + trioList);
                }
                LOGGER.info("Trio: " + trio + " -> " + trioList);
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


    }

    @Override
    protected Job setupJob(Job job, String archiveTable, String variantTable) throws IOException {

        Scan scan = new Scan();

        String region = getParam(VariantQueryParam.REGION.key(), "");
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
        VariantAlignedInputFormat.setBatchSize(job, SampleIndexDBLoader.BATCH_SIZE);

        VariantMapReduceUtil.setOutputHBaseTable(job, getTableNameGenerator().getSampleIndexTableName(getStudyId()));

        VariantMapReduceUtil.setNoneReduce(job);
        return job;
    }

    @Override
    protected void postExecution(boolean succeed) throws IOException, StorageEngineException {
        super.postExecution(succeed);
        if (succeed) {
            VariantStorageMetadataManager metadataManager = getMetadataManager();
            for (int i = 0; i < sampleIds.size(); i += 3) {
                Integer child = sampleIds.get(i + 2);
                metadataManager.updateSampleMetadata(getStudyId(), child, sampleMetadata -> {
                    sampleMetadata.setStatus("mendelian_error", TaskMetadata.Status.READY);
                    return sampleMetadata;
                });
            }

        }
    }

    @Override
    protected String getJobOperationName() {
        return "calculate_mendelian_errors";
    }

    public static class MendelianErrorMapper extends VariantTableSampleIndexOrderMapper<ImmutableBytesWritable, Put> {

        private Map<Integer, ByteArrayOutputStream> mendelianErrorsMap = new HashMap<>();
        private Map<Integer, Map<String, Integer>> genotypeCount = new HashMap<>();
        private List<List<Integer>> trios;
        private byte[] family;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            family = new GenomeHelper(context.getConfiguration()).getColumnFamily();

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
                    PArrayDataType.positionAtArrayElement(ptr, 0, PVarchar.INSTANCE, null);
                    String gt = Bytes.toString(ptr.get(), ptr.getOffset(), ptr.getLength());
                    gtMap.put(sampleId, gt);
                }
            }


            for (List<Integer> trio : trios) {
                Integer child = trio.get(2);

                String fatherGt = gtMap.get(trio.get(0));
                String motherGt = gtMap.get(trio.get(1));
                String childGt = gtMap.get(child);

                int idx = genotypeCount.get(child).merge(childGt, 1, Integer::sum) - 1;
                if (fatherGt != null && motherGt != null && childGt != null) {
                    Integer me = MendelianError.compute(new Genotype(fatherGt), new Genotype(motherGt), new Genotype(childGt), chromosome);
                    context.getCounter(COUNTER_GROUP_NAME, "me_" + me).increment(1);
                    if (me > 0) {
                        ByteArrayOutputStream stream = mendelianErrorsMap.get(child);
                        if (stream.size() != 0) {
                            stream.write(HBaseToSampleIndexConverter.SEPARATOR);
                        }
                        stream.write(Bytes.toBytes(variant.toString()));
                        stream.write('_');
                        stream.write(Bytes.toBytes(childGt));
                        stream.write('_');
                        stream.write(Bytes.toBytes(Integer.toString(idx)));
                    }
                }
            }

        }

        @Override
        public void flush(Context context, String chromosome, int position) throws IOException, InterruptedException {

            for (Map.Entry<Integer, ByteArrayOutputStream> entry : mendelianErrorsMap.entrySet()) {
                Integer sampleId = entry.getKey();
                ByteArrayOutputStream value = entry.getValue();


                byte[] row = HBaseToSampleIndexConverter.toRowKey(sampleId, chromosome, position);

                if (value.size() > 0) {
                    Put put = new Put(row);
                    // Copy value, as the ByteArrayOutputStream is erased
                    put.addColumn(family, HBaseToSampleIndexConverter.toMendelianErrorColumn(), value.toByteArray());

                    context.write(new ImmutableBytesWritable(row), put);
                    value.reset();
                }
            }

            genotypeCount.values().forEach(Map::clear);
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(new MendelianErrorDriver().privateMain(args, null));
        } catch (Exception e) {
            LOGGER.error("Error executing " + MendelianErrorDriver.class, e);
            System.exit(1);
        }
    }
}
