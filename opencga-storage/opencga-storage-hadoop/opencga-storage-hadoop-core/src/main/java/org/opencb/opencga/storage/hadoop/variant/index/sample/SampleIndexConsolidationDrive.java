package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.hadoop.variant.AbstractAnalysisTableDriver;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.gaps.FillGapsFromArchiveMapper;
import org.opencb.opencga.storage.hadoop.variant.mr.AnalysisTableMapReduceHelper;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine.LOADED_GENOTYPES;

/**
 * Creates and executes a MR job that finishes loading all the pending variants in the SampleIndex table.
 * For each variant with, reads all pending variants like _V_{VARIANT}_{GT}, and updates the list of variants
 * for that genotype and updates the genotype counters.
 *
 * Only genotypes without the main alternate (0/2, 2/3, ...) should be found as pending.
 * Genotypes with the main alternate (0/1, 1/1, 1/2, ...) are already loaded by {@link SampleIndexDBLoader}
 *
 * Created on 30/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexConsolidationDrive extends AbstractAnalysisTableDriver {
    public static final String GENOTYPES_COUNTER_GROUP_NAME = "genotypes";
    private final Logger logger = LoggerFactory.getLogger(SampleIndexConsolidationDrive.class);
    private String sampleIndexTable;
    private int[] samples;
    private boolean allSamples;

    @Override
    protected void parseAndValidateParameters() throws IOException {
        sampleIndexTable = getHelper().getHBaseVariantTableNameGenerator().getSampleIndexTableName(getStudyId());

        if (sampleIndexTable == null || sampleIndexTable.isEmpty()) {
            throw new IllegalArgumentException("Missing sampleIndex table!");
        }

        samples = getConf().getInts(FillGapsFromArchiveMapper.SAMPLES);
        if (samples == null || samples.length == 0) {
            allSamples = true;
        } else {
            allSamples = false;
        }
    }

    @Override
    protected Class<SampleIndexConsolidateMapper> getMapperClass() {
        return SampleIndexConsolidateMapper.class;
    }

    @Override
    protected Job setupJob(Job job, String archiveTable, String variantTable) throws IOException {


        List<Scan> scans = new ArrayList<>();

        int caching = job.getConfiguration().getInt(HadoopVariantStorageEngine.MAPREDUCE_HBASE_SCAN_CACHING, 100);
        logger.info("Scan set Caching to " + caching);
        Scan templateScan = new Scan();
        templateScan.setCaching(caching);        // 1 is the default in Scan
        templateScan.setCacheBlocks(false);  // don't set to true for MR jobs

        if (allSamples) {
            scans.add(templateScan);
        } else {
            for (int sample : samples) {
                Scan newScan = new Scan(templateScan);
                newScan.setRowPrefixFilter(SampleIndexConverter.toRowKey(sample));
                scans.add(newScan);
            }
        }

        for (int i = 0; i < scans.size(); i++) {
            Scan s = scans.get(i);
            logger.info("scan[" + i + "]= " + s.toJSON());
        }

        // set other scan attrs
        VariantMapReduceUtil.initTableMapperJob(job, sampleIndexTable, sampleIndexTable, scans, getMapperClass());

        job.setSpeculativeExecution(false);
//        job.getConfiguration().setInt(MRJobConfig.TASK_TIMEOUT, 20 * 60 * 1000);


        return job;
    }

    @Override
    protected void postExecution(Job job) throws IOException, StorageEngineException {
        super.postExecution(job);

        // Update list of loaded genotypes
        Set<String> gts = new HashSet<>();
        if (job.isSuccessful()) {
            for (Counter counter : job.getCounters().getGroup(GENOTYPES_COUNTER_GROUP_NAME)) {
                gts.add(counter.getName());
            }
            if (!gts.isEmpty()) {
                getStudyConfigurationManager().lockAndUpdate(getStudyId(), sc -> {
                    gts.addAll(sc.getAttributes().getAsStringList(LOADED_GENOTYPES));
                    sc.getAttributes().put(LOADED_GENOTYPES, gts);
                    return sc;
                });
            }
        }
    }

    @Override
    protected String getJobOperationName() {
        return "consolidate_sample_index";
    }

    public static class SampleIndexConsolidateMapper extends TableMapper<ImmutableBytesWritable, Mutation> {

        private byte[] family;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            family = new GenomeHelper(context.getConfiguration()).getColumnFamily();
        }

        @Override
        protected void map(ImmutableBytesWritable k, Result result, Context context) throws IOException, InterruptedException {
            Map<String, List<String>> map = new HashMap<>();
            Map<String, Cell> otherCells = new HashMap<>();

            Delete delete = new Delete(result.getRow());
            for (Cell cell : result.rawCells()) {
                byte[] column = CellUtil.cloneQualifier(cell);
                Pair<String, String> pair = SampleIndexConverter.parsePendingColumn(column);
                if (pair != null) {
                    delete.addColumn(family, column);
                    String variant = pair.getKey();
                    String gt = pair.getValue();

                    map.computeIfAbsent(gt, key -> new ArrayList<>()).add(variant);
                } else {
                    otherCells.put(Bytes.toString(column), cell);
                }
            }

            if (!delete.isEmpty()) {
                context.getCounter(AnalysisTableMapReduceHelper.COUNTER_GROUP_NAME, "consolidation").increment(1);
                Put put = new Put(result.getRow());
                for (Map.Entry<String, List<String>> entry : map.entrySet()) {
                    String gt = entry.getKey();
                    context.getCounter(GENOTYPES_COUNTER_GROUP_NAME, gt).increment(entry.getValue().size());

                    List<String> variants = entry.getValue();
                    Cell cell = otherCells.get(gt);
                    if (cell == null) {
                        context.getCounter(AnalysisTableMapReduceHelper.COUNTER_GROUP_NAME, "new_gt").increment(1);
                        put.addColumn(family, Bytes.toBytes(gt), Bytes.toBytes(String.join(",", variants)));
                        put.addColumn(family, SampleIndexConverter.toGenotypeCountColumn(gt), Bytes.toBytes(variants.size()));
                    } else {
                        context.getCounter(AnalysisTableMapReduceHelper.COUNTER_GROUP_NAME, "merged_gt").increment(1);
                        // Merge with existing values
                        TreeSet<Variant> variantsSet = new TreeSet<>(SampleIndexConverter.INTRA_CHROMOSOME_VARIANT_COMPARATOR);
                        List<Variant> loadedVariants = SampleIndexConverter.getVariants(cell);
                        variantsSet.addAll(loadedVariants);
                        for (String variant : variants) {
                            variantsSet.add(new Variant(variant));
                        }

                        if (loadedVariants.size() == variantsSet.size()) {
                            context.getCounter(AnalysisTableMapReduceHelper.COUNTER_GROUP_NAME, "merged_gt_skip").increment(1);
                        } else {
                            StringBuilder sb = new StringBuilder();
                            Iterator<Variant> iterator = variantsSet.iterator();
                            // Add first directly. List can not be empty.
                            sb.append(iterator.next().toString());
                            while (iterator.hasNext()) {
                                Variant variant = iterator.next();
                                sb.append(',');
                                sb.append(variant.toString());
                            }

                            put.addColumn(family, SampleIndexConverter.toGenotypeColumn(gt), Bytes.toBytes(sb.toString()));
                            put.addColumn(family, SampleIndexConverter.toGenotypeCountColumn(gt), Bytes.toBytes(variantsSet.size()));
                        }
                    }
                }

                if (!put.isEmpty()) {
                    context.write(k, put);
                }
                context.write(k, delete);
            }
        }
    }
}
