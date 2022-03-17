package org.opencb.opencga.storage.hadoop.variant.index.family;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Genotype;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.pedigree.MendelianError;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;
import org.opencb.opencga.storage.hadoop.variant.converters.VariantRow;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.mr.VariantTableSampleIndexOrderMapper;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantAlignedInputFormat;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
    public static final String TRIOS_COHORT = "triosCohort";
    public static final String TRIOS_COHORT_DELETE = "triosCohortDelete";
    public static final String OVERWRITE = "overwrite";
    public static final String OUTPUT = "output-table";
    public static final String SAMPLE_INDEX_VERSION = "sample-index-version";

    private static final String TRIOS_LIST = "FamilyIndexDriver.trios_list";
    // Samples where at least one parent is not in its file
    private static final String SAMPLES_WITH_UNKNOWN_PARENT_GENOTYPES = "FamilyIndexDriver.samples_with_unknown_parent_genotypes";
    private static final int MISSING_SAMPLE = -1;
    private List<Integer> sampleIds;
    private List<Integer> samplesWithUnknownParentGenotypes;
    private boolean partial;
    private String region;
    private String sampleIndexTableName;
    private int sampleIndexVersion;

    @Override
    protected Class<FamilyIndexMapper> getMapperClass() {
        return FamilyIndexMapper.class;
    }

    @Override
    protected Map<String, String> getParams() {
        Map<String, String> params = new HashMap<>();
        params.put("--" + TRIOS, "(father,mother,child;)**");
        params.put("--" + VariantQueryParam.REGION.key(), "<region>");
        params.put("--" + SAMPLE_INDEX_VERSION, "<version>");
        params.put("--" + OVERWRITE, "<true|false>");
        return params;
    }

    @Override
    protected void parseAndValidateParameters() throws IOException {
        super.parseAndValidateParameters();

        VariantStorageMetadataManager metadataManager = getMetadataManager();

        if (getParam(OUTPUT) == null || getParam(OUTPUT).isEmpty()) {
            throw new IllegalArgumentIOException("Missing output table");
        }
        sampleIndexTableName = getParam(OUTPUT);
        sampleIndexVersion = Integer.parseInt(getParam(SAMPLE_INDEX_VERSION));

        boolean overwrite = Boolean.parseBoolean(getParam(OVERWRITE));
        String triosCohort = getParam(TRIOS_COHORT);
        String triosStr = getParam(TRIOS);

        sampleIds = new LinkedList<>();
        samplesWithUnknownParentGenotypes = new LinkedList<>();
        if (StringUtils.isNotEmpty(triosStr)) {
            String[] trios = triosStr.split(";");
            List<Integer> trioList = new ArrayList<>(3);
            for (String trio : trios) {
                for (String sample : trio.split(",")) {
                    Integer sampleId;
                    if (sample.equals("-")) {
                        sampleId = MISSING_SAMPLE;
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
                if (!overwrite && sampleMetadata.getFamilyIndexStatus(sampleIndexVersion) == TaskMetadata.Status.READY) {
                    LOGGER.info("Skip sample " + sampleMetadata.getName() + ". Already precomputed!");
                } else {
                    sampleIds.addAll(trioList);
                    LOGGER.info("Trio: " + trio + " -> " + trioList);
                }
                trioList.clear();
            }
        } else if (StringUtils.isNotEmpty(triosCohort)) {
            CohortMetadata cohortMetadata;
            try {
                cohortMetadata = getMetadataManager().getCohortMetadata(getStudyId(), triosCohort);
            } finally {
                boolean triosCohortDelete = Boolean.parseBoolean(getParam(TRIOS_COHORT_DELETE));
                if (triosCohortDelete) {
                    getMetadataManager().removeCohort(getStudyId(), triosCohort);
                }
            }

            for (Integer sample : cohortMetadata.getSamples()) {
                SampleMetadata sampleMetadata = getMetadataManager().getSampleMetadata(getStudyId(), sample);
                if (sampleMetadata.getFather() == null) {
                    sampleIds.add(MISSING_SAMPLE);
                } else {
                    sampleIds.add(sampleMetadata.getFather());
                }
                if (sampleMetadata.getMother() == null) {
                    sampleIds.add(MISSING_SAMPLE);
                } else {
                    sampleIds.add(sampleMetadata.getMother());
                }
                sampleIds.add(sampleMetadata.getId());
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
        sampleIds = new ArrayList<>(sampleIds);
        for (int i = 0; i < sampleIds.size(); i += 3) {
            Integer father = sampleIds.get(i);
            Integer mother = sampleIds.get(i + 1);
            Integer child = sampleIds.get(i + 2);

            boolean parentsInSeparatedFile = false;
            SampleMetadata childMetadata = getMetadataManager().getSampleMetadata(getStudyId(), child);
            List<Integer> childFiles = childMetadata.getFiles();
            SampleMetadata fatherMetadata = null;
            if (father != MISSING_SAMPLE) {
                fatherMetadata = getMetadataManager().getSampleMetadata(getStudyId(), father);
                List<Integer> fatherFiles = fatherMetadata.getFiles();
                if (fatherFiles.size() != childFiles.size() || !fatherFiles.containsAll(childFiles)) {
                    parentsInSeparatedFile = true;
                }
            }
            SampleMetadata motherMetadata = null;
            if (mother != MISSING_SAMPLE) {
                motherMetadata = getMetadataManager().getSampleMetadata(getStudyId(), mother);
                List<Integer> motherFiles = motherMetadata.getFiles();
                if (motherFiles.size() != childFiles.size() || !motherFiles.containsAll(childFiles)) {
                    parentsInSeparatedFile = true;
                }
            }
            if (parentsInSeparatedFile) {
                samplesWithUnknownParentGenotypes.add(child);
                if (samplesWithUnknownParentGenotypes.size() < 20) {
                    LOGGER.info(" - Trio from multiple files: sample: " + childMetadata.getName()
                            + ", father: " + (fatherMetadata == null ? "none" : fatherMetadata.getName())
                            + ", mother: " + (motherMetadata == null ? "none" : motherMetadata.getName()));
                }
            }
        }
        LOGGER.info("Found {} trios where some parent was loaded from a different file", samplesWithUnknownParentGenotypes.size());

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


        job.getConfiguration().set(TRIOS_LIST,
                sampleIds.stream().map(Objects::toString).collect(Collectors.joining(",")));
        job.getConfiguration().set(SAMPLES_WITH_UNKNOWN_PARENT_GENOTYPES,
                samplesWithUnknownParentGenotypes.stream().map(Objects::toString).collect(Collectors.joining(",")));

        for (Integer sampleId : sampleIds) {
            if (sampleId != MISSING_SAMPLE) {
                SampleMetadata sampleMetadata = getMetadataManager().getSampleMetadata(getStudyId(), sampleId);
                for (PhoenixHelper.Column column : VariantPhoenixSchema.getSampleColumns(sampleMetadata)) {
                    scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, column.bytes());
                }
            }
        }
//        scan.addColumn(getHelper().getColumnFamily(), VariantPhoenixHelper.VariantColumn.FULL_ANNOTATION.bytes());


        VariantMapReduceUtil.configureMapReduceScan(scan, getConf());

        VariantMapReduceUtil.initTableMapperJob(job, variantTable,
                scan, getMapperClass(), VariantAlignedInputFormat.class);
        VariantAlignedInputFormat.setDelegatedInputFormat(job, TableInputFormat.class);
        VariantAlignedInputFormat.setBatchSize(job, SampleIndexSchema.BATCH_SIZE);

        VariantMapReduceUtil.setOutputHBaseTable(job, sampleIndexTableName);

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
                    sampleMetadata.setFamilyIndexStatus(TaskMetadata.Status.READY, sampleIndexVersion);
                    if (father > 0) {
                        sampleMetadata.setFather(father);
                    }
                    if (mother > 0) {
                        sampleMetadata.setMother(mother);
                    }
                });
            }
        }
    }

    @Override
    protected String getJobOperationName() {
        return "generate_family_index";
    }

    public static class FamilyIndexMapper extends VariantTableSampleIndexOrderMapper<ImmutableBytesWritable, Put> {

        private Map<Integer, Map<String, Integer>> genotypeCount = new HashMap<>();
        private Map<Integer, FamilyIndexPutBuilder> familyIndexBuilder = new HashMap<>();
        private List<List<Integer>> trios;
        private List<Boolean> triosWithUnknownGenotypes;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            int[] sampleIds = context.getConfiguration().getInts(TRIOS_LIST);
            Set<Integer> samplesWithUnknownParentGenotypes =
                    Arrays.stream(context.getConfiguration().getInts(SAMPLES_WITH_UNKNOWN_PARENT_GENOTYPES))
                            .boxed()
                            .collect(Collectors.toSet());
            trios = new ArrayList<>(sampleIds.length / 3);
            triosWithUnknownGenotypes = new ArrayList<>(sampleIds.length / 3);

            for (int i = 0; i < sampleIds.length; i += 3) {
                int father = sampleIds[i];
                int mother = sampleIds[i + 1];
                int child = sampleIds[i + 2];
                triosWithUnknownGenotypes.add(samplesWithUnknownParentGenotypes.contains(child));
                trios.add(Arrays.asList(
                        father,
                        mother,
                        child));
                familyIndexBuilder.put(child, new FamilyIndexPutBuilder(child));
                genotypeCount.put(child, new HashMap<>());
            }
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

            Map<Integer, String> gtMap = new HashMap<>();
            Map<Integer, Set<String>> discrepanciesGtMap = new HashMap<>();
            Variant variant = new VariantRow(value).walker().onSample(sampleColumn -> {
                String gt = sampleColumn.getGT();
                String oldGt = gtMap.put(sampleColumn.getSampleId(), gt);
                // If the sample already had a genotype, and it was different from the current, record as a discrepancy
                if (oldGt != null && !oldGt.equals(gt)) {
                    Set<String> gts = discrepanciesGtMap.computeIfAbsent(sampleColumn.getSampleId(), s -> new HashSet<>());
                    gts.add(oldGt);
                    gts.add(gt);
                }
            }).walk();

            for (int i = 0, triosSize = trios.size(); i < triosSize; i++) {
                List<Integer> trio = trios.get(i);
                Integer father = trio.get(0);
                Integer mother = trio.get(1);
                Integer child = trio.get(2);
                FamilyIndexPutBuilder builder = familyIndexBuilder.get(child);

                Set<String> fatherDiscrepancies = discrepanciesGtMap.get(father);
                Set<String> motherDiscrepancies = discrepanciesGtMap.get(mother);
                Set<String> childDiscrepancies = discrepanciesGtMap.get(child);

                String defaultGenotype = triosWithUnknownGenotypes.get(i) ? null : "0/0";
                if (fatherDiscrepancies == null && motherDiscrepancies == null && childDiscrepancies == null) {
                    String fatherGtStr;
                    if (father == MISSING_SAMPLE) {
                        fatherGtStr = null;
                    } else {
                        fatherGtStr = gtMap.get(father);
                        if (fatherGtStr == null) {
                            context.getCounter(COUNTER_GROUP_NAME, "missing_father_gt").increment(1);
                            fatherGtStr = defaultGenotype;
                        }
                    }
                    String motherGtStr;
                    if (mother == MISSING_SAMPLE) {
                        motherGtStr = null;
                    } else {
                        motherGtStr = gtMap.get(mother);
                        if (motherGtStr == null) {
                            context.getCounter(COUNTER_GROUP_NAME, "missing_mother_gt").increment(1);
                            motherGtStr = defaultGenotype;
                        }
                    }
                    String childGtStr = gtMap.getOrDefault(child, "0/0");
                    builder.addParents(childGtStr, fatherGtStr, motherGtStr);
                    int idx = genotypeCount.get(child).merge(childGtStr, 1, Integer::sum) - 1;

                    computeMendelianError(variant, father, mother, fatherGtStr, motherGtStr, childGtStr, context, builder, idx);
                } else {
                    if (fatherDiscrepancies == null) {
                        fatherDiscrepancies = Collections.singleton(father == MISSING_SAMPLE ? null
                                : gtMap.getOrDefault(father, defaultGenotype));
                    }
                    if (motherDiscrepancies == null) {
                        motherDiscrepancies = Collections.singleton(mother == MISSING_SAMPLE ? null
                                : gtMap.getOrDefault(mother, defaultGenotype));
                    }
                    if (childDiscrepancies == null) {
                        childDiscrepancies = Collections.singleton(gtMap.getOrDefault(child, "0/0"));
                    }

//                    System.out.println("variant = " + variant);
//                    System.out.println("trio = " + trio);
//                    System.out.println("child = " + child);
//                    System.out.println("father = " + father);
//                    System.out.println("mother = " + mother);
//                    System.out.println("childDiscrepancies = " + childDiscrepancies);
//                    System.out.println("motherDiscrepancies = " + motherDiscrepancies);
//                    System.out.println("fatherDiscrepancies = " + fatherDiscrepancies);
                    for (String childGtStr : childDiscrepancies) {
                        int idx = genotypeCount.get(child).merge(childGtStr, 1, Integer::sum) - 1;
                        builder.addParents(childGtStr, fatherDiscrepancies, motherDiscrepancies);
//                        System.out.println("childGtStr = " + childGtStr);

                        for (String fatherGtStr : fatherDiscrepancies) {
                            for (String motherGtStr : motherDiscrepancies) {
                                computeMendelianError(variant, father, mother, fatherGtStr, motherGtStr, childGtStr, context, builder, idx);
                            }
                        }
                    }
                }
            }
        }

        private void computeMendelianError(Variant variant,
                                           Integer father, Integer mother,
                                           String fatherGtStr, String motherGtStr, String childGtStr,
                                           Context context,
                                           FamilyIndexPutBuilder builder, int idx) throws IOException {
            if ((fatherGtStr != null || father == MISSING_SAMPLE)
                    && (motherGtStr != null || mother == MISSING_SAMPLE) && childGtStr != null) {
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
                    return;
                }
                Integer me = MendelianError.compute(fatherGt, motherGt, childGt, variant.getChromosome());
                context.getCounter(COUNTER_GROUP_NAME, "me_" + me).increment(1);

                builder.addMendelianError(variant, childGtStr, idx, me);
            }
        }

        @Override
        public void flush(Context context, String chromosome, int position) throws IOException, InterruptedException {
            for (FamilyIndexPutBuilder builder : familyIndexBuilder.values()) {
                Put put = builder.buildAndReset(chromosome, position);
                if (put != null) {
                    context.write(new ImmutableBytesWritable(put.getRow()), put);
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
