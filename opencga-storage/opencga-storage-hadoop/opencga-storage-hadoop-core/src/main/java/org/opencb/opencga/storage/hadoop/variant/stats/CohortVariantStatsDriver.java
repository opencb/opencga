package org.opencb.opencga.storage.hadoop.variant.stats;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.metadata.VariantSetStats;
import org.opencb.biodata.tools.variant.stats.VariantSetStatsCalculator;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.utils.AvroWritable;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.VariantTableAggregationDriver;
import org.opencb.opencga.storage.hadoop.variant.converters.VariantRow;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.HBaseToVariantAnnotationConverter;
import org.opencb.opencga.storage.hadoop.variant.gaps.VariantOverlappingStatus;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantRowMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.*;

public class CohortVariantStatsDriver extends VariantTableAggregationDriver {

    public static final String SAMPLES = "samples";
    public static final String COHORT = "cohort";
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String NUM_SAMPLES = "numSamples";
    private static final String NUM_FILES = "numFiles";
    private Query query;
    private QueryOptions queryOptions;
    private int numSamples;
    private int numFiles;
    private Set<Integer> fileIds;

    @Override
    protected Map<String, String> getParams() {
        Map<String, String> params = super.getParams();
        params.put(SAMPLES, "<samples>");
        params.put(COHORT, "<cohort>");

        return params;
    }

    @Override
    protected void parseAndValidateParameters() throws IOException {
        super.parseAndValidateParameters();

        String samples = getParam(SAMPLES);
        String cohort = getParam(COHORT);

        if (samples == null && cohort == null) {
            throw new IllegalArgumentException("Expected param " + SAMPLES + " or " + COHORT);
        }
        if (samples != null && cohort != null) {
            throw new IllegalArgumentException("Expected param " + SAMPLES + " or " + COHORT + " , not both");
        }

        VariantStorageMetadataManager metadataManager = getMetadataManager();
        List<Integer> sampleIds;
        if (samples != null) {
            sampleIds = metadataManager.getSampleIds(getStudyId(), samples);
        } else {
            CohortMetadata cohortMetadata = metadataManager.getCohortMetadata(getStudyId(), cohort);
            if (cohortMetadata == null) {
                throw VariantQueryException.cohortNotFound(cohort, getStudyId(), metadataManager);
            }
            sampleIds = cohortMetadata.getSamples();
        }
        numSamples = sampleIds.size();

        fileIds = metadataManager.getFileIdsFromSampleIds(getStudyId(), sampleIds);
        numFiles = fileIds.size();

        query = new Query(VariantMapReduceUtil.getQueryFromConfig(getConf()))
                .append(VariantQueryParam.STUDY.key(), getStudyId());
        if (numFiles < 100) {
            query.append(VariantQueryParam.FILE.key(), fileIds);
        } else {
            query.append(VariantQueryParam.INCLUDE_FILE.key(), fileIds);
        }
        query.remove(VariantQueryParam.COHORT.key());

        logger.info("Compute cohort variant stats for {} samples from {} files", numSamples, numFiles);

        queryOptions = new QueryOptions(QueryOptions.EXCLUDE, Arrays.asList(VariantField.STUDIES_SAMPLES, VariantField.STUDIES_STATS));
    }

    @Override
    protected Job setupJob(Job job, String archiveTable, String variantTable) throws IOException {
        super.setupJob(job, archiveTable, variantTable);

        job.getConfiguration().setInt(NUM_SAMPLES, numSamples);
        job.getConfiguration().setInt(NUM_FILES, numFiles);
        setFiles(job.getConfiguration(), fileIds);

        return job;
    }

    @Override
    protected Query getQuery() {
        return query;
    }

    @Override
    protected QueryOptions getQueryOptions() {
        return queryOptions;
    }

    @Override
    protected Class<? extends VariantRowMapper> getMapperClass() {
        return CohortVariantStatsMapper.class;
    }

    @Override
    protected String getJobOperationName() {
        return "CohortVariantStats";
    }

    @Override
    protected Class<?> getMapOutputKeyClass() {
        return NullWritable.class;
    }

    @Override
    protected Class<?> getMapOutputValueClass() {
        return CohortVariantStatsWritable.class;
    }

    @Override
    protected Class<? extends Reducer> getCombinerClass() {
        return CohortVariantStatsCombiner.class;
    }

    @Override
    protected Class<? extends Reducer> getReducerClass() {
        return CohortVariantStatsReducer.class;
    }

    @Override
    protected Class<?> getOutputKeyClass() {
        return NullWritable.class;
    }

    @Override
    protected Class<?> getOutputValueClass() {
        return Text.class;
    }

    @Override
    protected int getNumReduceTasks() {
        return 1;
    }

    @Override
    public boolean isOutputWithHeaders() {
        return true;
    }

    public static class CohortVariantStatsWritable extends AvroWritable<VariantSetStats> {
        protected int transitionsCount;
        protected int transversionsCount;
        protected double qualCount;
        protected double qualSum;
        protected double qualSumSq;

        public CohortVariantStatsWritable() {
            super(VariantSetStats.class);
        }

        public CohortVariantStatsWritable(VariantSetStats stats,
                                          int transitionsCount, int transversionsCount,
                                          double qualCount, double qualSum, double qualSumSq) {
            this();
            setValue(stats);
            this.transitionsCount = transitionsCount;
            this.transversionsCount = transversionsCount;
            this.qualCount = qualCount;
            this.qualSum = qualSum;
            this.qualSumSq = qualSumSq;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            super.write(dataOutput);
            dataOutput.writeInt(transitionsCount);
            dataOutput.writeInt(transversionsCount);
            dataOutput.writeDouble(qualCount);
            dataOutput.writeDouble(qualSum);
            dataOutput.writeDouble(qualSumSq);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            super.readFields(dataInput);
            transitionsCount = dataInput.readInt();
            transversionsCount = dataInput.readInt();
            qualCount = dataInput.readDouble();
            qualSum = dataInput.readDouble();
            qualSumSq = dataInput.readDouble();
        }

        public void merge(CohortVariantStatsWritable other) {
            if (this.value == null) {
                value = VariantSetStats.newBuilder(other.value).build();
            } else {
                VariantSetStatsCalculator.merge(this.value, other.value);
            }

            this.transitionsCount += other.transitionsCount;
            this.transversionsCount += other.transversionsCount;
            this.qualCount += other.qualCount;
            this.qualSum += other.qualSum;
            this.qualSumSq += other.qualSumSq;
        }
    }

    public static class ExposedVariantSetStatsCalculator extends VariantSetStatsCalculator {

        public ExposedVariantSetStatsCalculator(String studyId, int numFiles, int numSamples, Map<String, Long> chrLengthMap) {
            super(studyId, numFiles, numSamples, chrLengthMap);
        }

        public int getTransitionsCount() {
            return transitionsCount;
        }

        public ExposedVariantSetStatsCalculator setTransitionsCount(int transitionsCount) {
            this.transitionsCount = transitionsCount;
            return this;
        }

        public int getTransversionsCount() {
            return transversionsCount;
        }

        public ExposedVariantSetStatsCalculator setTransversionsCount(int transversionsCount) {
            this.transversionsCount = transversionsCount;
            return this;
        }

        public double getQualCount() {
            return qualCount;
        }

        public ExposedVariantSetStatsCalculator setQualCount(double qualCount) {
            this.qualCount = qualCount;
            return this;
        }

        public double getQualSum() {
            return qualSum;
        }

        public ExposedVariantSetStatsCalculator setQualSum(double qualSum) {
            this.qualSum = qualSum;
            return this;
        }

        public double getQualSumSq() {
            return qualSumSq;
        }

        public ExposedVariantSetStatsCalculator setQualSumSq(double qualSumSq) {
            this.qualSumSq = qualSumSq;
            return this;
        }

        public ExposedVariantSetStatsCalculator setStats(VariantSetStats value) {
            stats = value;
            return this;
        }
    }

    public static class CohortVariantStatsMapper
            extends VariantRowMapper<NullWritable, CohortVariantStatsWritable> {

        private String study;
        private ExposedVariantSetStatsCalculator calculator;
        private Set<Integer> fileIds;
        private HBaseToVariantAnnotationConverter converter;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            converter = new HBaseToVariantAnnotationConverter();
            int numSamples = context.getConfiguration().getInt(NUM_SAMPLES, 0);
            study = String.valueOf(getStudyId());
            fileIds = new HashSet<>(getFiles(context.getConfiguration()));
            calculator = new ExposedVariantSetStatsCalculator(study, fileIds.size(), numSamples, null);
        }

        @Override
        protected void map(Object key, VariantRow row, Context context) throws IOException, InterruptedException {

            List<FileEntry> entries = new ArrayList<>();
            StudyEntry studyEntry = new StudyEntry(study)
                    .setFiles(entries);

            Variant variant = row.walker().onFile(fileColumn -> {
                if (fileIds.contains(fileColumn.getFileId())) {
                    if (fileColumn.getOverlappingStatus().equals(VariantOverlappingStatus.NONE)) {
                        HashMap<String, String> attributes = new HashMap<>(2);
                        attributes.put(StudyEntry.QUAL, fileColumn.getQualString());
                        attributes.put(StudyEntry.FILTER, fileColumn.getFilter());
                        entries.add(new FileEntry(String.valueOf(fileColumn.getFileId()), fileColumn.getCall(), attributes));
                    }
                }
            }).walk();

            variant.setStudies(Collections.singletonList(studyEntry));
            variant.setAnnotation(row.getVariantAnnotation(converter));

            context.getCounter(COUNTER_GROUP_NAME, "hbase_rows").increment(1);
            if (entries.isEmpty()) {
                context.getCounter(COUNTER_GROUP_NAME, "skip_variants").increment(1);
            } else {
                context.getCounter(COUNTER_GROUP_NAME, "variants").increment(1);
                calculator.apply(Collections.singletonList(variant));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            CohortVariantStatsWritable cohortVariantStatsWritable = new CohortVariantStatsWritable(
                    calculator.getStats(),
                    calculator.getTransitionsCount(),
                    calculator.getTransversionsCount(),
                    calculator.getQualCount(),
                    calculator.getQualSum(),
                    calculator.getQualSumSq()
            );

            context.write(NullWritable.get(), cohortVariantStatsWritable);
        }
    }

    public static class CohortVariantStatsCombiner
            extends Reducer<NullWritable, CohortVariantStatsWritable, NullWritable, CohortVariantStatsWritable> {


        @Override
        protected void reduce(NullWritable n, Iterable<CohortVariantStatsWritable> values, Context context)
                throws IOException, InterruptedException {
            // Create a new instance, as the input values may be overwritten
            CohortVariantStatsWritable stats = new CohortVariantStatsWritable();

            for (CohortVariantStatsWritable value : values) {
                stats.merge(value);
            }

            context.write(n, stats);
        }

    }

    public static class CohortVariantStatsReducer
            extends Reducer<NullWritable, CohortVariantStatsWritable, NullWritable, Text> {

        @Override
        protected void reduce(NullWritable n, Iterable<CohortVariantStatsWritable> values, Context context)
                throws IOException, InterruptedException {
            // Create a new instance, as the input values may be overwritten
            CohortVariantStatsWritable stats = new CohortVariantStatsWritable();

            for (CohortVariantStatsWritable value : values) {
                stats.merge(value);
            }

            int numSamples = context.getConfiguration().getInt(NUM_SAMPLES, 0);
            int numFiles = context.getConfiguration().getInt(NUM_FILES, 0);
            Map<String, Long> chrLengthMap = null;

            ExposedVariantSetStatsCalculator calculator = new ExposedVariantSetStatsCalculator("", numFiles, numSamples, chrLengthMap)
                    .setTransitionsCount(stats.transitionsCount)
                    .setTransversionsCount(stats.transversionsCount)
                    .setQualCount(stats.qualCount)
                    .setQualSum(stats.qualSum)
                    .setQualSumSq(stats.qualSumSq)
                    .setStats(stats.getValue());

            calculator.post();

            context.write(n, new Text(stats.getValue().toString()));
        }
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        main(args, (Class<? extends AbstractVariantsTableDriver>) MethodHandles.lookup().lookupClass());
    }
}
