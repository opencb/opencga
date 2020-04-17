package org.opencb.opencga.storage.hadoop.variant.stats;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.opencb.biodata.models.clinical.pedigree.Member;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
import org.opencb.biodata.tools.variant.stats.SampleVariantStatsCalculator;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.utils.AvroWritable;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.VariantTableAggregationDriver;
import org.opencb.opencga.storage.hadoop.variant.converters.VariantRow;
import org.opencb.opencga.storage.hadoop.variant.gaps.VariantOverlappingStatus;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantRowMapper;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantsTableMapReduceHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine.STUDY_ID;

public class SampleVariantStatsDriver extends VariantTableAggregationDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final String SAMPLES = "samples";
//    public static final String STATS_PARTIAL_RESULTS = "stats.partial-results";
    //    public static final boolean STATS_PARTIAL_RESULTS_DEFAULT = true;
    private static final String TRIOS = "trios";
    private static final String WRITE_TO_DISK = "write";
    private static final String STATS_OPERATION_NAME = "sample_stats";
    private List<Integer> sampleIds;
    private String trios;

    @Override
    protected Map<String, String> getParams() {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("--" + VariantStorageOptions.STUDY.key(), "<study>*");
        params.put("--" + SAMPLES, "<samples|all|auto>*");
        params.put("--" + OUTPUT, "<output>");
        return params;
    }

    @Override
    protected void parseAndValidateParameters() throws IOException {
        super.parseAndValidateParameters();

        VariantStorageMetadataManager metadataManager = getMetadataManager();
        int studyId = getStudyId();

        String samplesStr = getParam(SAMPLES);
        if (StringUtils.isEmpty(samplesStr)) {
            throw new IllegalArgumentException("Missing samples");
        }

        List<String> samples = Arrays.asList(samplesStr.split(","));
        StringBuilder trios = new StringBuilder();
        Set<Integer> includeSample = new LinkedHashSet<>();
        if (samples.size() == 1 && (samples.get(0).equals("auto") || samples.get(0).equals("all"))) {
            boolean all = samples.get(0).equals("all");
            metadataManager.sampleMetadataIterator(studyId).forEachRemaining(sampleMetadata -> {
                if (sampleMetadata.isIndexed()) {
                    if (all || sampleMetadata.getStats() == null || MapUtils.isEmpty(sampleMetadata.getStats().getBiotypeCount())) {
                        addTrio(trios, includeSample, sampleMetadata);
                    }
                }
            });
        } else {
            for (String sample : samples) {
                Integer sampleId = metadataManager.getSampleId(studyId, sample);
                if (sampleId == null) {
                    throw VariantQueryException.sampleNotFound(sample, metadataManager.getStudyName(studyId));
                }
                addTrio(trios, includeSample, metadataManager.getSampleMetadata(studyId, sampleId));
            }
        }
        sampleIds = new ArrayList<>(includeSample);
        if (sampleIds.isEmpty()) {
            throw new IllegalArgumentException("Nothing to do!");
        }
        this.trios = trios.toString();

    }


    private void addTrio(StringBuilder trios, Set<Integer> includeSample, SampleMetadata sampleMetadata) {
        includeSample.add(sampleMetadata.getId());
        if (sampleMetadata.getFather() != null || sampleMetadata.getMother() != null) {
            // Make sure parents are included in the query
            if (sampleMetadata.getFather() != null) {
                includeSample.add(sampleMetadata.getFather());
            }
            if (sampleMetadata.getMother() != null) {
                includeSample.add(sampleMetadata.getMother());
            }
            trios.append(sampleMetadata.getId())
                    .append(",")
                    .append(sampleMetadata.getFather() == null ? "0" : sampleMetadata.getFather())
                    .append(",")
                    .append(sampleMetadata.getMother() == null ? "0" : sampleMetadata.getMother())
                    .append(";");
        }
    }

    private static Pedigree readPedigree(Configuration conf) {
        String[] trios = conf.get(TRIOS, "").split(";");
        Pedigree pedigree = new Pedigree("", new ArrayList<>(), null);
        for (String trio : trios) {
            if (trio.isEmpty()) {
                continue;
            }
            String[] members = trio.split(",");
            Member member = new Member(members[0], Member.Sex.UNKNOWN);
            if (!members[1].equals("0")) {
                member.setFather(new Member(members[1], Member.Sex.MALE));
            }
            if (!members[2].equals("0")) {
                member.setMother(new Member(members[2], Member.Sex.FEMALE));
            }
            pedigree.getMembers().add(member);
        }


        return pedigree;
    }

    @Override
    protected Query getQuery() {
        return new Query()
                .append(VariantQueryParam.STUDY.key(), getStudyId())
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), sampleIds);
    }

    @Override
    protected QueryOptions getQueryOptions() {
        return new QueryOptions(QueryOptions.EXCLUDE, VariantField.STUDIES_STATS);
    }

    @Override
    protected Class<? extends VariantRowMapper> getMapperClass() {
        return SampleVariantStatsMapper.class;
    }

    @Override
    protected Class<?> getMapOutputKeyClass() {
        return IntWritable.class;
    }

    @Override
    protected Class<?> getMapOutputValueClass() {
        return SampleVariantStatsWritable.class;
    }

    @Override
    protected Class<? extends Reducer> getCombinerClass() {
        return SampleVariantStatsCombiner.class;
    }

    @Override
    protected Class<? extends Reducer> getReducerClass() {
        return SampleVariantStatsReducer.class;
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
        return Math.min(sampleIds.size(), 10);
    }

    @Override
    protected String generateOutputFileName() {
        return "sample_variant_stats." + TimeUtils.getTime() + ".json";
    }

    @Override
    protected Job setupJob(Job job, String archiveTable, String variantTable) throws IOException {
        super.setupJob(job, archiveTable, variantTable);
        job.getConfiguration().set(SAMPLES, sampleIds.stream().map(Objects::toString).collect(Collectors.joining(",")));
        job.getConfiguration().setInt(STUDY_ID, getStudyId());
        job.getConfiguration().set(TRIOS, trios);
        if (outdir != null) {
            job.getConfiguration().setBoolean(WRITE_TO_DISK, true);
        }
        return job;
    }

    @Override
    protected String getJobOperationName() {
        return STATS_OPERATION_NAME;
    }

    public static class SampleVariantStatsWritable extends AvroWritable<SampleVariantStats> {
        private int sampleId;
        private int ti;
        private int tv;
        private int qualCount;
        private double qualSum;
        private double qualSumSq;


        public SampleVariantStatsWritable() {
            super(SampleVariantStats.class);
        }

        public SampleVariantStatsWritable(int sampleId) {
            this();
            this.sampleId = sampleId;
        }

        public SampleVariantStatsWritable(int sampleId, int ti, int tv, int qualCount, double qualSum, double qualSumSq,
                                          SampleVariantStats sampleStats) {
            this();
            this.sampleId = sampleId;
            this.ti = ti;
            this.tv = tv;
            this.qualCount = qualCount;
            this.qualSum = qualSum;
            this.qualSumSq = qualSumSq;
            this.value = sampleStats;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(sampleId);
            out.writeInt(ti);
            out.writeInt(tv);
            out.writeInt(qualCount);
            out.writeDouble(qualSum);
            out.writeDouble(qualSumSq);
            writeAvro(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            sampleId = in.readInt();
            ti = in.readInt();
            tv = in.readInt();
            qualCount = in.readInt();
            qualSum = in.readDouble();
            qualSumSq = in.readDouble();
            readAvro(in);
        }

        public void merge(SampleVariantStatsWritable other) {
            if (value == null) {
                value = SampleVariantStats.newBuilder(other.value).build();
            } else {
                SampleVariantStatsCalculator.merge(value, other.value);
            }

            this.ti += other.ti;
            this.tv += other.tv;
            this.qualCount += other.qualCount;
            this.qualSum += other.qualSum;
            this.qualSumSq += other.qualSumSq;
        }

    }

    public static class DistributedSampleVariantStatsCalculator extends SampleVariantStatsCalculator {

        private int[] sampleIds;

        public DistributedSampleVariantStatsCalculator(Pedigree pedigree, int[] samples) {
            super(pedigree, IntStream.of(samples).mapToObj(String::valueOf).collect(Collectors.toList()));
            sampleIds = samples;
        }

        public DistributedSampleVariantStatsCalculator(SampleVariantStatsWritable statsWritable) {
            super(null, Collections.singletonList(statsWritable.getValue().getId()));

            statsList = Collections.singletonList(statsWritable.getValue());
            ti = new int[]{statsWritable.ti};
            tv = new int[]{statsWritable.tv};
            qualCount = new int[]{statsWritable.qualCount};
            qualSum = new double[]{statsWritable.qualSum};
            qualSumSq = new double[]{statsWritable.qualSumSq};
        }

        public List<SampleVariantStatsWritable> getWritables() {
            List<SampleVariantStatsWritable> writables = new ArrayList<>(statsList.size());
            for (int i = 0; i < statsList.size(); i++) {
                writables.add(new SampleVariantStatsWritable(
                        sampleIds[i], ti[i], tv[i], qualCount[i], qualSum[i], qualSumSq[i], statsList.get(i)));
            }
            return writables;
        }
    }

    public static class SampleVariantStatsMapper extends VariantRowMapper<IntWritable, SampleVariantStatsWritable> {

        private int studyId;
        private int[] samples;

        protected final Logger logger = LoggerFactory.getLogger(SampleVariantStatsMapper.class);
        private VariantStorageMetadataManager vsm;
        private Map<Integer, List<Integer>> fileToSampleIds = new HashMap<>();
        private DistributedSampleVariantStatsCalculator calculator;
        private int[] sampleIdsPosition;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            studyId = context.getConfiguration().getInt(STUDY_ID, -1);
            samples = context.getConfiguration().getInts(SAMPLES);
            sampleIdsPosition = new int[IntStream.of(samples).max().orElse(0) + 1];
            for (int i = 0; i < samples.length; i++) {
                sampleIdsPosition[samples[i]] = i;
            }

            Pedigree pedigree = readPedigree(context.getConfiguration());
            calculator = new DistributedSampleVariantStatsCalculator(pedigree, samples);
            calculator.pre();

            vsm = this.getMetadataManager();
        }

        private List<Integer> getSamplesFromFileId(int fileId) {
            return fileToSampleIds.computeIfAbsent(fileId,
                    id -> new ArrayList<>(vsm.getFileMetadata(studyId, id).getSamples()));
        }

        @Override
        protected void map(Object key, VariantRow row, Context context) throws IOException, InterruptedException {
            VariantAnnotation annotation = row.getVariantAnnotation();

            List<String> gts = Arrays.asList(new String[samples.length]);
            List<String> quals = Arrays.asList(new String[samples.length]);
            List<String> filters = Arrays.asList(new String[samples.length]);

            Variant variant = row.walker().onSample(sampleCell -> {
                int sampleId = sampleCell.getSampleId();

                String gt = sampleCell.getGT();

                if (gt.isEmpty()) {
                    // This is a really weird situation, most likely due to errors in the input files
                    logger.error("Empty genotype at sample " + sampleId + " in variant " + row.getVariant());
                } else if (gt.equals(GenotypeClass.UNKNOWN_GENOTYPE)) {
                    // skip unknown genotypes
                    context.getCounter(COUNTER_GROUP_NAME, "unknownGt").increment(1);
                } else {
                    gts.set(sampleIdsPosition[sampleId], gt);
                }
            }).onFile(fileCell -> {
                int fileId = fileCell.getFileId();

                if (VariantOverlappingStatus.NONE == fileCell.getOverlappingStatus()) {
                    for (Integer sampleId : getSamplesFromFileId(fileId)) {
                        filters.set(sampleIdsPosition[sampleId], fileCell.getFilter());
                        quals.set(sampleIdsPosition[sampleId], fileCell.getQualString());
                    }
                }
            }).walk();

            calculator.update(variant, annotation, gts, quals, filters);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);

            for (SampleVariantStatsWritable writable: calculator.getWritables()) {
                context.write(new IntWritable(writable.sampleId), writable);
            }

        }
    }

    public static class SampleVariantStatsCombiner extends Reducer
            <IntWritable, SampleVariantStatsWritable, IntWritable, SampleVariantStatsWritable> {

        @Override
        protected void reduce(IntWritable sampleId, Iterable<SampleVariantStatsWritable> values, Context context)
                throws IOException, InterruptedException {
            // Create a new instance, as the input values may be overwritten
            SampleVariantStatsWritable stats = new SampleVariantStatsWritable(sampleId.get());

            for (SampleVariantStatsWritable value : values) {
                stats.merge(value);
            }

            context.write(sampleId, stats);
        }
    }

    public static class SampleVariantStatsReducer extends Reducer
            <IntWritable, SampleVariantStatsWritable, NullWritable, Text> {

        private int studyId;
        private VariantStorageMetadataManager vsm;
        private VariantsTableMapReduceHelper mrHelper;
        private boolean write;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            studyId = context.getConfiguration().getInt(STUDY_ID, -1);
            write = context.getConfiguration().getBoolean(WRITE_TO_DISK, false);

            mrHelper = new VariantsTableMapReduceHelper(context);
            vsm = mrHelper.getMetadataManager();
        }

        @Override
        protected void reduce(IntWritable sampleId, Iterable<SampleVariantStatsWritable> values, Context context)
                throws IOException, InterruptedException {
            // Create a new instance, as the input values may be overwritten
            SampleVariantStatsWritable statsWritable = new SampleVariantStatsWritable(sampleId.get());

            for (SampleVariantStatsWritable value : values) {
                statsWritable.merge(value);
            }

            DistributedSampleVariantStatsCalculator calculator = new DistributedSampleVariantStatsCalculator(statsWritable);
            calculator.post();
            SampleVariantStats stats = calculator.getSampleVariantStats().get(0);

//            stats.setMissingPositions(0);
            try {
                vsm.updateSampleMetadata(studyId, sampleId.get(), sampleMetadata -> {
                    stats.setId(sampleMetadata.getName());
                    if (sampleMetadata.getStats() == null) {
//                        stats.setMissingPositions(0); // Unknown. Unable to calculate using this MR
                        sampleMetadata.setStats(stats);
                    } else {
                        sampleMetadata.getStats().setVariantCount(stats.getVariantCount());
                        sampleMetadata.getStats().setChromosomeCount(stats.getChromosomeCount());
                        sampleMetadata.getStats().setTypeCount(stats.getTypeCount());
                        sampleMetadata.getStats().setIndelLengthCount(stats.getIndelLengthCount());
                        sampleMetadata.getStats().setTiTvRatio(stats.getTiTvRatio());

                        sampleMetadata.getStats().setGenotypeCount(stats.getGenotypeCount());
                        sampleMetadata.getStats().setFilterCount(stats.getFilterCount());
                        sampleMetadata.getStats().setQualityAvg(stats.getQualityAvg());
                        sampleMetadata.getStats().setQualityStdDev(stats.getQualityStdDev());
                        sampleMetadata.getStats().setMendelianErrorCount(stats.getMendelianErrorCount());
                        sampleMetadata.getStats().setHeterozygosityRate(stats.getHeterozygosityRate());

                        sampleMetadata.getStats().setConsequenceTypeCount(stats.getConsequenceTypeCount());
                        sampleMetadata.getStats().setBiotypeCount(stats.getBiotypeCount());

                        // Update all but missingCount!
                    }

                    return sampleMetadata;
                });

                if (write) {
                    context.getCounter(COUNTER_GROUP_NAME, "samples").increment(1);
                    context.write(NullWritable.get(), new Text(stats.toString()));
                }
            } catch (StorageEngineException e) {
                throw new IOException(e);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mrHelper.close();
        }
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        main(args, (Class<? extends AbstractVariantsTableDriver>) MethodHandles.lookup().lookupClass());
    }

}
