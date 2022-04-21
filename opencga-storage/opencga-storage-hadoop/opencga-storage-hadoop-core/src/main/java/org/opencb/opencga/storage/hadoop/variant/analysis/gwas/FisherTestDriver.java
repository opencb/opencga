package org.opencb.opencga.storage.hadoop.variant.analysis.gwas;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.converters.VariantRow;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.HBaseToVariantAnnotationConverter;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantRowMapper;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantTableHelper;
import org.opencb.opencga.storage.hadoop.variant.stats.HBaseVariantStatsCalculator;
import org.opencb.oskar.analysis.stats.FisherExactTest;
import org.opencb.oskar.analysis.stats.FisherTestResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil.getQueryFromConfig;
import static org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil.getQueryOptionsFromConfig;

public class FisherTestDriver extends AbstractVariantsTableDriver {
    private final Logger logger = LoggerFactory.getLogger(FisherTestDriver.class);

    // Output directory within DFS
    public static final String OUTPUT = "output";
//    // Move to local directory (remove from DFS)
//    public static final String MOVE_TO_LOCAL = "move-to-local";
    public static final String CASE_COHORT = "caseCohort";
    public static final String CONTROL_COHORT = "controlCohort";
//    public static final String DELETE_COHORTS = "deleteCohorts";

    private static final String CASE_COHORT_IDS = "caseCohortIds";
    private static final String CONTROL_COHORT_IDS = "controlCohortIds";

    private Integer caseCohortId;
    private Integer controlCohortId;
    private List<Integer> caseCohort;
    private List<Integer> controlCohort;
    private Path outdir;
    private Path localOutput;
    private Query query;
    private QueryOptions queryOptions;

    @Override
    protected Class<FisherTestMapper> getMapperClass() {
        return FisherTestMapper.class;
    }

    @Override
    protected Map<String, String> getParams() {
        Map<String, String> params = new HashMap<>();
        params.put("--" + VariantStorageOptions.STUDY.key(), "<study>*");
        params.put("--" + OUTPUT, "<output>*");
        params.put("--" + CASE_COHORT, "<case-cohort>*");
        params.put("--" + CONTROL_COHORT, "<control-cohort>*");
//        params.put("--" + MOVE_TO_LOCAL, "<local-output>");
//        params.put("--" + DELETE_COHORTS, "<true|false>");
        params.put("--" + VariantQueryParam.REGION.key(), "<region>");
        return params;
    }

    @Override
    protected void parseAndValidateParameters() throws IOException {
        super.parseAndValidateParameters();

        String caseCohortStr = getParam(CASE_COHORT);
        String controlCohortStr = getParam(CONTROL_COHORT);

        Pair<Integer, List<Integer>> pair = parseCohort(caseCohortStr, "Case cohort");
        caseCohortId = pair.getKey();
        caseCohort = pair.getValue();

        pair = parseCohort(controlCohortStr, "Control cohort");
        controlCohortId = pair.getKey();
        controlCohort = pair.getValue();

        List<Integer> commonSamples = new LinkedList<>();
        for (Integer s : caseCohort) {
            if (controlCohort.contains(s)) {
                commonSamples.add(s);
            }
        }
        if (!commonSamples.isEmpty()) {
            VariantStorageMetadataManager metadataManager = getMetadataManager();
            String msg = "Case cohort and Control cohort have " + commonSamples.size() + " samples in common: "
                    + commonSamples.stream()
                            .map(s -> metadataManager.getSampleName(getStudyId(), s))
                            .collect(Collectors.joining(", ", "[", "]"));
            throw new IllegalArgumentException(msg);
        }


        query = getQueryFromConfig(getConf());
        queryOptions = getQueryOptionsFromConfig(getConf());

        String region = query.getString(VariantQueryParam.REGION.key());
        if (StringUtils.isNotEmpty(region)) {
            logger.info(" * Calculate fisher test for region " + region);
        }

        ArrayList<Integer> includeSample = new ArrayList<>(controlCohort.size() + caseCohort.size());
        includeSample.addAll(controlCohort);
        includeSample.addAll(caseCohort);
        query.append(VariantQueryParam.INCLUDE_SAMPLE.key(), includeSample);
        queryOptions.remove(QueryOptions.INCLUDE);
        queryOptions.put(QueryOptions.EXCLUDE,
                Arrays.asList(
                        VariantField.STUDIES_FILES,
                        VariantField.STUDIES_SECONDARY_ALTERNATES,
                        VariantField.STUDIES_STATS));

        String outdirStr = getConf().get(OUTPUT);
        if (StringUtils.isEmpty(outdirStr)) {
            outdir = new Path("fisher." + TimeUtils.getTime() + ".tsv");
        } else {
            outdir = new Path(outdirStr);
            if (isLocal(outdir)) {
                localOutput = getLocalOutput(outdir, () -> "fisher_test." + TimeUtils.getTime() + ".tsv.gz");
                outdir = getTempOutdir("opencga_fisher_test", "." + localOutput.getName());
                outdir.getFileSystem(getConf()).deleteOnExit(outdir);
            }
            if (localOutput != null) {
                logger.info(" * Outdir file: " + localOutput.toUri());
                logger.info(" * Temporary outdir file: " + outdir.toUri());
            } else {
                logger.info(" * Outdir file: " + outdir.toUri());
            }
        }
    }

    private Pair<Integer, List<Integer>> parseCohort(String cohortStr, String cohortDescription) throws IOException {
        VariantStorageMetadataManager metadataManager = getMetadataManager();
        int studyId = getStudyId();
        Integer cohortId;
        List<Integer> cohort;
        if (cohortStr.contains(",")) {
            cohortId = null;
            String[] samples = cohortStr.split(",");
            cohort = new ArrayList<>(samples.length);
            for (String sample : samples) {
                Integer sampleId = metadataManager.getSampleId(studyId, sample);
                if (sampleId == null) {
                    throw VariantQueryException.sampleNotFound(sample, studyId);
                }
                cohort.add(sampleId);
            }
        } else {
            cohortId = metadataManager.getCohortId(studyId, cohortStr);
            if (cohortId == null) {
                throw VariantQueryException.cohortNotFound(cohortStr, studyId, metadataManager);
            }
            cohort = metadataManager.getCohortMetadata(studyId, cohortId).getSamples();
        }


        if (cohortId == null) {
            logger.info(" * " + cohortDescription + " with " + cohort.size() + " samples");
        } else {
            logger.info(" * " + cohortDescription + " '" + metadataManager.getCohortName(getStudyId(), cohortId) + "' with "
                    + cohort.size() + " samples");
        }

        return Pair.of(cohortId, cohort);
    }

    @Override
    protected Job setupJob(Job job, String archiveTable, String variantTable) throws IOException {

        VariantStorageMetadataManager metadataManager = getMetadataManager();
        VariantMapReduceUtil.initVariantRowMapperJob(job, FisherTestMapper.class, variantTable, metadataManager, query, queryOptions, true);

        job.getConfiguration().set(CASE_COHORT_IDS, caseCohort.stream().map(Objects::toString).collect(Collectors.joining(",")));
        job.getConfiguration().set(CONTROL_COHORT_IDS, controlCohort.stream().map(Objects::toString).collect(Collectors.joining(",")));

        job.setOutputFormatClass(TextOutputFormat.class);
        if (outdir.toString().toLowerCase().endsWith(".gz")) {
            TextOutputFormat.setCompressOutput(job, true);
            TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        }
        TextOutputFormat.setOutputPath(job, outdir);

        job.setReducerClass(FisherTestReducer.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);
        VariantMapReduceUtil.setNoneTimestamp(job);
        return job;
    }

    @Override
    protected String getJobOperationName() {
        return "fisher_test";
    }

    @Override
    protected void postExecution(boolean succeed) throws IOException, StorageEngineException {
        super.postExecution(succeed);
        if (succeed) {
            if (localOutput != null) {
                concatMrOutputToLocal(outdir, localOutput);
            }
        }
        if (localOutput != null) {
            deleteTemporaryFile(outdir);
        }
    }

    public static class FisherTestMapper  extends VariantRowMapper<NullWritable, Text> {
        protected HBaseVariantStatsCalculator caseCohortCalculator;
        protected HBaseVariantStatsCalculator controlCohortCalculator;
        protected HBaseToVariantAnnotationConverter converter;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            Configuration configuration = context.getConfiguration();

            List<Integer> caseCohortIds = Arrays.stream(configuration.getInts(CASE_COHORT_IDS)).boxed().collect(Collectors.toList());
            List<Integer> controlCohortIds = Arrays.stream(configuration.getInts(CONTROL_COHORT_IDS)).boxed().collect(Collectors.toList());

            VariantStorageMetadataManager metadataManager = getMetadataManager();
            StudyMetadata studyMetadata = getStudyMetadata();

            caseCohortCalculator = new HBaseVariantStatsCalculator(
                    metadataManager, studyMetadata, caseCohortIds, false, "0/0");
            controlCohortCalculator = new HBaseVariantStatsCalculator(
                    metadataManager, studyMetadata, controlCohortIds, false, "0/0");
            converter = new HBaseToVariantAnnotationConverter();
        }

        @Override
        protected void map(Object key, VariantRow result, Context context) throws IOException, InterruptedException {
            VariantStats caseStats = caseCohortCalculator.apply(result);
            VariantStats controlStats = controlCohortCalculator.apply(result);
            Variant variant = result.getVariant();

            int a = caseStats.getRefAlleleCount(); // case #REF
            int b = controlStats.getRefAlleleCount(); // control #REF
            int c = caseStats.getAltAlleleCount(); // case #ALT
            int d = controlStats.getAltAlleleCount(); // control #ALT

            if (a + b + c + d == 0) {
                context.getCounter(COUNTER_GROUP_NAME, "Empty variant").increment(1);
            } else {
                context.getCounter(COUNTER_GROUP_NAME, "Variant").increment(1);
                FisherTestResult fisherTestResult = new FisherExactTest().fisherTest(a, b, c, d);

                VariantAnnotation variantAnnotation = result.getVariantAnnotation(converter);
                String id = null;
                Set<String> genes = Collections.emptySet();
                if (variantAnnotation != null) {
                    id = variantAnnotation.getId();
                    genes = new HashSet<>();
                    if (variantAnnotation.getConsequenceTypes() != null) {
                        for (ConsequenceType consequenceType : variantAnnotation.getConsequenceTypes()) {
                            if (StringUtils.isNotEmpty(consequenceType.getGeneName())) {
                                genes.add(consequenceType.getGeneName());
                            }
                        }
                    }
                }
                if (StringUtils.isEmpty(id)) {
                    id = variant.toString();
                }
                if (genes.isEmpty()) {
                    genes = Collections.singleton(".");
                }

                context.write(NullWritable.get(), tsv(
                        id,
                        variant.toString(),
                        variant.getChromosome(), variant.getStart(), variant.getReference(), variant.getAlternate(),
                        String.join(",", genes),
                        a, b, c, d,
                        caseStats.getAlleleCount(),
                        controlStats.getAlleleCount(),
                        caseStats.getAltAlleleFreq(),
                        controlStats.getAltAlleleFreq(),
                        fisherTestResult.getpValue(),
                        fisherTestResult.getOddRatio()
                ));
            }
        }
    }

    public static class FisherTestReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Just add header

            VariantTableHelper helper = new VariantTableHelper(context.getConfiguration());
            int[] ints = context.getConfiguration().getInts(CASE_COHORT_IDS);
            List<Integer> caseCohortIds = new ArrayList<>(ints.length);
            for (int sampleId : ints) {
                caseCohortIds.add(sampleId);
            }
            ints = context.getConfiguration().getInts(CONTROL_COHORT_IDS);
            List<Integer> controlCohortIds = new ArrayList<>(ints.length);
            for (int sampleId : ints) {
                controlCohortIds.add(sampleId);
            }

            try (VariantStorageMetadataManager metadataManager = new VariantStorageMetadataManager(
                    new HBaseVariantStorageMetadataDBAdaptorFactory(helper))) {
                StudyMetadata studyMetadata = metadataManager.getStudyMetadata(helper.getStudyId());
                ProjectMetadata projectMetadata = metadataManager.getProjectMetadata();


                context.write(NullWritable.get(),
                        tsv("##GWAS=Fisher_test"));
                context.write(NullWritable.get(),
                        tsv("##date=" + new Date().toString()));
                context.write(NullWritable.get(),
                        tsv("##study=" + studyMetadata.getName()));
                context.write(NullWritable.get(),
                        tsv("##species=" + projectMetadata.getSpecies()));
                context.write(NullWritable.get(),
                        tsv("##assembly=" + projectMetadata.getAssembly()));
                context.write(NullWritable.get(),
                        tsv("##case=" + caseCohortIds.stream()
                                .map(id -> metadataManager.getSampleName(studyMetadata.getId(), id)).collect(Collectors.joining(","))));
                context.write(NullWritable.get(),
                        tsv("##control=" + controlCohortIds.stream()
                                .map(id -> metadataManager.getSampleName(studyMetadata.getId(), id)).collect(Collectors.joining(","))));
                context.write(NullWritable.get(),
                        tsv("#ID",
                                "VAR",
                                "CHROM",
                                "POS",
                                "REF",
                                "ALT",
                                "GENES",
                                "REF_CASE(a)",
                                "REF_CONTROL(b)",
                                "ALT_CASE(c)",
                                "ALT_CONTROL(d)",
                                "AN_CASE",
                                "AN_CONTROL",
                                "AF_CASE",
                                "AF_CONTROL",
                                "P_VALUE",
                                "ODD_RATIO"
                        ));
                }
        }
    }

    private static Text tsv(Object... objects) {
        StringJoiner joiner = new StringJoiner("\t");
        for (Object object : objects) {
            final String toString;
            if (object instanceof Double) {
                if (((Double) object).isNaN() || ((Double) object).isInfinite()) {
                    toString = "NA";
                } else {
                    toString = object.toString();
                }
            } else {
                toString = object.toString();
            }
            joiner.add(toString);
        }
        return new Text(joiner.toString());
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        main(args, (Class<? extends AbstractVariantsTableDriver>) MethodHandles.lookup().lookupClass());
    }
}
