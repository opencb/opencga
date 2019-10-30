package org.opencb.opencga.analysis.variant;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.hamcrest.CoreMatchers;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.gwas.GwasAnalysis;
import org.opencb.opencga.analysis.variant.stats.CohortVariantStatsAnalysis;
import org.opencb.opencga.analysis.variant.stats.SampleVariantStatsAnalysis;
import org.opencb.opencga.analysis.variant.stats.VariantStatsAnalysis;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.update.SampleUpdateParams;
import org.opencb.opencga.catalog.utils.AvroToAnnotationConverter;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.analysis.storage.OpenCGATestExternalResource;
import org.opencb.opencga.analysis.storage.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.metadata.models.VariantScoreMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine;
import org.opencb.opencga.core.analysis.result.AnalysisResult;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class VariantAnalysisTest {

    public static final String USER = "user";
    public static final String PASSWORD = "asdf";
    public static final String PROJECT = "project";
    public static final String STUDY = "study";
    public static final String PHENOTYPE_NAME = "myPhenotype";
    public static final Phenotype PHENOTYPE = new Phenotype(PHENOTYPE_NAME, PHENOTYPE_NAME, "mySource")
            .setStatus(Phenotype.Status.OBSERVED);
    public static final String DB_NAME = "opencga_test_" + USER + "_" + PROJECT;

    @Parameterized.Parameters(name="{0}")
    public static Object[][] parameters() {
        return new Object[][]{
                {MongoDBVariantStorageEngine.STORAGE_ENGINE_ID},
                {HadoopVariantStorageEngine.STORAGE_ENGINE_ID}};
    }

    public VariantAnalysisTest(String storageEngine) {
        if (!storageEngine.equals(VariantAnalysisTest.storageEngine)) {
            indexed = false;
        }
        VariantAnalysisTest.storageEngine = storageEngine;
    }


    private CatalogManager catalogManager;
    private VariantStorageManager variantStorageManager;

    @ClassRule
    public static OpenCGATestExternalResource opencga = new OpenCGATestExternalResource();
    @ClassRule
    public static HadoopVariantStorageTest.HadoopExternalResource hadoopExternalResource = new HadoopVariantStorageTest.HadoopExternalResource();

    private static String storageEngine;
    private static boolean indexed = false;
    private static String sessionId;
    private static File file;

    @Before
    public void setUp() throws Exception {
        catalogManager = opencga.getCatalogManager();
        variantStorageManager = new VariantStorageManager(catalogManager, opencga.getStorageEngineFactory());

        if (!indexed) {
            indexed = true;

            opencga.after();
            opencga.before();

            catalogManager = opencga.getCatalogManager();
            variantStorageManager = new VariantStorageManager(catalogManager, opencga.getStorageEngineFactory());

            opencga.clearStorageDB(DB_NAME);

            StorageConfiguration storageConfiguration = opencga.getStorageConfiguration();
            storageConfiguration.setDefaultStorageEngineId(storageEngine);
            if (storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID)) {
                HadoopVariantStorageTest.updateStorageConfiguration(storageConfiguration, hadoopExternalResource.getConf());
                ObjectMap variantHadoopOptions = storageConfiguration.getStorageEngine(HadoopVariantStorageEngine.STORAGE_ENGINE_ID).getVariant().getOptions();
                for (Map.Entry<String, String> entry : hadoopExternalResource.getConf()) {
                    variantHadoopOptions.put(entry.getKey(), entry.getValue());
                }
            }

            setUpCatalogManager();


            file = opencga.createFile(STUDY, "variant-test-file.vcf.gz", sessionId);
            variantStorageManager.index(STUDY, file.getId(), opencga.createTmpOutdir("_index"), new ObjectMap(VariantStorageEngine.Options.ANNOTATE.key(), true), sessionId);

            for (int i = 0; i < file.getSamples().size(); i++) {
                if (i % 2 == 0) {
                    String id = file.getSamples().get(i).getId();
                    SampleUpdateParams updateParams = new SampleUpdateParams().setPhenotypes(Collections.singletonList(PHENOTYPE));
                    catalogManager.getSampleManager().update(STUDY, id, updateParams, null, sessionId);
                }
            }


            if (storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID)) {
                VariantStorageEngine engine = opencga.getStorageEngineFactory().getVariantStorageEngine(HadoopVariantStorageEngine.STORAGE_ENGINE_ID, DB_NAME);
                VariantHbaseTestUtils.printVariants(((VariantHadoopDBAdaptor) engine.getDBAdaptor()), Paths.get(opencga.createTmpOutdir("_hbase")).toUri());
            }
        }
    }

    public void setUpCatalogManager() throws IOException, CatalogException {
        catalogManager.getUserManager().create(USER, "User Name", "mail@ebi.ac.uk", PASSWORD, "", null, Account.Type.FULL, null);
        sessionId = catalogManager.getUserManager().login("user", PASSWORD);

        String projectId = catalogManager.getProjectManager().create(PROJECT, "Project about some genomes", "", "ACME", "Homo sapiens",
                null, null, "GRCh38", new QueryOptions(), sessionId).first().getId();
        catalogManager.getStudyManager().create(projectId, STUDY, null, "Phase 1", Study.Type.TRIO, null, "Done", null, null, null, null,
                null, null, null, null, sessionId);

        // Create 10 samples not indexed
        for (int i = 0; i < 10; i++) {
            Sample sample = new Sample().setId("SAMPLE_" + i);
            if (i % 2 == 0) {
                sample.setPhenotypes(Collections.singletonList(PHENOTYPE));
            }
            catalogManager.getSampleManager().create(STUDY, sample, null, sessionId);
        }

    }

    @Test
    public void testVariantStats() throws Exception {
        ObjectMap executorParams = new ObjectMap();
        VariantStatsAnalysis analysis = new VariantStatsAnalysis();
        Path outDir = Paths.get(opencga.createTmpOutdir("_variant_stats"));
        System.out.println("output = " + outDir.toAbsolutePath());
        analysis.setUp(opencga.getOpencgaHome().toString(), catalogManager, variantStorageManager, executorParams, outDir, sessionId);
        List<String> samples = file.getSamples().stream().map(Sample::getId).collect(Collectors.toList());
        analysis.setStudy(STUDY)
                .setSamplesQuery(new Query(SampleDBAdaptor.QueryParams.ID.key(), samples.subList(1,3)));
        AnalysisResult ar = analysis.start();
        checkAnalysisResult(ar);

        MutableInt count = new MutableInt();
        FileUtils.lineIterator(outDir.resolve(ar.getOutputFiles().get(0).getPath()).toFile()).forEachRemaining(line->{
            if (!line.startsWith("#")) {
                count.increment();
            }
        });
        Assert.assertEquals(variantStorageManager.count(new Query(VariantQueryParam.STUDY.key(), STUDY), sessionId).first().intValue(),
                count.intValue());
    }

    @Test
    public void testVariantStatsWithFilter() throws Exception {
        ObjectMap executorParams = new ObjectMap();
        VariantStatsAnalysis analysis = new VariantStatsAnalysis();
        Path outDir = Paths.get(opencga.createTmpOutdir("_variant_stats_chr22"));
        System.out.println("output = " + outDir.toAbsolutePath());
        analysis.setUp(opencga.getOpencgaHome().toString(), catalogManager, variantStorageManager, executorParams, outDir, sessionId);
        List<String> samples = file.getSamples().stream().map(Sample::getId).collect(Collectors.toList());
        Query variantsQuery = new Query(VariantQueryParam.REGION.key(), "22");
        analysis.setStudy(STUDY)
                .setSamplesQuery(new Query(SampleDBAdaptor.QueryParams.ID.key(), samples.subList(1, 3)))
                .setVariantsQuery(variantsQuery);
        AnalysisResult ar = analysis.start();
        checkAnalysisResult(ar);

        MutableInt count = new MutableInt();
        FileUtils.lineIterator(outDir.resolve(ar.getOutputFiles().get(0).getPath()).toFile()).forEachRemaining(line->{
            if (!line.startsWith("#")) {
                count.increment();
            }
        });
        Assert.assertEquals(variantStorageManager.count(new Query(variantsQuery).append(VariantQueryParam.STUDY.key(), STUDY), sessionId).first().intValue(),
                count.intValue());
    }

    @Test
    public void testSampleStats() throws Exception {

        ObjectMap executorParams = new ObjectMap();
        SampleVariantStatsAnalysis analysis = new SampleVariantStatsAnalysis();
        Path outDir = Paths.get(opencga.createTmpOutdir("_sample_stats"));
        System.out.println("output = " + outDir.toAbsolutePath());
        analysis.setUp(opencga.getOpencgaHome().toString(), catalogManager, variantStorageManager, executorParams, outDir, sessionId);
        List<String> samples = file.getSamples().stream().map(Sample::getId).collect(Collectors.toList());
        analysis.setSampleNames(samples)
                .setStudy(STUDY)
                .setIndexResults(true);
        checkAnalysisResult(analysis.start());

        for (String sample : samples) {
            AnnotationSet annotationSet = catalogManager.getSampleManager().get(STUDY, sample, null, sessionId).first().getAnnotationSets().get(0);
            SampleVariantStats sampleVariantStats = AvroToAnnotationConverter.convertAnnotationToAvro(annotationSet, SampleVariantStats.class);
            System.out.println(JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(annotationSet));
            System.out.println(JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(sampleVariantStats));

//            Assert.assertEquals(DummySampleVariantStatsAnalysisExecutor.getSampleVariantStats(sample), sampleVariantStats);
        }
    }

    @Test
    public void testCohortStats() throws Exception {
        ObjectMap executorParams = new ObjectMap();
        CohortVariantStatsAnalysis analysis = new CohortVariantStatsAnalysis();
        Path outDir = Paths.get(opencga.createTmpOutdir("_cohort_stats"));
        System.out.println("output = " + outDir.toAbsolutePath());
        analysis.setUp(opencga.getOpencgaHome().toString(), catalogManager, variantStorageManager, executorParams, outDir, sessionId);
        List<String> samples = file.getSamples().stream().map(Sample::getId).collect(Collectors.toList());
        analysis.setStudy(STUDY)
                .setSamplesQuery(new Query(SampleDBAdaptor.QueryParams.ID.key(), samples.subList(0, 3)));
        checkAnalysisResult(analysis.start());
    }

    @Test
    public void testCohortStatsIndex() throws Exception {
        ObjectMap executorParams = new ObjectMap();
        CohortVariantStatsAnalysis analysis = new CohortVariantStatsAnalysis();
        Path outDir = Paths.get(opencga.createTmpOutdir("_cohort_stats_index"));
        System.out.println("output = " + outDir.toAbsolutePath());
        analysis.setUp(opencga.getOpencgaHome().toString(), catalogManager, variantStorageManager, executorParams, outDir, sessionId);

        analysis.setStudy(STUDY)
                .setCohortName(StudyEntry.DEFAULT_COHORT)
                .setIndexResults(true);
        checkAnalysisResult(analysis.start());
    }

    @Test
    public void testGwas() throws Exception {
        ObjectMap executorParams = new ObjectMap();
        GwasAnalysis analysis = new GwasAnalysis();
        Path outDir = Paths.get(opencga.createTmpOutdir("_gwas"));
        System.out.println("output = " + outDir.toAbsolutePath());
        analysis.setUp(opencga.getOpencgaHome().toString(), catalogManager, variantStorageManager, executorParams, outDir, sessionId);
        List<String> samples = file.getSamples().stream().map(Sample::getId).collect(Collectors.toList());
        analysis.setStudy(STUDY)
                .setCaseCohortSamplesQuery(new Query(SampleDBAdaptor.QueryParams.ID.key(), samples.subList(0, 2)))
                .setControlCohortSamplesQuery(new Query(SampleDBAdaptor.QueryParams.ID.key(), samples.subList(2, 4)));
        checkAnalysisResult(analysis.start());
    }

    @Test
    public void testGwasByPhenotype() throws Exception {
        ObjectMap executorParams = new ObjectMap();
        GwasAnalysis analysis = new GwasAnalysis();
        Path outDir = Paths.get(opencga.createTmpOutdir("_gwas_phenotype"));
        System.out.println("output = " + outDir.toAbsolutePath());
        analysis.setUp(opencga.getOpencgaHome().toString(), catalogManager, variantStorageManager, executorParams, outDir, sessionId);

        analysis.setStudy(STUDY)
                .setPhenotype(PHENOTYPE_NAME);
        checkAnalysisResult(analysis.start());
    }

    @Test
    public void testGwasIndex() throws Exception {
        // Variant scores can not be loaded in mongodb
        Assume.assumeThat(storageEngine, CoreMatchers.is(CoreMatchers.not(MongoDBVariantStorageEngine.STORAGE_ENGINE_ID)));

        ObjectMap executorParams = new ObjectMap();
        GwasAnalysis analysis = new GwasAnalysis();
        Path outDir = Paths.get(opencga.createTmpOutdir("_gwas_index"));
        System.out.println("output = " + outDir.toAbsolutePath());
        analysis.setUp(opencga.getOpencgaHome().toString(), catalogManager, variantStorageManager, executorParams, outDir, sessionId);

        catalogManager.getCohortManager().create(STUDY, new Cohort().setId("CASE").setSamples(file.getSamples().subList(0, 2)), new QueryOptions(), sessionId);
        catalogManager.getCohortManager().create(STUDY, new Cohort().setId("CONTROL").setSamples(file.getSamples().subList(2, 4)), new QueryOptions(), sessionId);

        analysis.setStudy(STUDY)
                .setCaseCohort("CASE")
                .setControlCohort("CONTROL")
                .setScoreName("GwasScore");
        checkAnalysisResult(analysis.start());

        List<VariantScoreMetadata> scores = variantStorageManager.listVariantScores(STUDY, sessionId);
        System.out.println("scores.get(0) = " + JacksonUtils.getDefaultObjectMapper().writeValueAsString(scores));
        Assert.assertEquals(1, scores.size());
        Assert.assertEquals("GwasScore", scores.get(0).getName());

        for (Variant variant : variantStorageManager.iterable(sessionId)) {
            Assert.assertEquals("GwasScore", variant.getStudies().get(0).getScores().get(0).getId());
        }
    }

    public void checkAnalysisResult(AnalysisResult ar) {
        if (storageEngine.equals("hadoop")) {
            Assert.assertEquals("hbase-mapreduce", ar.getExecutor().getId());
        } else {
            if (ar.getId().equals(VariantStatsAnalysis.ID)) {
                Assert.assertEquals("mongodb-local", ar.getExecutor().getId());
            } else {
                Assert.assertEquals("opencga-local", ar.getExecutor().getId());
            }
        }
    }
}