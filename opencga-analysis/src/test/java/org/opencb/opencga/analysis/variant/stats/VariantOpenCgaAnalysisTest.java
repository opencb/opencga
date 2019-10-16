package org.opencb.opencga.analysis.variant.stats;

import org.hamcrest.CoreMatchers;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.gwas.GwasOpenCgaAnalysis;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.utils.AvroToAnnotationConverter;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.manager.OpenCGATestExternalResource;
import org.opencb.opencga.storage.core.manager.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.metadata.models.VariantScoreMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine;
import org.opencb.oskar.analysis.result.AnalysisResult;
import org.opencb.oskar.analysis.variant.stats.VariantStatsAnalysis;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class VariantOpenCgaAnalysisTest {

    public static final String USER = "user";
    public static final String PASSWORD = "asdf";
    public static final String PROJECT = "project";
    public static final String STUDY = "study";
    public static final String DB_NAME = "opencga_test_" + USER + "_" + PROJECT;

    @Parameterized.Parameters(name="{0}")
    public static Object[][] parameters() {
        return new Object[][]{
                {MongoDBVariantStorageEngine.STORAGE_ENGINE_ID},
                {HadoopVariantStorageEngine.STORAGE_ENGINE_ID}};
    }

    public VariantOpenCgaAnalysisTest(String storageEngine) {
        if (!storageEngine.equals(VariantOpenCgaAnalysisTest.storageEngine)) {
            indexed = false;
        }
        VariantOpenCgaAnalysisTest.storageEngine = storageEngine;
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

            if (storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID)) {
                VariantStorageEngine engine = opencga.getStorageEngineFactory().getVariantStorageEngine(HadoopVariantStorageEngine.STORAGE_ENGINE_ID, DB_NAME);
                VariantHbaseTestUtils.printVariants(((VariantHadoopDBAdaptor) engine.getDBAdaptor()), Paths.get(opencga.createTmpOutdir("_hbase")).toUri());
            }
        }
    }

    public void setUpCatalogManager() throws IOException, CatalogException {
        catalogManager.getUserManager().create(USER, "User Name", "mail@ebi.ac.uk", PASSWORD, "", null, Account.Type.FULL, null, null);
        sessionId = catalogManager.getUserManager().login("user", PASSWORD);

        String projectId = catalogManager.getProjectManager().create(PROJECT, "Project about some genomes", "", "ACME", "Homo sapiens",
                null, null, "GRCh38", new QueryOptions(), sessionId).first().getId();
        catalogManager.getStudyManager().create(projectId, STUDY, null, "Phase 1", Study.Type.TRIO, null, "Done", null, null, null, null,
                null, null, null, null, sessionId);

    }

    @Test
    public void testVariantStats() throws Exception {
        ObjectMap executorParams = new ObjectMap();
        VariantStatsOpenCgaAnalysis analysis = new VariantStatsOpenCgaAnalysis();
        Path outDir = Paths.get(opencga.createTmpOutdir("_variant_stats"));
        System.out.println("output = " + outDir.toAbsolutePath());
        analysis.setUp(opencga.getOpencgaHome().toString(), catalogManager, variantStorageManager, executorParams, outDir, sessionId);
        List<String> samples = file.getSamples().stream().map(Sample::getId).collect(Collectors.toList());
        analysis.setStudy(STUDY)
                .setSamplesQuery(new Query(SampleDBAdaptor.QueryParams.ID.key(), samples.subList(1,3)));
        AnalysisResult ar = analysis.execute();
        checkAnalysisResult(ar);

        Assert.assertEquals(variantStorageManager.count(new Query(VariantQueryParam.STUDY.key(), STUDY), sessionId).first().intValue(),
                ar.getAttributes().getInt("numVariantStats"));
    }

    @Test
    public void testVariantStatsWithFilter() throws Exception {
        ObjectMap executorParams = new ObjectMap();
        VariantStatsOpenCgaAnalysis analysis = new VariantStatsOpenCgaAnalysis();
        Path outDir = Paths.get(opencga.createTmpOutdir("_variant_stats_chr22"));
        System.out.println("output = " + outDir.toAbsolutePath());
        analysis.setUp(opencga.getOpencgaHome().toString(), catalogManager, variantStorageManager, executorParams, outDir, sessionId);
        List<String> samples = file.getSamples().stream().map(Sample::getId).collect(Collectors.toList());
        Query variantsQuery = new Query(VariantQueryParam.REGION.key(), "22");
        analysis.setStudy(STUDY)
                .setSamplesQuery(new Query(SampleDBAdaptor.QueryParams.ID.key(), samples.subList(1, 3)))
                .setVariantsQuery(variantsQuery);
        AnalysisResult ar = analysis.execute();
        checkAnalysisResult(ar);

        Assert.assertEquals(variantStorageManager.count(new Query(variantsQuery).append(VariantQueryParam.STUDY.key(), STUDY), sessionId).first().intValue(),
                ar.getAttributes().getInt("numVariantStats"));
    }

    @Test
    public void testSampleStats() throws Exception {

        ObjectMap executorParams = new ObjectMap();
        SampleVariantStatsOpenCgaAnalysis analysis = new SampleVariantStatsOpenCgaAnalysis();
        Path outDir = Paths.get(opencga.createTmpOutdir("_sample_stats"));
        System.out.println("output = " + outDir.toAbsolutePath());
        analysis.setUp(opencga.getOpencgaHome().toString(), catalogManager, variantStorageManager, executorParams, outDir, sessionId);
        List<String> samples = file.getSamples().stream().map(Sample::getId).collect(Collectors.toList());
        analysis.setSampleNames(samples)
                .setStudy(STUDY)
                .setIndexResults(true);
        checkAnalysisResult(analysis.execute());

        for (String sample : samples) {
            AnnotationSet annotationSet = catalogManager.getSampleManager().get(STUDY, sample, null, sessionId).first().getAnnotationSets().get(0);
            SampleVariantStats sampleVariantStats = AvroToAnnotationConverter.convertAnnotationToAvro(annotationSet, SampleVariantStats.class);
            System.out.println(JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(annotationSet));
            System.out.println(JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(sampleVariantStats));

//            Assert.assertEquals(DummySampleVariantStatsAnalysisExecutor.getSampleVariantStats(sample), sampleVariantStats);
        }


    }

    @Test
    public void testGwas() throws Exception {
        ObjectMap executorParams = new ObjectMap();
        GwasOpenCgaAnalysis analysis = new GwasOpenCgaAnalysis();
        Path outDir = Paths.get(opencga.createTmpOutdir("_gwas"));
        System.out.println("output = " + outDir.toAbsolutePath());
        analysis.setUp(opencga.getOpencgaHome().toString(), catalogManager, variantStorageManager, executorParams, outDir, sessionId);
        List<String> samples = file.getSamples().stream().map(Sample::getId).collect(Collectors.toList());
        analysis.setStudy(STUDY)
                .setCaseCohortSamplesQuery(new Query(SampleDBAdaptor.QueryParams.ID.key(), samples.subList(0, 2)))
                .setControlCohortSamplesQuery(new Query(SampleDBAdaptor.QueryParams.ID.key(), samples.subList(2, 4)));
        checkAnalysisResult(analysis.execute());
    }

    @Test
    public void testGwasIndex() throws Exception {
        // Variant scores can not be loaded in mongodb
        Assume.assumeThat(storageEngine, CoreMatchers.is(CoreMatchers.not(MongoDBVariantStorageEngine.STORAGE_ENGINE_ID)));

        ObjectMap executorParams = new ObjectMap();
        GwasOpenCgaAnalysis analysis = new GwasOpenCgaAnalysis();
        Path outDir = Paths.get(opencga.createTmpOutdir("_gwas_index"));
        System.out.println("output = " + outDir.toAbsolutePath());
        analysis.setUp(opencga.getOpencgaHome().toString(), catalogManager, variantStorageManager, executorParams, outDir, sessionId);

        catalogManager.getCohortManager().create(STUDY, new Cohort().setId("CASE").setSamples(file.getSamples().subList(0, 2)), new QueryOptions(), sessionId);
        catalogManager.getCohortManager().create(STUDY, new Cohort().setId("CONTROL").setSamples(file.getSamples().subList(2, 4)), new QueryOptions(), sessionId);

        analysis.setStudy(STUDY)
                .setCaseCohort("CASE")
                .setControlCohort("CONTROL")
                .setScoreName("GwasScore");
        checkAnalysisResult(analysis.execute());

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
            Assert.assertEquals("hbase-mapreduce", ar.getExecutorId());
        } else {
            if (ar.getId().equals(VariantStatsAnalysis.ID)) {
                Assert.assertEquals("mongodb-local", ar.getExecutorId());
            } else {
                Assert.assertEquals("opencga-local", ar.getExecutorId());
            }
        }
    }
}