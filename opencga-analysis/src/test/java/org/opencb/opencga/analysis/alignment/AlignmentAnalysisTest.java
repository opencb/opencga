/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.analysis.alignment;

import org.apache.commons.lang3.StringUtils;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.opencb.biodata.formats.alignment.samtools.SamtoolsFlagstats;
import org.opencb.biodata.formats.alignment.samtools.SamtoolsStats;
import org.opencb.biodata.formats.sequence.fastqc.FastQcMetrics;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.analysis.alignment.qc.AlignmentGeneCoverageStatsAnalysis;
import org.opencb.opencga.analysis.alignment.qc.AlignmentQcAnalysis;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.analysis.variant.OpenCGATestExternalResource;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.alignment.AlignmentGeneCoverageStatsParams;
import org.opencb.opencga.core.models.alignment.AlignmentIndexParams;
import org.opencb.opencga.core.models.alignment.AlignmentQcParams;
import org.opencb.opencga.core.models.alignment.CoverageIndexParams;
import org.opencb.opencga.core.models.file.*;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.core.tools.result.ExecutionResult;
import org.opencb.opencga.core.tools.result.ToolStep;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.opencb.opencga.core.models.alignment.AlignmentQcParams.*;

@RunWith(Parameterized.class)
@Category(MediumTests.class)
public class AlignmentAnalysisTest {

    public static final String ORGANIZATION = "test";
    public static final String USER = "user";
    public static final String PASSWORD = TestParamConstants.PASSWORD;
    public static final String PROJECT = "project";
    public static final String STUDY = "study";
    public static final String PHENOTYPE_NAME = "myPhenotype";
    public static final Phenotype PHENOTYPE = new Phenotype(PHENOTYPE_NAME, PHENOTYPE_NAME, "mySource")
            .setStatus(Phenotype.Status.OBSERVED);
    public static final String DB_NAME = VariantStorageManager.buildDatabaseName("opencga_test", ORGANIZATION, PROJECT);
    private ToolRunner toolRunner;
    private static String father = "NA19661";
    private static String mother = "NA19660";
    private static String son = "NA19685";
    private static String daughter = "NA19600";

    public static final String CANCER_STUDY = "cancer";
    private static String cancer_sample = "AR2.10039966-01T";
    private static String germline_sample = "AR2.10039966-01G";

    private String bamFilename = "HG00096.chrom20.small.bam";
    private String baiFilename = "HG00096.chrom20.small.bam.bai";

    @Parameterized.Parameters(name = "{0}")
    public static Object[][] parameters() {
        return new Object[][]{
//                {MongoDBVariantStorageEngine.STORAGE_ENGINE_ID},
                {HadoopVariantStorageEngine.STORAGE_ENGINE_ID}
        };
    }

    public AlignmentAnalysisTest(String storageEngine) {
        if (!storageEngine.equals(AlignmentAnalysisTest.storageEngine)) {
            indexed = false;
        }
        AlignmentAnalysisTest.storageEngine = storageEngine;
    }


    private CatalogManager catalogManager;
    private VariantStorageManager variantStorageManager;

    @ClassRule
    public static OpenCGATestExternalResource opencga = new OpenCGATestExternalResource();
    public static HadoopVariantStorageTest.HadoopExternalResource hadoopExternalResource = new HadoopVariantStorageTest.HadoopExternalResource();

    private static String storageEngine;
    private static boolean indexed = false;
    private static String token;
    private static File file;

    @Before
    public void setUp() throws Throwable {
//        System.setProperty("opencga.log.level", "INFO");
//        Configurator.reconfigure();
        if (!indexed) {
            indexed = true;

            if (storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID)) {
                hadoopExternalResource.before();
            }
            opencga.after();
            opencga.before();

            catalogManager = opencga.getCatalogManager();
            variantStorageManager = new VariantStorageManager(catalogManager, opencga.getStorageEngineFactory());

            opencga.clearStorageDB(DB_NAME);

            StorageConfiguration storageConfiguration = opencga.getStorageConfiguration();
            storageConfiguration.getVariant().setDefaultEngine(storageEngine);
            if (storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID)) {
                HadoopVariantStorageTest.updateStorageConfiguration(storageConfiguration, hadoopExternalResource.getConf());
                ObjectMap variantHadoopOptions = storageConfiguration.getVariantEngine(HadoopVariantStorageEngine.STORAGE_ENGINE_ID).getOptions();
                for (Map.Entry<String, String> entry : hadoopExternalResource.getConf()) {
                    variantHadoopOptions.put(entry.getKey(), entry.getValue());
                }
            }

            setUpCatalogManager();

            opencga.getStorageConfiguration().getVariant().setDefaultEngine(storageEngine);
            VariantStorageEngine engine = opencga.getStorageEngineFactory().getVariantStorageEngine(storageEngine, DB_NAME);
        }

        // Reset engines
        opencga.getStorageEngineFactory().close();
        catalogManager = opencga.getCatalogManager();
        variantStorageManager = new VariantStorageManager(catalogManager, opencga.getStorageEngineFactory());
        toolRunner = new ToolRunner(opencga.getOpencgaHome().toString(), catalogManager, StorageEngineFactory.get(variantStorageManager.getStorageConfiguration()));
        token = catalogManager.getUserManager().login(ORGANIZATION, "user", PASSWORD).getToken();
    }

    @AfterClass
    public static void afterClass() {
        if (storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID)) {
            hadoopExternalResource.after();
        }
        opencga.after();
    }

    public void setUpCatalogManager() throws CatalogException, IOException {
        catalogManager.getOrganizationManager().create(new OrganizationCreateParams().setId("test"), null, opencga.getAdminToken());
        catalogManager.getUserManager().create(new User().setId(USER).setName("User Name").setEmail("mail@ebi.ac.uk").setOrganization("test"),
                PASSWORD, opencga.getAdminToken());
        catalogManager.getOrganizationManager().update("test", new OrganizationUpdateParams().setOwner(USER), null, opencga.getAdminToken());

        token = catalogManager.getUserManager().login(ORGANIZATION, "user", PASSWORD).getToken();

        String projectId = catalogManager.getProjectManager().create(PROJECT, "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token).first().getId();
        catalogManager.getStudyManager().create(projectId, STUDY, null, "Phase 1", "Done", null, null, null, null, null, token);


        // BAM and BAI files
        catalogManager.getFileManager().link(STUDY, new FileLinkParams(opencga.getResourceUri("biofiles/" + bamFilename).toString(),
                "", "", "", null, null, null, null, null), false, token).first();
        catalogManager.getFileManager().link(STUDY, new FileLinkParams(opencga.getResourceUri("biofiles/" + baiFilename).toString(),
                "", "", "", null, null, null, null, null), false, token).first();
    }

    @Test
    public void geneCoverageStatsTest() throws IOException, ToolException, CatalogException {
        Path outdir = Paths.get(opencga.createTmpOutdir("_genecoveragestats"));

        // setup BAM files
//        String bamFilename = opencga.getResourceUri("biofiles/HG00096.chrom20.small.bam").toString();
//        String baiFilename = opencga.getResourceUri("biofiles/HG00096.chrom20.small.bam.bai").toString();
        //String bamFilename = getClass().getResource("/biofiles/NA19600.chrom20.small.bam").getFile();
//        File bamFile = catalogManager.getFileManager().link(STUDY, new FileLinkParams(bamFilename, "", "", "", null, null, null,
//                null, null), false, token).first();
        File bamFile = getCatalogFile(bamFilename);
        assertEquals(0, bamFile.getQualityControl().getCoverage().getGeneCoverageStats().size());

        AlignmentGeneCoverageStatsParams params = new AlignmentGeneCoverageStatsParams();
        params.setBamFile(bamFile.getId());
        String geneName = "BRCA2";
        params.setGenes(Arrays.asList(geneName));

        toolRunner.execute(AlignmentGeneCoverageStatsAnalysis.class, params, new ObjectMap(), outdir, "coverage-job-id", token);

        bamFile = getCatalogFile(bamFilename);
        assertEquals(1, bamFile.getQualityControl().getCoverage().getGeneCoverageStats().size());
        assertEquals(geneName, bamFile.getQualityControl().getCoverage().getGeneCoverageStats().get(0).getGeneName());
        assertEquals(10, bamFile.getQualityControl().getCoverage().getGeneCoverageStats().get(0).getStats().size());
    }

    @Test
    public void testAlignmentQc() throws IOException, ToolException, CatalogException {
        Path outDir = Paths.get(opencga.createTmpOutdir("_alignment_qc"));

        File bamFile = getCatalogFile(bamFilename);
        resetAlignemntQc(bamFile);

        AlignmentQcParams params = new AlignmentQcParams();
        params.setBamFile(bamFile.getId());

        ExecutionResult executionResult = toolRunner.execute(AlignmentQcAnalysis.class, params, new ObjectMap(), outDir, null, token);
        assertTrue(executionResult.getSteps().stream().map(ToolStep::getId).collect(Collectors.toList()).contains(AlignmentQcAnalysis.SAMTOOLS_FLAGSTATS_STEP));
        assertTrue(executionResult.getSteps().stream().map(ToolStep::getId).collect(Collectors.toList()).contains(AlignmentQcAnalysis.SAMTOOLS_STATS_STEP));
        assertTrue(executionResult.getSteps().stream().map(ToolStep::getId).collect(Collectors.toList()).contains(AlignmentQcAnalysis.PLOT_BAMSTATS_STEP));
        assertTrue(executionResult.getSteps().stream().map(ToolStep::getId).collect(Collectors.toList()).contains(AlignmentQcAnalysis.FASTQC_METRICS_STEP));

        // Check
        bamFile = catalogManager.getFileManager().get(STUDY, bamFile.getId(), QueryOptions.empty(), token).first();
        checkSamtoolsFlagstats(bamFile.getQualityControl().getAlignment().getSamtoolsFlagStats());
        checkSamtoolsStats(bamFile.getQualityControl().getAlignment().getSamtoolsStats());
        checkFastQcMetrics(bamFile.getQualityControl().getAlignment().getFastQcMetrics());
        System.out.println("outdir = " + outDir);
    }

    @Test
    public void testAlignmentQcSamtoolsFlagstat() throws IOException, ToolException, CatalogException {
        Path outDir = Paths.get(opencga.createTmpOutdir("_alignment_qc_samtools_flagstat"));

        File bamFile = getCatalogFile(bamFilename);
        resetAlignemntQc(bamFile);
        System.out.println("bamFile.getQualityControl().getAlignment() = " + bamFile.getQualityControl().getAlignment());

        AlignmentQcParams params = new AlignmentQcParams();
        params.setBamFile(bamFile.getId());
        params.setSkip(StringUtils.join(Arrays.asList(STATS_SKIP_VALUE, FASTQC_METRICS_SKIP_VALUE), ","));

        ExecutionResult executionResult = toolRunner.execute(AlignmentQcAnalysis.class, STUDY, params, outDir, null, token);
        assertTrue(executionResult.getSteps().stream().map(ToolStep::getId).collect(Collectors.toList()).contains(AlignmentQcAnalysis.SAMTOOLS_FLAGSTATS_STEP));

        // Check
        bamFile = catalogManager.getFileManager().get(STUDY, bamFile.getId(), QueryOptions.empty(), token).first();
        checkSamtoolsFlagstats(bamFile.getQualityControl().getAlignment().getSamtoolsFlagStats());
        assertEquals(null, bamFile.getQualityControl().getAlignment().getSamtoolsStats());
        assertEquals(null, bamFile.getQualityControl().getAlignment().getFastQcMetrics());
        System.out.println("outdir = " + outDir);
    }

    @Test
    public void testAlignmentQcSamtoolsStatsPlots() throws IOException, ToolException, CatalogException {
        Path outDir = Paths.get(opencga.createTmpOutdir("_alignment_qc_samtools_stats_plots"));

        File bamFile = getCatalogFile(bamFilename);
        resetAlignemntQc(bamFile);
        System.out.println("bamFile.getQualityControl().getAlignment() = " + bamFile.getQualityControl().getAlignment());

        AlignmentQcParams params = new AlignmentQcParams();
        params.setBamFile(bamFile.getId());
        params.setSkip(StringUtils.join(Arrays.asList(FLAGSTATS_SKIP_VALUE, FASTQC_METRICS_SKIP_VALUE), ","));

        ExecutionResult executionResult = toolRunner.execute(AlignmentQcAnalysis.class, STUDY, params, outDir, null, token);
        assertTrue(executionResult.getSteps().stream().map(ToolStep::getId).collect(Collectors.toList()).contains(AlignmentQcAnalysis.SAMTOOLS_STATS_STEP));
        assertTrue(executionResult.getSteps().stream().map(ToolStep::getId).collect(Collectors.toList()).contains(AlignmentQcAnalysis.PLOT_BAMSTATS_STEP));

        // Check
        bamFile = catalogManager.getFileManager().get(STUDY, bamFile.getId(), QueryOptions.empty(), token).first();
        checkSamtoolsStats(bamFile.getQualityControl().getAlignment().getSamtoolsStats());
        assertEquals(null, bamFile.getQualityControl().getAlignment().getSamtoolsFlagStats());
        assertEquals(null, bamFile.getQualityControl().getAlignment().getFastQcMetrics());
        System.out.println("outdir = " + outDir);
    }

    @Test
    public void testAlignmentQcFastqc() throws IOException, ToolException, CatalogException {
        Path outDir = Paths.get(opencga.createTmpOutdir("_alignment_qc_fastqc"));

        File bamFile = getCatalogFile(bamFilename);
        resetAlignemntQc(bamFile);

        AlignmentQcParams params = new AlignmentQcParams();
        params.setBamFile(bamFile.getId());
        params.setSkip(StringUtils.join(Arrays.asList(STATS_SKIP_VALUE, FLAGSTATS_SKIP_VALUE), ","));

        ExecutionResult executionResult = toolRunner.execute(AlignmentQcAnalysis.class, STUDY, params, outDir, null, token);
        assertTrue(executionResult.getSteps().stream().map(ToolStep::getId).collect(Collectors.toList()).contains(AlignmentQcAnalysis.FASTQC_METRICS_STEP));

        // Check
        bamFile = catalogManager.getFileManager().get(STUDY, bamFile.getId(), QueryOptions.empty(), token).first();
        assertEquals(null, bamFile.getQualityControl().getAlignment().getSamtoolsStats());
        assertEquals(null, bamFile.getQualityControl().getAlignment().getSamtoolsFlagStats());
        checkFastQcMetrics(bamFile.getQualityControl().getAlignment().getFastQcMetrics());
        System.out.println("outdir = " + outDir);
    }

    @Test
    public void testAlignmentQcFastqcAndOverwrite() throws IOException, ToolException, CatalogException {
        Path outDir = Paths.get(opencga.createTmpOutdir("_alignment_qc_fastqc_and_overwrite"));

        File bamFile = getCatalogFile(bamFilename);
        resetAlignemntQc(bamFile);

        AlignmentQcParams params = new AlignmentQcParams();
        params.setBamFile(bamFile.getId());
        params.setSkip(StringUtils.join(Arrays.asList(STATS_SKIP_VALUE, FLAGSTATS_SKIP_VALUE), ","));

        ExecutionResult executeResult = toolRunner.execute(AlignmentQcAnalysis.class, STUDY, params, outDir, null, token);
        assertTrue(executeResult.getSteps().stream().map(ToolStep::getId).collect(Collectors.toList()).contains(AlignmentQcAnalysis.FASTQC_METRICS_STEP));

        outDir = Paths.get(opencga.createTmpOutdir("_alignment_qc_fastqc_overwrite_and_overwrite_2"));
        params.setOverwrite(true);
        executeResult = toolRunner.execute(AlignmentQcAnalysis.class, STUDY, params, outDir, null, token);
        assertTrue(executeResult.getSteps().stream().map(ToolStep::getId).collect(Collectors.toList()).contains(AlignmentQcAnalysis.FASTQC_METRICS_STEP));

        // Check
        bamFile = catalogManager.getFileManager().get(STUDY, bamFile.getId(), QueryOptions.empty(), token).first();
        assertEquals(null, bamFile.getQualityControl().getAlignment().getSamtoolsStats());
        assertEquals(null, bamFile.getQualityControl().getAlignment().getSamtoolsFlagStats());
        checkFastQcMetrics(bamFile.getQualityControl().getAlignment().getFastQcMetrics());
        System.out.println("outdir = " + outDir);
    }

    @Test
    public void testAlignmentQcFastqcAndDoNotOverwrite() throws IOException, ToolException, CatalogException {
        Path outDir = Paths.get(opencga.createTmpOutdir("_alignment_qc_fastqc_and_do_not_overwrite"));

        File bamFile = getCatalogFile(bamFilename);
        resetAlignemntQc(bamFile);

        AlignmentQcParams params = new AlignmentQcParams();
        params.setBamFile(bamFile.getId());
        params.setSkip(StringUtils.join(Arrays.asList(STATS_SKIP_VALUE, FLAGSTATS_SKIP_VALUE), ","));

        ExecutionResult executeResult = toolRunner.execute(AlignmentQcAnalysis.class, STUDY, params, outDir, null, token);
        assertTrue(executeResult.getSteps().stream().map(ToolStep::getId).collect(Collectors.toList()).contains(AlignmentQcAnalysis.FASTQC_METRICS_STEP));

        // Check
        bamFile = catalogManager.getFileManager().get(STUDY, bamFile.getId(), QueryOptions.empty(), token).first();
        assertEquals(null, bamFile.getQualityControl().getAlignment().getSamtoolsStats());
        assertEquals(null, bamFile.getQualityControl().getAlignment().getSamtoolsFlagStats());
        checkFastQcMetrics(bamFile.getQualityControl().getAlignment().getFastQcMetrics());

        outDir = Paths.get(opencga.createTmpOutdir("_alignment_qc_fastqc_and_do_not_overwrite_2"));

        executeResult = toolRunner.execute(AlignmentQcAnalysis.class, STUDY, params, outDir, null, token);
        assertFalse(executeResult.getSteps().stream().map(ToolStep::getId).collect(Collectors.toList()).contains(AlignmentQcAnalysis.FASTQC_METRICS_STEP));

        System.out.println("outdir = " + outDir);
    }

    private void checkSamtoolsStats(SamtoolsStats stats) {
        System.out.println("stats = " + stats);
        assertTrue(stats != null);
        assertEquals(108, stats.getSequences());
        assertEquals(55, stats.getLastFragments());
        assertEquals(0, stats.getReadsDuplicated());
        assertEquals(0, stats.getReadsQcFailed());
        assertEquals(10800, stats.getTotalLength());
        assertEquals(10047, stats.getBasesMappedCigar());
        assertEquals(49, stats.getMismatches());
        assertEquals(31.0, stats.getAverageQuality(), 0.001f);
        assertEquals(1, stats.getFiles().stream().filter(n -> n.endsWith("quals.png")).collect(Collectors.toList()).size());
        assertEquals(1, stats.getFiles().stream().filter(n -> n.endsWith("quals3.png")).collect(Collectors.toList()).size());
        assertEquals(1, stats.getFiles().stream().filter(n -> n.endsWith("coverage.png")).collect(Collectors.toList()).size());
        assertEquals(1, stats.getFiles().stream().filter(n -> n.endsWith("insert-size.png")).collect(Collectors.toList()).size());
        assertEquals(1, stats.getFiles().stream().filter(n -> n.endsWith("gc-content.png")).collect(Collectors.toList()).size());
        assertEquals(1, stats.getFiles().stream().filter(n -> n.endsWith("acgt-cycles.png")).collect(Collectors.toList()).size());
        assertEquals(1, stats.getFiles().stream().filter(n -> n.endsWith("quals2.png")).collect(Collectors.toList()).size());
    }

    private void checkSamtoolsFlagstats(SamtoolsFlagstats flagstats) {
        System.out.println("flagstats = " + flagstats);
        assertTrue(flagstats != null);
        assertEquals(108, flagstats.getTotalReads());
        assertEquals(0, flagstats.getSecondaryAlignments());
        assertEquals(53, flagstats.getRead1());
        assertEquals(55, flagstats.getRead2());
        assertEquals(104, flagstats.getProperlyPaired());
    }

    private void checkFastQcMetrics(FastQcMetrics metrics) {
        System.out.println("metrics = " + metrics);
        assertTrue(metrics != null);
        assertEquals("PASS", metrics.getSummary().getBasicStatistics());
        assertEquals("FAIL", metrics.getSummary().getPerSeqGcContent());
        assertEquals("WARN", metrics.getSummary().getOverrepresentedSeqs());
        assertEquals(7, metrics.getBasicStats().size());
        assertEquals("108", metrics.getBasicStats().get("Total Sequences"));
        assertEquals("100", metrics.getBasicStats().get("Sequence length"));
        assertEquals("46", metrics.getBasicStats().get("%GC"));
        assertEquals(8, metrics.getFiles().size());
        assertEquals(1, metrics.getFiles().stream().filter(n -> n.endsWith("per_sequence_quality.png")).collect(Collectors.toList()).size());
        assertEquals(1, metrics.getFiles().stream().filter(n -> n.endsWith("duplication_levels.png")).collect(Collectors.toList()).size());
        assertEquals(1, metrics.getFiles().stream().filter(n -> n.endsWith("per_base_quality.png")).collect(Collectors.toList()).size());
        assertEquals(1, metrics.getFiles().stream().filter(n -> n.endsWith("adapter_content.png")).collect(Collectors.toList()).size());
    }

    private File getCatalogFile(String name) throws CatalogException {
        return catalogManager.getFileManager().search(STUDY, new Query("name", name), QueryOptions.empty(), token).first();
    }

    private void resetAlignemntQc(File bamFile) throws CatalogException {
        FileUpdateParams updateParams = new FileUpdateParams();
        updateParams.setQualityControl(new FileQualityControl());
        catalogManager.getFileManager().update(STUDY, new Query("id", bamFile.getId()), updateParams, QueryOptions.empty(), token);
    }

    @Test
    public void testNonReadOnlyAlignmentIndex() throws Exception {
        Path nonReadOnlyDir = Paths.get(opencga.createTmpOutdir("_non_readonly_alignment_index"));
        Path bamPath = Paths.get(opencga.getResourceUri("biofiles/HG00096.chrom20.small.bam").getPath());
        String bamFilename = "NonReadOnlyAligmentIndex_" + bamPath.getFileName();
        Files.copy(bamPath, nonReadOnlyDir.resolve(bamFilename));

        File bamFile = catalogManager.getFileManager().link(STUDY, new FileLinkParams(nonReadOnlyDir.resolve(bamFilename).toAbsolutePath().toString(), "non_readonly_alignment_index", "", "", null, null, null,
                null, null), true, token).first();

        // Run alignment index
        AlignmentIndexParams params = new AlignmentIndexParams();
        params.setFileId(bamFile.getId());
        Path alignmentIndexOutdir = Paths.get(opencga.createTmpOutdir("_alignment_index"));
        toolRunner.execute(AlignmentIndexOperation.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), alignmentIndexOutdir, "jobId-non-readonly-coverage-index", token);

        // Checking BAI file
        Path baiPath = nonReadOnlyDir.resolve(bamFilename + AlignmentConstants.BAI_EXTENSION);
        Assert.assertTrue(Files.exists(baiPath));

        // Checking BAI file is registered in the BAM file internals
        File baiFile = catalogManager.getFileManager().get(STUDY, Collections.singletonList(bamFilename + AlignmentConstants.BAI_EXTENSION), QueryOptions.empty(), true, token).first();
        bamFile = catalogManager.getFileManager().get(STUDY, Collections.singletonList(bamFile.getId()), QueryOptions.empty(), true, token).first();
        Assert.assertEquals(baiFile.getId(), bamFile.getInternal().getAlignment().getIndex().getFileId());
    }

    @Test
    public void testReadOnlyAlignmentIndex() throws Exception {
        Path readOnlyDir = Paths.get(opencga.createTmpOutdir("_readonly_for_alignment_index"));
        Path bamPath = Paths.get(opencga.getResourceUri("biofiles/HG00096.chrom20.small.bam").getPath());
        String bamFilename = "ReadOnlyAligmentIndex_" + bamPath.getFileName();
        Files.copy(bamPath, readOnlyDir.resolve(bamFilename));

        // Make read-only
        Runtime.getRuntime().exec("chmod 555 " + readOnlyDir.toAbsolutePath());

        File bamFile = catalogManager.getFileManager().link(STUDY, new FileLinkParams(readOnlyDir.resolve(bamFilename).toAbsolutePath().toString(), "readonly_alignment_index", "", "", null, null, null,
                null, null), true, token).first();

        // Run alignment index
        AlignmentIndexParams params = new AlignmentIndexParams();
        params.setFileId(bamFile.getId());
        Path alignmentIndexOutdir = Paths.get(opencga.createTmpOutdir("_alignment_index"));
        toolRunner.execute(AlignmentIndexOperation.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), alignmentIndexOutdir, "jobId-readonly-coverage-index", token);

        // Checking BAI file
        Path baiPath = alignmentIndexOutdir.resolve(bamFilename + AlignmentConstants.BAI_EXTENSION);
        Assert.assertTrue(Files.exists(baiPath));

        // Checking BAI file is registered in the BAM file internals
        File baiFile = catalogManager.getFileManager().get(STUDY, Collections.singletonList(bamFilename + AlignmentConstants.BAI_EXTENSION), QueryOptions.empty(), true, token).first();
        bamFile = catalogManager.getFileManager().get(STUDY, Collections.singletonList(bamFile.getId()), QueryOptions.empty(), true, token).first();
        Assert.assertEquals(baiFile.getId(), bamFile.getInternal().getAlignment().getIndex().getFileId());

        Runtime.getRuntime().exec("chmod 777 " + readOnlyDir.toAbsolutePath());
    }

    @Test
    public void testNonReadOnlyCoverageIndex() throws Exception {
        Path nonReadOnlyDir = Paths.get(opencga.createTmpOutdir("_non_readonly_for_coverage_index"));
        Path bamPath = Paths.get(opencga.getResourceUri("biofiles/HG00096.chrom20.small.bam").getPath());
        String bamFilename = "NonReadOnlyCoverageIndex_" + bamPath.getFileName();
        Files.copy(bamPath, nonReadOnlyDir.resolve(bamFilename));

        File bamFile = catalogManager.getFileManager().link(STUDY, new FileLinkParams(nonReadOnlyDir.resolve(bamFilename).toAbsolutePath().toString(), "non_readonly_alignment_coverage_index", "", "", null, null, null,
                null, null), true, token).first();

        // Run alignment index
        AlignmentIndexParams indexParams = new AlignmentIndexParams();
        indexParams.setFileId(bamFile.getId());
        Path alignmentIndexOutdir = Paths.get(opencga.createTmpOutdir("_alignment_index"));
        toolRunner.execute(AlignmentIndexOperation.class, indexParams, new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), alignmentIndexOutdir, "jobId-non-readonly-alignment-coverage-index", token);

        // Checking BAI file
        Path baiPath = nonReadOnlyDir.resolve(bamFilename + AlignmentConstants.BAI_EXTENSION);
        Assert.assertTrue(Files.exists(baiPath));

        // Checking BAI file is registered in the BAM file internals
        File baiFile = catalogManager.getFileManager().get(STUDY, Collections.singletonList(bamFilename + AlignmentConstants.BAI_EXTENSION), QueryOptions.empty(), true, token).first();
        bamFile = catalogManager.getFileManager().get(STUDY, Collections.singletonList(bamFile.getId()), QueryOptions.empty(), true, token).first();
        Assert.assertEquals(baiFile.getId(), bamFile.getInternal().getAlignment().getIndex().getFileId());

        // Run coverage index
        CoverageIndexParams coverageOarams = new CoverageIndexParams();
        coverageOarams.setBamFileId(bamFile.getId());
        coverageOarams.setBaiFileId(baiFile.getId());
        Path coverageIndexOutdir = Paths.get(opencga.createTmpOutdir("_coverage_index"));
        toolRunner.execute(AlignmentCoverageAnalysis.class, coverageOarams, new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), coverageIndexOutdir, "jobId-readonly-coverage-index", token);

        // Checking BW file
        Path bwPath = nonReadOnlyDir.resolve(bamFilename + AlignmentConstants.BIGWIG_EXTENSION);
        Assert.assertTrue(Files.exists(bwPath));

        // Checking BAM file is registered in the related files of BW file
        File bwFile = catalogManager.getFileManager().get(STUDY, Collections.singletonList(bwPath.getFileName().toString()), QueryOptions.empty(), true, token).first();
        Assert.assertEquals(bamFile.getId(), bwFile.getRelatedFiles().get(0).getFile().getId());
        Assert.assertEquals(FileRelatedFile.Relation.ALIGNMENT, bwFile.getRelatedFiles().get(0).getRelation());
    }

    @Test
    public void testReadOnlyCoverageIndex() throws Exception {
        Path readOnlyDir = Paths.get(opencga.createTmpOutdir("_readonly_for_coverage_index"));
        Path bamPath = Paths.get(opencga.getResourceUri("biofiles/HG00096.chrom20.small.bam").getPath());
        String bamFilename = "ReadOnlyCoverageIndex_" + bamPath.getFileName();
        Files.copy(bamPath, readOnlyDir.resolve(bamFilename));

        File bamFile = catalogManager.getFileManager().link(STUDY, new FileLinkParams(readOnlyDir.resolve(bamFilename).toAbsolutePath().toString(), "readonly_alignment_coverage_index", "", "", null, null, null,
                null, null), true, token).first();

        // Run alignment index
        AlignmentIndexParams indexParams = new AlignmentIndexParams();
        indexParams.setFileId(bamFile.getId());
        Path alignmentIndexOutdir = Paths.get(opencga.createTmpOutdir("_alignment_index"));
        toolRunner.execute(AlignmentIndexOperation.class, indexParams, new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), alignmentIndexOutdir, "jobId-readonly-coverage-index", token);

        // Checking BAI file
        Path baiPath = readOnlyDir.resolve(bamFilename + AlignmentConstants.BAI_EXTENSION);
        Assert.assertTrue(Files.exists(baiPath));

        // Checking BAI file is registered in the BAM file internals
        File baiFile = catalogManager.getFileManager().get(STUDY, Collections.singletonList(bamFilename + AlignmentConstants.BAI_EXTENSION), QueryOptions.empty(), true, token).first();
        bamFile = catalogManager.getFileManager().get(STUDY, Collections.singletonList(bamFile.getId()), QueryOptions.empty(), true, token).first();
        Assert.assertEquals(baiFile.getId(), bamFile.getInternal().getAlignment().getIndex().getFileId());

        // Make read-only
        Runtime.getRuntime().exec("chmod 555 " + readOnlyDir.toAbsolutePath());

        // Run coverage index
        CoverageIndexParams coverageOarams = new CoverageIndexParams();
        coverageOarams.setBamFileId(bamFile.getId());
        coverageOarams.setBaiFileId(baiFile.getId());
        Path coverageIndexOutdir = Paths.get(opencga.createTmpOutdir("_coverage_index"));
        toolRunner.execute(AlignmentCoverageAnalysis.class, coverageOarams, new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), coverageIndexOutdir, "jobId-readonly-coverage-index", token);

        // Checking BW file
        Path bwPath = coverageIndexOutdir.resolve(bamFilename + AlignmentConstants.BIGWIG_EXTENSION);
        Assert.assertTrue(Files.exists(bwPath));

        // Checking BAM file is registered in the related files of BW file
        File bwFile = catalogManager.getFileManager().get(STUDY, Collections.singletonList(bwPath.getFileName().toString()), QueryOptions.empty(), true, token).first();
        Assert.assertEquals(bamFile.getId(), bwFile.getRelatedFiles().get(0).getFile().getId());
        Assert.assertEquals(FileRelatedFile.Relation.ALIGNMENT, bwFile.getRelatedFiles().get(0).getRelation());

        Runtime.getRuntime().exec("chmod 777 " + readOnlyDir.toAbsolutePath());
    }
}