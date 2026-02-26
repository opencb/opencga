package org.opencb.opencga.analysis.clinical.pharmacogenomics;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assume;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.analysis.variant.OpenCGATestExternalResource;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.storage.CellBaseConfiguration;
import org.opencb.opencga.core.models.clinical.PharmacogenomicsAlleleTyperToolParams;
import org.opencb.opencga.core.models.clinical.pharmacogenomics.AlleleTyperResult;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.project.ProjectCreateParams;
import org.opencb.opencga.core.models.project.ProjectOrganism;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.core.tools.result.ExecutionResult;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;

@Category(MediumTests.class)
public class PharmacogenomicsAlleleTyperAnalysisToolTest {

    private static final String GENOTYPING_RESOURCE  = "/opt/pharmacogenomics-star-alleles/3G_Export_DO_TrueMark_128_Export_DO_TrueMark_128_Genotyping_07-11-2025-074600.txt.gz";
    private static final String TRANSLATION_RESOURCE = "/opt/pharmacogenomics-star-alleles/PGX_SNP_CNV_128_OA_translation_RevC.csv.gz";

    private static final String ORGANIZATION_ID = "testorg";
    private static final String USER_ID = "testuser";
    private static final String PROJECT_ID = "pgx_project";
    private static final String STUDY_ID = "pgx_study";

    @ClassRule
    public static OpenCGATestExternalResource opencga = new OpenCGATestExternalResource();

    private CatalogManager catalogManager;
    private ToolRunner toolRunner;
    private String token;
    private String studyFqn;
    private String genotypingContent;
    private String translationContent;

    @Before
    public void setUp() throws Exception {
        Assume.assumeTrue("Genotyping file not found: " + GENOTYPING_RESOURCE, Paths.get(GENOTYPING_RESOURCE).toFile().exists());
        Assume.assumeTrue("Translation file not found: " + TRANSLATION_RESOURCE, Paths.get(TRANSLATION_RESOURCE).toFile().exists());

        opencga.clear();

        catalogManager = opencga.getCatalogManager();
        // Create a fresh ToolRunner bound to the new CatalogManager produced by clear()
        toolRunner = new ToolRunner(opencga.getOpencgaHome().toAbsolutePath().toString(),
                catalogManager, opencga.getVariantStorageManager());

        // Create organisation, owner user and login
        catalogManager.getOrganizationManager().create(
                new OrganizationCreateParams().setId(ORGANIZATION_ID).setName("Test Organisation"),
                null, opencga.getAdminToken());
        catalogManager.getUserManager().create(USER_ID, "Test User", "test@test.com",
                TestParamConstants.PASSWORD, ORGANIZATION_ID, null, opencga.getAdminToken());
        catalogManager.getOrganizationManager().update(ORGANIZATION_ID,
                new OrganizationUpdateParams().setOwner(USER_ID),
                null, opencga.getAdminToken());
        token = catalogManager.getUserManager().login(ORGANIZATION_ID, USER_ID, TestParamConstants.PASSWORD).first().getToken();

        // Create project with CellBase v6.7
        Project project = catalogManager.getProjectManager().create(
                new ProjectCreateParams()
                        .setId(PROJECT_ID)
                        .setOrganism(new ProjectOrganism("hsapiens", "grch38"))
                        .setCellbase(new CellBaseConfiguration(ParamConstants.CELLBASE_URL, "v6.7")),
                new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token).first();

        // Create study
        Study study = catalogManager.getStudyManager()
                .create(project.getId(), new Study().setId(STUDY_ID),
                        new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token).first();
        studyFqn = study.getFqn();

        // Load genotyping file content (gzip-compressed)
        genotypingContent = readGzipFile(GENOTYPING_RESOURCE);

        // Load translation file content (gzip-compressed)
        translationContent = readGzipFile(TRANSLATION_RESOURCE);
    }

    @Test
    public void testAlleleTyperAnalysis() throws Exception {
        Path outDir = Paths.get(opencga.createTmpOutdir("_pgx_allele_typer_analysis"));

        PharmacogenomicsAlleleTyperToolParams params = new PharmacogenomicsAlleleTyperToolParams(
                genotypingContent, translationContent, true, null);

        ExecutionResult executionResult = toolRunner.execute(
                PharmacogenomicsAlleleTyperAnalysisTool.class,
                params,
                new ObjectMap(ParamConstants.STUDY_PARAM, studyFqn),
                outDir, null, false, token);

        assertNotNull("Execution result should not be null", executionResult);

        // Check that the results directory exists and contains one JSON file per sample
        Path resultsDir = outDir.resolve(PharmacogenomicsAlleleTyperAnalysisTool.RESULTS_DIR);
        assertTrue("Results directory should exist: " + resultsDir, Files.exists(resultsDir));

        List<Path> resultFiles;
        try (Stream<Path> stream = Files.list(resultsDir)) {
            resultFiles = stream.filter(p -> p.toString().endsWith(".json")).collect(Collectors.toList());
        }
        assertFalse("Results directory should contain at least one JSON file", resultFiles.isEmpty());
        System.out.printf("Allele typer results: %d sample file(s) in %s%n", resultFiles.size(), resultsDir);

        // Deserialise each per-sample file and verify star allele results and annotations are present
        ObjectMapper objectMapper = new ObjectMapper();
        int annotatedCallCount = 0;
        for (Path sampleFile : resultFiles) {
            assertTrue("Sample result file should not be empty: " + sampleFile, Files.size(sampleFile) > 0);
            AlleleTyperResult sampleResult = objectMapper.readValue(sampleFile.toFile(), AlleleTyperResult.class);
            assertNotNull("Sample result should not be null", sampleResult);
            if (sampleResult.getAlleleTyperResults() == null) {
                continue;
            }
            for (AlleleTyperResult.StarAlleleResult starAlleleResult : sampleResult.getAlleleTyperResults()) {
                if (starAlleleResult.getAlleleCalls() == null) {
                    continue;
                }
                for (AlleleTyperResult.AlleleCall alleleCall : starAlleleResult.getAlleleCalls()) {
                    if (alleleCall.getAnnotation() != null) {
                        annotatedCallCount++;
                    }
                }
            }
        }
        assertTrue("At least some allele calls should have been annotated", annotatedCallCount > 0);
        System.out.println("Total annotated allele calls: " + annotatedCallCount);
    }

    private static String readGzipFile(String path) throws Exception {
        try (GZIPInputStream gzis = new GZIPInputStream(Files.newInputStream(Paths.get(path)))) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            byte[] buf = new byte[4096];
            int n;
            while ((n = gzis.read(buf)) != -1) {
                baos.write(buf, 0, n);
            }
            return baos.toString(StandardCharsets.UTF_8.name());
        }
    }
}
