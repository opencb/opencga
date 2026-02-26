package org.opencb.opencga.analysis.clinical.pharmacogenomics;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.analysis.variant.OpenCGATestExternalResource;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.storage.CellBaseConfiguration;
import org.opencb.opencga.core.models.clinical.PharmacogenomicsAnnotationAnalysisToolParams;
import org.opencb.opencga.core.models.clinical.pharmacogenomics.AlleleTyperResult;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.project.ProjectCreateParams;
import org.opencb.opencga.core.models.project.ProjectOrganism;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.core.tools.result.ExecutionResult;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.Assert.*;

@Category(MediumTests.class)
public class PharmacogenomicsAnnotationAnalysisToolTest {

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
    private String alleleTyperContent;

    @Before
    public void setUp() throws Exception {
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
                .create(project.getId(), new Study().setId(STUDY_ID), new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token).first();
        studyFqn = study.getFqn();

        // Build allele typer JSON from the shared CSV test resource
        InputStream is = getClass().getClassLoader().getResourceAsStream(
                "pharmacogenomics/TrueMark128_detail_result.csv.gz");
        assertNotNull("Test resource pharmacogenomics/TrueMark128_detail_result.csv.gz not found", is);
        List<AlleleTyperResult> results = PharmacogenomicsManagerTest.buildResultsFromCsv(is);
        assertFalse("AlleleTyperResult list should not be empty", results.isEmpty());
        alleleTyperContent = new ObjectMapper().writeValueAsString(results);
    }

    @Test
    public void testAnnotationAnalysis() throws Exception {
        Path outDir = Paths.get(opencga.createTmpOutdir("_pgx_annotation_analysis"));

        PharmacogenomicsAnnotationAnalysisToolParams params =
                new PharmacogenomicsAnnotationAnalysisToolParams(alleleTyperContent);

        ExecutionResult executionResult = toolRunner.execute(
                PharmacogenomicsAnnotationAnalysisTool.class,
                params,
                new ObjectMap(ParamConstants.STUDY_PARAM, studyFqn),
                outDir, null, false, token);

        assertNotNull("Execution result should not be null", executionResult);

        // Check the output file exists and is non-empty
        Path annotatedPath = outDir.resolve("pgx_annotated_results.json");
        assertTrue("Annotated results file should exist: " + annotatedPath, Files.exists(annotatedPath));
        long fileSizeBytes = Files.size(annotatedPath);
        assertTrue("Annotated results file should not be empty", fileSizeBytes > 0);
        System.out.printf("Annotated results file size: %.2f KB%n", fileSizeBytes / 1024.0);

        // Deserialise and verify annotations are present
        ObjectMapper objectMapper = new ObjectMapper();
        List<AlleleTyperResult> annotatedResults = objectMapper.readValue(
                annotatedPath.toFile(),
                objectMapper.getTypeFactory().constructCollectionType(List.class, AlleleTyperResult.class));

        assertFalse("Annotated results should not be empty", annotatedResults.isEmpty());

        int annotatedCallCount = 0;
        for (AlleleTyperResult sampleResult : annotatedResults) {
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
}
