package org.opencb.opencga.analysis.clinical.pharmacogenomics;

import com.fasterxml.jackson.core.type.TypeReference;
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
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.project.ProjectCreateParams;
import org.opencb.opencga.core.models.project.ProjectOrganism;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.core.tools.result.ExecutionResult;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    private File alleleTyperFile;

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
        InputStream is = getClass().getClassLoader().getResourceAsStream("pharmacogenomics/TrueMark128_detail_result.csv.gz");
        assertNotNull("Test resource pharmacogenomics/TrueMark128_detail_result.csv.gz not found", is);
        List<AlleleTyperResult> results = PharmacogenomicsManagerTest.buildResultsFromCsv(is);
        assertFalse("AlleleTyperResult list should not be empty", results.isEmpty());
        alleleTyperContent = new ObjectMapper().writeValueAsString(results);

        // Create one catalog sample per allele-typer result so that storeResultsInCatalog can persist attributes
        for (AlleleTyperResult r : results) {
            if ("NTC".equalsIgnoreCase(r.getSampleId())) {
                continue;
            }
            catalogManager.getSampleManager().create(studyFqn,
                    new Sample().setId(r.getSampleId()), QueryOptions.empty(), token);
        }

        // Write the alleleTyperContent JSON to a temporary file and link it to catalog
        // to be used later in the testAnnotationAnalysisUsingFile
        Path jsonFilePath = Paths.get(opencga.createTmpOutdir("_pgx_allele_typer_input")).resolve("allele_typer_results.json");
        Files.write(jsonFilePath, alleleTyperContent.getBytes(StandardCharsets.UTF_8));
        alleleTyperFile = catalogManager.getFileManager()
                .link(studyFqn, new FileLinkParams().setUri(jsonFilePath.toUri().toString()), false, token).first();
    }

    @Test
    public void testAnnotationAnalysisUsingContent() throws Exception {
        Path outDir = Paths.get(opencga.createTmpOutdir("_pgx_annotation_analysis"));

        PharmacogenomicsAnnotationAnalysisToolParams params = new PharmacogenomicsAnnotationAnalysisToolParams();
        params.setAlleleTyperContent(alleleTyperContent);

        ExecutionResult executionResult = toolRunner.execute(
                PharmacogenomicsAnnotationAnalysisTool.class,
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
        System.out.printf("Annotated results: %d sample file(s) in %s%n", resultFiles.size(), resultsDir);

        // Deserialise each per-sample file and verify annotations are present
        ObjectMapper objectMapper = new ObjectMapper();
        int annotatedDiplotypeCount = 0;
        for (Path sampleFile : resultFiles) {
            assertTrue("Sample result file should not be empty: " + sampleFile, Files.size(sampleFile) > 0);
            AlleleTyperResult sampleResult = objectMapper.readValue(sampleFile.toFile(), AlleleTyperResult.class);
            assertNotNull("Sample result should not be null", sampleResult);
            if (sampleResult.getAlleleTyperResults() == null) {
                continue;
            }
            for (AlleleTyperResult.StarAlleleResult starAlleleResult : sampleResult.getAlleleTyperResults()) {
                if (starAlleleResult.getDiplotypeAnnotation() == null) {
                    continue;
                }
                if (starAlleleResult.getDiplotypeAnnotation().getDiplotypeInfo() != null) {
                    annotatedDiplotypeCount++;
                }
            }
        }
        assertTrue("At least some diplotypes should have been annotated", annotatedDiplotypeCount > 0);
        System.out.println("Total annotated diplotypes: " + annotatedDiplotypeCount);

        // Verify OPENCGA_PHARMACOGENOMICS_PATH attribute was persisted in catalog for each sample
        assertSamplesHavePharmacogenomicsAttribute(objectMapper);
    }

    @Test
    public void testAnnotationAnalysisUsingFile() throws Exception {
        Path outDir = Paths.get(opencga.createTmpOutdir("_pgx_annotation_analysis"));

        PharmacogenomicsAnnotationAnalysisToolParams params = new PharmacogenomicsAnnotationAnalysisToolParams();
        params.setAlleleTyperFile(alleleTyperFile.getId());

        ExecutionResult executionResult = toolRunner.execute(
                PharmacogenomicsAnnotationAnalysisTool.class,
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
        System.out.printf("Annotated results: %d sample file(s) in %s%n", resultFiles.size(), resultsDir);

        // Deserialise each per-sample file and verify annotations are present
        ObjectMapper objectMapper = new ObjectMapper();
        int annotatedDiplotypeCount = 0;
        for (Path sampleFile : resultFiles) {
            assertTrue("Sample result file should not be empty: " + sampleFile, Files.size(sampleFile) > 0);
            AlleleTyperResult sampleResult = objectMapper.readValue(sampleFile.toFile(), AlleleTyperResult.class);
            assertNotNull("Sample result should not be null", sampleResult);
            if (sampleResult.getAlleleTyperResults() == null) {
                continue;
            }
            for (AlleleTyperResult.StarAlleleResult starAlleleResult : sampleResult.getAlleleTyperResults()) {
                if (starAlleleResult.getDiplotypeAnnotation() == null) {
                    continue;
                }
                if (starAlleleResult.getDiplotypeAnnotation().getDiplotypeInfo() != null) {
                    annotatedDiplotypeCount++;
                }
            }
        }
        assertTrue("At least some diplotypes should have been annotated", annotatedDiplotypeCount > 0);
        System.out.println("Total annotated diplotypes: " + annotatedDiplotypeCount);

        // Verify OPENCGA_PHARMACOGENOMICS_PATH attribute was persisted in catalog for each sample
        assertSamplesHavePharmacogenomicsAttribute(objectMapper);
    }

    /**
     * Verifies that every non-NTC sample in alleleTyperContent has the OPENCGA_PHARMACOGENOMICS_PATH
     * attribute set in the catalog after the annotation tool has run.
     */
    private void assertSamplesHavePharmacogenomicsAttribute(ObjectMapper objectMapper) throws Exception {
        List<AlleleTyperResult> parsedResults = objectMapper.readValue(alleleTyperContent,
                new TypeReference<List<AlleleTyperResult>>() { });
        int samplesChecked = 0;
        int samplesWithAttribute = 0;
        for (AlleleTyperResult r : parsedResults) {
            if ("NTC".equalsIgnoreCase(r.getSampleId())) {
                continue;
            }
            samplesChecked++;
            Sample sample = catalogManager.getSampleManager()
                    .get(studyFqn, r.getSampleId(), QueryOptions.empty(), token).first();
            if (sample.getAttributes() != null
                    && sample.getAttributes().containsKey("OPENCGA_PHARMACOGENOMICS_PATH")) {
                samplesWithAttribute++;
            }
        }
        assertTrue("At least one non-NTC sample should have OPENCGA_PHARMACOGENOMICS_PATH attribute set"
                + " (checked " + samplesChecked + " samples)", samplesWithAttribute > 0);
        System.out.println("Samples with OPENCGA_PHARMACOGENOMICS_PATH attribute: "
                + samplesWithAttribute + "/" + samplesChecked);
    }
}
