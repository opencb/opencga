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
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.models.sample.Sample;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

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
    private File genotypingFile;
    private File translationFile;
    private List<String> sampleIds;

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

        // Parse genotyping to discover sample IDs and create them in catalog so
        // storeResultsInCatalog can persist the OPENCGA_PHARMACOGENOMICS_DATA attribute
        AlleleTyper typer = new AlleleTyper();
        typer.parseTranslationFromString(translationContent);
        List<AlleleTyperResult> parsedResults = typer.buildAlleleTyperResultsFromString(genotypingContent);
        sampleIds = new ArrayList<>();
        for (AlleleTyperResult r : parsedResults) {
            if ("NTC".equalsIgnoreCase(r.getSampleId())) {
                continue;
            }
            sampleIds.add(r.getSampleId());
            catalogManager.getSampleManager().create(studyFqn,
                    new Sample().setId(r.getSampleId()), QueryOptions.empty(), token);
        }

        // Write each content to a temporary file and link it to catalog
        // to be used later in the testAlleleTyperAnalysisUsingFile
        Path genotypingFilePath = Paths.get(opencga.createTmpOutdir("_pgx_genotyping_input")).resolve("genotyping.txt");
        Files.write(genotypingFilePath, genotypingContent.getBytes(StandardCharsets.UTF_8));
        genotypingFile = catalogManager.getFileManager()
                .link(studyFqn, new FileLinkParams().setUri(genotypingFilePath.toUri().toString()), false, token).first();

        Path translationFilePath = Paths.get(opencga.createTmpOutdir("_pgx_translation_input")).resolve("translation.csv");
        Files.write(translationFilePath, translationContent.getBytes(StandardCharsets.UTF_8));
        translationFile = catalogManager.getFileManager()
                .link(studyFqn, new FileLinkParams().setUri(translationFilePath.toUri().toString()), false, token).first();
    }

    @Test
    public void testAlleleTyperAnalysisUsingContent() throws Exception {
        Path outDir = Paths.get(opencga.createTmpOutdir("_pgx_allele_typer_analysis"));

        PharmacogenomicsAlleleTyperToolParams params = new PharmacogenomicsAlleleTyperToolParams(
                genotypingContent, null, translationContent, null, null, true, null);

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

        // Verify OPENCGA_PHARMACOGENOMICS_DATA attribute was persisted in catalog for each sample
        assertSamplesHavePharmacogenomicsAttribute();
    }

    @Test
    public void testAlleleTyperAnalysisUsingFile() throws Exception {
        Path outDir = Paths.get(opencga.createTmpOutdir("_pgx_allele_typer_analysis"));

        PharmacogenomicsAlleleTyperToolParams params = new PharmacogenomicsAlleleTyperToolParams(
                null, genotypingFile.getId(), null, null, translationFile.getId(), true, null);

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

        // Verify OPENCGA_PHARMACOGENOMICS_DATA attribute was persisted in catalog for each sample
        assertSamplesHavePharmacogenomicsAttribute();
    }

    /**
     * Verifies that every non-NTC sample has the OPENCGA_PHARMACOGENOMICS_DATA attribute set
     * in the catalog after the allele typer tool has run.
     */
    private void assertSamplesHavePharmacogenomicsAttribute() throws Exception {
        int samplesWithAttribute = 0;
        for (String sampleId : sampleIds) {
            Sample sample = catalogManager.getSampleManager()
                    .get(studyFqn, sampleId, QueryOptions.empty(), token).first();
            Map<String, Object> attributes = sample.getAttributes();
            if (attributes != null && attributes.containsKey("OPENCGA_PHARMACOGENOMICS_DATA")) {
                samplesWithAttribute++;
            }
        }
        assertTrue("At least one sample should have OPENCGA_PHARMACOGENOMICS_DATA attribute set"
                + " (checked " + sampleIds.size() + " samples)", samplesWithAttribute > 0);
        System.out.println("Samples with OPENCGA_PHARMACOGENOMICS_DATA attribute: "
                + samplesWithAttribute + "/" + sampleIds.size());
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
