package org.opencb.opencga.analysis.clinical.ngspipeline;

import org.junit.*;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.analysis.variant.OpenCGATestExternalResource;
import org.opencb.opencga.analysis.wrappers.ngspipeline.NgsPipelineWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.ngspipeline.NgsPipelineWrapperAnalysisExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.NgsPipelineWrapperParams;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.project.ProjectCreateParams;
import org.opencb.opencga.core.models.project.ProjectOrganism;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.variant.VariantSetupParams;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.query.VariantQueryResult;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(MediumTests.class)
public class NgsPipelineWrapperAnalysisTest {

//    private static AbstractClinicalManagerTest clinicalTest;
//    private static ResourceManager resourceManager;

    private final String organizationId = "test";
    private final String projectId = "Project1";
    private final String studyId = "Study1";

    private String userId = "user";
    private String password = "Password1234;";
    private String token;

    @Rule
    public OpenCGATestExternalResource opencga = new OpenCGATestExternalResource(true);

    @Before
    public void setUp() throws Exception {
        opencga.clear();

        CatalogManager catalogManager = opencga.getCatalogManager();

        // Create new organization, owner and admins
        catalogManager.getOrganizationManager().create(new OrganizationCreateParams().setId(organizationId).setName("Test"), QueryOptions.empty(), opencga.getAdminToken());
        catalogManager.getUserManager().create(userId, "User Name", "mail@ebi.ac.uk", password, organizationId, null, opencga.getAdminToken());

        catalogManager.getOrganizationManager().update(organizationId,
                new OrganizationUpdateParams().setOwner("user"),
                null, opencga.getAdminToken());

        token = catalogManager.getUserManager().login(organizationId, userId, password).first().getToken();
        System.out.println("token = " + token);

        QueryOptions queryOptions = new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true);

        ProjectCreateParams projectCreateParams = new ProjectCreateParams()
                .setId(projectId)
                .setOrganism(new ProjectOrganism("hsapiens", "grch38"));
        Project project = catalogManager.getProjectManager().create(projectCreateParams, queryOptions, token).first();
        System.out.println("project.getFqn() = " + project.getFqn());

        Study study = catalogManager.getStudyManager().create(projectId, new Study().setId(studyId), queryOptions, token).first();
        System.out.println("study.getFqn() = " + study.getFqn());

        // Variant setup
        VariantSetupParams variantSetupParams = new VariantSetupParams();
        variantSetupParams.setExpectedFiles(10);
        variantSetupParams.setExpectedSamples(4);
        variantSetupParams.setAverageFileSize("10MB");
        opencga.getVariantStorageManager().variantSetup(study.getFqn(), variantSetupParams, token);
    }


    @After
    public void tearDown() throws Exception {
        opencga.clear();
    }

    @Test
    public void testNgsPipelinePrepareRefUrl() throws IOException, ToolException, CatalogException {
        System.out.println("opencga.getOpencgaHome() = " + opencga.getOpencgaHome().toAbsolutePath());

        Path outDir = Paths.get(opencga.createTmpOutdir("_ngs_pipeline_prepare"));
        System.out.println("outDir = " + outDir.toAbsolutePath());

        String refBasename = "Homo_sapiens.GRCh38.dna.chromosome.MT";

        //---------------------------------
        // Prepare NGS pipeline
        //---------------------------------

        NgsPipelineWrapperParams params = new NgsPipelineWrapperParams();
        params.setCommand(NgsPipelineWrapperAnalysisExecutor.PREPARE_CMD);
        params.setInput(Collections.singletonList("https://ftp.ensembl.org/pub/release-115/fasta/homo_sapiens/dna/" + refBasename + ".fa.gz"));
        params.setPrepareIndices(Arrays.asList(NgsPipelineWrapperAnalysisExecutor.PREPARE_INDEX_REFERENCE_GENOME, NgsPipelineWrapperAnalysisExecutor.PREPARE_INDEX_BWA));

        ToolRunner toolRunner = new ToolRunner(opencga.getOpencgaHome().toString(), opencga.getCatalogManager(),
                StorageEngineFactory.get(opencga.getVariantStorageManager().getStorageConfiguration()));

        toolRunner.execute(NgsPipelineWrapperAnalysis.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, studyId), outDir, null,
                false, token);

        // Check reference genome index files
        assertTrue(Files.exists(outDir.resolve("reference-genome-index")));
        assertTrue(Files.exists(outDir.resolve("reference-genome-index").resolve(refBasename + ".fa")));
        assertTrue(Files.exists(outDir.resolve("reference-genome-index").resolve(refBasename + ".fa.fai")));
        assertTrue(Files.exists(outDir.resolve("reference-genome-index").resolve(refBasename + ".dict")));

        // Check BWA index files
        assertTrue(Files.exists(outDir.resolve("bwa-index")));
        assertTrue(Files.exists(outDir.resolve("bwa-index").resolve(refBasename + ".fa")));
        assertTrue(Files.exists(outDir.resolve("bwa-index").resolve(refBasename + ".fa.pac")));
        assertTrue(Files.exists(outDir.resolve("bwa-index").resolve(refBasename + ".fa.bwt")));
        assertTrue(Files.exists(outDir.resolve("bwa-index").resolve(refBasename + ".fa.ann")));
        assertTrue(Files.exists(outDir.resolve("bwa-index").resolve(refBasename + ".fa.amb")));
        assertTrue(Files.exists(outDir.resolve("bwa-index").resolve(refBasename + ".fa.sa")));
    }

    @Test
    public void testNgsPipelineRefFile() throws IOException, ToolException, CatalogException, StorageEngineException {
        Path ngsPipelinePath = Paths.get("/opt/ngs-pipeline");
        Assume.assumeTrue(Files.exists(ngsPipelinePath));
        System.out.println("opencga.getOpencgaHome() = " + opencga.getOpencgaHome().toAbsolutePath());

        Path outDir = Paths.get(opencga.createTmpOutdir("_ngs_pipeline_ref_file_prepare"));
        System.out.println("outDir = " + outDir.toAbsolutePath());

        // Get the pipeline parameters from the json file in resources and load them into an ObjectMap
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("ngspipeline/pipeline.json");
        ObjectMap ngsPipelineParams = JacksonUtils.getDefaultObjectMapper().readerFor(ObjectMap.class).readValue(inputStream);

        String refBasename = "Homo_sapiens.GRCh38.dna.primary_assembly";

        // Referenge genome file
        Path refLocalPath = ngsPipelinePath.resolve(refBasename + ".fa.gz");
        inputStream = Files.newInputStream(refLocalPath);
        File refFile = opencga.getCatalogManager().getFileManager().upload(studyId, inputStream,
                new File().setPath("data/" + refLocalPath.getFileName()), false, true, false, token).first();
        System.out.println("refFile.getUri() = " + refFile.getUri());

        // FastQ #1
        Path fastq1LocalPath = ngsPipelinePath.resolve("HI.4019.002.index_7.ANN0831_R1.fastq.gz");
        inputStream = Files.newInputStream(fastq1LocalPath);
        File fastq1File = opencga.getCatalogManager().getFileManager().upload(studyId, inputStream,
                new File().setPath("data/" + fastq1LocalPath.getFileName()), false, true, false, token).first();
        System.out.println("fastq1File.getUri() = " + fastq1File.getUri());

        // FastQ #2
        Path fastq2LocalPath = ngsPipelinePath.resolve("HI.4019.002.index_7.ANN0831_R2.fastq.gz");
        inputStream = Files.newInputStream(fastq1LocalPath);
        File fastq2File = opencga.getCatalogManager().getFileManager().upload(studyId, inputStream,
                new File().setPath("data/" + fastq2LocalPath.getFileName()), false, true, false, token).first();
        System.out.println("fastq2File.getUri() = " + fastq2File.getUri());

        //---------------------------------
        // Prepare NGS pipeline
        //---------------------------------

        NgsPipelineWrapperParams params = new NgsPipelineWrapperParams();
        params.setCommand(NgsPipelineWrapperAnalysisExecutor.PREPARE_CMD);
        params.setInput(Collections.singletonList(refFile.getId()));
        params.setPrepareIndices(Arrays.asList(NgsPipelineWrapperAnalysisExecutor.PREPARE_INDEX_REFERENCE_GENOME, NgsPipelineWrapperAnalysisExecutor.PREPARE_INDEX_BWA));
        params.setPipelineParams(ngsPipelineParams);

        ToolRunner toolRunner = new ToolRunner(opencga.getOpencgaHome().toString(), opencga.getCatalogManager(),
                StorageEngineFactory.get(opencga.getVariantStorageManager().getStorageConfiguration()));

        toolRunner.execute(NgsPipelineWrapperAnalysis.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, studyId), outDir, null,
                false, token);

        // Check reference genome index files
        assertTrue(Files.exists(outDir.resolve("reference-genome-index")));
        assertTrue(Files.exists(outDir.resolve("reference-genome-index").resolve(refBasename + ".fa")));
        assertTrue(Files.exists(outDir.resolve("reference-genome-index").resolve(refBasename + ".fa.fai")));
        assertTrue(Files.exists(outDir.resolve("reference-genome-index").resolve(refBasename + ".dict")));

        // Check BWA index files
        assertTrue(Files.exists(outDir.resolve("bwa-index")));
        assertTrue(Files.exists(outDir.resolve("bwa-index").resolve(refBasename + ".fa")));
        assertTrue(Files.exists(outDir.resolve("bwa-index").resolve(refBasename + ".fa.pac")));
        assertTrue(Files.exists(outDir.resolve("bwa-index").resolve(refBasename + ".fa.bwt")));
        assertTrue(Files.exists(outDir.resolve("bwa-index").resolve(refBasename + ".fa.ann")));
        assertTrue(Files.exists(outDir.resolve("bwa-index").resolve(refBasename + ".fa.amb")));
        assertTrue(Files.exists(outDir.resolve("bwa-index").resolve(refBasename + ".fa.sa")));

        //---------------------------------
        // Run NGS pipeline
        //---------------------------------

        Path outDir2 = Paths.get(opencga.createTmpOutdir("_ngs_pipeline_run_ref_file"));
        System.out.println("outDir2 = " + outDir2.toAbsolutePath());

        // Link in OpenCGA catalog the outdir created in the previous step as indexDir for the run step
        File indexDirFile = opencga.getCatalogManager().getFileManager().link(studyId, new FileLinkParams(outDir.toAbsolutePath().toString(),
                "", "", "", null, null, null, null, null), false, token).first();

        params = new NgsPipelineWrapperParams();
        params.setCommand(NgsPipelineWrapperAnalysisExecutor.PIPELINE_CMD);
        params.setInput(Arrays.asList(fastq1File.getId(), fastq2File.getId()));
        params.setIndexDir(indexDirFile.getId());
        params.setPipelineParams(ngsPipelineParams);

        toolRunner = new ToolRunner(opencga.getOpencgaHome().toString(), opencga.getCatalogManager(),
                StorageEngineFactory.get(opencga.getVariantStorageManager().getStorageConfiguration()));

        toolRunner.execute(NgsPipelineWrapperAnalysis.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, studyId), outDir2, null,
                false, token);


        VariantQueryResult<Variant> variantQueryResult = opencga.getVariantStorageManager()
                .get(new Query(ParamConstants.STUDY_PARAM, studyId), QueryOptions.empty(), token);
        for (Variant variant : variantQueryResult.getResults()) {
            System.out.println(variant.toStringSimple());
        }
        System.out.println("variantQueryResult.getNumResults() = " + variantQueryResult.getNumResults());
        assertEquals(10, variantQueryResult.getNumResults());
    }
}
