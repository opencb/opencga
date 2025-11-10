package org.opencb.opencga.analysis.clinical.pipeline;

import org.junit.*;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.analysis.variant.OpenCGATestExternalResource;
import org.opencb.opencga.analysis.wrappers.clinicalpipeline.ClinicalPipelineUtils;
import org.opencb.opencga.analysis.wrappers.clinicalpipeline.affy.AffyClinicalPipelineWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.clinicalpipeline.genomics.GenomicsClinicalPipelineWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.clinicalpipeline.prepare.PrepareClinicalPipelineWrapperAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.pipeline.affy.AffyClinicalPipelineParams;
import org.opencb.opencga.core.models.clinical.pipeline.affy.AffyClinicalPipelineWrapperParams;
import org.opencb.opencga.core.models.clinical.pipeline.affy.AffyPipelineConfig;
import org.opencb.opencga.core.models.clinical.pipeline.genomics.GenomicsClinicalPipelineParams;
import org.opencb.opencga.core.models.clinical.pipeline.genomics.GenomicsClinicalPipelineWrapperParams;
import org.opencb.opencga.core.models.clinical.pipeline.genomics.GenomicsPipelineConfig;
import org.opencb.opencga.core.models.clinical.pipeline.prepare.PrepareClinicalPipelineParams;
import org.opencb.opencga.core.models.clinical.pipeline.prepare.PrepareClinicalPipelineWrapperParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.job.JobType;
import org.opencb.opencga.core.models.operations.variant.VariantIndexParams;
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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(MediumTests.class)
public class ClinicalPipelineWrapperAnalysisTest {

//    private static AbstractClinicalManagerTest clinicalTest;
//    private static ResourceManager resourceManager;

    private final static Path NGS_PIPELINE_DATA_PATH = Paths.get("/opt/ngs-pipeline");

    private final String organizationId = "test";
    private final String projectId = "Project1";
    private final String studyId = "Study1";

    private String userId = "user";
    private String password = "Password1234;";
    private String token;

    private ToolRunner toolRunner;

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

        toolRunner = new ToolRunner(opencga.getOpencgaHome().toString(), opencga.getCatalogManager(),
                StorageEngineFactory.get(opencga.getVariantStorageManager().getStorageConfiguration()));
    }


    @After
    public void tearDown() throws Exception {
        opencga.clear();
    }

    @Test
    public void testClinicalPipelinePrepareUrl() throws IOException, ToolException, CatalogException {
        System.out.println("opencga.getOpencgaHome() = " + opencga.getOpencgaHome().toAbsolutePath());

        Path outDir = Paths.get(opencga.createTmpOutdir("_clinical_pipeline_prepare"));
        System.out.println("outDir = " + outDir.toAbsolutePath());

        String refBasename = "Homo_sapiens.GRCh38.dna.chromosome.MT";

        //---------------------------------
        // Prepare NGS pipeline
        //---------------------------------

        PrepareClinicalPipelineWrapperParams prepareWrapperParams = new PrepareClinicalPipelineWrapperParams();
        PrepareClinicalPipelineParams prepareParams = new PrepareClinicalPipelineParams();
        prepareParams.setReferenceGenome("https://ftp.ensembl.org/pub/release-115/fasta/homo_sapiens/dna/" + refBasename + ".fa.gz");
        prepareParams.setIndexes(Collections.singletonList(ClinicalPipelineUtils.BWA_INDEX));
        prepareWrapperParams.setPipelineParams(prepareParams);

        execute(PrepareClinicalPipelineWrapperAnalysis.ID, "prepare", prepareWrapperParams.toParams(), outDir);

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
    public void testClinicalPipelineGenomics() throws IOException, ToolException, CatalogException, StorageEngineException {
        Assume.assumeTrue(isDataAvailable());
        System.out.println("opencga.getOpencgaHome() = " + opencga.getOpencgaHome().toAbsolutePath());

        Path outDir = Paths.get(opencga.createTmpOutdir("_clinical_pipeline_prepare"));
        System.out.println("outDir = " + outDir.toAbsolutePath());

        // Get the pipeline parameters from the json file in resources and load them into an ObjectMap
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("ngspipeline/pipeline.json");
        GenomicsPipelineConfig ngsPipeline = JacksonUtils.getDefaultObjectMapper().readerFor(GenomicsPipelineConfig.class).readValue(inputStream);
        // Remove the tool freebayes of the step variant-calling for the test
        ngsPipeline.getSteps().setVariantCalling(ngsPipeline.getSteps().getVariantCalling().setTools(
                ngsPipeline.getSteps().getVariantCalling().getTools().stream().filter(t -> !t.getId().equals("freebayes")).collect(Collectors.toList())
        ));

        String refBasename = "Homo_sapiens.GRCh38.dna.primary_assembly";

        // Reference genome file
        Path refLocalPath = NGS_PIPELINE_DATA_PATH.resolve(refBasename + ".fa.gz");
        inputStream = Files.newInputStream(refLocalPath);
        File refFile = opencga.getCatalogManager().getFileManager().upload(studyId, inputStream,
                new File().setPath("data/" + refLocalPath.getFileName()), false, true, false, token).first();
        System.out.println("refFile.getUri() = " + refFile.getUri());

        // FastQ #1
        Path fastq1LocalPath = NGS_PIPELINE_DATA_PATH.resolve("HI.4019.002.index_7.ANN0831_R1.fastq.gz");
        inputStream = Files.newInputStream(fastq1LocalPath);
        File fastq1File = opencga.getCatalogManager().getFileManager().upload(studyId, inputStream,
                new File().setPath("data/" + fastq1LocalPath.getFileName()), false, true, false, token).first();
        System.out.println("fastq1File.getUri() = " + fastq1File.getUri());

        // FastQ #2
        Path fastq2LocalPath = NGS_PIPELINE_DATA_PATH.resolve("HI.4019.002.index_7.ANN0831_R2.fastq.gz");
        inputStream = Files.newInputStream(fastq1LocalPath);
        File fastq2File = opencga.getCatalogManager().getFileManager().upload(studyId, inputStream,
                new File().setPath("data/" + fastq2LocalPath.getFileName()), false, true, false, token).first();
        System.out.println("fastq2File.getUri() = " + fastq2File.getUri());

        //---------------------------------
        // Prepare clinical pipeline
        //---------------------------------

        PrepareClinicalPipelineWrapperParams prepareWrapperParams = new PrepareClinicalPipelineWrapperParams();
        PrepareClinicalPipelineParams prepareParams = new PrepareClinicalPipelineParams();
        prepareParams.setReferenceGenome(refFile.getId());
        prepareParams.setIndexes(Collections.singletonList(ClinicalPipelineUtils.BWA_INDEX));
        prepareWrapperParams.setPipelineParams(prepareParams);

        execute(PrepareClinicalPipelineWrapperAnalysis.ID, "prepare-genomics", prepareWrapperParams.toParams(), outDir);

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
        // Run clinical genomics pipeline
        //---------------------------------

        Path outDir2 = Paths.get(opencga.createTmpOutdir("_ngs_pipeline_genomics"));
        System.out.println("outDir2 = " + outDir2.toAbsolutePath());

        // Link in OpenCGA catalog the outdir created in the previous step as indexDir for the run step
        File indexDirFile = opencga.getCatalogManager().getFileManager().link(studyId, new FileLinkParams(outDir.toAbsolutePath().toString(),
                "", "", "", null, null, null, null, null), false, token).first();

        GenomicsClinicalPipelineWrapperParams genomicsWrapperParams = new GenomicsClinicalPipelineWrapperParams();
        GenomicsClinicalPipelineParams genomicsParams = new GenomicsClinicalPipelineParams();
        // Set sample with the two fastq files
        genomicsParams.setSamples(Collections.singletonList("ANN0831" + ClinicalPipelineUtils.SAMPLE_FIELD_SEP
                + fastq1File.getId() + ClinicalPipelineUtils.SAMPLE_FILE_SEP + fastq2File.getId()));
        // Set index dir
        genomicsParams.setIndexDir(indexDirFile.getId());
        // Set pipeline config
        genomicsParams.setPipeline(ngsPipeline);
        // Variant index parameters
        VariantIndexParams variantIndexParams = new VariantIndexParams();
        variantIndexParams.setAnnotate(true);
        variantIndexParams.setCalculateStats(true);
        genomicsParams.setVariantIndexParams(variantIndexParams);

        genomicsWrapperParams.setPipelineParams(genomicsParams);

        execute(GenomicsClinicalPipelineWrapperAnalysis.ID, "run-genomics", genomicsWrapperParams.toParams(), outDir2);

        VariantQueryResult<Variant> variantQueryResult = opencga.getVariantStorageManager()
                .get(new Query(ParamConstants.STUDY_PARAM, studyId), QueryOptions.empty(), token);
        for (Variant variant : variantQueryResult.getResults()) {
            System.out.println(variant.toStringSimple());
        }
        System.out.println("variantQueryResult.getNumResults() = " + variantQueryResult.getNumResults());
        assertEquals(10, variantQueryResult.getNumResults());
    }

    @Test
    public void testClinicalPipelineGenomicsRef() throws IOException, ToolException, CatalogException, StorageEngineException {
        Assume.assumeTrue(isDataAvailable());
        System.out.println("opencga.getOpencgaHome() = " + opencga.getOpencgaHome().toAbsolutePath());

        Path outDir = Paths.get(opencga.createTmpOutdir("_clinical_pipeline_prepare"));
        System.out.println("outDir = " + outDir.toAbsolutePath());

        // Get the pipeline parameters from the json file in resources and load them into an ObjectMap
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("ngspipeline/pipeline.json");
        GenomicsPipelineConfig ngsPipeline = JacksonUtils.getDefaultObjectMapper().readerFor(GenomicsPipelineConfig.class).readValue(inputStream);
        // Remove the tool freebayes of the step variant-calling for the test
        ngsPipeline.getSteps().setVariantCalling(ngsPipeline.getSteps().getVariantCalling().setTools(
                ngsPipeline.getSteps().getVariantCalling().getTools().stream().filter(t -> !t.getId().equals("freebayes")).collect(Collectors.toList())
        ));

        String refBasename = "Homo_sapiens.GRCh38.dna.primary_assembly";

        // Reference genome file
        Path refLocalPath = NGS_PIPELINE_DATA_PATH.resolve(refBasename + ".fa.gz");
        inputStream = Files.newInputStream(refLocalPath);
        File refFile = opencga.getCatalogManager().getFileManager().upload(studyId, inputStream,
                new File().setPath("data/" + refLocalPath.getFileName()), false, true, false, token).first();
        System.out.println("refFile.getUri() = " + refFile.getUri());

        // FastQ #1
        Path fastq1LocalPath = NGS_PIPELINE_DATA_PATH.resolve("HI.4019.002.index_7.ANN0831_R1.fastq.gz");
        inputStream = Files.newInputStream(fastq1LocalPath);
        File fastq1File = opencga.getCatalogManager().getFileManager().upload(studyId, inputStream,
                new File().setPath("data/" + fastq1LocalPath.getFileName()), false, true, false, token).first();
        System.out.println("fastq1File.getUri() = " + fastq1File.getUri());

        // FastQ #2
        Path fastq2LocalPath = NGS_PIPELINE_DATA_PATH.resolve("HI.4019.002.index_7.ANN0831_R2.fastq.gz");
        inputStream = Files.newInputStream(fastq1LocalPath);
        File fastq2File = opencga.getCatalogManager().getFileManager().upload(studyId, inputStream,
                new File().setPath("data/" + fastq2LocalPath.getFileName()), false, true, false, token).first();
        System.out.println("fastq2File.getUri() = " + fastq2File.getUri());

        //---------------------------------
        // Prepare clinical pipeline
        //---------------------------------

        PrepareClinicalPipelineWrapperParams prepareWrapperParams = new PrepareClinicalPipelineWrapperParams();
        PrepareClinicalPipelineParams prepareParams = new PrepareClinicalPipelineParams();
        prepareParams.setReferenceGenome(refFile.getId());
        prepareParams.setIndexes(Collections.singletonList(ClinicalPipelineUtils.BWA_INDEX));
        prepareWrapperParams.setPipelineParams(prepareParams);

        execute(PrepareClinicalPipelineWrapperAnalysis.ID, "prepare-genomics-ref", prepareWrapperParams.toParams(), outDir);

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
        // Run clinical genomics pipeline
        //---------------------------------

        Path outDir2 = Paths.get(opencga.createTmpOutdir("_ngs_pipeline_genomics"));
        System.out.println("outDir2 = " + outDir2.toAbsolutePath());

        // Link in OpenCGA catalog the outdir created in the previous step as indexDir for the run step
        File indexDirFile = opencga.getCatalogManager().getFileManager().link(studyId, new FileLinkParams(outDir.toAbsolutePath().toString(),
                "", "", "", null, null, null, null, null), false, token).first();

        GenomicsClinicalPipelineWrapperParams genomicsWrapperParams = new GenomicsClinicalPipelineWrapperParams();
        GenomicsClinicalPipelineParams genomicsParams = new GenomicsClinicalPipelineParams();
        // Set sample with the two fastq files
        genomicsParams.setSamples(Collections.singletonList("ANN0831" + ClinicalPipelineUtils.SAMPLE_FIELD_SEP
                + fastq1File.getId() + ClinicalPipelineUtils.SAMPLE_FILE_SEP + fastq2File.getId()));
        // Set index dir
        genomicsParams.setIndexDir(indexDirFile.getId());
        // Set pipeline config
        genomicsParams.setPipeline(ngsPipeline);
        // Variant index parameters
        VariantIndexParams variantIndexParams = new VariantIndexParams();
        variantIndexParams.setAnnotate(true);
        variantIndexParams.setCalculateStats(true);
        genomicsParams.setVariantIndexParams(variantIndexParams);

        genomicsWrapperParams.setPipelineParams(genomicsParams);

        execute(GenomicsClinicalPipelineWrapperAnalysis.ID, "run-genomics-ref", genomicsWrapperParams.toParams(), outDir2);

        VariantQueryResult<Variant> variantQueryResult = opencga.getVariantStorageManager()
                .get(new Query(ParamConstants.STUDY_PARAM, studyId), QueryOptions.empty(), token);
        for (Variant variant : variantQueryResult.getResults()) {
            System.out.println(variant.toStringSimple());
        }
        System.out.println("variantQueryResult.getNumResults() = " + variantQueryResult.getNumResults());
        assertEquals(10, variantQueryResult.getNumResults());
    }

    @Test
    public void testClinicalPipelineAffyDataDir() throws IOException, ToolException, CatalogException, StorageEngineException {
        Assume.assumeTrue(isDataAvailable());
        System.out.println("opencga.getOpencgaHome() = " + opencga.getOpencgaHome().toAbsolutePath());

        Path outDir = Paths.get(opencga.createTmpOutdir("_clinical_pipeline_affy"));
        System.out.println("outDir = " + outDir.toAbsolutePath());

        // Get the pipeline parameters from the json file in resources and load them into an ObjectMap
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("ngspipeline/affy-pipeline.json");
        AffyPipelineConfig ngsPipeline = JacksonUtils.getDefaultObjectMapper().readerFor(AffyPipelineConfig.class).readValue(inputStream);

        Path localPath;

        // CEL files
        List<File> opencgaCelFiles = new ArrayList<>();
        String dataDirname = "data";
        List<String> filenames = Arrays.asList("Axiom_KU8_A01.CEL"); // "1521b99hpp_av06.CEL.gz", "1532a99hpp_av04.CEL.gz");
        for (String filename: filenames) {
            localPath = NGS_PIPELINE_DATA_PATH.resolve("affy").resolve(filename);
            inputStream = Files.newInputStream(localPath);
            File ceFile = opencga.getCatalogManager().getFileManager().upload(studyId, inputStream,
                    new File().setPath(dataDirname + "/" + localPath.getFileName()), false, true, false, token).first();
            System.out.println("ceFile.getUri() = " + ceFile.getUri());
            opencgaCelFiles.add(ceFile);
        }
        File dataDirFile = opencga.getCatalogManager().getFileManager().get(studyId, dataDirname, QueryOptions.empty(), token).first();
        System.out.println("dataDirFile.getUri() = " + dataDirFile.getUri());

        // Preparing index dir
        String indexDirname = "affy-index";
        filenames = Arrays.asList("Axiom_KU8.r2.spf", "Axiom_KU8.r2.generic_prior.txt", "Axiom_KU8.na36.r1.a2.annot.csv",
                "Homo_sapiens.GRCh38.dna.primary_assembly.fa", "Homo_sapiens.GRCh38.dna.primary_assembly.fa.fai");
        for (String filename : filenames) {
            localPath = NGS_PIPELINE_DATA_PATH.resolve("affy").resolve(filename);
            inputStream = Files.newInputStream(localPath);
            File opencgaFile = opencga.getCatalogManager().getFileManager().upload(studyId, inputStream,
                    new File().setPath(indexDirname + "/" + localPath.getFileName()), false, true, false, token).first();
            System.out.println("opencgaFile.getUri() = " + opencgaFile.getUri());
        }
        File indexDirFile = opencga.getCatalogManager().getFileManager().get(studyId, indexDirname, QueryOptions.empty(), token).first();
        System.out.println("indexDirFile.getUri() = " + indexDirFile.getUri());

        //---------------------------------
        // Run affy clinical pipeline
        //---------------------------------

        AffyClinicalPipelineWrapperParams affyWrapperParams = new AffyClinicalPipelineWrapperParams();
        AffyClinicalPipelineParams affyParams = new AffyClinicalPipelineParams();
        affyParams.setDataDir(dataDirFile.getId());
        affyParams.setIndexDir(indexDirFile.getId());
        affyParams.setPipeline(ngsPipeline);
        affyParams.setVariantIndexParams(null);
        affyWrapperParams.setPipelineParams(affyParams);

        execute(AffyClinicalPipelineWrapperAnalysis.ID, "affy-pipeline-job", affyWrapperParams.toParams(), outDir);

        VariantQueryResult<Variant> variantQueryResult = opencga.getVariantStorageManager()
                .get(new Query(ParamConstants.STUDY_PARAM, studyId), QueryOptions.empty(), token);
        for (Variant variant : variantQueryResult.getResults()) {
            System.out.println(variant.toStringSimple());
        }
        System.out.println("variantQueryResult.getNumResults() = " + variantQueryResult.getNumResults());
        assertEquals(10, variantQueryResult.getNumResults());
    }

    private void execute(String toolId, String inputJobId, Map<String, Object> params, Path outDir) throws CatalogException, ToolException {
        String jobId = opencga.getCatalogManager().getJobManager().submit(studyId, JobType.NATIVE_TOOL, toolId,
                Enums.Priority.MEDIUM, params, inputJobId, "", null, null, null, null, false, token).first().getId();
        Assert.assertEquals(inputJobId, jobId);
        Job job = opencga.getCatalogManager().getJobManager().get(studyId, jobId, null, token).first();
        toolRunner.execute(studyId, job, outDir, token);
    }

    private boolean isDataAvailable() {
        if (!Files.exists(NGS_PIPELINE_DATA_PATH) || !Files.isDirectory(NGS_PIPELINE_DATA_PATH)) {
            return false;
        }

        List<String> filenames = Arrays.asList("Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz",
                "HI.4019.002.index_7.ANN0831_R1.fastq.gz",
                "HI.4019.002.index_7.ANN0831_R2.fastq.gz");
        for (String filename : filenames) {
            Path path = NGS_PIPELINE_DATA_PATH.resolve(filename);
            if (!Files.exists(path) || path.toFile().length() == 0 || !Files.isRegularFile(path)) {
                return false;
            }
        }
        return true;
    }
}

