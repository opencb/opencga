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

import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.analysis.alignment.qc.AlignmentGeneCoverageStatsAnalysis;
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
import org.opencb.opencga.core.models.alignment.CoverageIndexParams;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.models.file.FileRelatedFile;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
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

import static org.junit.Assert.assertEquals;

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

//            file = opencga.createFile(STUDY, "variant-test-file.vcf.gz", token);
//            variantStorageManager.index(STUDY, file.getId(), opencga.createTmpOutdir("_index"), new ObjectMap(VariantStorageOptions.ANNOTATE.key(), true), token);

//            for (int i = 0; i < file.getSampleIds().size(); i++) {
//                String id = file.getSampleIds().get(i);
//                if (id.equals(son)) {
//                    SampleUpdateParams updateParams = new SampleUpdateParams().setSomatic(true);
//                    catalogManager.getSampleManager().update(STUDY, id, updateParams, null, token);
//                }
//                if (i % 2 == 0) {
//                    SampleUpdateParams updateParams = new SampleUpdateParams().setPhenotypes(Collections.singletonList(PHENOTYPE));
//                    catalogManager.getSampleManager().update(STUDY, id, updateParams, null, token);
//                }
//            }

//            catalogManager.getCohortManager().create(STUDY, new CohortCreateParams().setId("c1")
//                            .setSamples(file.getSampleIds().subList(0, 2).stream().map(s -> new SampleReferenceParam().setId(s)).collect(Collectors.toList())),
//                    null, null, null, token);
//            catalogManager.getCohortManager().create(STUDY, new CohortCreateParams().setId("c2")
//                            .setSamples(file.getSampleIds().subList(2, 4).stream().map(s -> new SampleReferenceParam().setId(s)).collect(Collectors.toList())),
//                    null, null, null, token);

//            Phenotype phenotype = new Phenotype("phenotype", "phenotype", "");
//            Disorder disorder = new Disorder("disorder", "disorder", "", "", Collections.singletonList(phenotype), Collections.emptyMap());
//            List<Individual> individuals = new ArrayList<>(4);
//
//            // Father
//            individuals.add(catalogManager.getIndividualManager()
//                    .create(STUDY, new Individual(father, father, new Individual(), new Individual(), new Location(), SexOntologyTermAnnotation.initMale(), null, null, null, null, "",
//                            Collections.emptyList(), false, 0, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), IndividualInternal.init(), Collections.emptyMap()), Collections.singletonList(father), new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token).first());
//            // Mother
//            individuals.add(catalogManager.getIndividualManager()
//                    .create(STUDY, new Individual(mother, mother, new Individual(), new Individual(), new Location(), SexOntologyTermAnnotation.initFemale(), null, null, null, null, "",
//                            Collections.emptyList(), false, 0, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), IndividualInternal.init(), Collections.emptyMap()), Collections.singletonList(mother), new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token).first());
//            // Son
//            individuals.add(catalogManager.getIndividualManager()
//                    .create(STUDY, new Individual(son, son, new Individual(), new Individual(), new Location(), SexOntologyTermAnnotation.initMale(), null, null, null, null, "",
//                            Collections.emptyList(), false, 0, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), IndividualInternal.init(), Collections.emptyMap()).setFather(individuals.get(0)).setMother(individuals.get(1)).setDisorders(Collections.singletonList(disorder)), Collections.singletonList(son), new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token).first());
//            // Daughter
//            individuals.add(catalogManager.getIndividualManager()
//                    .create(STUDY, new Individual(daughter, daughter, new Individual(), new Individual(), new Location(), SexOntologyTermAnnotation.initFemale(), null, null, null, null, "",
//                            Collections.emptyList(), false, 0, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), IndividualInternal.init(), Collections.emptyMap()).setFather(individuals.get(0)).setMother(individuals.get(1)), Collections.singletonList(daughter), new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token).first());
//            catalogManager.getFamilyManager().create(
//                    STUDY,
//                    new Family("f1", "f1", Collections.singletonList(phenotype), Collections.singletonList(disorder), null, null, 3, null, null),
//                    individuals.stream().map(Individual::getId).collect(Collectors.toList()), new QueryOptions(),
//                    token);
//
//            // Cancer (SV)
//            ObjectMap config = new ObjectMap();
////            config.put(VariantStorageOptions.ANNOTATE.key(), true);
//            config.put(VariantStorageOptions.LOAD_SPLIT_DATA.key(), VariantStorageEngine.SplitData.MULTI);
//
//            file = opencga.createFile(CANCER_STUDY, "AR2.10039966-01T_vs_AR2.10039966-01G.annot.brass.vcf.gz", token);
//            variantStorageManager.index(CANCER_STUDY, file.getId(), opencga.createTmpOutdir("_index"), config, token);
//            file = opencga.createFile(CANCER_STUDY, "AR2.10039966-01T.copynumber.caveman.vcf.gz", token);
//            variantStorageManager.index(CANCER_STUDY, file.getId(), opencga.createTmpOutdir("_index"), config, token);
//            file = opencga.createFile(CANCER_STUDY, "AR2.10039966-01T_vs_AR2.10039966-01G.annot.pindel.vcf.gz", token);
//            variantStorageManager.index(CANCER_STUDY, file.getId(), opencga.createTmpOutdir("_index"), config, token);
//
//            SampleUpdateParams updateParams = new SampleUpdateParams().setSomatic(true);
//            catalogManager.getSampleManager().update(CANCER_STUDY, cancer_sample, updateParams, null, token);

            opencga.getStorageConfiguration().getVariant().setDefaultEngine(storageEngine);
            VariantStorageEngine engine = opencga.getStorageEngineFactory().getVariantStorageEngine(storageEngine, DB_NAME);
//            if (storageEngine.equals(HadoopVariantStorageEngine.STORAGE_ENGINE_ID)) {
//                VariantHbaseTestUtils.printVariants(((VariantHadoopDBAdaptor) engine.getDBAdaptor()), Paths.get(opencga.createTmpOutdir("_hbase_print_variants")).toUri());
//            }
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

    public void setUpCatalogManager() throws CatalogException {
        catalogManager.getOrganizationManager().create(new OrganizationCreateParams().setId("test"), null, opencga.getAdminToken());
        catalogManager.getUserManager().create(new User().setId(USER).setName("User Name").setEmail("mail@ebi.ac.uk").setOrganization("test"),
                PASSWORD, opencga.getAdminToken());
        catalogManager.getOrganizationManager().update("test", new OrganizationUpdateParams().setOwner(USER), null, opencga.getAdminToken());

        token = catalogManager.getUserManager().login(ORGANIZATION, "user", PASSWORD).getToken();

        String projectId = catalogManager.getProjectManager().create(PROJECT, "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token).first().getId();
        catalogManager.getStudyManager().create(projectId, STUDY, null, "Phase 1", "Done", null, null, null, null, null, token);

        // Create 10 samples not indexed
//        for (int i = 0; i < 10; i++) {
//            Sample sample = new Sample().setId("SAMPLE_" + i);
//            if (i % 2 == 0) {
//                sample.setPhenotypes(Collections.singletonList(PHENOTYPE));
//            }
//            catalogManager.getSampleManager().create(STUDY, sample, null, token);
//        }
//
//        // Cancer
//        List<Sample> samples = new ArrayList<>();
//        catalogManager.getStudyManager().create(projectId, CANCER_STUDY, null, "Phase 1", "Done", null, null, null, null, null, token);
//        Sample sample = new Sample().setId(cancer_sample).setSomatic(true);
//        samples.add(sample);
////        catalogManager.getSampleManager().create(CANCER_STUDY, sample, null, token);
//        sample = new Sample().setId(germline_sample);
//        samples.add(sample);
////        catalogManager.getSampleManager().create(CANCER_STUDY, sample, null, token);
//        Individual individual = catalogManager.getIndividualManager()
//                .create(CANCER_STUDY, new Individual("AR2.10039966-01", "AR2.10039966-01", new Individual(), new Individual(), new Location(), SexOntologyTermAnnotation.initMale(), null, null, null, null, "",
//                        samples, false, 0, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), IndividualInternal.init(), Collections.emptyMap()), Collections.emptyList(), new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token).first();
//        assertEquals(2, individual.getSamples().size());
    }

    @Test
    public void geneCoverageStatsTest() throws IOException, ToolException, CatalogException {
        Path outdir = Paths.get(opencga.createTmpOutdir("_genecoveragestats"));

        // setup BAM files
        String bamFilename = opencga.getResourceUri("biofiles/HG00096.chrom20.small.bam").toString();
        String baiFilename = opencga.getResourceUri("biofiles/HG00096.chrom20.small.bam.bai").toString();
        //String bamFilename = getClass().getResource("/biofiles/NA19600.chrom20.small.bam").getFile();
        File bamFile = catalogManager.getFileManager().link(STUDY, new FileLinkParams(bamFilename, "", "", "", null, null, null,
                null, null), false, token).first();
         assertEquals(0, bamFile.getQualityControl().getCoverage().getGeneCoverageStats().size());

        AlignmentGeneCoverageStatsParams params = new AlignmentGeneCoverageStatsParams();
        params.setBamFile(bamFile.getId());
        String geneName = "BRCA2";
        params.setGenes(Arrays.asList(geneName));

        toolRunner.execute(AlignmentGeneCoverageStatsAnalysis.class, params, new ObjectMap(), outdir, "coverage-job-id", token);

        bamFile = catalogManager.getFileManager().link(STUDY, new FileLinkParams(bamFilename, "", "", "", null, null, null,
                null, null), false, token).first();
        assertEquals(1, bamFile.getQualityControl().getCoverage().getGeneCoverageStats().size());
        assertEquals(geneName, bamFile.getQualityControl().getCoverage().getGeneCoverageStats().get(0).getGeneName());
        assertEquals(10, bamFile.getQualityControl().getCoverage().getGeneCoverageStats().get(0).getStats().size());
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