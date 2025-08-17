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

package org.opencb.opencga.analysis.transcriptomics;

import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.analysis.variant.OpenCGATestExternalResource;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.wrappers.WrapperUtils;
import org.opencb.opencga.analysis.wrappers.star.StarWrapperAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.models.wrapper.star.StarWrapperParams;
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
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
@Category(MediumTests.class)
public class StarAnalysisTest {

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


    private static Path starDataPath = Paths.get("/opt/star-data");
    private static String genomeRelativePath = "genome";
    private static String genomeIndexRelativePath = "genome-index";
    private static String fastqRelativePath = "fastq";

    private static String fastaFilename = "Homo_sapiens.GRCh38.dna.chromosome.20.fa";
    private static String fastqFilename = "HG00096.chrom20.small.fastq";


    @Parameterized.Parameters(name = "{0}")
    public static Object[][] parameters() {
        return new Object[][]{
//                {MongoDBVariantStorageEngine.STORAGE_ENGINE_ID},
                {HadoopVariantStorageEngine.STORAGE_ENGINE_ID}
        };
    }

    public StarAnalysisTest(String storageEngine) {
        if (!storageEngine.equals(StarAnalysisTest.storageEngine)) {
            indexed = false;
        }
        StarAnalysisTest.storageEngine = storageEngine;
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
        token = catalogManager.getUserManager().login(ORGANIZATION, "user", PASSWORD).first().getToken();
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

        token = catalogManager.getUserManager().login(ORGANIZATION, "user", PASSWORD).first().getToken();

        String projectId = catalogManager.getProjectManager().create(PROJECT, "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), token).first().getId();
        catalogManager.getStudyManager().create(projectId, STUDY, null, "Phase 1", "Done", null, null, null, null, null, token);
    }

    @Test
    public void testStarGenerateGenome() throws IOException, ToolException, CatalogException {
        // Assume STAR data exists
        Assume.assumeTrue(Files.exists(starDataPath));

        Path datadir = Paths.get(opencga.createTmpOutdir("_start_generate_genome_data"));
        Path outdir = Paths.get(opencga.createTmpOutdir("_start_generate_genome"));

        // Prepare STAR data, copying it to the data dir
        prepareStarData(datadir);

        Path genomeIndexPath = datadir.resolve(genomeIndexRelativePath).toAbsolutePath();
        File genomeDir = catalogManager.getFileManager().link(STUDY, new FileLinkParams(genomeIndexPath.toString(), "", "", "", null, null,
                null, null, null), false, token).first();
        Path fastaFilePath = datadir.resolve(genomeRelativePath).resolve(fastaFilename).toAbsolutePath();
        File fastaFile = catalogManager.getFileManager().link(STUDY, new FileLinkParams(fastaFilePath.toString(), "", "", "", null, null,
                null, null, null), false, token).first();

        StarWrapperParams params = new StarWrapperParams();
        params.getStarParams().getOptions().put("--runMode", "genomeGenerate");
        params.getStarParams().getOptions().put("--genomeDir", genomeDir.getId());
        params.getStarParams().getOptions().put("--genomeFastaFiles", fastaFile.getId());
        params.getStarParams().getOptions().put("--runThreadN", "8");

        toolRunner.execute(StarWrapperAnalysis.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), outdir, "star-generate-genome", false, token);

        List<String> outFilenames = Arrays.asList("chrStart.txt", "chrName.txt", "chrNameLength.txt", "chrLength.txt",
                "genomeParameters.txt", "Genome", "SA", "SAindex", "Log.out");
        for (String outFilename : outFilenames) {
            Assert.assertTrue(Files.exists(outdir.resolve(outFilename)));
        }
    }

    @Test
    
    public void testStarAlignReads() throws IOException, ToolException, CatalogException {
        // Assume STAR data exists
        Assume.assumeTrue(Files.exists(starDataPath));

        Path datadir = Paths.get(opencga.createTmpOutdir("_start_align_readas_data"));
        Path outdir1 = Paths.get(opencga.createTmpOutdir("_start_generate_genome"));

        // Prepare STAR data, copying it to the data dir
        prepareStarData(datadir);

        Path genomeIndexPath = datadir.resolve(genomeIndexRelativePath).toAbsolutePath();
        File genomeDir = catalogManager.getFileManager().link(STUDY, new FileLinkParams(genomeIndexPath.toString(), "", "", "", null, null,
                null, null, null), false, token).first();
        Path fastaFilePath = datadir.resolve(genomeRelativePath).resolve(fastaFilename).toAbsolutePath();
        File fastaFile = catalogManager.getFileManager().link(STUDY, new FileLinkParams(fastaFilePath.toString(), "", "", "", null, null,
                null, null, null), false, token).first();

        StarWrapperParams params = new StarWrapperParams();
        params.getStarParams().getOptions().put("--runMode", "genomeGenerate");
        params.getStarParams().getOptions().put("--genomeDir", genomeDir.getId());
        params.getStarParams().getOptions().put("--genomeFastaFiles", fastaFile.getId());
        params.getStarParams().getOptions().put("--runThreadN", "8");

        toolRunner.execute(StarWrapperAnalysis.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), outdir1, "star-generate-genome", false, token);

        List<String> outFilenames = Arrays.asList("chrStart.txt", "chrName.txt", "chrNameLength.txt", "chrLength.txt",
                "genomeParameters.txt", "Genome", "SA", "SAindex", "Log.out");
        for (String outFilename : outFilenames) {
            Assert.assertTrue(Files.exists(outdir1.resolve(outFilename)));
        }

        Path outdir2 = Paths.get(opencga.createTmpOutdir("_start_align_reads"));

        File genomeIndexDir = catalogManager.getFileManager().link(STUDY, new FileLinkParams(outdir1.toString(), "", "", "", null, null,
                null, null, null), false, token).first();
        Path fastqFilePath = datadir.resolve(fastqRelativePath).resolve(fastqFilename).toAbsolutePath();
        File fastqFile = catalogManager.getFileManager().link(STUDY, new FileLinkParams(fastqFilePath.toString(), "", "", "", null, null,
                null, null, null), false, token).first();

        StarWrapperParams params2 = new StarWrapperParams();
        params2.getStarParams().getOptions().put("--runMode", "alignReads");
        params2.getStarParams().getOptions().put("--genomeDir", genomeIndexDir.getId());
        params2.getStarParams().getOptions().put("--readFilesIn", fastqFile.getId());

        toolRunner.execute(StarWrapperAnalysis.class, params2, new ObjectMap(ParamConstants.STUDY_PARAM, STUDY), outdir2, "star-align-reads", false, token);

    }

    private void prepareStarData(Path datadir) throws IOException {
        // Genome (FASTA files)
        Path path = Files.createDirectories(datadir.resolve(genomeRelativePath));
        Files.copy(starDataPath.resolve(genomeRelativePath).resolve(fastaFilename), path.resolve(fastaFilename));

        // Genome index (SA index files)
        Files.createDirectories(datadir.resolve(genomeIndexRelativePath));

        // Read (FASTQ files)
        path = Files.createDirectories(datadir.resolve(fastqRelativePath));
        Files.copy(starDataPath.resolve(fastqRelativePath).resolve(fastqFilename), path.resolve(fastqFilename));
    }
}