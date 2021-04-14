package org.opencb.opencga.analysis.rga;

import io.grpc.Context;
import jdk.nashorn.internal.parser.Token;
import org.apache.solr.client.solrj.SolrServerException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.rga.exceptions.RgaException;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.analysis.variant.OpenCGATestExternalResource;
import org.opencb.opencga.analysis.variant.knockout.KnockoutAnalysis;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByIndividual;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByVariant;
import org.opencb.opencga.core.models.analysis.knockout.RgaKnockoutByGene;
import org.opencb.opencga.core.models.cohort.CohortCreateParams;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualAclEntry;
import org.opencb.opencga.core.models.individual.IndividualAclParams;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleAclEntry;
import org.opencb.opencga.core.models.sample.SampleAclParams;
import org.opencb.opencga.core.models.sample.SampleUpdateParams;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.models.variant.KnockoutAnalysisParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.result.ExecutionResult;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RgaManagerTest {

    public static final String USER = "user";
    public static final String PASSWORD = "asdf";
    public static final String PROJECT = "project";
    public static final String STUDY = "study";
    public static final String PHENOTYPE_NAME = "myPhenotype";
    public static final Phenotype PHENOTYPE = new Phenotype(PHENOTYPE_NAME, PHENOTYPE_NAME, "mySource")
            .setStatus(Phenotype.Status.OBSERVED);
    public static final String DB_NAME = "opencga_test_" + USER + "_" + PROJECT;

    private CatalogManager catalogManager;
    private VariantStorageManager variantStorageManager;
    private RgaManager rgaManager;

    public static OpenCGATestExternalResource opencga = new OpenCGATestExternalResource();

    public static RgaSolrExtenalResource solr = new RgaSolrExtenalResource();

    private static String storageEngine = MongoDBVariantStorageEngine.STORAGE_ENGINE_ID;
    private static RgaEngine rgaEngine;
    private static boolean indexed = false;
    private static String token;
    private static File file;

    @Before
    public void setUp() throws Throwable {
        if (!indexed) {
            opencga.after();
            opencga.before();

            catalogManager = opencga.getCatalogManager();
            variantStorageManager = new VariantStorageManager(catalogManager, opencga.getStorageEngineFactory());

            opencga.clearStorageDB(DB_NAME);

            StorageConfiguration storageConfiguration = opencga.getStorageConfiguration();
            storageConfiguration.getVariant().setDefaultEngine(storageEngine);

            setUpCatalogManager();


            file = opencga.createFile(STUDY, "variant-test-file.vcf.gz", token);
            variantStorageManager.index(STUDY, file.getId(), opencga.createTmpOutdir("_index"), new ObjectMap(VariantStorageOptions.ANNOTATE.key(), true), token);

            for (int i = 0; i < file.getSampleIds().size(); i++) {
                if (i % 2 == 0) {
                    String id = file.getSampleIds().get(i);
                    SampleUpdateParams updateParams = new SampleUpdateParams().setPhenotypes(Collections.singletonList(PHENOTYPE));
                    catalogManager.getSampleManager().update(STUDY, id, updateParams, null, token);
                }
            }

            catalogManager.getCohortManager().create(STUDY, new CohortCreateParams().setId("c1").setSamples(file.getSampleIds().subList(0, 2)), null, null, null, token);
            catalogManager.getCohortManager().create(STUDY, new CohortCreateParams().setId("c2").setSamples(file.getSampleIds().subList(2, 4)), null, null, null, token);

            Phenotype phenotype = new Phenotype("phenotype", "phenotype", "");
            Disorder disorder = new Disorder("disorder", "disorder", "", "", Collections.singletonList(phenotype), Collections.emptyMap());
            List<Individual> individuals = new ArrayList<>(4);

            String father = "NA19661";
            String mother = "NA19660";
            String son = "NA19685";
            String daughter = "NA19600";
            // Father
            individuals.add(catalogManager.getIndividualManager()
                    .create(STUDY, new Individual(father, father, IndividualProperty.Sex.MALE, null, null, 0, Collections.emptyList(), Collections.emptyMap()), Collections.singletonList(father), null, token).first());
            // Mother
            individuals.add(catalogManager.getIndividualManager()
                    .create(STUDY, new Individual(mother, mother, IndividualProperty.Sex.FEMALE, null, null, 0, Collections.emptyList(), Collections.emptyMap()), Collections.singletonList(mother), null, token).first());
            // Son
            individuals.add(catalogManager.getIndividualManager()
                    .create(STUDY, new Individual(son, son, IndividualProperty.Sex.MALE, null, null, 0, Collections.emptyList(), Collections.emptyMap()).setFather(individuals.get(0)).setMother(individuals.get(1)).setDisorders(Collections.singletonList(disorder)), Collections.singletonList(son), null, token).first());
            // Daughter
            individuals.add(catalogManager.getIndividualManager()
                    .create(STUDY, new Individual(daughter, daughter, IndividualProperty.Sex.FEMALE, null, null, 0, Collections.emptyList(), Collections.emptyMap()).setFather(individuals.get(0)).setMother(individuals.get(1)), Collections.singletonList(daughter), null, token).first());
            catalogManager.getFamilyManager().create(
                    STUDY,
                    new Family("f1", "f1", Collections.singletonList(phenotype), Collections.singletonList(disorder), null, null, 3, null, null),
                    individuals.stream().map(Individual::getId).collect(Collectors.toList()), new QueryOptions(), token);

            solr.after();
            solr.before();

            rgaEngine = solr.configure(opencga.getStorageConfiguration());
        }

        catalogManager = opencga.getCatalogManager();
        variantStorageManager = new VariantStorageManager(catalogManager, opencga.getStorageEngineFactory());
        rgaManager = new RgaManager(catalogManager.getConfiguration(), opencga.getStorageConfiguration(), variantStorageManager, rgaEngine);

        if (!indexed) {
            indexed = true;

            ToolRunner toolRunner = new ToolRunner(opencga.getOpencgaHome().toString(), catalogManager, StorageEngineFactory.get(variantStorageManager.getStorageConfiguration()));

            Path outDir = Paths.get(opencga.createTmpOutdir("_knockout_genes"));
            System.out.println("outDir = " + outDir);
            KnockoutAnalysisParams params = new KnockoutAnalysisParams();
            params.setSample(file.getSampleIds());

            toolRunner.execute(KnockoutAnalysis.class, params.toObjectMap(), outDir, null, token);

            File file = catalogManager.getFileManager().link(STUDY,
                    new FileLinkParams()
                            .setUri(outDir.resolve("knockout.individuals.json.gz").toUri().toString())
                            .setPath("."),
                    false, token).first();
            rgaManager.index(STUDY, file.getPath(), token);
        }
    }

    @AfterClass
    public static void afterClass() {
        opencga.after();
    }

    public void setUpCatalogManager() throws CatalogException {
        catalogManager.getUserManager().create(USER, "User Name", "mail@ebi.ac.uk", PASSWORD, "", null, Account.AccountType.FULL, null);
        token = catalogManager.getUserManager().login("user", PASSWORD).getToken();

        String projectId = catalogManager.getProjectManager().create(PROJECT, "Project about some genomes", "", "Homo sapiens",
                null, "GRCh37", new QueryOptions(), token).first().getId();
        catalogManager.getStudyManager().create(projectId, STUDY, null, "Phase 1", "Done", null, null, null, null, null, token);

        // Create 10 samples not indexed
        for (int i = 0; i < 10; i++) {
            Sample sample = new Sample().setId("SAMPLE_" + i);
            if (i % 2 == 0) {
                sample.setPhenotypes(Collections.singletonList(PHENOTYPE));
            }
            catalogManager.getSampleManager().create(STUDY, sample, null, token);
        }
    }

    @Test
    public void testIndividualQuery() throws IOException, CatalogException, RgaException {
        OpenCGAResult<KnockoutByIndividual> result = rgaManager.individualQuery(STUDY, new Query(), QueryOptions.empty(), token);
        assertEquals(4, result.getNumResults());
    }

    @Test
    public void testGeneQuery() throws IOException, CatalogException, RgaException {
        OpenCGAResult<RgaKnockoutByGene> result = rgaManager.geneQuery(STUDY, new Query(), QueryOptions.empty(), token);
        assertEquals(10, result.getNumResults());
    }

    @Test
    public void testVariantQuery() throws IOException, CatalogException, RgaException {
        OpenCGAResult<KnockoutByVariant> result = rgaManager.variantQuery(STUDY, new Query(), QueryOptions.empty(), token);
        assertEquals(10, result.getNumResults());
    }



//    private StorageConfiguration storageConfiguration;
//
//    @Rule
//    public RgaSolrExtenalResource solr = new RgaSolrExtenalResource();
//
//    @Rule
//    public CatalogManagerExternalResource catalogManagerResource = new CatalogManagerExternalResource();
//
//    private CatalogManager catalogManager;
//    private RgaEngine rgaEngine;
//    private RgaManager rgaManager;
//
//    private String ownerToken;
//    private String userToken;
//    private String study;
//
//    @Before
//    public void before() throws IOException, CatalogException, RgaException, SolrServerException {
//        try (InputStream is = RgaEngineTest.class.getClassLoader().getResourceAsStream("storage-configuration.yml")) {
//            storageConfiguration = StorageConfiguration.load(is);
//        }
//
//        catalogManager = catalogManagerResource.getCatalogManager();
//        VariantStorageManager variantStorageManager = new VariantStorageManager(catalogManager, StorageEngineFactory.get(storageConfiguration));
//
//        rgaEngine = solr.configure(storageConfiguration);
//        rgaManager = new RgaManager(catalogManagerResource.getConfiguration(), storageConfiguration, variantStorageManager, rgaEngine);
//
//        loadCatalog();
//        loadSolr();
//    }
//
//    private void loadCatalog() throws CatalogException {
//        catalogManager.getUserManager().create("user", "User Name", "mail@ebi.ac.uk", "user", "", null, Account.AccountType.FULL, null);
//        ownerToken = catalogManager.getUserManager().login("user", "user").getToken();
//
//        catalogManager.getUserManager().create("user2", "User Name", "mail@ebi.ac.uk", "user", "", null, Account.AccountType.FULL, null);
//        userToken = catalogManager.getUserManager().login("user2", "user").getToken();
//
//        String projectId = catalogManager.getProjectManager().create("1000G", "Project about some genomes", "", "Homo sapiens",
//                null, "GRCh38", new QueryOptions(), ownerToken).first().getId();
//        study = catalogManager.getStudyManager().create(projectId, "phase1", null, "Phase 1", "Done", null, null, null, null, null, ownerToken)
//                .first().getFqn();
//
//        catalogManager.getIndividualManager().create(study,
//                new Individual()
//                        .setId("id1")
//                        .setSamples(Collections.singletonList(new Sample().setId("sample1"))),
//                QueryOptions.empty(), ownerToken);
//        catalogManager.getIndividualManager().create(study,
//                new Individual()
//                        .setId("id2")
//                        .setSamples(Collections.singletonList(new Sample().setId("sample2"))),
//                QueryOptions.empty(), ownerToken);
//    }
//
//    private void loadSolr() throws RgaException, IOException, SolrServerException {
//        String collection = getCollectionName();
//        rgaEngine.create(collection);
//
//        List<KnockoutByIndividual> knockoutByIndividualList = new ArrayList<>(2);
//        knockoutByIndividualList.add(RgaUtilsTest.createKnockoutByIndividual(1));
//        knockoutByIndividualList.add(RgaUtilsTest.createKnockoutByIndividual(2));
//
//        IndividualRgaConverter rgaConverter = new IndividualRgaConverter();
//        List<RgaDataModel> rgaDataModelList = rgaConverter.convertToStorageType(knockoutByIndividualList);
//        rgaEngine.insert(collection, rgaDataModelList);
//    }
//
//    private String getCollectionName() {
//        return catalogManager.getConfiguration().getDatabasePrefix() + "-rga-" + study.replace("@", "_").replace(":", "_");
//    }
//
//    @Test
//    public void testIndividualQueryPermissions() throws CatalogException, IOException, RgaException {
//        OpenCGAResult<KnockoutByIndividual> result = rgaManager.individualQuery(study, new Query(), QueryOptions.empty(), ownerToken);
//        assertEquals(2, result.getNumResults());
//
//        // Grant permissions to sample1
//        catalogManager.getIndividualManager().updateAcl(study, Collections.emptyList(), "user2", new IndividualAclParams("sample1",
//                IndividualAclEntry.IndividualPermissions.VIEW.name()), ParamUtils.AclAction.ADD, false, ownerToken);
//        catalogManager.getSampleManager().updateAcl(study, Collections.singletonList("sample1"), "user2",
//                new SampleAclParams("", "", "", "", SampleAclEntry.SamplePermissions.VIEW.name() + ","
//                        + SampleAclEntry.SamplePermissions.VIEW_VARIANTS.name()), ParamUtils.AclAction.ADD, ownerToken);
//        result = rgaManager.individualQuery(study, new Query(), QueryOptions.empty(), userToken);
//        assertEquals(1, result.getNumResults());
//        assertEquals("sample1", result.first().getSampleId());
//        assertEquals("id1", result.first().getId());
//    }
//
//    @Test
//    public void testIndividualQueryLimit() throws CatalogException, IOException, RgaException {
//        OpenCGAResult<KnockoutByIndividual> result = rgaManager.individualQuery(study, new Query(), new QueryOptions(QueryOptions.LIMIT, 1),
//                ownerToken);
//        assertEquals(1, result.getNumResults());
//        assertEquals("sample1", result.first().getSampleId());
//    }
//
//    @Test
//    public void testIndividualQuerySkip() throws CatalogException, IOException, RgaException {
//        OpenCGAResult<KnockoutByIndividual> result = rgaManager.individualQuery(study, new Query(), new QueryOptions(QueryOptions.SKIP, 1),
//                ownerToken);
//        assertEquals(1, result.getNumResults());
//        assertEquals("sample2", result.first().getSampleId());
//    }
//
//    @Test
//    public void testGeneQueryPermissions() throws CatalogException, IOException, RgaException {
//        OpenCGAResult<RgaKnockoutByGene> result = rgaManager.geneQuery(study, new Query(), QueryOptions.empty(), ownerToken);
//        assertEquals(4, result.getNumResults());
//        for (RgaKnockoutByGene gene : result.getResults()) {
//            assertEquals(1, gene.getIndividuals().size());
//        }
//
//        // Grant permissions to sample1
//        catalogManager.getIndividualManager().updateAcl(study, Collections.emptyList(), "user2", new IndividualAclParams("sample1",
//                IndividualAclEntry.IndividualPermissions.VIEW.name()), ParamUtils.AclAction.ADD, false, ownerToken);
//        catalogManager.getSampleManager().updateAcl(study, Collections.singletonList("sample1"), "user2", new SampleAclParams("", "", "", "",
//                        SampleAclEntry.SamplePermissions.VIEW.name() + "," + SampleAclEntry.SamplePermissions.VIEW_VARIANTS.name()),
//                ParamUtils.AclAction.ADD, ownerToken);
//        result = rgaManager.geneQuery(study, new Query(), QueryOptions.empty(), userToken);
//        assertEquals(4, result.getNumResults());
//        List<RgaKnockoutByGene> genes = result.getResults().stream().filter(g -> !g.getIndividuals().isEmpty()).collect(Collectors.toList());
//        assertEquals(2, genes.size());
//        for (RgaKnockoutByGene gene : genes) {
//            assertEquals(1, gene.getIndividuals().size());
//            assertEquals("sample1", gene.getIndividuals().get(0).getSampleId());
//        }
//    }
//
//    @Test
//    public void testGeneQueryLimit() throws CatalogException, IOException, RgaException {
//        OpenCGAResult<RgaKnockoutByGene> result = rgaManager.geneQuery(study, new Query(), new QueryOptions(QueryOptions.LIMIT, 1),
//                ownerToken);
//        assertEquals("geneId1", result.first().getId());
//        assertEquals(1, result.getNumResults());
//        assertEquals(1, result.first().getIndividuals().size());
//    }
//
//    @Test
//    public void testGeneQuerySkip() throws CatalogException, IOException, RgaException {
//        OpenCGAResult<RgaKnockoutByGene> result = rgaManager.geneQuery(study, new Query(), new QueryOptions(QueryOptions.SKIP, 1),
//                ownerToken);
//        assertEquals(3, result.getNumResults());
//        assertTrue(result.getResults().stream().map(RgaKnockoutByGene::getId).collect(Collectors.toSet())
//                .containsAll(Arrays.asList("geneId11", "geneId12", "geneId2")));
//        for (RgaKnockoutByGene gene : result.getResults()) {
//            assertEquals(1, gene.getIndividuals().size());
//        }
//    }
//
//    @Test
//    public void testVariantQueryPermissions() throws CatalogException, IOException, RgaException {
//        OpenCGAResult<KnockoutByVariant> result = rgaManager.variantQuery(study, new Query(), QueryOptions.empty(), ownerToken);
//        assertEquals(6, result.getNumResults());
//        assertEquals(4, result.getResults().stream().filter(v -> v.getIndividuals().size() == 2).count());
//        assertEquals(2, result.getResults().stream().filter(v -> v.getIndividuals().size() == 1).count());
//
//        // Grant permissions to sample1
//        catalogManager.getIndividualManager().updateAcl(study, Collections.emptyList(), "user2", new IndividualAclParams("sample1",
//                IndividualAclEntry.IndividualPermissions.VIEW.name()), ParamUtils.AclAction.ADD, false, ownerToken);
//        catalogManager.getSampleManager().updateAcl(study, Collections.singletonList("sample1"), "user2", new SampleAclParams("", "", "", "",
//                        SampleAclEntry.SamplePermissions.VIEW.name() + "," + SampleAclEntry.SamplePermissions.VIEW_VARIANTS.name()),
//                ParamUtils.AclAction.ADD, ownerToken);
//        result = rgaManager.variantQuery(study, new Query(), QueryOptions.empty(), userToken);
//        assertEquals(6, result.getNumResults());
//        assertEquals(5, result.getResults().stream().filter(v -> v.getIndividuals().size() == 1).count());
//        assertEquals(1, result.getResults().stream().filter(v -> v.getIndividuals().size() == 0).count());
//
//        for (KnockoutByVariant variant : result.getResults()) {
//            if (variant.getIndividuals().size() == 1) {
//                assertEquals("sample1", variant.getIndividuals().get(0).getSampleId());
//            }
//        }
//    }
//
//    @Test
//    public void testVariantQueryLimit() throws CatalogException, IOException, RgaException {
//        OpenCGAResult<KnockoutByVariant> result = rgaManager.variantQuery(study, new Query(), new QueryOptions(QueryOptions.LIMIT, 1),
//                ownerToken);
//        assertEquals("variant2", result.first().getId());
//        assertEquals(1, result.getNumResults());
//        assertEquals(2, result.first().getIndividuals().size());
//    }
//
//    @Test
//    public void testVariantQuerySkip() throws CatalogException, IOException, RgaException {
//        OpenCGAResult<KnockoutByVariant> result = rgaManager.variantQuery(study, new Query(), new QueryOptions(QueryOptions.SKIP, 1),
//                ownerToken);
//        assertEquals(5, result.getNumResults());
//        assertTrue(result.getResults().stream().map(KnockoutByVariant::getId).collect(Collectors.toSet())
//                .containsAll(Arrays.asList("variant1", "variant3", "variant13", "variant12", "variant11")));
//        assertEquals(3, result.getResults().stream().filter(v -> v.getIndividuals().size() == 2).count());
//        assertEquals(2, result.getResults().stream().filter(v -> v.getIndividuals().size() == 1).count());
//
//        result = rgaManager.variantQuery(study, new Query(), new QueryOptions(QueryOptions.SKIP, 1).append(QueryOptions.LIMIT, 2),
//                ownerToken);
//        assertEquals(2, result.getNumResults());
//        assertTrue(result.getResults().stream().map(KnockoutByVariant::getId).collect(Collectors.toSet())
//                .containsAll(Arrays.asList("variant1", "variant3")));
//        for (KnockoutByVariant variant : result.getResults()) {
//            assertEquals(2, variant.getIndividuals().size());
//        }
//    }

}