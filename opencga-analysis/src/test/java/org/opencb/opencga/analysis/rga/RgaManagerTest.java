package org.opencb.opencga.analysis.rga;

import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.Before;
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
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.models.analysis.knockout.*;
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
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class RgaManagerTest {

    public static final String OWNER = "owner";
    public static final String USER = "user";
    public static final String PASSWORD = "asdf";
    public static final String PROJECT = "project";
    public static final String STUDY = "study";
    public static final String PHENOTYPE_NAME = "myPhenotype";
    public static final Phenotype PHENOTYPE = new Phenotype(PHENOTYPE_NAME, PHENOTYPE_NAME, "mySource")
            .setStatus(Phenotype.Status.OBSERVED);
    public static final String DB_NAME = "opencga_test_" + OWNER + "_" + PROJECT;

    public static final String FATHER = "NA19661";
    public static final String MOTHER = "NA19660";
    public static final String SON = "NA19685";
    public static final String DAUGHTER = "NA19600";

    private CatalogManager catalogManager;
    private VariantStorageManager variantStorageManager;
    private RgaManager rgaManager;

    public static OpenCGATestExternalResource opencga = new OpenCGATestExternalResource();

    public static RgaSolrExtenalResource solr = new RgaSolrExtenalResource();

    private static String storageEngine = MongoDBVariantStorageEngine.STORAGE_ENGINE_ID;
    private static RgaEngine rgaEngine;
    private static boolean indexed = false;
    private static String ownerToken;
    private static String userToken;
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

            file = opencga.createFile(STUDY, "variant-test-file.vcf.gz", ownerToken);
            variantStorageManager.index(STUDY, file.getId(), opencga.createTmpOutdir("_index"), new ObjectMap(VariantStorageOptions.ANNOTATE.key(), true), ownerToken);

            for (int i = 0; i < file.getSampleIds().size(); i++) {
                if (i % 2 == 0) {
                    String id = file.getSampleIds().get(i);
                    SampleUpdateParams updateParams = new SampleUpdateParams().setPhenotypes(Collections.singletonList(PHENOTYPE));
                    catalogManager.getSampleManager().update(STUDY, id, updateParams, null, ownerToken);
                }
            }

            catalogManager.getCohortManager().create(STUDY, new CohortCreateParams().setId("c1").setSamples(file.getSampleIds().subList(0, 2)), null, null, null, ownerToken);
            catalogManager.getCohortManager().create(STUDY, new CohortCreateParams().setId("c2").setSamples(file.getSampleIds().subList(2, 4)), null, null, null, ownerToken);

            Phenotype phenotype = new Phenotype("phenotype", "phenotype", "");
            Disorder disorder = new Disorder("disorder", "disorder", "", "", Collections.singletonList(phenotype), Collections.emptyMap());
            List<Individual> individuals = new ArrayList<>(4);

            // Father
            individuals.add(catalogManager.getIndividualManager()
                    .create(STUDY, new Individual(FATHER, FATHER, IndividualProperty.Sex.MALE, null, null, 0, Collections.emptyList(), Collections.emptyMap()), Collections.singletonList(FATHER), null, ownerToken).first());
            // Mother
            individuals.add(catalogManager.getIndividualManager()
                    .create(STUDY, new Individual(MOTHER, MOTHER, IndividualProperty.Sex.FEMALE, null, null, 0, Collections.emptyList(), Collections.emptyMap()), Collections.singletonList(MOTHER), null, ownerToken).first());
            // Son
            individuals.add(catalogManager.getIndividualManager()
                    .create(STUDY, new Individual(SON, SON, IndividualProperty.Sex.MALE, null, null, 0, Collections.emptyList(), Collections.emptyMap()).setFather(individuals.get(0)).setMother(individuals.get(1)).setDisorders(Collections.singletonList(disorder)), Collections.singletonList(SON), null, ownerToken).first());
            // Daughter
            individuals.add(catalogManager.getIndividualManager()
                    .create(STUDY, new Individual(DAUGHTER, DAUGHTER, IndividualProperty.Sex.FEMALE, null, null, 0, Collections.emptyList(), Collections.emptyMap()).setFather(individuals.get(0)).setMother(individuals.get(1)), Collections.singletonList(DAUGHTER), null, ownerToken).first());
            catalogManager.getFamilyManager().create(
                    STUDY,
                    new Family("f1", "f1", Collections.singletonList(phenotype), Collections.singletonList(disorder), null, null, 3, null, null),
                    individuals.stream().map(Individual::getId).collect(Collectors.toList()), new QueryOptions(), ownerToken);

            // Share father, mother and daughter with USER but not son
            catalogManager.getIndividualManager().updateAcl(STUDY, Arrays.asList(FATHER, MOTHER, DAUGHTER), USER,
                    new IndividualAclParams().setPermissions(IndividualAclEntry.IndividualPermissions.VIEW.name()),
                    ParamUtils.AclAction.SET, false, ownerToken);
            catalogManager.getSampleManager().updateAcl(STUDY, Arrays.asList(FATHER, MOTHER, DAUGHTER), USER,
                    new SampleAclParams().setPermissions(SampleAclEntry.SamplePermissions.VIEW_VARIANTS.name()), ParamUtils.AclAction.SET,
                    ownerToken);

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

            toolRunner.execute(KnockoutAnalysis.class, params.toObjectMap(), outDir, null, ownerToken);

            File file = catalogManager.getFileManager().link(STUDY,
                    new FileLinkParams()
                            .setUri(outDir.resolve("knockout.individuals.json.gz").toUri().toString())
                            .setPath("."),
                    false, ownerToken).first();
            rgaManager.index(STUDY, file.getPath(), ownerToken);
            rgaManager.generateAuxiliarCollection(STUDY, ownerToken);
        }
    }

    @AfterClass
    public static void afterClass() {
        opencga.after();
    }

    public void setUpCatalogManager() throws CatalogException {
        catalogManager.getUserManager().create(OWNER, "User Name", "mail@ebi.ac.uk", PASSWORD, "", null, Account.AccountType.FULL, null);
        ownerToken = catalogManager.getUserManager().login(OWNER, PASSWORD).getToken();

        String projectId = catalogManager.getProjectManager().create(PROJECT, "Project about some genomes", "", "Homo sapiens",
                null, "GRCh37", new QueryOptions(), ownerToken).first().getId();
        catalogManager.getStudyManager().create(projectId, STUDY, null, "Phase 1", "Done", null, null, null, null, null, ownerToken);

        // Create 10 samples not indexed
        for (int i = 0; i < 10; i++) {
            Sample sample = new Sample().setId("SAMPLE_" + i);
            if (i % 2 == 0) {
                sample.setPhenotypes(Collections.singletonList(PHENOTYPE));
            }
            catalogManager.getSampleManager().create(STUDY, sample, null, ownerToken);
        }

        catalogManager.getUserManager().create(USER, "Other Name", "mail2@ebi.ac.uk", PASSWORD, "", null, Account.AccountType.GUEST, null);
        userToken = catalogManager.getUserManager().login(USER, PASSWORD).getToken();
    }

    @Test
    public void testVariantQuery() throws IOException, CatalogException, RgaException {
        OpenCGAResult<KnockoutByVariant> result = rgaManager.variantQuery(STUDY, new Query(), QueryOptions.empty(), ownerToken);
        assertEquals(10, result.getNumResults());
    }

    @Test
    public void testIndividualQueryPermissions() throws CatalogException, IOException, RgaException {
        OpenCGAResult<KnockoutByIndividual> result = rgaManager.individualQuery(STUDY, new Query(), QueryOptions.empty(), ownerToken);
        assertEquals(4, result.getNumResults());

        result = rgaManager.individualQuery(STUDY, new Query(), QueryOptions.empty(), userToken);
        assertEquals(3, result.getNumResults());
        assertTrue(Arrays.asList(FATHER, MOTHER, DAUGHTER)
                .containsAll(result.getResults().stream().map(KnockoutByIndividual::getSampleId).collect(Collectors.toList())));
    }

    @Test
    public void testIndividualQueryLimit() throws CatalogException, IOException, RgaException {
        Set<String> sampleIds = new HashSet<>();

        for (int skip = 0; skip < 4; skip++) {
            OpenCGAResult<KnockoutByIndividual> result = rgaManager.individualQuery(STUDY, new Query(),
                    new QueryOptions(QueryOptions.LIMIT, 1).append(QueryOptions.SKIP, skip),
                    ownerToken);
            assertEquals(1, result.getNumResults());
            assertTrue(StringUtils.isNotEmpty(result.first().getSampleId()));
            assertFalse(sampleIds.contains(result.first().getSampleId()));
            sampleIds.add(result.first().getSampleId());
        }
        assertEquals(4, sampleIds.size());
    }

    @Test
    public void testIndividualSummary() throws CatalogException, IOException, RgaException {
        OpenCGAResult<KnockoutByIndividualSummary> result = rgaManager.individualSummary(STUDY, new Query(), QueryOptions.empty(), ownerToken);
        assertEquals(4, result.getNumResults());
        assertTrue(result.getAttributes().containsKey("totalIndividuals"));
        assertEquals(4, result.getAttributes().getInt("totalIndividuals"));
        for (KnockoutByIndividualSummary individual : result.getResults()) {
            if ("NA19685".equals(individual.getId()) || "NA19600".equals(individual.getId())) {
                assertEquals(2, individual.getNumParents());
            } else {
                assertEquals(0, individual.getNumParents());
            }
            assertNotNull(individual.getId());
            assertNotNull(individual.getVariantStats());
            assertTrue(individual.getGenes().size() > 0);
            assertTrue(individual.getVariantStats().getCount() > 0);
            assertTrue(individual.getVariantStats().getNumCompHet() > 0 || individual.getVariantStats().getNumDelOverlap() > 0
                    || individual.getVariantStats().getNumHetAlt() > 0 || individual.getVariantStats().getNumHomAlt() > 0);
        }

        result = rgaManager.individualSummary(STUDY, new Query(RgaQueryParams.KNOCKOUT.key(), KnockoutVariant.KnockoutType.COMP_HET.name()),
                QueryOptions.empty(), ownerToken);
        assertEquals(2, result.getNumResults());
        for (KnockoutByIndividualSummary individual : result.getResults()) {
            assertNotNull(individual.getId());
            assertNotNull(individual.getVariantStats());
            assertTrue(individual.getGenes().size() > 0);
            assertTrue(individual.getVariantStats().getCount() > 0);
            assertTrue(individual.getVariantStats().getNumCompHet() > 0);
            assertEquals(0, individual.getVariantStats().getNumDelOverlap());
            assertEquals(0, individual.getVariantStats().getNumHetAlt());
            assertEquals(0, individual.getVariantStats().getNumHomAlt());
        }

        result = rgaManager.individualSummary(STUDY, new Query(RgaQueryParams.KNOCKOUT.key(), KnockoutVariant.KnockoutType.HOM_ALT.name()),
                QueryOptions.empty(), ownerToken);
        assertEquals(4, result.getNumResults());
        for (KnockoutByIndividualSummary individual : result.getResults()) {
            assertNotNull(individual.getId());
            assertNotNull(individual.getVariantStats());
            assertTrue(individual.getGenes().size() > 0);
            assertTrue(individual.getVariantStats().getCount() > 0);
            assertTrue(individual.getVariantStats().getNumHomAlt() > 0);
            assertEquals(0, individual.getVariantStats().getNumDelOverlap());
            assertEquals(0, individual.getVariantStats().getNumHetAlt());
            assertEquals(0, individual.getVariantStats().getNumCompHet());
        }

        result = rgaManager.individualSummary(STUDY, new Query(RgaQueryParams.CONSEQUENCE_TYPE.key(), "SO:0001630"), QueryOptions.empty(), ownerToken);
        assertEquals(4, result.getNumResults());
        for (KnockoutByIndividualSummary individual : result.getResults()) {
            assertNotNull(individual.getId());
            assertNotNull(individual.getVariantStats());
            assertTrue(individual.getGenes().size() > 0);
            assertTrue(individual.getVariantStats().getCount() > 0);
            assertTrue(individual.getVariantStats().getNumHomAlt() > 0);
            assertEquals(0, individual.getVariantStats().getNumDelOverlap());
            assertEquals(0, individual.getVariantStats().getNumHetAlt());
            assertEquals(0, individual.getVariantStats().getNumCompHet());
        }

        result = rgaManager.individualSummary(STUDY,
                new Query(RgaQueryParams.POPULATION_FREQUENCY.key(), RgaUtils.THOUSAND_GENOMES_STUDY + "<0.001;"
                        + RgaUtils.GNOMAD_GENOMES_STUDY + ">0.01"), QueryOptions.empty(),
                ownerToken);
        assertEquals(4, result.getNumResults());
        boolean compHetFound = false;
        for (KnockoutByIndividualSummary individual : result.getResults()) {
            assertNotNull(individual.getId());
            assertNotNull(individual.getVariantStats());
            assertTrue(individual.getGenes().size() > 0);
            assertTrue(individual.getVariantStats().getCount() > 0);
            assertTrue(individual.getVariantStats().getNumHomAlt() > 0);
            assertEquals(0, individual.getVariantStats().getNumDelOverlap());
            assertEquals(0, individual.getVariantStats().getNumHetAlt());
            if (individual.getVariantStats().getNumCompHet() > 0) {
                compHetFound = true;
            }
        }
        assertTrue(compHetFound);
    }

    @Test
    public void testGeneQueryPermissions() throws CatalogException, IOException, RgaException {
        OpenCGAResult<RgaKnockoutByGene> result = rgaManager.geneQuery(STUDY, new Query(), QueryOptions.empty(), ownerToken);
        assertEquals(10, result.getNumResults());

        result = rgaManager.geneQuery(STUDY, new Query(RgaQueryParams.GENE_NAME.key(), "TBCK,MGA"), QueryOptions.empty(), ownerToken);
        assertEquals(2, result.getNumResults());
        for (RgaKnockoutByGene gene : result.getResults()) {
            if ("TBCK".equals(gene.getName())) {
                assertEquals(4, gene.getNumIndividuals());
                assertEquals(4, gene.getIndividuals().size());
            } else if ("MGA".equals(gene.getName())) {
                assertEquals(3, gene.getNumIndividuals());
                assertEquals(3, gene.getIndividuals().size());
            } else {
                fail("Unexpected gene '" + gene.getName() + "'");
            }
            assertTrue(gene.getIndividuals().stream().map(KnockoutByGene.KnockoutIndividual::getId).collect(Collectors.toSet()).contains(SON));
        }

        result = rgaManager.geneQuery(STUDY, new Query(RgaQueryParams.GENE_NAME.key(), "TBCK,MGA"), QueryOptions.empty(), userToken);
        assertEquals(2, result.getNumResults());
        for (RgaKnockoutByGene gene : result.getResults()) {
            if ("TBCK".equals(gene.getName())) {
                assertEquals(4, gene.getNumIndividuals());
                assertEquals(3, gene.getIndividuals().size());
            } else if ("MGA".equals(gene.getName())) {
                assertEquals(3, gene.getNumIndividuals());
                assertEquals(2, gene.getIndividuals().size());
            } else {
                fail("Unexpected gene '" + gene.getName() + "'");
            }
            assertFalse(gene.getIndividuals().stream().map(KnockoutByGene.KnockoutIndividual::getId).collect(Collectors.toSet()).contains(SON));
        }
    }

    @Test
    public void testGeneQueryLimitNestedIndividual() throws CatalogException, IOException, RgaException {
        Map<String, Set<String>> geneIndividualMap = new HashMap<>();
        QueryOptions options = new QueryOptions(QueryOptions.LIMIT, 5)
                .append(RgaQueryParams.LIMIT_INDIVIDUAL, 1);

        for (int skip = 0; skip < 4; skip++) {
            System.out.println("Skip " + skip);
            options.put(RgaQueryParams.SKIP_INDIVIDUAL, skip);
            OpenCGAResult<RgaKnockoutByGene> result = rgaManager.geneQuery(STUDY, new Query(), options, ownerToken);

            assertEquals(5, result.getNumResults());
            for (RgaKnockoutByGene gene : result.getResults()) {
                assertTrue(gene.getNumIndividuals() >= 1);
                assertTrue(gene.getIndividuals().size() <= 1);

                if (skip == 0 && gene.getNumIndividuals() > 1) {
                    assertTrue(gene.isHasNextIndividual());
                }

                if (!gene.getIndividuals().isEmpty()) {
                    if (!geneIndividualMap.containsKey(gene.getId())) {
                        geneIndividualMap.put(gene.getId(), new HashSet<>());
                    }

                    Set<String> individualSet = geneIndividualMap.get(gene.getId());
//                    assertFalse(individualSet.contains(gene.getIndividuals().get(0).getId()));
                    individualSet.add(gene.getIndividuals().get(0).getId());
                }
            }
        }

        assertEquals(5, geneIndividualMap.size());
        for (Set<String> individualSet : geneIndividualMap.values()) {
            assertTrue(individualSet.size() >= 1);
        }

        // Skip all nested individuals
        options.put(RgaQueryParams.SKIP_INDIVIDUAL, 4);
        OpenCGAResult<RgaKnockoutByGene> result = rgaManager.geneQuery(STUDY, new Query(), options, ownerToken);
        assertEquals(5, result.getNumResults());
        for (RgaKnockoutByGene gene : result.getResults()) {
            assertTrue(gene.getNumIndividuals() >= 1);
            assertTrue(gene.getIndividuals().isEmpty());
            assertFalse(gene.isHasNextIndividual());
        }
    }

    @Test
    public void testGeneQueryLimit() throws CatalogException, IOException, RgaException {
        Set<String> geneIds = new HashSet<>();

        for (int skip = 0; skip < 60; skip = skip + 20) {
            OpenCGAResult<RgaKnockoutByGene> result = rgaManager.geneQuery(STUDY, new Query(),
                    new QueryOptions(QueryOptions.LIMIT, 20).append(QueryOptions.SKIP, skip),
                    ownerToken);
            assertEquals(20, result.getNumResults());
            assertEquals(0, result.getResults().stream().map(RgaKnockoutByGene::getId).filter(StringUtils::isEmpty).count());
            assertEquals(0, result.getResults().stream().map(RgaKnockoutByGene::getId).filter(geneIds::contains).count());
            geneIds.addAll(result.getResults().stream().map(RgaKnockoutByGene::getId).collect(Collectors.toList()));
        }
        assertEquals(60, geneIds.size());
    }

    @Test
    public void testGeneSummary() throws CatalogException, IOException, RgaException {
        OpenCGAResult<KnockoutByGeneSummary> result = rgaManager.geneSummary(STUDY, new Query(), QueryOptions.empty(), ownerToken);
        assertEquals(10, result.getNumResults());
        for (KnockoutByGeneSummary gene : result.getResults()) {
            assertNotNull(gene.getId());
            assertNotNull(gene.getVariantStats());
            assertTrue(gene.getVariantStats().getCount() > 0);
            assertTrue(gene.getVariantStats().getNumCompHet() > 0 || gene.getVariantStats().getNumDelOverlap() > 0
                    || gene.getVariantStats().getNumHetAlt() > 0 || gene.getVariantStats().getNumHomAlt() > 0);
            assertNotNull(gene.getIndividualStats());
            assertTrue(gene.getIndividualStats().getCount() > 0);
            assertTrue(gene.getIndividualStats().getNumCompHet() > 0 || gene.getIndividualStats().getNumDelOverlap() > 0
                    || gene.getIndividualStats().getNumHetAlt() > 0 || gene.getIndividualStats().getNumHomAlt() > 0);
        }

        result = rgaManager.geneSummary(STUDY, new Query(RgaQueryParams.KNOCKOUT.key(), KnockoutVariant.KnockoutType.COMP_HET), QueryOptions.empty(), ownerToken);
        assertEquals(2, result.getNumResults());
        for (KnockoutByGeneSummary gene : result.getResults()) {
            assertNotNull(gene.getId());
            assertNotNull(gene.getVariantStats());
            assertTrue(gene.getVariantStats().getCount() > 0);
            assertTrue(gene.getVariantStats().getNumCompHet() > 0);
            assertTrue(gene.getVariantStats().getNumPairedCompHet() > 0);
            assertNotNull(gene.getIndividualStats());
            assertTrue(gene.getIndividualStats().getCount() > 0);
            assertTrue(gene.getIndividualStats().getNumCompHet() > 0);
        }
    }

    @Test
    public void testVariantQueryPermissions() throws CatalogException, IOException, RgaException {
        OpenCGAResult<KnockoutByVariant> result = rgaManager.variantQuery(STUDY, new Query(), QueryOptions.empty(), ownerToken);
        assertEquals(10, result.getNumResults());

        result = rgaManager.variantQuery(STUDY, new Query(RgaQueryParams.VARIANTS.key(),
                Arrays.asList("15:41991315:A:T", "1:2441358:T:C", "1:59125683:C:T")), QueryOptions.empty(), ownerToken);
        assertEquals(3, result.getNumResults());
        for (KnockoutByVariant variant : result.getResults()) {
            switch (variant.getId()) {
                case "15:41991315:A:T":
                    assertEquals(3, variant.getNumIndividuals()); // 19600, 19685, 19660
                    assertEquals(3, variant.getIndividuals().size());
                    assertTrue(variant.getIndividuals().stream().map(KnockoutByIndividual::getId).collect(Collectors.toSet()).contains(SON));
                    break;
                case "1:2441358:T:C"://  1
                    assertEquals(1, variant.getNumIndividuals()); // 19685
                    assertEquals(1, variant.getIndividuals().size());
                    break;
                case "1:59125683:C:T": //  1
                    assertEquals(1, variant.getNumIndividuals()); // 19660
                    assertEquals(1, variant.getIndividuals().size()); // 19660
                    break;
                default:
                    fail("Unexpected variant id '" + variant.getId() + "'");
            }
        }

        result = rgaManager.variantQuery(STUDY, new Query(), QueryOptions.empty(), userToken);
        assertEquals(10, result.getNumResults());

        result = rgaManager.variantQuery(STUDY, new Query(), new QueryOptions(QueryOptions.SKIP, 10), userToken);
        assertEquals(10, result.getNumResults());

        result = rgaManager.variantQuery(STUDY, new Query(), new QueryOptions(QueryOptions.SKIP, 20), userToken);
        assertEquals(10, result.getNumResults());

        result = rgaManager.variantQuery(STUDY, new Query(RgaQueryParams.VARIANTS.key(),
                Arrays.asList("15:41991315:A:T", "1:2441358:T:C", "1:59125683:C:T")), QueryOptions.empty(), userToken);
        assertEquals(3, result.getNumResults());
        for (KnockoutByVariant variant : result.getResults()) {
            switch (variant.getId()) {
                case "15:41991315:A:T":
                    assertEquals(3, variant.getNumIndividuals()); // 19600, 19685, 19660
                    assertEquals(2, variant.getIndividuals().size()); // 19685 should not be in this list
                    assertFalse(variant.getIndividuals().stream().map(KnockoutByIndividual::getId).collect(Collectors.toSet()).contains(SON));
                    break;
                case "1:2441358:T:C"://  1
                    assertEquals(1, variant.getNumIndividuals()); // 19685
                    assertEquals(0, variant.getIndividuals().size()); // 19685 should not be in this list
                    break;
                case "1:59125683:C:T": //  1
                    assertEquals(1, variant.getNumIndividuals()); // 19660
                    assertEquals(1, variant.getIndividuals().size()); // 19660
                    break;
                default:
                    fail("Unexpected variant id '" + variant.getId() + "'");
            }
        }
    }

    @Test
    public void testVariantQueryLimitNestedIndividual() throws CatalogException, IOException, RgaException {
        Map<String, Set<String>> variantIndividualMap = new HashMap<>();
        QueryOptions options = new QueryOptions(QueryOptions.LIMIT, 5)
                .append(RgaQueryParams.LIMIT_INDIVIDUAL, 1);

        for (int skip = 0; skip < 4; skip++) {
            System.out.println("Skip " + skip);
            options.put(RgaQueryParams.SKIP_INDIVIDUAL, skip);
            OpenCGAResult<KnockoutByVariant> result = rgaManager.variantQuery(STUDY, new Query(), options, ownerToken);

            assertEquals(5, result.getNumResults());
            for (KnockoutByVariant variant : result.getResults()) {
                assertTrue(variant.getNumIndividuals() >= 1);
                assertTrue(variant.getIndividuals().size() <= 1);

                if (variant.getNumIndividuals() > 1 && skip == 0) {
                    assertTrue(variant.isHasNextIndividual());
                }

                if (!variant.getIndividuals().isEmpty()) {
                    if (!variantIndividualMap.containsKey(variant.getId())) {
                        variantIndividualMap.put(variant.getId(), new HashSet<>());
                    }

                    Set<String> individualSet = variantIndividualMap.get(variant.getId());
                    assertFalse(individualSet.contains(variant.getIndividuals().get(0).getId()));
                    individualSet.add(variant.getIndividuals().get(0).getId());
                }
            }
        }

        assertEquals(5, variantIndividualMap.size());
        for (Set<String> individualSet : variantIndividualMap.values()) {
            assertTrue(individualSet.size() >= 1);
        }

        // Skip all nested individuals
        options.put(RgaQueryParams.SKIP_INDIVIDUAL, 4);
        OpenCGAResult<KnockoutByVariant> result = rgaManager.variantQuery(STUDY, new Query(), options, ownerToken);
        assertEquals(5, result.getNumResults());
        for (KnockoutByVariant variant : result.getResults()) {
            assertTrue(variant.getNumIndividuals() >= 1);
            assertTrue(variant.getIndividuals().isEmpty());
            assertFalse(variant.isHasNextIndividual());
        }
    }

    @Test
    public void testVariantQueryLimit() throws CatalogException, IOException, RgaException {
        Set<String> variantIds = new HashSet<>();

        for (int skip = 0; skip < 60; skip = skip + 20) {
            OpenCGAResult<KnockoutByVariant> result = rgaManager.variantQuery(STUDY, new Query(),
                    new QueryOptions(QueryOptions.LIMIT, 20).append(QueryOptions.SKIP, skip),
                    ownerToken);
            assertEquals(20, result.getNumResults());
            assertEquals(0, result.getResults().stream().map(KnockoutByVariant::getId).filter(StringUtils::isEmpty).count());
            assertEquals(0, result.getResults().stream().map(KnockoutByVariant::getId).filter(variantIds::contains).count());
            variantIds.addAll(result.getResults().stream().map(KnockoutByVariant::getId).collect(Collectors.toList()));
        }
        assertEquals(60, variantIds.size());
    }


    @Test
    public void testVariantSummary() throws CatalogException, IOException, RgaException {
        OpenCGAResult<KnockoutByVariantSummary> result = rgaManager.variantSummary(STUDY, new Query(), QueryOptions.empty(), ownerToken);
        assertEquals(10, result.getNumResults());
        for (KnockoutByVariantSummary variant : result.getResults()) {
            assertNotNull(variant.getId());
            assertNotNull(variant.getDbSnp());
            assertFalse(variant.getGenes().isEmpty());
        }

        result = rgaManager.variantSummary(STUDY, new Query(RgaQueryParams.KNOCKOUT.key(), KnockoutVariant.KnockoutType.COMP_HET),
                QueryOptions.empty(), ownerToken);
        assertEquals(4, result.getNumResults());
        for (KnockoutByVariantSummary variant : result.getResults()) {
            assertFalse(variant.getTranscriptChPairs().isEmpty());
            for (List<String> value : variant.getTranscriptChPairs().values()) {
                assertTrue(value.size() >= 2);
            }
            assertTrue(variant.getAllelePairs().size() >= 2);

            assertNotNull(variant.getId());
            assertNotNull(variant.getDbSnp());
            assertFalse(variant.getGenes().isEmpty());
        }

    }

}