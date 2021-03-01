package org.opencb.opencga.clinical.rga;

import org.apache.solr.client.solrj.SolrServerException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.analysis.knockout.*;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualAclEntry;
import org.opencb.opencga.core.models.individual.IndividualAclParams;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleAclEntry;
import org.opencb.opencga.core.models.sample.SampleAclParams;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.RgaException;
import org.opencb.opencga.storage.core.rga.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.rga.RgaUtilsTest.createKnockoutByIndividual;

public class RgaManagerTest {

    private StorageConfiguration storageConfiguration;

    @Rule
    public RgaSolrExtenalResource solr = new RgaSolrExtenalResource();

    @Rule
    public CatalogManagerExternalResource catalogManagerResource = new CatalogManagerExternalResource();

    private CatalogManager catalogManager;
    private RgaEngine rgaEngine;
    private RgaManager rgaManager;

    private String ownerToken;
    private String userToken;
    private String study;

    @Before
    public void before() throws IOException, CatalogException, RgaException, SolrServerException {
        try (InputStream is = RgaEngineTest.class.getClassLoader().getResourceAsStream("storage-configuration.yml")) {
            storageConfiguration = StorageConfiguration.load(is);
        }

        catalogManager = catalogManagerResource.getCatalogManager();

        rgaEngine = solr.configure(storageConfiguration);
        rgaManager = new RgaManager(catalogManagerResource.getConfiguration(), storageConfiguration, rgaEngine);

        loadCatalog();
        loadSolr();
    }

    private void loadCatalog() throws CatalogException {
        catalogManager.getUserManager().create("user", "User Name", "mail@ebi.ac.uk", "user", "", null, Account.AccountType.FULL, null);
        ownerToken = catalogManager.getUserManager().login("user", "user").getToken();

        catalogManager.getUserManager().create("user2", "User Name", "mail@ebi.ac.uk", "user", "", null, Account.AccountType.FULL, null);
        userToken = catalogManager.getUserManager().login("user2", "user").getToken();

        String projectId = catalogManager.getProjectManager().create("1000G", "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", new QueryOptions(), ownerToken).first().getId();
        study = catalogManager.getStudyManager().create(projectId, "phase1", null, "Phase 1", "Done", null, null, null, null, null, ownerToken)
                .first().getFqn();

        catalogManager.getIndividualManager().create(study,
                new Individual()
                        .setId("id1")
                        .setSamples(Collections.singletonList(new Sample().setId("sample1"))),
                QueryOptions.empty(), ownerToken);
        catalogManager.getIndividualManager().create(study,
                new Individual()
                        .setId("id2")
                        .setSamples(Collections.singletonList(new Sample().setId("sample2"))),
                QueryOptions.empty(), ownerToken);
    }

    private void loadSolr() throws RgaException, IOException, SolrServerException {
        String collection = getCollectionName();
        rgaEngine.create(collection);

        List<KnockoutByIndividual> knockoutByIndividualList = new ArrayList<>(2);
        knockoutByIndividualList.add(createKnockoutByIndividual(1));
        knockoutByIndividualList.add(createKnockoutByIndividual(2));

        rgaEngine.insert(collection, knockoutByIndividualList, Collections.emptyMap(), Collections.emptyMap());
    }

    private String getCollectionName() {
        return catalogManager.getConfiguration().getDatabasePrefix() + "-rga-" + study.replace("@", "_").replace(":", "_");
    }

    @Test
    public void testIndividualQueryPermissions() throws CatalogException, IOException, RgaException {
        OpenCGAResult<KnockoutByIndividual> result = rgaManager.individualQuery(study, new Query(), QueryOptions.empty(), ownerToken);
        assertEquals(2, result.getNumResults());

        // Grant permissions to sample1
        catalogManager.getIndividualManager().updateAcl(study, Collections.emptyList(), "user2", new IndividualAclParams("sample1",
                IndividualAclEntry.IndividualPermissions.VIEW.name()), ParamUtils.AclAction.ADD, false, ownerToken);
        catalogManager.getSampleManager().updateAcl(study, Collections.singletonList("sample1"), "user2", new SampleAclParams("", "", "", "",
                SampleAclEntry.SamplePermissions.VIEW.name() + "," + SampleAclEntry.SamplePermissions.VIEW_VARIANTS.name()),
                ParamUtils.AclAction.ADD, ownerToken);
        result = rgaManager.individualQuery(study, new Query(), QueryOptions.empty(), userToken);
        assertEquals(1, result.getNumResults());
        assertEquals("sample1", result.first().getSampleId());
        assertEquals("id1", result.first().getId());
    }

    @Test
    public void testIndividualQueryLimit() throws CatalogException, IOException, RgaException {
        OpenCGAResult<KnockoutByIndividual> result = rgaManager.individualQuery(study, new Query(), new QueryOptions(QueryOptions.LIMIT, 1),
                ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals("sample1", result.first().getSampleId());
    }

    @Test
    public void testIndividualQuerySkip() throws CatalogException, IOException, RgaException {
        OpenCGAResult<KnockoutByIndividual> result = rgaManager.individualQuery(study, new Query(), new QueryOptions(QueryOptions.SKIP, 1),
                ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals("sample2", result.first().getSampleId());
    }

    @Test
    public void testGeneQueryPermissions() throws CatalogException, IOException, RgaException {
        OpenCGAResult<KnockoutByGene> result = rgaManager.geneQuery(study, new Query(), QueryOptions.empty(), ownerToken);
        assertEquals(4, result.getNumResults());
        for (KnockoutByGene gene : result.getResults()) {
            assertEquals(1, gene.getIndividuals().size());
        }

        // Grant permissions to sample1
        catalogManager.getIndividualManager().updateAcl(study, Collections.emptyList(), "user2", new IndividualAclParams("sample1",
                IndividualAclEntry.IndividualPermissions.VIEW.name()), ParamUtils.AclAction.ADD, false, ownerToken);
        catalogManager.getSampleManager().updateAcl(study, Collections.singletonList("sample1"), "user2", new SampleAclParams("", "", "", "",
                        SampleAclEntry.SamplePermissions.VIEW.name() + "," + SampleAclEntry.SamplePermissions.VIEW_VARIANTS.name()),
                ParamUtils.AclAction.ADD, ownerToken);
        result = rgaManager.geneQuery(study, new Query(), QueryOptions.empty(), userToken);
        assertEquals(4, result.getNumResults());
        List<KnockoutByGene> genes = result.getResults().stream().filter(g -> !g.getIndividuals().isEmpty()).collect(Collectors.toList());
        assertEquals(2, genes.size());
        for (KnockoutByGene gene : genes) {
            assertEquals(1, gene.getIndividuals().size());
            assertEquals("sample1", gene.getIndividuals().get(0).getSampleId());
        }
    }

    @Test
    public void testGeneQueryLimit() throws CatalogException, IOException, RgaException {
        OpenCGAResult<KnockoutByGene> result = rgaManager.geneQuery(study, new Query(), new QueryOptions(QueryOptions.LIMIT, 1),
                ownerToken);
        assertEquals("geneId1", result.first().getId());
        assertEquals(1, result.getNumResults());
        assertEquals(1, result.first().getIndividuals().size());
    }

    @Test
    public void testGeneQuerySkip() throws CatalogException, IOException, RgaException {
        OpenCGAResult<KnockoutByGene> result = rgaManager.geneQuery(study, new Query(), new QueryOptions(QueryOptions.SKIP, 1),
                ownerToken);
        assertEquals(3, result.getNumResults());
        assertTrue(result.getResults().stream().map(KnockoutByGene::getId).collect(Collectors.toSet())
                .containsAll(Arrays.asList("geneId11", "geneId12", "geneId2")));
        for (KnockoutByGene gene : result.getResults()) {
            assertEquals(1, gene.getIndividuals().size());
        }
    }

    @Test
    public void testVariantQueryPermissions() throws CatalogException, IOException, RgaException {
        OpenCGAResult<KnockoutByVariant> result = rgaManager.variantQuery(study, new Query(), QueryOptions.empty(), ownerToken);
        assertEquals(6, result.getNumResults());
        assertEquals(4, result.getResults().stream().filter(v -> v.getIndividuals().size() == 2).count());
        assertEquals(2, result.getResults().stream().filter(v -> v.getIndividuals().size() == 1).count());

        // Grant permissions to sample1
        catalogManager.getIndividualManager().updateAcl(study, Collections.emptyList(), "user2", new IndividualAclParams("sample1",
                IndividualAclEntry.IndividualPermissions.VIEW.name()), ParamUtils.AclAction.ADD, false, ownerToken);
        catalogManager.getSampleManager().updateAcl(study, Collections.singletonList("sample1"), "user2", new SampleAclParams("", "", "", "",
                        SampleAclEntry.SamplePermissions.VIEW.name() + "," + SampleAclEntry.SamplePermissions.VIEW_VARIANTS.name()),
                ParamUtils.AclAction.ADD, ownerToken);
        result = rgaManager.variantQuery(study, new Query(), QueryOptions.empty(), userToken);
        assertEquals(6, result.getNumResults());
        assertEquals(5, result.getResults().stream().filter(v -> v.getIndividuals().size() == 1).count());
        assertEquals(1, result.getResults().stream().filter(v -> v.getIndividuals().size() == 0).count());

        for (KnockoutByVariant variant : result.getResults()) {
            if (variant.getIndividuals().size() == 1) {
                assertEquals("sample1", variant.getIndividuals().get(0).getSampleId());
            }
        }
    }

    @Test
    public void testVariantQueryLimit() throws CatalogException, IOException, RgaException {
        OpenCGAResult<KnockoutByVariant> result = rgaManager.variantQuery(study, new Query(), new QueryOptions(QueryOptions.LIMIT, 1),
                ownerToken);
        assertEquals("variant2", result.first().getId());
        assertEquals(1, result.getNumResults());
        assertEquals(2, result.first().getIndividuals().size());
    }

    @Test
    public void testVariantQuerySkip() throws CatalogException, IOException, RgaException {
        OpenCGAResult<KnockoutByVariant> result = rgaManager.variantQuery(study, new Query(), new QueryOptions(QueryOptions.SKIP, 1),
                ownerToken);
        assertEquals(5, result.getNumResults());
        assertTrue(result.getResults().stream().map(KnockoutByVariant::getId).collect(Collectors.toSet())
                .containsAll(Arrays.asList("variant1", "variant3", "variant13", "variant12", "variant11")));
        assertEquals(3, result.getResults().stream().filter(v -> v.getIndividuals().size() == 2).count());
        assertEquals(2, result.getResults().stream().filter(v -> v.getIndividuals().size() == 1).count());

        result = rgaManager.variantQuery(study, new Query(), new QueryOptions(QueryOptions.SKIP, 1).append(QueryOptions.LIMIT, 2),
                ownerToken);
        assertEquals(2, result.getNumResults());
        assertTrue(result.getResults().stream().map(KnockoutByVariant::getId).collect(Collectors.toSet())
                .containsAll(Arrays.asList("variant1", "variant3")));
        for (KnockoutByVariant variant : result.getResults()) {
            assertEquals(2, variant.getIndividuals().size());
        }
    }

    @Test
    public void testIndividualQuery() throws Exception {
        RgaEngine rgaEngine = solr.configure(storageConfiguration);

        String collection = solr.coreName;
        rgaEngine.create(collection);

        List<KnockoutByIndividual> knockoutByIndividualList = new ArrayList<>(2);
        knockoutByIndividualList.add(createKnockoutByIndividual(1));
        knockoutByIndividualList.add(createKnockoutByIndividual(2));

        rgaEngine.insert(collection, knockoutByIndividualList, Collections.emptyMap(), Collections.emptyMap());
        OpenCGAResult<KnockoutByIndividual> result = rgaEngine.individualQuery(collection, new Query(), new QueryOptions());

        assertEquals(2, result.getNumResults());
        for (int i = 0; i < knockoutByIndividualList.size(); i++) {
            assertEquals(JacksonUtils.getDefaultObjectMapper().writeValueAsString(knockoutByIndividualList.get(i)),
                    JacksonUtils.getDefaultObjectMapper().writeValueAsString(result.getResults().get(i)));
        }

        result = rgaEngine.individualQuery(collection, new Query(), new QueryOptions());
        assertEquals(2, result.getNumResults());

        Query query = new Query(RgaQueryParams.DISORDERS.key(), "disorderId1");
        result = rgaEngine.individualQuery(collection, query, new QueryOptions());
        assertEquals(1, result.getNumResults());
        assertEquals("id1", result.first().getId());

        query = new Query(RgaQueryParams.DISORDERS.key(), "disorderId2");
        result = rgaEngine.individualQuery(collection, query, new QueryOptions());
        assertEquals(1, result.getNumResults());
        assertEquals("id2", result.first().getId());

        query = new Query(RgaQueryParams.DISORDERS.key(), "disorderId6");
        result = rgaEngine.individualQuery(collection, query, new QueryOptions());
        assertEquals(0, result.getNumResults());

        query = new Query(RgaQueryParams.CONSEQUENCE_TYPE.key(), "SO:0001891");
        result = rgaEngine.individualQuery(collection, query, new QueryOptions());
        assertEquals(2, result.getNumResults());

        query = new Query(RgaQueryParams.CONSEQUENCE_TYPE.key(), "SO:0001822");
        result = rgaEngine.individualQuery(collection, query, new QueryOptions());
        assertEquals(0, result.getNumResults());

        query = new Query()
                .append(RgaQueryParams.DISORDERS.key(), "disorderId1")
                .append(RgaQueryParams.CONSEQUENCE_TYPE.key(), "SO:0001891");
        result = rgaEngine.individualQuery(collection, query, new QueryOptions());
        assertEquals(1, result.getNumResults());

        query = new Query()
                .append(RgaQueryParams.DISORDERS.key(), "disorderId1,disorder")
                .append(RgaQueryParams.CONSEQUENCE_TYPE.key(), "SO:0001891");
        result = rgaEngine.individualQuery(collection, query, new QueryOptions());
        assertEquals(1, result.getNumResults());
    }

    @Test
    public void testIncludeExcludeIndividualQuery() throws Exception {
        RgaEngine rgaEngine = solr.configure(storageConfiguration);

        String collection = solr.coreName;
        rgaEngine.create(collection);

        List<KnockoutByIndividual> knockoutByIndividualList = new ArrayList<>(2);
        knockoutByIndividualList.add(createKnockoutByIndividual(1));
        knockoutByIndividualList.add(createKnockoutByIndividual(2));

        rgaEngine.insert(collection, knockoutByIndividualList, Collections.emptyMap(), Collections.emptyMap());

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList("sampleId", "disorders", "genesMap.name"));
        OpenCGAResult<KnockoutByIndividual> result = rgaEngine.individualQuery(collection, new Query(), options);
        assertEquals(2, result.getNumResults());
        for (int i = 0; i < knockoutByIndividualList.size(); i++) {
            assertNotNull(result.getResults().get(i).getId());
            assertTrue(result.getResults().get(i).getPhenotypes().isEmpty());
            assertNotNull(result.getResults().get(i).getSampleId());
            assertNotNull(result.getResults().get(i).getDisorders());
            assertNotNull(result.getResults().get(i).getGenes());
            for (KnockoutByIndividual.KnockoutGene gene : result.getResults().get(i).getGenes()) {
                assertNotNull(gene.getId());
                assertNotNull(gene.getName());
                assertNull(gene.getStrand());
                assertTrue(gene.getTranscripts().isEmpty());
            }
        }

        options = new QueryOptions(QueryOptions.EXCLUDE, Arrays.asList("sampleId", "disorders", "genesMap.name",
                "genesMap.transcriptsMap.variants"));
        result = rgaEngine.individualQuery(collection, new Query(), options);
        assertEquals(2, result.getNumResults());
        for (int i = 0; i < knockoutByIndividualList.size(); i++) {
            assertNotNull(result.getResults().get(i).getId());
            assertFalse(result.getResults().get(i).getPhenotypes().isEmpty());
            assertNull(result.getResults().get(i).getSampleId());
            assertTrue(result.getResults().get(i).getDisorders().isEmpty());
            assertNotNull(result.getResults().get(i).getGenes());
            for (KnockoutByIndividual.KnockoutGene gene : result.getResults().get(i).getGenes()) {
                assertNotNull(gene.getId());
                assertNull(gene.getName());
//                assertNotNull(gene.getStrand());
                assertFalse(gene.getTranscripts().isEmpty());
                for (KnockoutTranscript transcript : gene.getTranscripts()) {
                    assertNotNull(transcript.getId());
                    assertNotNull(transcript.getBiotype());
                    assertTrue(transcript.getVariants().isEmpty());
                }
            }
        }

        // It should be not possible to completely exclude knockoutType without excluding the whole variant information
        options = new QueryOptions(QueryOptions.EXCLUDE, Arrays.asList("sampleId", "disorders", "genesMap.name",
                "genesMap.transcriptsMap.variants.knockoutType"));
        result = rgaEngine.individualQuery(collection, new Query(), options);
        assertEquals(2, result.getNumResults());
        for (int i = 0; i < knockoutByIndividualList.size(); i++) {
            assertNotNull(result.getResults().get(i).getId());
            assertFalse(result.getResults().get(i).getPhenotypes().isEmpty());
            assertNull(result.getResults().get(i).getSampleId());
            assertTrue(result.getResults().get(i).getDisorders().isEmpty());
            assertNotNull(result.getResults().get(i).getGenes());
            for (KnockoutByIndividual.KnockoutGene gene : result.getResults().get(i).getGenes()) {
                assertNotNull(gene.getId());
                assertNull(gene.getName());
//                assertNotNull(gene.getStrand());
                assertFalse(gene.getTranscripts().isEmpty());
                for (KnockoutTranscript transcript : gene.getTranscripts()) {
                    assertNotNull(transcript.getId());
                    assertNotNull(transcript.getBiotype());
                    assertFalse(transcript.getVariants().isEmpty());
                    for (KnockoutVariant variant : transcript.getVariants()) {
                        assertNotNull(variant.getId());
                        assertNotNull(variant.getKnockoutType());
                        assertFalse(variant.getPopulationFrequencies().isEmpty());
                    }
                }
            }
        }

    }

    @Test
    public void testGeneQuery() throws Exception {
        RgaEngine rgaEngine = solr.configure(storageConfiguration);

        String collection = solr.coreName;
        rgaEngine.create(collection);

        List<KnockoutByIndividual> knockoutByIndividualList = new ArrayList<>(2);
        knockoutByIndividualList.add(createKnockoutByIndividual(1));
        knockoutByIndividualList.add(createKnockoutByIndividual(2));

        rgaEngine.insert(collection, knockoutByIndividualList, Collections.emptyMap(), Collections.emptyMap());
        OpenCGAResult<KnockoutByGene> result = rgaEngine.geneQuery(collection, new Query(), new QueryOptions());

        assertEquals(4, result.getNumResults());
        for (KnockoutByGene resultResult : result.getResults()) {
            assertEquals(1, resultResult.getIndividuals().size());
            assertTrue(resultResult.getIndividuals().get(0).getId().equals("id1")
                    || resultResult.getIndividuals().get(0).getId().equals("id2"));
        }

        Query query = new Query(RgaQueryParams.DISORDERS.key(), "disorderId1");
        result = rgaEngine.geneQuery(collection, query, new QueryOptions());
        assertEquals(2, result.getNumResults());
        for (KnockoutByGene resultResult : result.getResults()) {
            assertEquals(1, resultResult.getIndividuals().size());
            assertEquals("id1", resultResult.getIndividuals().get(0).getId());
        }

        query = new Query(RgaQueryParams.DISORDERS.key(), "disorderId2");
        result = rgaEngine.geneQuery(collection, query, new QueryOptions());
        assertEquals(2, result.getNumResults());
        for (KnockoutByGene resultResult : result.getResults()) {
            assertEquals(1, resultResult.getIndividuals().size());
            assertEquals("id2", resultResult.getIndividuals().get(0).getId());
        }

        query = new Query(RgaQueryParams.DISORDERS.key(), "disorderId6");
        result = rgaEngine.geneQuery(collection, query, new QueryOptions());
        assertEquals(0, result.getNumResults());

        query = new Query(RgaQueryParams.CONSEQUENCE_TYPE.key(), "SO:0001891");
        result = rgaEngine.geneQuery(collection, query, new QueryOptions());
        assertEquals(4, result.getNumResults());
        for (KnockoutByGene resultResult : result.getResults()) {
            assertEquals(1, resultResult.getIndividuals().size());
            assertTrue(resultResult.getIndividuals().get(0).getId().equals("id1")
                    || resultResult.getIndividuals().get(0).getId().equals("id2"));
        }

        query = new Query(RgaQueryParams.CONSEQUENCE_TYPE.key(), "SO:0001822");
        result = rgaEngine.geneQuery(collection, query, new QueryOptions());
        assertEquals(0, result.getNumResults());

        query = new Query()
                .append(RgaQueryParams.DISORDERS.key(), "disorderId1")
                .append(RgaQueryParams.CONSEQUENCE_TYPE.key(), "SO:0001891");
        result = rgaEngine.geneQuery(collection, query, new QueryOptions());
        assertEquals(2, result.getNumResults());

        query = new Query()
                .append(RgaQueryParams.DISORDERS.key(), "disorderId1,disorder")
                .append(RgaQueryParams.CONSEQUENCE_TYPE.key(), "SO:0001891");
        result = rgaEngine.geneQuery(collection, query, new QueryOptions());
        assertEquals(2, result.getNumResults());
    }

    @Test
    public void testIncludeExcludeGeneQuery() throws Exception {
        RgaEngine rgaEngine = solr.configure(storageConfiguration);

        String collection = solr.coreName;
        rgaEngine.create(collection);

        List<KnockoutByIndividual> knockoutByIndividualList = new ArrayList<>(2);
        knockoutByIndividualList.add(createKnockoutByIndividual(1));
        knockoutByIndividualList.add(createKnockoutByIndividual(2));

        rgaEngine.insert(collection, knockoutByIndividualList, Collections.emptyMap(), Collections.emptyMap());

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList("name", "individuals.transcriptsMap.chromosome"));
        OpenCGAResult<KnockoutByGene> result = rgaEngine.geneQuery(collection, new Query(), options);
        assertEquals(4, result.getNumResults());
        for (KnockoutByGene gene : result.getResults()) {
            assertNotNull(gene.getId());
            assertNotNull(gene.getName());
            assertNull(gene.getBiotype());
            for (KnockoutByGene.KnockoutIndividual individual : gene.getIndividuals()) {
                assertNotNull(individual.getId());
                assertNull(individual.getSampleId());
                for (KnockoutTranscript transcript : individual.getTranscripts()) {
                    assertNotNull(transcript.getId());
//                    assertNotNull(transcript.getChromosome());
                    assertNull(transcript.getBiotype());
                    assertTrue(transcript.getVariants().isEmpty());
                }
            }
        }

        options = new QueryOptions(QueryOptions.EXCLUDE, Arrays.asList("name", "individuals.transcriptsMap.chromosome",
                "individuals.transcriptsMap.variants"));
        result = rgaEngine.geneQuery(collection, new Query(), options);
        assertEquals(4, result.getNumResults());
        for (KnockoutByGene gene : result.getResults()) {
            assertNotNull(gene.getId());
            assertNull(gene.getName());
//            assertNotNull(gene.getBiotype());
            for (KnockoutByGene.KnockoutIndividual individual : gene.getIndividuals()) {
                assertNotNull(individual.getId());
                assertNotNull(individual.getSampleId());
                for (KnockoutTranscript transcript : individual.getTranscripts()) {
                    assertNotNull(transcript.getId());
                    assertNull(transcript.getChromosome());
                    assertNotNull(transcript.getBiotype());
                    assertTrue(transcript.getVariants().isEmpty());
                }
            }
        }

        // It should be not possible to completely exclude knockoutType without excluding the whole variant information
        options = new QueryOptions(QueryOptions.EXCLUDE, Arrays.asList("name", "individuals.transcriptsMap.chromosome",
                "individuals.transcriptsMap.variants.knockoutType"));
        result = rgaEngine.geneQuery(collection, new Query(), options);
        assertEquals(4, result.getNumResults());
        for (KnockoutByGene gene : result.getResults()) {
            assertNotNull(gene.getId());
            assertNull(gene.getName());
//            assertNotNull(gene.getBiotype());
            for (KnockoutByGene.KnockoutIndividual individual : gene.getIndividuals()) {
                assertNotNull(individual.getId());
                assertNotNull(individual.getSampleId());
                for (KnockoutTranscript transcript : individual.getTranscripts()) {
                    assertNotNull(transcript.getId());
                    assertNull(transcript.getChromosome());
                    assertNotNull(transcript.getBiotype());
                    assertFalse(transcript.getVariants().isEmpty());
                    for (KnockoutVariant variant : transcript.getVariants()) {
                        assertNotNull(variant.getId());
                        assertNotNull(variant.getKnockoutType());
                        assertFalse(variant.getPopulationFrequencies().isEmpty());
                    }
                }
            }
        }
    }

    @Test
    public void testVariantQuery() throws Exception {
        RgaEngine rgaEngine = solr.configure(storageConfiguration);

        String collection = solr.coreName;
        rgaEngine.create(collection);

        List<KnockoutByIndividual> knockoutByIndividualList = new ArrayList<>(2);
        knockoutByIndividualList.add(createKnockoutByIndividual(1));
        knockoutByIndividualList.add(createKnockoutByIndividual(2));

        rgaEngine.insert(collection, knockoutByIndividualList, Collections.emptyMap(), Collections.emptyMap());
        OpenCGAResult<KnockoutByVariant> result = rgaEngine.variantQuery(collection, new Query(), new QueryOptions());

        assertEquals(6, result.getNumResults());
    }

    @Test
    public void testIncludeExcludeVariantQuery() throws Exception {
        RgaEngine rgaEngine = solr.configure(storageConfiguration);

        String collection = solr.coreName;
        rgaEngine.create(collection);

        List<KnockoutByIndividual> knockoutByIndividualList = new ArrayList<>(2);
        knockoutByIndividualList.add(createKnockoutByIndividual(1));
        knockoutByIndividualList.add(createKnockoutByIndividual(2));

        rgaEngine.insert(collection, knockoutByIndividualList, Collections.emptyMap(), Collections.emptyMap());

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList("individuals.sampleId", "individuals.disorders",
                "individuals.genesMap.name"));
        OpenCGAResult<KnockoutByVariant> result = rgaEngine.variantQuery(collection, new Query(), options);
        assertEquals(6, result.getNumResults());
        for (KnockoutByVariant variant : result.getResults()) {
            assertNotNull(variant.getId());
            for (KnockoutByIndividual individual : variant.getIndividuals()) {
                assertNotNull(individual.getId());
                assertTrue(individual.getPhenotypes().isEmpty());
                assertNotNull(individual.getSampleId());
                assertNotNull(individual.getDisorders());
                assertNotNull(individual.getGenes());
                for (KnockoutByIndividual.KnockoutGene gene : individual.getGenes()) {
                    assertNotNull(gene.getId());
                    assertNotNull(gene.getName());
                    assertNull(gene.getStrand());
                    assertFalse(gene.getTranscripts().isEmpty());
                }

            }
        }

        // Not possible excluding variants object because that's basic for this data model
        options = new QueryOptions(QueryOptions.EXCLUDE, Arrays.asList("individuals.sampleId", "individuals.disorders",
                "individuals.genesMap.name", "individuals.genesMap.transcriptsMap.variants"));
        result = rgaEngine.variantQuery(collection, new Query(), options);
        assertEquals(6, result.getNumResults());
        for (KnockoutByVariant variant : result.getResults()) {
            assertNotNull(variant.getId());
            for (KnockoutByIndividual individual : variant.getIndividuals()) {
                assertNotNull(individual.getId());
                assertFalse(individual.getPhenotypes().isEmpty());
                assertNull(individual.getSampleId());
                assertTrue(individual.getDisorders().isEmpty());
                assertNotNull(individual.getGenes());
                for (KnockoutByIndividual.KnockoutGene gene : individual.getGenes()) {
                    assertNotNull(gene.getId());
                    assertNull(gene.getName());
//                assertNotNull(gene.getStrand());
                    assertFalse(gene.getTranscripts().isEmpty());
                    for (KnockoutTranscript transcript : gene.getTranscripts()) {
                        assertNotNull(transcript.getId());
                        assertNotNull(transcript.getBiotype());
                        assertFalse(transcript.getVariants().isEmpty());
                    }

                }
            }
        }

        // It should be not possible to completely exclude knockoutType without excluding the whole variant information
        options = new QueryOptions(QueryOptions.EXCLUDE, Arrays.asList("individuals.sampleId", "individuals.disorders",
                "individuals.genesMap.name", "individuals.genesMap.transcriptsMap.variants.knockoutType"));
        result = rgaEngine.variantQuery(collection, new Query(), options);
        assertEquals(6, result.getNumResults());
        for (KnockoutByVariant variant : result.getResults()) {
            assertNotNull(variant.getId());
            for (KnockoutByIndividual individual : variant.getIndividuals()) {
                assertNotNull(individual.getId());
                assertFalse(individual.getPhenotypes().isEmpty());
                assertNull(individual.getSampleId());
                assertTrue(individual.getDisorders().isEmpty());
                assertNotNull(individual.getGenes());
                for (KnockoutByIndividual.KnockoutGene gene : individual.getGenes()) {
                    assertNotNull(gene.getId());
                    assertNull(gene.getName());
//                assertNotNull(gene.getStrand());
                    assertFalse(gene.getTranscripts().isEmpty());
                    for (KnockoutTranscript transcript : gene.getTranscripts()) {
                        assertNotNull(transcript.getId());
                        assertNotNull(transcript.getBiotype());
                        assertFalse(transcript.getVariants().isEmpty());
                        for (KnockoutVariant tmpVariant : transcript.getVariants()) {
                            assertNotNull(tmpVariant.getId());
                            assertNotNull(tmpVariant.getKnockoutType());
                            assertFalse(tmpVariant.getPopulationFrequencies().isEmpty());
                        }
                    }
                }
            }
        }

    }


    @Test
    public void testFacet() throws Exception {
        RgaEngine rgaEngine = solr.configure(storageConfiguration);

        String collection = solr.coreName;
        rgaEngine.create(collection);

        List<KnockoutByIndividual> knockoutByIndividualList = new ArrayList<>(2);
        knockoutByIndividualList.add(createKnockoutByIndividual(1));
        knockoutByIndividualList.add(createKnockoutByIndividual(2));

        rgaEngine.insert(collection, knockoutByIndividualList, Collections.emptyMap(), Collections.emptyMap());

        QueryOptions options = new QueryOptions(QueryOptions.FACET, RgaQueryParams.DISORDERS.key());
        DataResult<FacetField> facetFieldDataResult = rgaEngine.facetedQuery(collection, new Query(), options);
        assertEquals(1, facetFieldDataResult.getNumResults());
        assertEquals(RgaDataModel.DISORDERS, facetFieldDataResult.first().getName());
        assertEquals(4, facetFieldDataResult.first().getBuckets().size());
    }

}