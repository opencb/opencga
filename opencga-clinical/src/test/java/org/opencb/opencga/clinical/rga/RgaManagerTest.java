package org.opencb.opencga.clinical.rga;

import org.apache.solr.client.solrj.SolrServerException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByIndividual;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByVariant;
import org.opencb.opencga.core.models.analysis.knockout.RgaKnockoutByGene;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualAclEntry;
import org.opencb.opencga.core.models.individual.IndividualAclParams;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleAclEntry;
import org.opencb.opencga.core.models.sample.SampleAclParams;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.RgaException;
import org.opencb.opencga.storage.core.rga.RgaEngine;
import org.opencb.opencga.storage.core.rga.RgaEngineTest;
import org.opencb.opencga.storage.core.rga.RgaSolrExtenalResource;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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

        rgaEngine.insert(collection, knockoutByIndividualList);
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
        catalogManager.getSampleManager().updateAcl(study, Collections.singletonList("sample1"), "user2",
                new SampleAclParams("", "", "", "", SampleAclEntry.SamplePermissions.VIEW.name() + ","
                        + SampleAclEntry.SamplePermissions.VIEW_VARIANTS.name()), ParamUtils.AclAction.ADD, ownerToken);
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
        OpenCGAResult<RgaKnockoutByGene> result = rgaManager.geneQuery(study, new Query(), QueryOptions.empty(), ownerToken);
        assertEquals(4, result.getNumResults());
        for (RgaKnockoutByGene gene : result.getResults()) {
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
        List<RgaKnockoutByGene> genes = result.getResults().stream().filter(g -> !g.getIndividuals().isEmpty()).collect(Collectors.toList());
        assertEquals(2, genes.size());
        for (RgaKnockoutByGene gene : genes) {
            assertEquals(1, gene.getIndividuals().size());
            assertEquals("sample1", gene.getIndividuals().get(0).getSampleId());
        }
    }

    @Test
    public void testGeneQueryLimit() throws CatalogException, IOException, RgaException {
        OpenCGAResult<RgaKnockoutByGene> result = rgaManager.geneQuery(study, new Query(), new QueryOptions(QueryOptions.LIMIT, 1),
                ownerToken);
        assertEquals("geneId1", result.first().getId());
        assertEquals(1, result.getNumResults());
        assertEquals(1, result.first().getIndividuals().size());
    }

    @Test
    public void testGeneQuerySkip() throws CatalogException, IOException, RgaException {
        OpenCGAResult<RgaKnockoutByGene> result = rgaManager.geneQuery(study, new Query(), new QueryOptions(QueryOptions.SKIP, 1),
                ownerToken);
        assertEquals(3, result.getNumResults());
        assertTrue(result.getResults().stream().map(RgaKnockoutByGene::getId).collect(Collectors.toSet())
                .containsAll(Arrays.asList("geneId11", "geneId12", "geneId2")));
        for (RgaKnockoutByGene gene : result.getResults()) {
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

}