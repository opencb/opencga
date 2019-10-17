package org.opencb.opencga.catalog.managers;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.update.PanelUpdateParams;
import org.opencb.opencga.core.models.Account;
import org.opencb.opencga.core.models.Panel;
import org.opencb.opencga.core.models.Study;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class PanelManagerTest extends GenericTest {

    public final static String PASSWORD = "asdf";
    private String studyFqn = "user@1000G:phase1";
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public CatalogManagerExternalResource catalogManagerResource = new CatalogManagerExternalResource();

    protected CatalogManager catalogManager;
    protected String sessionIdUser;

    private PanelManager panelManager;
    private String adminToken;

    @Before
    public void setUp() throws IOException, CatalogException {
        catalogManager = catalogManagerResource.getCatalogManager();
        panelManager = catalogManager.getPanelManager();
        setUpCatalogManager(catalogManager);
        adminToken = catalogManager.getUserManager().login("admin", "admin");
    }

    private void setUpCatalogManager(CatalogManager catalogManager) throws IOException, CatalogException {
        catalogManager.getUserManager().create("user", "User Name", "mail@ebi.ac.uk", PASSWORD, "", null, Account.Type.FULL, null);
        sessionIdUser = catalogManager.getUserManager().login("user", PASSWORD);

        String projectId = catalogManager.getProjectManager().create("1000G", "Project about some genomes", "", "ACME", "Homo sapiens",
                null, null, "GRCh38", new QueryOptions(), sessionIdUser).first().getId();
        catalogManager.getStudyManager().create(projectId, "phase1", null, "Phase 1", Study.Type.TRIO, null, "Done", null, null, null, null,
                null, null, null, null, sessionIdUser);
    }

    @Test
    @Ignore
    public void importFromPanelAppTest() throws CatalogException, IOException {
        String token = catalogManager.getUserManager().login("admin", "admin");
        panelManager.importPanelApp(token, false);
        assertEquals(221, panelManager.count(PanelManager.INSTALLATION_PANELS, new Query(), token).getNumTotalResults());
    }

    @Test
    public void createTest() throws IOException, CatalogException {
        Panel panel = Panel.load(getClass().getResource("/disease_panels/panel1.json").openStream());

        DataResult<Panel> diseasePanelDataResult = panelManager.create(studyFqn, panel, null, sessionIdUser);
        assertEquals(1, diseasePanelDataResult.getNumResults());
        assertEquals(panel.getId(), diseasePanelDataResult.first().getId());
        assertEquals(panel.toString(), diseasePanelDataResult.first().toString());
    }

    @Test
    public void createInstallationPanel() throws CatalogException, IOException {
        Panel panel = Panel.load(getClass().getResource("/disease_panels/panel1.json").openStream());

        panelManager.create(panel, false, adminToken);
        DataResult<Panel> diseasePanelDataResult = panelManager.get(PanelManager.INSTALLATION_PANELS, panel.getId(),
                QueryOptions.empty(), null);
        assertEquals(1, diseasePanelDataResult.getNumResults());
        assertEquals(panel.getId(), diseasePanelDataResult.first().getId());
        assertEquals(panel.toString(), diseasePanelDataResult.first().toString());
    }

    @Test
    public void createInstallationPanelNoOverwrite() throws CatalogException, IOException {
        Panel panel = Panel.load(getClass().getResource("/disease_panels/panel1.json").openStream());

        panelManager.create(panel, false, adminToken);
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("already exists");
        panelManager.create(panel, false, adminToken);
    }

    @Test
    public void createInstallationPanelOverwrite() throws CatalogException, IOException {
        Panel panel = Panel.load(getClass().getResource("/disease_panels/panel1.json").openStream());

        panelManager.create(panel, false, adminToken);
        DataResult<Panel> diseasePanelDataResult = panelManager.get(PanelManager.INSTALLATION_PANELS, panel.getId(),
                QueryOptions.empty(), null);
        panelManager.create(panel, true, adminToken);
        DataResult<Panel> diseasePanelDataResult2 = panelManager.get(PanelManager.INSTALLATION_PANELS, panel.getId(),
                QueryOptions.empty(), null);

        assertNotEquals(diseasePanelDataResult.first().getUuid(), diseasePanelDataResult2.first().getUuid());
    }

    @Test
    public void createInstallationPanelNoAdmin() throws CatalogException, IOException {
        Panel panel = Panel.load(getClass().getResource("/disease_panels/panel1.json").openStream());

        thrown.expect(CatalogAuthorizationException.class);
        panelManager.create(panel, false, sessionIdUser);
    }

    @Test
    public void importGlobalPanel() throws CatalogException, IOException {
        Panel panel = Panel.load(getClass().getResource("/disease_panels/panel1.json").openStream());
        panelManager.create(panel, false, adminToken);

        Panel installationPanel = panelManager.get(PanelManager.INSTALLATION_PANELS, panel.getId(),
                QueryOptions.empty(), null).first();

        DataResult<Panel> diseasePanelDataResult = panelManager.importGlobalPanels(studyFqn,
                Collections.singletonList(panel.getId()),
                QueryOptions.empty(), sessionIdUser);

        assertEquals(1, diseasePanelDataResult.getNumResults());
        assertNotEquals(installationPanel.getUuid(), diseasePanelDataResult.first().getUuid());
    }

    @Test
    public void importAllGlobalPanels() throws CatalogException, IOException {
        Panel panel = Panel.load(getClass().getResource("/disease_panels/panel1.json").openStream());
        panelManager.create(panel, false, adminToken);

        Panel panel2 = Panel.load(getClass().getResource("/disease_panels/panel2.json").openStream());
        panelManager.create(panel2, false, adminToken);

        DataResult<Panel> diseasePanelDataResult = panelManager.importAllGlobalPanels(studyFqn, QueryOptions.empty(), sessionIdUser);

        assertEquals(2, diseasePanelDataResult.getNumMatches());
    }

    @Test
    public void updateTest() throws IOException, CatalogException {
        Panel panel = Panel.load(getClass().getResource("/disease_panels/panel1.json").openStream());
        panelManager.create(panel, false, adminToken);
        Panel diseasePanelDataResult = panelManager.importGlobalPanels(studyFqn,
                Collections.singletonList(panel.getId()), null, sessionIdUser).first();

        DiseasePanel.RegionPanel regionPanel = new DiseasePanel.RegionPanel();
        regionPanel.setCoordinates(Collections.singletonList(new DiseasePanel.Coordinate("", "chr1:1-1000", "")));

        DiseasePanel.VariantPanel variantPanel = new DiseasePanel.VariantPanel();
        variantPanel.setId("variant1");

        DiseasePanel.GenePanel genePanel = new DiseasePanel.GenePanel();
        genePanel.setId("BRCA2");

        PanelUpdateParams updateParams = new PanelUpdateParams()
                .setSource(new DiseasePanel.SourcePanel().setAuthor("author"))
                .setRegions(Collections.singletonList(regionPanel))
                .setPhenotypes(Collections.singletonList(new Phenotype().setId("ontologyTerm")))
                .setVariants(Collections.singletonList(variantPanel))
                .setGenes(Collections.singletonList(genePanel));

        DataResult<Panel> updateResult = panelManager.update(studyFqn, diseasePanelDataResult.getId(), updateParams, null, sessionIdUser);
        assertEquals(1, updateResult.getNumUpdated());

        Panel updatedPanel = panelManager.get(studyFqn, diseasePanelDataResult.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertEquals("author", updatedPanel.getSource().getAuthor());
        assertEquals(1, updatedPanel.getRegions().size());
        assertEquals("chr1:1-1000", updatedPanel.getRegions().get(0).getCoordinates().get(0).getLocation());
        assertEquals(1, updatedPanel.getGenes().size());
        assertEquals("BRCA2", updatedPanel.getGenes().get(0).getId());
        assertEquals(1, updatedPanel.getPhenotypes().size());
        assertEquals("ontologyTerm", updatedPanel.getPhenotypes().get(0).getId());
        assertEquals(1, updatedPanel.getVariants().size());
        assertEquals("variant1", updatedPanel.getVariants().get(0).getId());
    }

}
