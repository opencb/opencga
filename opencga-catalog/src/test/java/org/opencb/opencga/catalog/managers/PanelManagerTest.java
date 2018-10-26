package org.opencb.opencga.catalog.managers;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.catalog.db.api.PanelDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.Account;
import org.opencb.opencga.core.models.Panel;
import org.opencb.opencga.core.models.Status;
import org.opencb.opencga.core.models.Study;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

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
        catalogManager.getUserManager().create("user", "User Name", "mail@ebi.ac.uk", PASSWORD, "", null, Account.FULL, null, null);
        sessionIdUser = catalogManager.getUserManager().login("user", PASSWORD);

        String projectId = catalogManager.getProjectManager().create("1000G", "Project about some genomes", "", "ACME", "Homo sapiens",
                null, null, "GRCh38", new QueryOptions(), sessionIdUser).first().getId();
        catalogManager.getStudyManager().create(projectId, "phase1", null, "Phase 1", Study.Type.TRIO, null, "Done", null, null, null, null,
                null, null, null, null, sessionIdUser);
    }

    @Test
    public void importFromPanelAppTest() throws CatalogException, IOException {
        String token = catalogManager.getUserManager().login("admin", "admin");
        panelManager.importPanelApp(token, false);
        assertEquals(190, panelManager.count(PanelManager.INSTALLATION_PANELS, new Query(), token).getNumTotalResults());
    }

    @Test
    public void createTest() throws IOException, CatalogException {
        Panel panel = Panel.load(getClass().getResource("/disease_panels/panel1.json").openStream());

        QueryResult<Panel> diseasePanelQueryResult = panelManager.create(studyFqn, panel, null, sessionIdUser);
        assertEquals(1, diseasePanelQueryResult.getNumResults());
        assertEquals(panel.getDiseasePanel().getId(), diseasePanelQueryResult.first().getDiseasePanel().getId());
        assertEquals(panel.toString(), diseasePanelQueryResult.first().toString());
    }

    @Test
    public void createInstallationPanel() throws CatalogException, IOException {
        Panel panel = Panel.load(getClass().getResource("/disease_panels/panel1.json").openStream());

        panelManager.create(panel, false, adminToken);
        QueryResult<Panel> diseasePanelQueryResult = panelManager.get(PanelManager.INSTALLATION_PANELS, panel.getDiseasePanel().getId(),
                QueryOptions.empty(), null);
        assertEquals(1, diseasePanelQueryResult.getNumResults());
        assertEquals(panel.getDiseasePanel().getId(), diseasePanelQueryResult.first().getDiseasePanel().getId());
        assertEquals(panel.toString(), diseasePanelQueryResult.first().toString());
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
        QueryResult<Panel> diseasePanelQueryResult = panelManager.get(PanelManager.INSTALLATION_PANELS, panel.getDiseasePanel().getId(),
                QueryOptions.empty(), null);
        panelManager.create(panel, true, adminToken);
        QueryResult<Panel> diseasePanelQueryResult2 = panelManager.get(PanelManager.INSTALLATION_PANELS, panel.getDiseasePanel().getId(),
                QueryOptions.empty(), null);

        assertNotEquals(diseasePanelQueryResult.first().getUuid(), diseasePanelQueryResult2.first().getUuid());
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

        Panel installationPanel = panelManager.get(PanelManager.INSTALLATION_PANELS, panel.getDiseasePanel().getId(),
                QueryOptions.empty(), null).first();

        QueryResult<Panel> diseasePanelQueryResult = panelManager.importInstallationPanel(studyFqn, panel.getDiseasePanel().getId(),
                QueryOptions.empty(), sessionIdUser);

        assertEquals(1, diseasePanelQueryResult.getNumResults());
        assertNotEquals(installationPanel.getUuid(), diseasePanelQueryResult.first().getUuid());
    }

    @Test
    public void updateTest() throws IOException, CatalogException {
        Panel panel = Panel.load(getClass().getResource("/disease_panels/panel1.json").openStream());
        panelManager.create(panel, false, adminToken);
        Panel diseasePanelQueryResult = panelManager.importInstallationPanel(studyFqn, panel.getDiseasePanel().getId(), null, sessionIdUser).first();

        ObjectMap params = new ObjectMap()
                .append(PanelDBAdaptor.UpdateParams.AUTHOR.key(), "author")
                .append(PanelDBAdaptor.UpdateParams.REGIONS.key(), Collections.singletonList(
                        new ObjectMap("location", "chr1:1-1000")
                ))
                .append(PanelDBAdaptor.UpdateParams.PHENOTYPES.key(), Collections.singletonList(
                        new ObjectMap("id", "ontologyTerm")
                ))
                .append(PanelDBAdaptor.UpdateParams.VARIANTS.key(), Collections.singletonList(
                        new ObjectMap("id", "variant1")
                ))
                .append(PanelDBAdaptor.UpdateParams.GENES.key(), Collections.singletonList(
                        new ObjectMap("id", "BRCA2")
                ));
        Panel panelUpdated = panelManager.update(studyFqn, diseasePanelQueryResult.getDiseasePanel().getId(), params, null, sessionIdUser)
                .first();

        assertEquals("author", panelUpdated.getDiseasePanel().getSource().getAuthor());
        assertEquals(1, panelUpdated.getDiseasePanel().getRegions().size());
        assertEquals("chr1:1-1000", panelUpdated.getDiseasePanel().getRegions().get(0).getLocation());
        assertEquals(1, panelUpdated.getDiseasePanel().getGenes().size());
        assertEquals("BRCA2", panelUpdated.getDiseasePanel().getGenes().get(0).getId());
        assertEquals(1, panelUpdated.getDiseasePanel().getPhenotypes().size());
        assertEquals("ontologyTerm", panelUpdated.getDiseasePanel().getPhenotypes().get(0).getId());
        assertEquals(1, panelUpdated.getDiseasePanel().getVariants().size());
        assertEquals("variant1", panelUpdated.getDiseasePanel().getVariants().get(0).getId());
    }

}
