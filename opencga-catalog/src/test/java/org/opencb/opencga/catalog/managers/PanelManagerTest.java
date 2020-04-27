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

package org.opencb.opencga.catalog.managers;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.models.core.OntologyTerm;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.panel.PanelUpdateParams;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

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
        adminToken = catalogManager.getUserManager().loginAsAdmin("admin").getToken();
    }

    private void setUpCatalogManager(CatalogManager catalogManager) throws IOException, CatalogException {
        catalogManager.getUserManager().create("user", "User Name", "mail@ebi.ac.uk", PASSWORD, "", null, Account.AccountType.FULL, null);
        sessionIdUser = catalogManager.getUserManager().login("user", PASSWORD).getToken();

        String projectId = catalogManager.getProjectManager().create("1000G", "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", new QueryOptions(), sessionIdUser).first().getId();
        catalogManager.getStudyManager().create(projectId, "phase1", null, "Phase 1", "Done", null, null, null, null, null, sessionIdUser);
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
    public void importFromSource() throws CatalogException {
        OpenCGAResult<Panel> cancer = panelManager.importFromSource(studyFqn, "cancer-gene-census", null, sessionIdUser);
        assertEquals(1, cancer.getNumInserted());

        OpenCGAResult<Panel> panelApp = panelManager.importFromSource(studyFqn, "panelapp", "Thoracic_aortic_aneurysm_and_dissection-PanelAppId-700,VACTERL-like_phenotypes-PanelAppId-101", sessionIdUser);
        assertEquals(2, panelApp.getNumInserted());
    }

    @Test
    public void importFromSourceInvalidId() throws CatalogException {
        thrown.expect(CatalogException.class);
        thrown.expectMessage("Unknown panel");
        panelManager.importFromSource(studyFqn, "cancer-gene-census", "ZSR222", sessionIdUser);
    }

    @Test
    public void importFromInvalidSource() throws CatalogException {
        thrown.expect(CatalogException.class);
        thrown.expectMessage("Unknown source");
        panelManager.importFromSource(studyFqn, "cancer-gene-census-wrong", null, sessionIdUser);
    }

    @Test
    public void updateTest() throws CatalogException {
        panelManager.importFromSource(studyFqn, "cancer-gene-census", null, sessionIdUser);

        DiseasePanel.RegionPanel regionPanel = new DiseasePanel.RegionPanel();
        regionPanel.setCoordinates(Collections.singletonList(new DiseasePanel.Coordinate("", "chr1:1-1000", "")));

        DiseasePanel.VariantPanel variantPanel = new DiseasePanel.VariantPanel();
        variantPanel.setId("variant1");

        DiseasePanel.GenePanel genePanel = new DiseasePanel.GenePanel();
        genePanel.setId("BRCA2");

        PanelUpdateParams updateParams = new PanelUpdateParams()
                .setSource(new DiseasePanel.SourcePanel().setAuthor("author"))
                .setRegions(Collections.singletonList(regionPanel))
                .setDisorders(Collections.singletonList(new OntologyTerm().setId("ontologyTerm")))
                .setVariants(Collections.singletonList(variantPanel))
                .setGenes(Collections.singletonList(genePanel));

        DataResult<Panel> updateResult = panelManager.update(studyFqn, "gene-census", updateParams, null, sessionIdUser);
        assertEquals(1, updateResult.getNumUpdated());

        Panel updatedPanel = panelManager.get(studyFqn, "gene-census", QueryOptions.empty(), sessionIdUser).first();
        assertEquals("author", updatedPanel.getSource().getAuthor());
        assertEquals(1, updatedPanel.getRegions().size());
        assertEquals("chr1:1-1000", updatedPanel.getRegions().get(0).getCoordinates().get(0).getLocation());
        assertEquals(1, updatedPanel.getGenes().size());
        assertEquals("BRCA2", updatedPanel.getGenes().get(0).getId());
        assertEquals(1, updatedPanel.getDisorders().size());
        assertEquals("ontologyTerm", updatedPanel.getDisorders().get(0).getId());
        assertEquals(1, updatedPanel.getVariants().size());
        assertEquals("variant1", updatedPanel.getVariants().get(0).getId());
    }

}
