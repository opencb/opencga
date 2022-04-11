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
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.models.core.OntologyTerm;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.catalog.db.api.PanelDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisUpdateParams;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.panel.PanelUpdateParams;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class PanelManagerTest extends GenericTest {

    private String studyFqn = "user@1000G:phase1";
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public CatalogManagerExternalResource catalogManagerResource = new CatalogManagerExternalResource();

    protected CatalogManager catalogManager;
    protected String sessionIdUser;

    private PanelManager panelManager;
    private String adminToken;

    private static final QueryOptions INCLUDE_RESULT = new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true);

    @Before
    public void setUp() throws IOException, CatalogException {
        catalogManager = catalogManagerResource.getCatalogManager();
        panelManager = catalogManager.getPanelManager();
        setUpCatalogManager(catalogManager);
        adminToken = catalogManager.getUserManager().loginAsAdmin(TestParamConstants.ADMIN_PASSWORD).getToken();
    }

    private void setUpCatalogManager(CatalogManager catalogManager) throws IOException, CatalogException {
        catalogManager.getUserManager().create("user", "User Name", "mail@ebi.ac.uk", TestParamConstants.PASSWORD, "", null, Account.AccountType.FULL, null);
        sessionIdUser = catalogManager.getUserManager().login("user", TestParamConstants.PASSWORD).getToken();

        String projectId = catalogManager.getProjectManager().create("1000G", "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", INCLUDE_RESULT, sessionIdUser).first().getId();
        catalogManager.getStudyManager().create(projectId, "phase1", null, "Phase 1", "Done", null, null, null, null, null, sessionIdUser);
    }

    @Test
    public void createTest() throws IOException, CatalogException {
        Panel panel = Panel.load(getClass().getResource("/disease_panels/panel1.json").openStream());

        DataResult<Panel> diseasePanelDataResult = panelManager.create(studyFqn, panel, INCLUDE_RESULT, sessionIdUser);
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
        Panel panel = panelManager.get(studyFqn, "gene-census", QueryOptions.empty(), sessionIdUser).first();
        assertEquals(1, panel.getVersion());
        assertEquals((int) panel.getStats().get("numberOfRegions"), panel.getVariants().size());
        assertEquals((int) panel.getStats().get("numberOfVariants"), panel.getVariants().size());
        assertEquals((int) panel.getStats().get("numberOfGenes"), panel.getGenes().size());

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
        assertEquals(2, updatedPanel.getVersion());
        assertEquals("author", updatedPanel.getSource().getAuthor());
        assertEquals(1, updatedPanel.getRegions().size());
        assertEquals("chr1:1-1000", updatedPanel.getRegions().get(0).getCoordinates().get(0).getLocation());
        assertEquals(1, updatedPanel.getGenes().size());
        assertEquals("BRCA2", updatedPanel.getGenes().get(0).getId());
        assertEquals(1, updatedPanel.getDisorders().size());
        assertEquals("ontologyTerm", updatedPanel.getDisorders().get(0).getId());
        assertEquals(1, updatedPanel.getVariants().size());
        assertEquals("variant1", updatedPanel.getVariants().get(0).getId());

        assertEquals((int) updatedPanel.getStats().get("numberOfRegions"), updatedPanel.getVariants().size());
        assertEquals((int) updatedPanel.getStats().get("numberOfVariants"), updatedPanel.getVariants().size());
        assertEquals((int) updatedPanel.getStats().get("numberOfGenes"), updatedPanel.getGenes().size());

        Query query = new Query()
                .append(PanelDBAdaptor.QueryParams.VERSION.key(), 1);
        panel = panelManager.get(studyFqn, Collections.singletonList("gene-census"), query, QueryOptions.empty(), false, sessionIdUser).first();
        assertEquals("gene-census", panel.getId());
        assertEquals(1, panel.getVersion());
    }

    @Test
    public void deletePanelTest() throws CatalogException {
        panelManager.importFromSource(studyFqn, "cancer-gene-census", null, sessionIdUser);
        Panel panel = panelManager.get(studyFqn, "gene-census", QueryOptions.empty(), sessionIdUser).first();
        assertEquals(1, panel.getVersion());

        OpenCGAResult<?> result = panelManager.delete(studyFqn, Collections.singletonList("gene-census"), QueryOptions.empty(), sessionIdUser);
        assertEquals(1, result.getNumDeleted());

        result = panelManager.get(studyFqn, Collections.singletonList("gene-census"), QueryOptions.empty(), true, sessionIdUser);
        assertEquals(0, result.getNumResults());

        Query query = new Query()
                .append(ParamConstants.DELETED_PARAM, true);
        result = panelManager.get(studyFqn, Collections.singletonList("gene-census"), query, QueryOptions.empty(), false, sessionIdUser);
        assertEquals(1, result.getNumResults());
    }

    @Test
    public void deletePanelWithVersionsTest() throws CatalogException {
        panelManager.importFromSource(studyFqn, "cancer-gene-census", null, sessionIdUser);
        Panel panel = panelManager.get(studyFqn, "gene-census", QueryOptions.empty(), sessionIdUser).first();
        assertEquals(1, panel.getVersion());

        PanelUpdateParams updateParams = new PanelUpdateParams()
                .setSource(new DiseasePanel.SourcePanel().setAuthor("author"))
                .setDisorders(Collections.singletonList(new OntologyTerm().setId("ontologyTerm")));
        DataResult<Panel> updateResult = panelManager.update(studyFqn, "gene-census", updateParams, null, sessionIdUser);
        assertEquals(1, updateResult.getNumUpdated());

        OpenCGAResult<?> result = panelManager.delete(studyFqn, Collections.singletonList("gene-census"), QueryOptions.empty(), sessionIdUser);
        assertEquals(1, result.getNumDeleted());

        result = panelManager.get(studyFqn, Collections.singletonList("gene-census"), QueryOptions.empty(), true, sessionIdUser);
        assertEquals(0, result.getNumResults());

        Query query = new Query()
                .append(Constants.ALL_VERSIONS, true)
                .append(ParamConstants.DELETED_PARAM, true);
        result = panelManager.get(studyFqn, Collections.singletonList("gene-census"), query, QueryOptions.empty(), false, sessionIdUser);
        assertEquals(2, result.getNumResults());
    }

    @Test
    public void panelIncrementVersionWithCasesAndInterpretations() throws CatalogException {
        Panel panel1 = DummyModelUtils.getDummyPanel("panel1");
        Panel panel2 = DummyModelUtils.getDummyPanel("panel2");
        Panel panel3 = DummyModelUtils.getDummyPanel("panel3");
        Panel panel4 = DummyModelUtils.getDummyPanel("panel4");

        Family family1 = DummyModelUtils.getDummyCaseFamily("family");

        // Unlocked case
        ClinicalAnalysis case1 = DummyModelUtils.getDummyClinicalAnalysis(family1.getMembers().get(0), family1, Arrays.asList(panel1, panel2));
        Interpretation interpretation1 = DummyModelUtils.getDummyInterpretation(Collections.singletonList(panel1))
                .setLocked(true);
        Interpretation interpretation2 = DummyModelUtils.getDummyInterpretation(Collections.singletonList(panel2));
        Interpretation interpretation3 = DummyModelUtils.getDummyInterpretation(Collections.singletonList(panel1));

        // panelLocked true
        ClinicalAnalysis case2 = DummyModelUtils.getDummyClinicalAnalysis(family1.getMembers().get(0), family1, Arrays.asList(panel1, panel3));
        Interpretation interpretation4 = DummyModelUtils.getDummyInterpretation(Collections.singletonList(panel1))
                .setLocked(true);
        Interpretation interpretation5 = DummyModelUtils.getDummyInterpretation(Arrays.asList(panel1, panel3));

        // locked true
        ClinicalAnalysis case3 = DummyModelUtils.getDummyClinicalAnalysis(family1.getMembers().get(0), family1, Arrays.asList(panel1, panel4));
        Interpretation interpretation6 = DummyModelUtils.getDummyInterpretation(Collections.singletonList(panel1))
                .setLocked(true);
        Interpretation interpretation7 = DummyModelUtils.getDummyInterpretation(Collections.singletonList(panel1));

        // unlocked
        ClinicalAnalysis case4 = DummyModelUtils.getDummyClinicalAnalysis(family1.getMembers().get(0), family1, Arrays.asList(panel3, panel4));
        Interpretation interpretation8 = DummyModelUtils.getDummyInterpretation(Collections.singletonList(panel3))
                .setLocked(true);
        Interpretation interpretation9 = DummyModelUtils.getDummyInterpretation(Collections.singletonList(panel3));

        catalogManager.getPanelManager().create(studyFqn, panel1, QueryOptions.empty(), sessionIdUser);
        catalogManager.getPanelManager().create(studyFqn, panel2, QueryOptions.empty(), sessionIdUser);
        catalogManager.getPanelManager().create(studyFqn, panel3, QueryOptions.empty(), sessionIdUser);
        catalogManager.getPanelManager().create(studyFqn, panel4, QueryOptions.empty(), sessionIdUser);

        catalogManager.getFamilyManager().create(studyFqn, family1, QueryOptions.empty(), sessionIdUser);

        catalogManager.getClinicalAnalysisManager().create(studyFqn, case1, QueryOptions.empty(), sessionIdUser);
        catalogManager.getClinicalAnalysisManager().create(studyFqn, case2, QueryOptions.empty(), sessionIdUser);
        catalogManager.getClinicalAnalysisManager().create(studyFqn, case3, QueryOptions.empty(), sessionIdUser);
        catalogManager.getClinicalAnalysisManager().create(studyFqn, case4, QueryOptions.empty(), sessionIdUser);

        // case1
        catalogManager.getInterpretationManager().create(studyFqn, case1.getId(), interpretation1,
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), sessionIdUser);
        catalogManager.getInterpretationManager().create(studyFqn, case1.getId(), interpretation2,
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), sessionIdUser);
        catalogManager.getInterpretationManager().create(studyFqn, case1.getId(), interpretation3,
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), sessionIdUser);

        // case 2
        catalogManager.getInterpretationManager().create(studyFqn, case2.getId(), interpretation4,
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), sessionIdUser);
        catalogManager.getInterpretationManager().create(studyFqn, case2.getId(), interpretation5,
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), sessionIdUser);
        catalogManager.getClinicalAnalysisManager().update(studyFqn, case2.getId(), new ClinicalAnalysisUpdateParams().setPanelLock(true),
                QueryOptions.empty(), sessionIdUser);

        // case 3
        catalogManager.getInterpretationManager().create(studyFqn, case3.getId(), interpretation6,
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), sessionIdUser);
        catalogManager.getInterpretationManager().create(studyFqn, case3.getId(), interpretation7,
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), sessionIdUser);
        catalogManager.getClinicalAnalysisManager().update(studyFqn, case3.getId(), new ClinicalAnalysisUpdateParams().setLocked(true),
                QueryOptions.empty(), sessionIdUser);

        // case 4
        catalogManager.getInterpretationManager().create(studyFqn, case4.getId(), interpretation8,
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), sessionIdUser);
        catalogManager.getInterpretationManager().create(studyFqn, case4.getId(), interpretation9,
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), sessionIdUser);
        catalogManager.getClinicalAnalysisManager().update(studyFqn, case4.getId(), new ClinicalAnalysisUpdateParams().setLocked(true),
                QueryOptions.empty(), sessionIdUser);

        // Update panel1 ...
        panelManager.update(studyFqn, panel1.getId(), new PanelUpdateParams().setName("name"), QueryOptions.empty(), sessionIdUser);
        OpenCGAResult<Panel> resultPanel = panelManager.get(studyFqn, panel1.getId(), QueryOptions.empty(), sessionIdUser);
        assertEquals(2, resultPanel.first().getVersion());
        assertEquals("name", resultPanel.first().getName());

        Query query = new Query()
                .append(PanelDBAdaptor.QueryParams.ID.key(), panel1.getId())
                .append(PanelDBAdaptor.QueryParams.VERSION.key(), 1);
        resultPanel = panelManager.search(studyFqn, query, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, resultPanel.first().getVersion());
        assertNotEquals("name", resultPanel.first().getName());

        OpenCGAResult<ClinicalAnalysis> result = catalogManager.getClinicalAnalysisManager().get(studyFqn,
                Arrays.asList(case1.getId(), case2.getId(), case3.getId(), case4.getId()), QueryOptions.empty(), sessionIdUser);
        case1 = result.getResults().get(0);
        case2 = result.getResults().get(1);
        case3 = result.getResults().get(2);
        case4 = result.getResults().get(3);

        // case1 checks
        assertEquals(2, case1.getPanels().size());
        assertEquals(2, case1.getPanels().get(0).getVersion());
        assertEquals(1, case1.getPanels().get(1).getVersion());

        assertEquals(2, case1.getInterpretation().getPanels().size());
        assertEquals(2, case1.getInterpretation().getPanels().get(0).getVersion());
        assertEquals(1, case1.getInterpretation().getPanels().get(1).getVersion());

        assertEquals(1, case1.getSecondaryInterpretations().get(0).getPanels().get(0).getVersion()); // interpretation1
        assertEquals(1, case1.getSecondaryInterpretations().get(1).getPanels().get(0).getVersion()); // interpretation2
        assertEquals(2, case1.getSecondaryInterpretations().get(2).getPanels().get(0).getVersion()); // interpretation3

        // case2 checks
        assertEquals(2, case2.getPanels().size());
        assertEquals(1, case2.getPanels().get(0).getVersion());
        assertEquals(1, case2.getPanels().get(1).getVersion());

        assertEquals(2, case2.getInterpretation().getPanels().size());
        assertEquals(1, case2.getInterpretation().getPanels().get(0).getVersion());
        assertEquals(1, case2.getInterpretation().getPanels().get(1).getVersion());

        assertEquals(1, case2.getSecondaryInterpretations().get(0).getPanels().get(0).getVersion()); // interpretation4
        assertEquals(1, case2.getSecondaryInterpretations().get(1).getPanels().get(0).getVersion()); // interpretation5

        // case3 checks
        assertEquals(2, case3.getPanels().size());
        assertEquals(1, case3.getPanels().get(0).getVersion());
        assertEquals(1, case3.getPanels().get(1).getVersion());

        assertEquals(2, case3.getInterpretation().getPanels().size());
        assertEquals(1, case3.getInterpretation().getPanels().get(0).getVersion());
        assertEquals(1, case3.getInterpretation().getPanels().get(1).getVersion());

        assertEquals(1, case3.getSecondaryInterpretations().get(0).getPanels().get(0).getVersion()); // interpretation6
        assertEquals(1, case3.getSecondaryInterpretations().get(1).getPanels().get(0).getVersion()); // interpretation7

        // case4 checks
        assertEquals(2, case4.getPanels().size());
        assertEquals(1, case4.getPanels().get(0).getVersion());
        assertEquals(1, case4.getPanels().get(1).getVersion());

        assertEquals(2, case4.getInterpretation().getPanels().size());
        assertEquals(1, case4.getInterpretation().getPanels().get(0).getVersion());
        assertEquals(1, case4.getInterpretation().getPanels().get(1).getVersion());

        assertEquals(1, case4.getSecondaryInterpretations().get(0).getPanels().get(0).getVersion()); // interpretation8
        assertEquals(1, case4.getSecondaryInterpretations().get(1).getPanels().get(0).getVersion()); // interpretation9
    }

}
