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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.clinical.ClinicalProperty;
import org.opencb.biodata.models.clinical.interpretation.CancerPanel;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.models.core.OntologyTerm;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
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
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

@Category(MediumTests.class)
public class PanelManagerTest extends AbstractManagerTest {

    private PanelManager panelManager;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        panelManager = catalogManager.getPanelManager();
    }

    @Test
    public void createTest() throws IOException, CatalogException {
        Panel panel = Panel.load(getClass().getResource("/disease_panels/panel1.json").openStream());

        DataResult<Panel> diseasePanelDataResult = panelManager.create(studyFqn, panel, INCLUDE_RESULT, ownerToken);
        assertEquals(1, diseasePanelDataResult.getNumResults());
        assertEquals(panel.getId(), diseasePanelDataResult.first().getId());
        assertEquals(panel.toString(), diseasePanelDataResult.first().toString());
    }

    @Test
    public void importFromSource() throws CatalogException {
        OpenCGAResult<Panel> cancer = panelManager.importFromSource(studyFqn, "gene-census", null, ownerToken);
        assertEquals(1, cancer.getNumInserted());

        OpenCGAResult<Panel> panelApp = panelManager.importFromSource(studyFqn, "panelapp", "Thoracic_aortic_aneurysm_and_dissection-PanelAppId-700,VACTERL-like_phenotypes-PanelAppId-101", ownerToken);
        assertEquals(2, panelApp.getNumInserted());
    }

    @Test
    public void importFromSourceInvalidId() throws CatalogException {
        thrown.expect(CatalogException.class);
        thrown.expectMessage("Unknown panel");
        panelManager.importFromSource(studyFqn, "gene-census", "ZSR222", ownerToken);
    }

    @Test
    public void importFromInvalidSource() throws CatalogException {
        thrown.expect(CatalogException.class);
        thrown.expectMessage("Unknown source");
        panelManager.importFromSource(studyFqn, "gene-census-wrong", null, ownerToken);
    }

    @Test
    public void updateTest() throws CatalogException {
        panelManager.importFromSource(studyFqn, "gene-census", null, ownerToken);
        Panel panel = panelManager.get(studyFqn, "gene-census", QueryOptions.empty(), ownerToken).first();
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

        DataResult<Panel> updateResult = panelManager.update(studyFqn, "gene-census", updateParams, null, ownerToken);
        assertEquals(1, updateResult.getNumUpdated());

        Panel updatedPanel = panelManager.get(studyFqn, "gene-census", QueryOptions.empty(), ownerToken).first();
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
        panel = panelManager.get(studyFqn, Collections.singletonList("gene-census"), query, QueryOptions.empty(), false, ownerToken).first();
        assertEquals("gene-census", panel.getId());
        assertEquals(1, panel.getVersion());
    }

    @Test
    public void deletePanelTest() throws CatalogException {
        panelManager.importFromSource(studyFqn, "gene-census", null, ownerToken);
        Panel panel = panelManager.get(studyFqn, "gene-census", QueryOptions.empty(), ownerToken).first();
        assertEquals(1, panel.getVersion());

        OpenCGAResult<?> result = panelManager.delete(studyFqn, Collections.singletonList("gene-census"), QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumDeleted());

        result = panelManager.get(studyFqn, Collections.singletonList("gene-census"), QueryOptions.empty(), true, ownerToken);
        assertEquals(0, result.getNumResults());

        Query query = new Query()
                .append(ParamConstants.DELETED_PARAM, true);
        result = panelManager.get(studyFqn, Collections.singletonList("gene-census"), query, QueryOptions.empty(), false, ownerToken);
        assertEquals(1, result.getNumResults());
    }

    @Test
    public void deletePanelWithVersionsTest() throws CatalogException {
        panelManager.importFromSource(studyFqn, "gene-census", null, ownerToken);
        Panel panel = panelManager.get(studyFqn, "gene-census", QueryOptions.empty(), ownerToken).first();
        assertEquals(1, panel.getVersion());

        PanelUpdateParams updateParams = new PanelUpdateParams()
                .setSource(new DiseasePanel.SourcePanel().setAuthor("author"))
                .setDisorders(Collections.singletonList(new OntologyTerm().setId("ontologyTerm")));
        DataResult<Panel> updateResult = panelManager.update(studyFqn, "gene-census", updateParams, null, ownerToken);
        assertEquals(1, updateResult.getNumUpdated());

        OpenCGAResult<?> result = panelManager.delete(studyFqn, Collections.singletonList("gene-census"), QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumDeleted());

        result = panelManager.get(studyFqn, Collections.singletonList("gene-census"), QueryOptions.empty(), true, ownerToken);
        assertEquals(0, result.getNumResults());

        Query query = new Query()
                .append(Constants.ALL_VERSIONS, true)
                .append(ParamConstants.DELETED_PARAM, true);
        result = panelManager.get(studyFqn, Collections.singletonList("gene-census"), query, QueryOptions.empty(), false, ownerToken);
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

        catalogManager.getPanelManager().create(studyFqn, panel1, QueryOptions.empty(), ownerToken);
        catalogManager.getPanelManager().create(studyFqn, panel2, QueryOptions.empty(), ownerToken);
        catalogManager.getPanelManager().create(studyFqn, panel3, QueryOptions.empty(), ownerToken);
        catalogManager.getPanelManager().create(studyFqn, panel4, QueryOptions.empty(), ownerToken);

        catalogManager.getFamilyManager().create(studyFqn, family1, QueryOptions.empty(), ownerToken);

        catalogManager.getClinicalAnalysisManager().create(studyFqn, case1, QueryOptions.empty(), ownerToken);
        catalogManager.getClinicalAnalysisManager().create(studyFqn, case2, QueryOptions.empty(), ownerToken);
        catalogManager.getClinicalAnalysisManager().create(studyFqn, case3, QueryOptions.empty(), ownerToken);
        catalogManager.getClinicalAnalysisManager().create(studyFqn, case4, QueryOptions.empty(), ownerToken);

        // case1
        catalogManager.getInterpretationManager().create(studyFqn, case1.getId(), interpretation1,
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), ownerToken);
        catalogManager.getInterpretationManager().create(studyFqn, case1.getId(), interpretation2,
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), ownerToken);
        catalogManager.getInterpretationManager().create(studyFqn, case1.getId(), interpretation3,
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), ownerToken);

        // case 2
        catalogManager.getInterpretationManager().create(studyFqn, case2.getId(), interpretation4,
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), ownerToken);
        catalogManager.getInterpretationManager().create(studyFqn, case2.getId(), interpretation5,
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), ownerToken);
        catalogManager.getClinicalAnalysisManager().update(studyFqn, case2.getId(), new ClinicalAnalysisUpdateParams().setPanelLock(true),
                QueryOptions.empty(), ownerToken);

        // case 3
        catalogManager.getInterpretationManager().create(studyFqn, case3.getId(), interpretation6,
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), ownerToken);
        catalogManager.getInterpretationManager().create(studyFqn, case3.getId(), interpretation7,
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), ownerToken);
        catalogManager.getClinicalAnalysisManager().update(studyFqn, case3.getId(), new ClinicalAnalysisUpdateParams().setLocked(true),
                QueryOptions.empty(), ownerToken);

        // case 4
        catalogManager.getInterpretationManager().create(studyFqn, case4.getId(), interpretation8,
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), ownerToken);
        catalogManager.getInterpretationManager().create(studyFqn, case4.getId(), interpretation9,
                ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), ownerToken);
        catalogManager.getClinicalAnalysisManager().update(studyFqn, case4.getId(), new ClinicalAnalysisUpdateParams().setLocked(true),
                QueryOptions.empty(), ownerToken);

        // Update panel1 ...
        panelManager.update(studyFqn, panel1.getId(), new PanelUpdateParams().setName("name"), QueryOptions.empty(), ownerToken);
        OpenCGAResult<Panel> resultPanel = panelManager.get(studyFqn, panel1.getId(), QueryOptions.empty(), ownerToken);
        assertEquals(2, resultPanel.first().getVersion());
        assertEquals("name", resultPanel.first().getName());

        Query query = new Query()
                .append(PanelDBAdaptor.QueryParams.ID.key(), panel1.getId())
                .append(PanelDBAdaptor.QueryParams.VERSION.key(), 1);
        resultPanel = panelManager.search(studyFqn, query, QueryOptions.empty(), ownerToken);
        assertEquals(1, resultPanel.first().getVersion());
        assertNotEquals("name", resultPanel.first().getName());

        OpenCGAResult<ClinicalAnalysis> result = catalogManager.getClinicalAnalysisManager().get(studyFqn,
                Arrays.asList(case1.getId(), case2.getId(), case3.getId(), case4.getId()), QueryOptions.empty(), ownerToken);
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

    private DiseasePanel.GenePanel createGenePanel(String id, String name) {
        DiseasePanel.GenePanel gene = new DiseasePanel.GenePanel();
        gene.setId(id);
        gene.setName(name);
        gene.setModesOfInheritance(Arrays.asList(ClinicalProperty.ModeOfInheritance.AUTOSOMAL_DOMINANT, ClinicalProperty.ModeOfInheritance.AUTOSOMAL_RECESSIVE));
        gene.setCancer(new CancerPanel().setRoles(Arrays.asList(ClinicalProperty.RoleInCancer.BOTH, ClinicalProperty.RoleInCancer.ONCOGENE)));
        return gene;
    }

    @Test
    public void geneIdAndNameQueryTest() throws CatalogException {
        Panel panel1 = new Panel()
                .setId("panel1")
                .setGenes(Arrays.asList(createGenePanel("geneId1", "geneName1"), createGenePanel("geneId2", "geneName2")));
        Panel panel2 = new Panel()
                .setId("panel2")
                .setGenes(Arrays.asList(createGenePanel("geneId1", "geneName1"), createGenePanel("geneId3", "geneName3")));

        panelManager.create(studyFqn, panel1, QueryOptions.empty(), ownerToken);
        panelManager.create(studyFqn, panel2, QueryOptions.empty(), ownerToken);

        OpenCGAResult<Panel> result = panelManager.search(studyFqn, new Query(PanelDBAdaptor.QueryParams.GENES.key(), "geneName2"), QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals("panel1", result.first().getId());

        result = panelManager.search(studyFqn, new Query(PanelDBAdaptor.QueryParams.GENES.key(), "geneId2"), QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals("panel1", result.first().getId());

        result = panelManager.search(studyFqn, new Query(PanelDBAdaptor.QueryParams.GENES.key(), "geneName1"), QueryOptions.empty(), ownerToken);
        assertEquals(2, result.getNumResults());
        assertTrue(result.getResults().stream().map(Panel::getId).collect(Collectors.toList()).containsAll(Arrays.asList("panel1", "panel2")));

        result = panelManager.search(studyFqn, new Query(PanelDBAdaptor.QueryParams.GENES.key(), "geneId1"), QueryOptions.empty(), ownerToken);
        assertEquals(2, result.getNumResults());
        assertTrue(result.getResults().stream().map(Panel::getId).collect(Collectors.toList()).containsAll(Arrays.asList("panel1", "panel2")));

        result = panelManager.search(studyFqn, new Query(PanelDBAdaptor.QueryParams.GENES.key(), "geneId3"), QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals("panel2", result.first().getId());

        result = panelManager.search(studyFqn, new Query(PanelDBAdaptor.QueryParams.GENES.key(), "geneName3"), QueryOptions.empty(), ownerToken);
        assertEquals(1, result.getNumResults());
        assertEquals("panel2", result.first().getId());

        result = panelManager.search(studyFqn, new Query(PanelDBAdaptor.QueryParams.GENES.key(), "geneId4"), QueryOptions.empty(), ownerToken);
        assertEquals(0, result.getNumResults());
    }

}
