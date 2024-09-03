package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.clinical.*;
import org.opencb.opencga.core.models.common.StatusParam;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.panel.PanelReferenceParam;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.StudyAclParams;
import org.opencb.opencga.core.models.study.StudyPermissions;
import org.opencb.opencga.core.models.study.configuration.ClinicalAnalysisStudyConfiguration;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

@Category(MediumTests.class)
public class InterpretationManagerTest extends AbstractManagerTest {

    private FamilyManager familyManager;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        familyManager = catalogManager.getFamilyManager();
    }

    private DataResult<Family> createDummyFamily() throws CatalogException {
        Family family = DummyModelUtils.getDummyFamily("family");
        return familyManager.create(studyFqn, family, INCLUDE_RESULT, ownerToken);
    }

    private DataResult<ClinicalAnalysis> createDummyEnvironment(boolean createFamily, boolean createDefaultInterpretation) throws CatalogException {

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setStatus(new ClinicalStatus().setId("READY_FOR_INTERPRETATION"))
                .setId("analysis" + RandomStringUtils.randomAlphanumeric(3))
                .setDescription("My description").setType(ClinicalAnalysis.Type.FAMILY)
                .setProband(new Individual().setId("child1").setSamples(Arrays.asList(new Sample().setId("sample2"))));

        if (createFamily) {
            createDummyFamily();
        }
        clinicalAnalysis.setFamily(new Family().setId("family")
                .setMembers(Arrays.asList(new Individual().setId("child1").setSamples(Arrays.asList(new Sample().setId("sample2"))))));

        return catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, !createDefaultInterpretation,
                INCLUDE_RESULT, ownerToken);
    }

    @Test
    public void deleteLockedInterpretationTest() throws CatalogException {
        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();
        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), new Interpretation().setLocked(true),
                ParamUtils.SaveInterpretationAs.PRIMARY, QueryOptions.empty(), ownerToken);
        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(), ownerToken).first();

        Interpretation interpretation = catalogManager.getInterpretationManager().get(studyFqn, ca.getInterpretation().getId(),
                QueryOptions.empty(), ownerToken).first();
        assertTrue(interpretation.isLocked());

        // Try to delete interpretation
        try {
            catalogManager.getInterpretationManager().delete(studyFqn, ca.getId(), Collections.singletonList(ca.getInterpretation().getId()),
                    ownerToken);
            fail("Interpretation is locked so it should not allow this");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("locked"));
        }

        // Unlock interpretation
        catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getInterpretation().getId(),
                new InterpretationUpdateParams().setLocked(false), null, QueryOptions.empty(), ownerToken);
        interpretation = catalogManager.getInterpretationManager().get(studyFqn, ca.getInterpretation().getId(),
                QueryOptions.empty(), ownerToken).first();
        assertFalse(interpretation.isLocked());

        // Delete interpretation
        catalogManager.getInterpretationManager().delete(studyFqn, ca.getId(), Collections.singletonList(ca.getInterpretation().getId()),
                ownerToken);
        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(), ownerToken).first();
        assertNull(ca.getInterpretation());
    }

    @Test
    public void automaticallyLockInterpretationTest() throws CatalogException {
        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();
        Interpretation interpretation = catalogManager.getInterpretationManager().create(studyFqn, ca.getId(),
                new Interpretation(),  ParamUtils.SaveInterpretationAs.PRIMARY, INCLUDE_RESULT, ownerToken).first();
        assertEquals(ClinicalStatusValue.ClinicalStatusType.NOT_STARTED, interpretation.getStatus().getType());
        assertFalse(interpretation.isLocked());

        interpretation = catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), interpretation.getId(),
                new InterpretationUpdateParams().setStatus(new StatusParam("READY")), null, INCLUDE_RESULT, ownerToken).first();
        assertEquals("READY", interpretation.getStatus().getId());
        assertTrue(interpretation.isLocked());

        interpretation = catalogManager.getInterpretationManager().create(studyFqn, ca.getId(),
                new Interpretation()
                        .setStatus(new ClinicalStatus("REJECTED", "", null, "", "", "", "")),
                ParamUtils.SaveInterpretationAs.PRIMARY, INCLUDE_RESULT, ownerToken).first();
        assertEquals("REJECTED", interpretation.getStatus().getId());
        assertTrue(interpretation.isLocked());
    }

    @Test
    public void interpretationLockedTest() throws CatalogException {
        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();

        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), new Interpretation().setLocked(true),
                ParamUtils.SaveInterpretationAs.PRIMARY, QueryOptions.empty(), ownerToken);
        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), ownerToken);
        catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), ownerToken);

        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(), ownerToken).first();
        assertTrue(ca.getInterpretation().isLocked());
        for (Interpretation secondaryInterpretation : ca.getSecondaryInterpretations()) {
            assertFalse(secondaryInterpretation.isLocked());
        }

        // Add ADMIN permissions to the user2
        catalogManager.getStudyManager().updateAcl(studyFqn, normalUserId2,
                new StudyAclParams(StudyPermissions.Permissions.ADMIN_CLINICAL_ANALYSIS.name(), null), ParamUtils.AclAction.SET, ownerToken);

        // Try to update interpretation 1
        try {
            catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getInterpretation().getId(),
                    new InterpretationUpdateParams().setDescription("blabla"), null, QueryOptions.empty(), normalToken1);
            fail("Interpretation is locked so it should not allow this");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains(ClinicalAnalysisPermissions.ADMIN.name()));
        }

        // Try to update interpretation 1
        catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getInterpretation().getId(),
                new InterpretationUpdateParams().setDescription("blabla"), null, QueryOptions.empty(), normalToken2);
        Interpretation interpretation = catalogManager.getInterpretationManager().get(studyFqn, ca.getInterpretation().getId(),
                QueryOptions.empty(), ownerToken).first();
        assertEquals("blabla", interpretation.getDescription());
        assertTrue(interpretation.isLocked());

        // Try to update interpretation 1
        catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getInterpretation().getId(),
                new InterpretationUpdateParams().setDescription("blabla2"), null, QueryOptions.empty(), ownerToken);
        interpretation = catalogManager.getInterpretationManager().get(studyFqn, ca.getInterpretation().getId(), QueryOptions.empty(), ownerToken).first();
        assertEquals("blabla2", interpretation.getDescription());
        assertTrue(interpretation.isLocked());

        // Update interpretation 2
        catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getSecondaryInterpretations().get(0).getId(),
                new InterpretationUpdateParams().setDescription("blabla"), null, QueryOptions.empty(), ownerToken);
        Interpretation interpretation2 = catalogManager.getInterpretationManager().get(studyFqn,
                ca.getSecondaryInterpretations().get(0).getId(), QueryOptions.empty(), ownerToken).first();
        assertEquals("blabla", interpretation2.getDescription());
        assertFalse(interpretation2.isLocked());

        catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getSecondaryInterpretations().get(0).getId(),
                new InterpretationUpdateParams().setDescription("bloblo").setLocked(true), null, QueryOptions.empty(), ownerToken);
        interpretation2 = catalogManager.getInterpretationManager().get(studyFqn,
                ca.getSecondaryInterpretations().get(0).getId(), QueryOptions.empty(), ownerToken).first();
        assertEquals("bloblo", interpretation2.getDescription());
        assertTrue(interpretation2.isLocked());

        // Unlock and update interpretation 2
        catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getSecondaryInterpretations().get(0).getId(),
                new InterpretationUpdateParams().setDescription("blabla").setLocked(false), null, QueryOptions.empty(), ownerToken);
        interpretation2 = catalogManager.getInterpretationManager().get(studyFqn,
                ca.getSecondaryInterpretations().get(0).getId(), QueryOptions.empty(), ownerToken).first();
        assertEquals("blabla", interpretation2.getDescription());
        assertFalse(interpretation2.isLocked());

        // Lock and update interpretation 2
        catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getSecondaryInterpretations().get(0).getId(),
                new InterpretationUpdateParams().setDescription("bloblo").setLocked(true), null, QueryOptions.empty(), ownerToken);
        interpretation2 = catalogManager.getInterpretationManager().get(studyFqn,
                ca.getSecondaryInterpretations().get(0).getId(), QueryOptions.empty(), ownerToken).first();
        assertEquals("bloblo", interpretation2.getDescription());
        assertTrue(interpretation2.isLocked());

        // Lock case
        catalogManager.getClinicalAnalysisManager().update(studyFqn, ca.getId(), new ClinicalAnalysisUpdateParams().setLocked(true),
                QueryOptions.empty(), ownerToken);
        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(), ownerToken).first();
        assertTrue(ca.isLocked());
        assertTrue(ca.getInterpretation().isLocked());
        for (Interpretation secondaryInterpretation : ca.getSecondaryInterpretations()) {
            assertTrue(secondaryInterpretation.isLocked());
        }

        // Try to update the interpretation 1
        try {
            catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getInterpretation().getId(),
                    new InterpretationUpdateParams().setDescription("new description"), null, QueryOptions.empty(), normalToken1);
            fail("Case and Interpretation are locked so it should not allow this");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains(ClinicalAnalysisPermissions.ADMIN.name()) && e.getMessage().toLowerCase().contains("permission denied"));
        }

        // Try to update the interpretation 1 (ADMIN permission)
        catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getInterpretation().getId(),
                new InterpretationUpdateParams().setDescription("new description"), null, QueryOptions.empty(), normalToken2);
        interpretation = catalogManager.getInterpretationManager().get(studyFqn, ca.getInterpretation().getId(), QueryOptions.empty(), ownerToken).first();
        assertEquals("new description", interpretation.getDescription());
        assertTrue(interpretation.isLocked());

        // Try to update the interpretation 1 (owner user)
        catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getInterpretation().getId(),
                new InterpretationUpdateParams().setDescription("new description2"), null, QueryOptions.empty(), ownerToken);
        interpretation = catalogManager.getInterpretationManager().get(studyFqn, ca.getInterpretation().getId(), QueryOptions.empty(), ownerToken).first();
        assertEquals("new description2", interpretation.getDescription());
        assertTrue(interpretation.isLocked());

        // Try to unlock interpretation 1
        try {
            catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getInterpretation().getId(),
                    new InterpretationUpdateParams().setLocked(false), null, QueryOptions.empty(), ownerToken);
            fail("Case is locked so it should not allow this");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("locked") && e.getMessage().toLowerCase().contains("case"));
        }

        // Unlock case
        catalogManager.getClinicalAnalysisManager().update(studyFqn, ca.getId(), new ClinicalAnalysisUpdateParams().setLocked(false),
                QueryOptions.empty(), ownerToken);
        ca = catalogManager.getClinicalAnalysisManager().get(studyFqn, ca.getId(), QueryOptions.empty(), ownerToken).first();
        assertFalse(ca.isLocked());
        assertTrue(ca.getInterpretation().isLocked());
        for (Interpretation secondaryInterpretation : ca.getSecondaryInterpretations()) {
            assertTrue(secondaryInterpretation.isLocked());
        }
    }

    @Test
    public void interpretationStatusTest() throws CatalogException {
        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();

        Interpretation interpretation = catalogManager.getInterpretationManager().create(studyFqn, ca.getId(), new Interpretation(),
                ParamUtils.SaveInterpretationAs.PRIMARY, INCLUDE_RESULT, ownerToken).first();

        // Create 2 allowed statuses of type CLOSED
        ClinicalAnalysisStudyConfiguration studyConfiguration = ClinicalAnalysisStudyConfiguration.defaultConfiguration();
        List<ClinicalStatusValue> statusValueList = new ArrayList<>();
        for (ClinicalStatusValue status : studyConfiguration.getInterpretation().getStatus()) {
            if (!status.getType().equals(ClinicalStatusValue.ClinicalStatusType.CLOSED)) {
                statusValueList.add(status);
            }
        }
        // Add two statuses of type CLOSED
        statusValueList.add(new ClinicalStatusValue("closed1", "my desc", ClinicalStatusValue.ClinicalStatusType.CLOSED));
        statusValueList.add(new ClinicalStatusValue("closed2", "my desc", ClinicalStatusValue.ClinicalStatusType.CLOSED));
        studyConfiguration.getInterpretation().setStatus(statusValueList);
        catalogManager.getClinicalAnalysisManager().configureStudy(studyFqn, studyConfiguration, studyAdminToken1);

        // Update status to one of the new statuses
        catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), interpretation.getId(),
                new InterpretationUpdateParams().setStatus(new StatusParam("closed1")), null, QueryOptions.empty(), studyAdminToken1);
        interpretation = catalogManager.getInterpretationManager().get(studyFqn, interpretation.getId(), QueryOptions.empty(), studyAdminToken1).first();
        assertEquals("closed1", interpretation.getStatus().getId());
        assertEquals(ClinicalStatusValue.ClinicalStatusType.CLOSED, interpretation.getStatus().getType());
        assertTrue(interpretation.isLocked());

        // Update status to the other new CLOSED status
        catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), interpretation.getId(),
                new InterpretationUpdateParams().setStatus(new StatusParam("closed2")), null, QueryOptions.empty(), studyAdminToken1);
        assertEquals("closed1", interpretation.getStatus().getId());
        assertEquals(ClinicalStatusValue.ClinicalStatusType.CLOSED, interpretation.getStatus().getType());
        assertTrue(interpretation.isLocked());
    }

    @Test
    public void createInterpretationWithSubsetOfPanels() throws CatalogException {
        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();

        List<PanelReferenceParam> panelReferenceParamList = new ArrayList<>(3);
        for (int i = 0; i < 3; i++) {
            Panel panel = new Panel().setId("panel" + i);
            panelReferenceParamList.add(new PanelReferenceParam(panel.getId()));
            catalogManager.getPanelManager().create(studyFqn, panel, QueryOptions.empty(), ownerToken);
        }

        // Add panels to the case and set panelLock to true
        ClinicalAnalysisUpdateParams updateParams = new ClinicalAnalysisUpdateParams()
                .setPanels(panelReferenceParamList);
        catalogManager.getClinicalAnalysisManager().update(studyFqn, ca.getId(), updateParams, QueryOptions.empty(), ownerToken);

        updateParams = new ClinicalAnalysisUpdateParams()
                .setPanelLocked(true);
        catalogManager.getClinicalAnalysisManager().update(studyFqn, ca.getId(), updateParams, QueryOptions.empty(), ownerToken);

        // Create interpretation with just panel1
        InterpretationCreateParams interpretationCreateParams = new InterpretationCreateParams()
                .setPanels(panelReferenceParamList.subList(0, 1));
        Interpretation interpretation = catalogManager.getInterpretationManager().create(studyFqn, ca.getId(),
                interpretationCreateParams.toClinicalInterpretation(), ParamUtils.SaveInterpretationAs.PRIMARY,
                new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), ownerToken).first();
        assertEquals(1, interpretation.getPanels().size());
        assertEquals(panelReferenceParamList.get(0).getId(), interpretation.getPanels().get(0).getId());
    }

}