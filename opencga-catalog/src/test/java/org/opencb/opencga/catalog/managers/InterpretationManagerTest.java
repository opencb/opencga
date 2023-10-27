package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.common.Status;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.clinical.*;
import org.opencb.opencga.core.models.common.StatusParam;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.panel.PanelReferenceParam;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import java.util.*;

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
                .setStatus(new Status().setId(ClinicalAnalysisStatus.READY_FOR_INTERPRETATION))
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

    private List<File> registerDummyFiles() throws CatalogException {
        List<File> files = new LinkedList<>();

        String vcfFile = getClass().getResource("/biofiles/variant-test-file.vcf.gz").getFile();
        files.add(catalogManager.getFileManager().link(studyFqn, new FileLinkParams(vcfFile, "", "", "", null, null, null, null,
                null), false, ownerToken).first());
        vcfFile = getClass().getResource("/biofiles/family.vcf").getFile();
        files.add(catalogManager.getFileManager().link(studyFqn, new FileLinkParams(vcfFile, "", "", "", null, null, null, null,
                null), false, ownerToken).first());
        String bamFile = getClass().getResource("/biofiles/HG00096.chrom20.small.bam").getFile();
        files.add(catalogManager.getFileManager().link(studyFqn, new FileLinkParams(bamFile, "", "", "", null, null, null, null,
                null), false, ownerToken).first());
        bamFile = getClass().getResource("/biofiles/NA19600.chrom20.small.bam").getFile();
        files.add(catalogManager.getFileManager().link(studyFqn, new FileLinkParams(bamFile, "", "", "", null, null, null, null,
                null), false, ownerToken).first());

        return files;
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
        assertTrue(StringUtils.isEmpty(interpretation.getStatus().getId()));
        assertFalse(interpretation.isLocked());

        interpretation = catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), interpretation.getId(),
                new InterpretationUpdateParams().setStatus(new StatusParam("READY")), null, INCLUDE_RESULT, ownerToken).first();
        assertEquals("READY", interpretation.getStatus().getId());
        assertTrue(interpretation.isLocked());

        interpretation = catalogManager.getInterpretationManager().create(studyFqn, ca.getId(),
                new Interpretation()
                        .setStatus(new Status("REJECTED", "", "", "")),
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

        // Try to update interpretation 1
        try {
            catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getInterpretation().getId(),
                    new InterpretationUpdateParams().setDescription("blabla"), null, QueryOptions.empty(), ownerToken);
            fail("Interpretation is locked so it should not allow this");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("locked"));
        }

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

        // Try to lock again and update interpretation 2
        try {
            catalogManager.getInterpretationManager().update(studyFqn, ca.getId(), ca.getSecondaryInterpretations().get(0).getId(),
                    new InterpretationUpdateParams().setDescription("blabla").setLocked(true), null, QueryOptions.empty(), ownerToken);
            fail("Interpretation was already locked so it should not allow this");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("locked"));
        }

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
                .setPanelLock(true);
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