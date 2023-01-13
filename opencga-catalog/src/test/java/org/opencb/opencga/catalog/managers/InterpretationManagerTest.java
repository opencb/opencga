package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.common.Status;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.TestParamConstants;
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
import org.opencb.opencga.core.models.user.Account;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class InterpretationManagerTest extends GenericTest {

    public final static String STUDY = "user@1000G:phase1";
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public CatalogManagerExternalResource catalogManagerResource = new CatalogManagerExternalResource();

    protected CatalogManager catalogManager;
    private String opencgaToken;
    protected String sessionIdUser;
    private FamilyManager familyManager;

    private static final QueryOptions INCLUDE_RESULT = new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true);

    @Before
    public void setUp() throws IOException, CatalogException {
        catalogManager = catalogManagerResource.getCatalogManager();
        familyManager = catalogManager.getFamilyManager();
        setUpCatalogManager(catalogManager);
    }

    public void setUpCatalogManager(CatalogManager catalogManager) throws IOException, CatalogException {
        opencgaToken = catalogManager.getUserManager().loginAsAdmin(TestParamConstants.ADMIN_PASSWORD).getToken();

        catalogManager.getUserManager().create("user", "User Name", "mail@ebi.ac.uk", TestParamConstants.PASSWORD, "", null, Account.AccountType.FULL, opencgaToken);
        sessionIdUser = catalogManager.getUserManager().login("user", TestParamConstants.PASSWORD).getToken();

        catalogManager.getUserManager().create("user2", "User Name2", "mail2@ebi.ac.uk", TestParamConstants.PASSWORD, "", null, Account.AccountType.GUEST,
                opencgaToken);

        String projectId = catalogManager.getProjectManager().create("1000G", "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", INCLUDE_RESULT, sessionIdUser).first().getId();
        catalogManager.getStudyManager().create(projectId, "phase1", null, "Phase 1", "Done", null, null, null, null, null, sessionIdUser);
    }

    @After
    public void tearDown() throws Exception {
    }

    private DataResult<Family> createDummyFamily() throws CatalogException {
        Family family = DummyModelUtils.getDummyFamily("family");
        return familyManager.create(STUDY, family, INCLUDE_RESULT, sessionIdUser);
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

        return catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis, !createDefaultInterpretation,
                INCLUDE_RESULT, sessionIdUser);
    }

    private List<File> registerDummyFiles() throws CatalogException {
        List<File> files = new LinkedList<>();

        String vcfFile = getClass().getResource("/biofiles/variant-test-file.vcf.gz").getFile();
        files.add(catalogManager.getFileManager().link(STUDY, new FileLinkParams(vcfFile, "", "", "", null, null, null,
                null), false, sessionIdUser).first());
        vcfFile = getClass().getResource("/biofiles/family.vcf").getFile();
        files.add(catalogManager.getFileManager().link(STUDY, new FileLinkParams(vcfFile, "", "", "", null, null, null,
                null), false, sessionIdUser).first());
        String bamFile = getClass().getResource("/biofiles/HG00096.chrom20.small.bam").getFile();
        files.add(catalogManager.getFileManager().link(STUDY, new FileLinkParams(bamFile, "", "", "", null, null, null,
                null), false, sessionIdUser).first());
        bamFile = getClass().getResource("/biofiles/NA19600.chrom20.small.bam").getFile();
        files.add(catalogManager.getFileManager().link(STUDY, new FileLinkParams(bamFile, "", "", "", null, null, null,
                null), false, sessionIdUser).first());

        return files;
    }

    @Test
    public void deleteLockedInterpretationTest() throws CatalogException {
        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();
        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), new Interpretation().setLocked(true),
                ParamUtils.SaveInterpretationAs.PRIMARY, QueryOptions.empty(), sessionIdUser);
        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();

        Interpretation interpretation = catalogManager.getInterpretationManager().get(STUDY, ca.getInterpretation().getId(),
                QueryOptions.empty(), sessionIdUser).first();
        assertTrue(interpretation.isLocked());

        // Try to delete interpretation
        try {
            catalogManager.getInterpretationManager().delete(STUDY, ca.getId(), Collections.singletonList(ca.getInterpretation().getId()),
                    sessionIdUser);
            fail("Interpretation is locked so it should not allow this");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("locked"));
        }

        // Unlock interpretation
        catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getInterpretation().getId(),
                new InterpretationUpdateParams().setLocked(false), null, QueryOptions.empty(), sessionIdUser);
        interpretation = catalogManager.getInterpretationManager().get(STUDY, ca.getInterpretation().getId(),
                QueryOptions.empty(), sessionIdUser).first();
        assertFalse(interpretation.isLocked());

        // Delete interpretation
        catalogManager.getInterpretationManager().delete(STUDY, ca.getId(), Collections.singletonList(ca.getInterpretation().getId()),
                sessionIdUser);
        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertNull(ca.getInterpretation());
    }

    @Test
    public void automaticallyLockInterpretationTest() throws CatalogException {
        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();
        Interpretation interpretation = catalogManager.getInterpretationManager().create(STUDY, ca.getId(),
                new Interpretation(),  ParamUtils.SaveInterpretationAs.PRIMARY, INCLUDE_RESULT, sessionIdUser).first();
        assertTrue(StringUtils.isEmpty(interpretation.getStatus().getId()));
        assertFalse(interpretation.isLocked());

        interpretation = catalogManager.getInterpretationManager().update(STUDY, ca.getId(), interpretation.getId(),
                new InterpretationUpdateParams().setStatus(new StatusParam("READY")), null, INCLUDE_RESULT, sessionIdUser).first();
        assertEquals("READY", interpretation.getStatus().getId());
        assertTrue(interpretation.isLocked());

        interpretation = catalogManager.getInterpretationManager().create(STUDY, ca.getId(),
                new Interpretation()
                        .setStatus(new Status("REJECTED", "", "", "")),
                ParamUtils.SaveInterpretationAs.PRIMARY, INCLUDE_RESULT, sessionIdUser).first();
        assertEquals("REJECTED", interpretation.getStatus().getId());
        assertTrue(interpretation.isLocked());
    }

    @Test
    public void interpretationLockedTest() throws CatalogException {
        ClinicalAnalysis ca = createDummyEnvironment(true, false).first();

        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), new Interpretation().setLocked(true),
                ParamUtils.SaveInterpretationAs.PRIMARY, QueryOptions.empty(), sessionIdUser);
        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), sessionIdUser);
        catalogManager.getInterpretationManager().create(STUDY, ca.getId(), new Interpretation(), ParamUtils.SaveInterpretationAs.SECONDARY,
                QueryOptions.empty(), sessionIdUser);

        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertTrue(ca.getInterpretation().isLocked());
        for (Interpretation secondaryInterpretation : ca.getSecondaryInterpretations()) {
            assertFalse(secondaryInterpretation.isLocked());
        }

        // Try to update interpretation 1
        try {
            catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getInterpretation().getId(),
                    new InterpretationUpdateParams().setDescription("blabla"), null, QueryOptions.empty(), sessionIdUser);
            fail("Interpretation is locked so it should not allow this");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("locked"));
        }

        // Update interpretation 2
        catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getSecondaryInterpretations().get(0).getId(),
                new InterpretationUpdateParams().setDescription("blabla"), null, QueryOptions.empty(), sessionIdUser);
        Interpretation interpretation2 = catalogManager.getInterpretationManager().get(STUDY,
                ca.getSecondaryInterpretations().get(0).getId(), QueryOptions.empty(), sessionIdUser).first();
        assertEquals("blabla", interpretation2.getDescription());
        assertFalse(interpretation2.isLocked());

        catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getSecondaryInterpretations().get(0).getId(),
                new InterpretationUpdateParams().setDescription("bloblo").setLocked(true), null, QueryOptions.empty(), sessionIdUser);
        interpretation2 = catalogManager.getInterpretationManager().get(STUDY,
                ca.getSecondaryInterpretations().get(0).getId(), QueryOptions.empty(), sessionIdUser).first();
        assertEquals("bloblo", interpretation2.getDescription());
        assertTrue(interpretation2.isLocked());

        // Try to lock again and update interpretation 2
        try {
            catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getSecondaryInterpretations().get(0).getId(),
                    new InterpretationUpdateParams().setDescription("blabla").setLocked(true), null, QueryOptions.empty(), sessionIdUser);
            fail("Interpretation was already locked so it should not allow this");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("locked"));
        }

        // Unlock and update interpretation 2
        catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getSecondaryInterpretations().get(0).getId(),
                new InterpretationUpdateParams().setDescription("blabla").setLocked(false), null, QueryOptions.empty(), sessionIdUser);
        interpretation2 = catalogManager.getInterpretationManager().get(STUDY,
                ca.getSecondaryInterpretations().get(0).getId(), QueryOptions.empty(), sessionIdUser).first();
        assertEquals("blabla", interpretation2.getDescription());
        assertFalse(interpretation2.isLocked());

        // Lock and update interpretation 2
        catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getSecondaryInterpretations().get(0).getId(),
                new InterpretationUpdateParams().setDescription("bloblo").setLocked(true), null, QueryOptions.empty(), sessionIdUser);
        interpretation2 = catalogManager.getInterpretationManager().get(STUDY,
                ca.getSecondaryInterpretations().get(0).getId(), QueryOptions.empty(), sessionIdUser).first();
        assertEquals("bloblo", interpretation2.getDescription());
        assertTrue(interpretation2.isLocked());

        // Lock case
        catalogManager.getClinicalAnalysisManager().update(STUDY, ca.getId(), new ClinicalAnalysisUpdateParams().setLocked(true),
                QueryOptions.empty(), sessionIdUser);
        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
        assertTrue(ca.isLocked());
        assertTrue(ca.getInterpretation().isLocked());
        for (Interpretation secondaryInterpretation : ca.getSecondaryInterpretations()) {
            assertTrue(secondaryInterpretation.isLocked());
        }

        // Try to unlock interpretation 1
        try {
            catalogManager.getInterpretationManager().update(STUDY, ca.getId(), ca.getInterpretation().getId(),
                    new InterpretationUpdateParams().setLocked(false), null, QueryOptions.empty(), sessionIdUser);
            fail("Case is locked so it should not allow this");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("locked") && e.getMessage().toLowerCase().contains("case"));
        }

        // Unlock case
        catalogManager.getClinicalAnalysisManager().update(STUDY, ca.getId(), new ClinicalAnalysisUpdateParams().setLocked(false),
                QueryOptions.empty(), sessionIdUser);
        ca = catalogManager.getClinicalAnalysisManager().get(STUDY, ca.getId(), QueryOptions.empty(), sessionIdUser).first();
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
            catalogManager.getPanelManager().create(STUDY, panel, QueryOptions.empty(), sessionIdUser);
        }

        // Add panels to the case and set panelLock to true
        ClinicalAnalysisUpdateParams updateParams = new ClinicalAnalysisUpdateParams()
                .setPanels(panelReferenceParamList);
        catalogManager.getClinicalAnalysisManager().update(STUDY, ca.getId(), updateParams, QueryOptions.empty(), sessionIdUser);

        updateParams = new ClinicalAnalysisUpdateParams()
                .setPanelLock(true);
        catalogManager.getClinicalAnalysisManager().update(STUDY, ca.getId(), updateParams, QueryOptions.empty(), sessionIdUser);

        // Create interpretation with just panel1
        InterpretationCreateParams interpretationCreateParams = new InterpretationCreateParams()
                .setPanels(panelReferenceParamList.subList(0, 1));
        Interpretation interpretation = catalogManager.getInterpretationManager().create(STUDY, ca.getId(),
                interpretationCreateParams.toClinicalInterpretation(), ParamUtils.SaveInterpretationAs.PRIMARY,
                new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true), sessionIdUser).first();
        assertEquals(1, interpretation.getPanels().size());
        assertEquals(panelReferenceParamList.get(0).getId(), interpretation.getPanels().get(0).getId());
    }

}