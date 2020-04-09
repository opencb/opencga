package org.opencb.opencga.catalog.managers;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class ClinicalAnalysisManagerTest extends GenericTest {

    public final static String PASSWORD = "asdf";
    public final static String STUDY = "user@1000G:phase1";
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public CatalogManagerExternalResource catalogManagerResource = new CatalogManagerExternalResource();

    protected CatalogManager catalogManager;
    private FamilyManager familyManager;
    protected String sessionIdUser;

    @Before
    public void setUp() throws IOException, CatalogException {
        catalogManager = catalogManagerResource.getCatalogManager();
        familyManager = catalogManager.getFamilyManager();
        setUpCatalogManager(catalogManager);
    }

    public void setUpCatalogManager(CatalogManager catalogManager) throws IOException, CatalogException {

        catalogManager.getUserManager().create("user", "User Name", "mail@ebi.ac.uk", PASSWORD, "", null, Account.Type.FULL, null);
        sessionIdUser = catalogManager.getUserManager().login("user", PASSWORD).getToken();

        long projectId = catalogManager.getProjectManager().create("Project about some genomes", "1000G", "", "ACME", "Homo sapiens",
                null, null, "GRCh38", new QueryOptions(), sessionIdUser).first().getId();
        catalogManager.getStudyManager().create(String.valueOf(projectId), "Phase 1", "phase1", Study.Type.TRIO, null, "Done", null, null,
                null, null, null, null, null, null, sessionIdUser);

    }

    @After
    public void tearDown() throws Exception {
    }

    private QueryResult<Family> createDummyFamily() throws CatalogException {
        OntologyTerm disease1 = new OntologyTerm("dis1", "Disease 1", "HPO");
        OntologyTerm disease2 = new OntologyTerm("dis2", "Disease 2", "HPO");

        Individual father = new Individual().setName("father").setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT")));
        Individual mother = new Individual().setName("mother").setPhenotypes(Arrays.asList(new OntologyTerm("dis2", "dis2", "OT")));

        // We create a new father and mother with the same information to mimic the behaviour of the webservices. Otherwise, we would be
        // ingesting references to exactly the same object and this test would not work exactly the same way.
        Individual relFather = new Individual().setName("father").setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT")));
        Individual relMother = new Individual().setName("mother").setPhenotypes(Arrays.asList(new OntologyTerm("dis2", "dis2", "OT")));

        Individual relChild1 = new Individual().setName("child1")
                .setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT"), new OntologyTerm("dis2", "dis2", "OT")))
                .setFather(father)
                .setMother(mother)
                .setSamples(Arrays.asList(
                        new Sample().setName("sample1"),
                        new Sample().setName("sample2"),
                        new Sample().setName("sample3"),
                        new Sample().setName("sample4")
                ))
                .setMultiples(new Multiples("multiples", Arrays.asList("child2", "child3")))
                .setParentalConsanguinity(true);
        Individual relChild2 = new Individual().setName("child2")
                .setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT")))
                .setFather(father)
                .setMother(mother)
                .setMultiples(new Multiples("multiples", Arrays.asList("child1", "child3")))
                .setParentalConsanguinity(true);
        Individual relChild3 = new Individual().setName("child3")
                .setPhenotypes(Arrays.asList(new OntologyTerm("dis1", "dis1", "OT")))
                .setFather(father)
                .setMother(mother)
                .setMultiples(new Multiples("multiples", Arrays.asList("child1", "child2")))
                .setParentalConsanguinity(true);

        Family family = new Family("family", Arrays.asList(disease1, disease2),
                Arrays.asList(relChild1, relChild2, relChild3, relFather, relMother),"", Collections.emptyList(), Collections.emptyMap());

        return familyManager.create(STUDY, family, QueryOptions.empty(), sessionIdUser);
    }

    private QueryResult<ClinicalAnalysis> createDummyEnvironment(boolean createFamily) throws CatalogException {

        createDummyFamily();
        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setName("analysis").setDescription("My description").setType(ClinicalAnalysis.Type.FAMILY)
                .setSubjects(Arrays.asList(new Individual().setName("child1").setSamples(Arrays.asList(new Sample().setName("sample2")))));

        if (createFamily) {
            clinicalAnalysis.setFamily(new Family().setName("family"));
        }

        return catalogManager.getClinicalAnalysisManager().create(STUDY, clinicalAnalysis, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void createClinicalAnalysisTest() throws CatalogException {
        QueryResult<ClinicalAnalysis> dummyEnvironment = createDummyEnvironment(true);

        assertEquals(1, dummyEnvironment.getNumResults());
        assertEquals(0, dummyEnvironment.first().getInterpretations().size());

        assertEquals(catalogManager.getFamilyManager().getId("family", STUDY, sessionIdUser).getResourceId(),
                dummyEnvironment.first().getFamily().getId());
        assertEquals(catalogManager.getIndividualManager().getId("child1", STUDY, sessionIdUser).getResourceId(),
                dummyEnvironment.first().getSubjects().get(0).getId());
        assertEquals(1, dummyEnvironment.first().getSubjects().get(0).getSamples().size());
        assertEquals(catalogManager.getSampleManager().getId("sample2", STUDY, sessionIdUser).getResourceId(),
                dummyEnvironment.first().getSubjects().get(0).getSamples().get(0).getId());
    }

    @Test
    public void createClinicalAnalysisNoFamilyTest() throws CatalogException {
        QueryResult<ClinicalAnalysis> dummyEnvironment = createDummyEnvironment(false);

        assertEquals(1, dummyEnvironment.getNumResults());
        assertEquals(0, dummyEnvironment.first().getInterpretations().size());

        assertEquals(catalogManager.getIndividualManager().getId("child1", STUDY, sessionIdUser).getResourceId(),
                dummyEnvironment.first().getSubjects().get(0).getId());
        assertEquals(1, dummyEnvironment.first().getSubjects().get(0).getSamples().size());
        assertEquals(catalogManager.getSampleManager().getId("sample2", STUDY, sessionIdUser).getResourceId(),
                dummyEnvironment.first().getSubjects().get(0).getSamples().get(0).getId());
    }

    @Test
    public void updateSubjectsNoFamilyTest() throws CatalogException {
        createDummyEnvironment(false);

        ObjectMap params = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.SUBJECTS.key(),
                Arrays.asList(new Individual().setName("child1").setSamples(Arrays.asList(new Sample().setName("sample2")))));
        QueryResult<ClinicalAnalysis> updateResult = catalogManager.getClinicalAnalysisManager().update(STUDY, "analysis", params,
                QueryOptions.empty(), sessionIdUser);

        assertEquals(1, updateResult.getNumResults());
        assertEquals(0, updateResult.first().getInterpretations().size());

        assertEquals(catalogManager.getIndividualManager().getId("child1", STUDY, sessionIdUser).getResourceId(),
                updateResult.first().getSubjects().get(0).getId());
        assertEquals(1, updateResult.first().getSubjects().get(0).getSamples().size());
        assertEquals(catalogManager.getSampleManager().getId("sample2", STUDY, sessionIdUser).getResourceId(),
                updateResult.first().getSubjects().get(0).getSamples().get(0).getId());
    }

    @Test
    public void updateSubjectsAndFamilyTest() throws CatalogException {
        createDummyEnvironment(false);

        ObjectMap params = new ObjectMap()
                .append(ClinicalAnalysisDBAdaptor.QueryParams.SUBJECTS.key(),
                        Arrays.asList(new Individual().setName("child1").setSamples(Arrays.asList(new Sample().setName("sample2")))))
                .append(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key(), new Family().setName("family"));
        QueryResult<ClinicalAnalysis> updateResult = catalogManager.getClinicalAnalysisManager().update(STUDY, "analysis", params,
                QueryOptions.empty(), sessionIdUser);

        assertEquals(1, updateResult.getNumResults());
        assertEquals(0, updateResult.first().getInterpretations().size());

        assertEquals(catalogManager.getFamilyManager().getId("family", STUDY, sessionIdUser).getResourceId(),
                updateResult.first().getFamily().getId());
        assertEquals(catalogManager.getIndividualManager().getId("child1", STUDY, sessionIdUser).getResourceId(),
                updateResult.first().getSubjects().get(0).getId());
        assertEquals(1, updateResult.first().getSubjects().get(0).getSamples().size());
        assertEquals(catalogManager.getSampleManager().getId("sample2", STUDY, sessionIdUser).getResourceId(),
                updateResult.first().getSubjects().get(0).getSamples().get(0).getId());
    }

}
