package org.opencb.opencga.analysis.family;

import org.apache.commons.lang.StringUtils;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.core.SexOntologyTermAnnotation;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.analysis.variant.OpenCGATestExternalResource;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FamilyManager;
import org.opencb.opencga.catalog.utils.PedigreeGraphUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.family.FamilyUpdateParams;
import org.opencb.opencga.core.models.family.PedigreeGraph;
import org.opencb.opencga.core.models.family.PedigreeGraphAnalysisParams;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualUpdateParams;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleReferenceParam;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.storage.core.StorageEngineFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FamilyAnalysisTest extends GenericTest {

    public final static String STUDY = "user@1000G:phase1";
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @ClassRule
    public static OpenCGATestExternalResource opencga = new OpenCGATestExternalResource();

    protected static CatalogManager catalogManager;
    private static FamilyManager familyManager;
    private static String opencgaToken;
    protected static String sessionIdUser;

    protected static Family family;
    protected static Family family2;
    protected static Family family3;

    protected static String projectId;
    protected static String studyId;

    private static final QueryOptions INCLUDE_RESULT = new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true);

    @BeforeClass
    public static void setUp() throws IOException, CatalogException {
        catalogManager = opencga.getCatalogManager();
        familyManager = catalogManager.getFamilyManager();
        setUpCatalogManager(catalogManager);
    }

    public static void setUpCatalogManager(CatalogManager catalogManager) throws CatalogException {
        opencgaToken = catalogManager.getUserManager().loginAsAdmin(TestParamConstants.ADMIN_PASSWORD).getToken();

        catalogManager.getUserManager().create("user", "User Name", "mail@ebi.ac.uk", TestParamConstants.PASSWORD, "", null,
                Account.AccountType.FULL, opencgaToken);
        sessionIdUser = catalogManager.getUserManager().login("user", TestParamConstants.PASSWORD).getToken();

        projectId = catalogManager.getProjectManager().create("1000G", "Project about some genomes", "", "Homo sapiens", null, "GRCh38",
                INCLUDE_RESULT, sessionIdUser).first().getId();
        studyId = catalogManager.getStudyManager().create(projectId, "phase1", null, "Phase 1", "Done", null, null, null, null, null,
                sessionIdUser).first().getId();

        try {
            family = createDummyFamily("Martinez-Martinez").first();
        } catch (CatalogException e) {
        }
        try {
            family2 = createDummyFamily("Lopez-Lopez", Arrays.asList("father11-sample", "mother11-sample"), 1).first();
        } catch (CatalogException e) {
        }
        try {
            family3 = createDummyFamily("Perez-Perez", Arrays.asList("father22-sample", "mother22-sample", "child222-sample"), 0).first();
        } catch (CatalogException e) {
        }
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void creationTest() {
        PedigreeGraph pedigreeGraph = family.getPedigreeGraph();
        assertTrue(pedigreeGraph.getBase64().startsWith("iVBORw0KGgoAAAANSUhEUgAAAeA"));
        assertTrue(pedigreeGraph.getBase64().endsWith("AIDB6Bwfs3Rj5UIf81hI8AAAAASUVORK5CYII="));
    }

    @Test
    public void twoMemberFamilyTest() throws CatalogException {
        FamilyUpdateParams updateParams = new FamilyUpdateParams();

        QueryOptions queryOptions = new QueryOptions()
                .append(ParamConstants.FAMILY_UPDATE_ROLES_PARAM, true)
                .append(ParamConstants.INCLUDE_RESULT_PARAM, true);
        Family updatedFamily = catalogManager.getFamilyManager().update(studyId, family2.getId(), updateParams, queryOptions, sessionIdUser)
                .first();

        PedigreeGraph pedigreeGraph = updatedFamily.getPedigreeGraph();
        assertTrue(StringUtils.isEmpty(pedigreeGraph.getBase64()));
    }

    @Test
    public void threeMemberNoDisorderFamilyTest() throws CatalogException {
        FamilyUpdateParams updateParams = new FamilyUpdateParams();

        QueryOptions queryOptions = new QueryOptions()
                .append(ParamConstants.FAMILY_UPDATE_ROLES_PARAM, true)
                .append(ParamConstants.INCLUDE_RESULT_PARAM, true);
        Family updatedFamily = catalogManager.getFamilyManager().update(studyId, family3.getId(), updateParams, queryOptions, sessionIdUser)
                .first();

        PedigreeGraph pedigreeGraph = updatedFamily.getPedigreeGraph();
        assertTrue(pedigreeGraph.getBase64().startsWith("iVBORw0KGgoAAAANSUhEUgAAAeAAAAH"));
        assertTrue(pedigreeGraph.getBase64().endsWith("2WENFPAsd1MAAAAASUVORK5CYII="));
    }

    @Test
    public void threeGenerationFamilyTest() throws CatalogException {
        Family threeGenFamily = createThreeGenerationFamily("Cos-Cos", true).first();
        PedigreeGraph pedigreeGraph = threeGenFamily.getPedigreeGraph();
        assertTrue(pedigreeGraph.getBase64().startsWith("iVBORw0KGgoAAAANSUhEUgAAAeAAAAHgCA"));
        assertTrue(pedigreeGraph.getBase64().endsWith("h9S2DROnwXOvwAAAABJRU5ErkJggg=="));
    }

    @Test
    public void threeGenerationFamilyWithoutDisorderTest() throws CatalogException {
        Family threeGenFamily = createThreeGenerationFamily("Hello-Hello", false).first();
        PedigreeGraph pedigreeGraph = threeGenFamily.getPedigreeGraph();
        assertTrue(pedigreeGraph.getBase64().startsWith("iVBORw0KGgoAAAANSUhEUgAAAeAAAAHgC"));
        assertTrue(pedigreeGraph.getBase64().endsWith("wNJj9EVvh8HVQAAAABJRU5ErkJggg=="));
    }

    @Test
    public void test2Member2GenerationFamilyTest() throws CatalogException {
        Family family = create2Member2GenerationDummyFamily("Colo-Colo", "father222-sample", "child2222-sample").first();

        PedigreeGraph pedigreeGraph = family.getPedigreeGraph();
        assertTrue(pedigreeGraph.getBase64().startsWith("iVBORw0KGgoAAAANSUhEUgAAA"));
        assertTrue(pedigreeGraph.getBase64().endsWith("qkAAAAASUVORK5CYII="));
    }

    @Test
    public void updateTest() throws CatalogException {
        FamilyUpdateParams updateParams = new FamilyUpdateParams();

        QueryOptions queryOptions = new QueryOptions()
                .append(ParamConstants.FAMILY_UPDATE_ROLES_PARAM, true)
                .append(ParamConstants.INCLUDE_RESULT_PARAM, true);
        Family updatedFamily = catalogManager.getFamilyManager().update(studyId, family.getId(), updateParams, queryOptions, sessionIdUser)
                .first();

        PedigreeGraph pedigreeGraph = updatedFamily.getPedigreeGraph();
        assertTrue(pedigreeGraph.getBase64().startsWith("iVBORw0KGgoAAAANSUhEUgAAAeAAAAHg"));
        assertTrue(pedigreeGraph.getBase64().endsWith("5UIf81hI8AAAAASUVORK5CYII="));
    }

    @Test
    public void testPedigreeGraphAnalysis() throws ToolException, IOException {
        Path outDir = Paths.get(opencga.createTmpOutdir("_pedigree_graph"));
        System.out.println("out dir = " + outDir.toAbsolutePath());
        System.out.println("opencga home = " + opencga.getOpencgaHome().toAbsolutePath());
        System.out.println(Paths.get("workspace parent = " + catalogManager.getConfiguration().getWorkspace()).getParent());

        VariantStorageManager variantStorageManager = new VariantStorageManager(catalogManager, opencga.getStorageEngineFactory());
        ToolRunner toolRunner = new ToolRunner(opencga.getOpencgaHome().toString(), catalogManager,
                StorageEngineFactory.get(variantStorageManager.getStorageConfiguration()));

        // Pedigree graph params
        PedigreeGraphAnalysisParams params = new PedigreeGraphAnalysisParams();
        params.setFamilyId(family.getId());

        toolRunner.execute(PedigreeGraphAnalysis.class, params, new ObjectMap(ParamConstants.STUDY_PARAM, studyId), outDir, null,
                sessionIdUser);

        String b64Image = PedigreeGraphUtils.getB64Image(outDir);
        assertTrue(b64Image.startsWith("iVBORw0KGgoAAAANSUhEUgAAAeAAAAHg"));
        assertTrue(b64Image.endsWith("s3Rj5UIf81hI8AAAAASUVORK5CYII="));

        assertEquals(family.getPedigreeGraph().getBase64(), b64Image);
    }

    private static DataResult<Family> createDummyFamily(String familyName) throws CatalogException {
        return createDummyFamily(familyName, Arrays.asList("father-sample", "mother-sample", "child1-sample", "child2-sample",
                "child3-sample"), 2);
    }

    private static DataResult<Family> createDummyFamily(String familyName, List<String> sampleNames, int numDisorders)
            throws CatalogException {
        int numMembers = sampleNames.size();
        if (numMembers > 0) {
            Sample sample = new Sample().setId(sampleNames.get(0));
            catalogManager.getSampleManager().create(STUDY, sample, QueryOptions.empty(), sessionIdUser);
        }

        if (numMembers > 1) {
            Sample sample = new Sample().setId(sampleNames.get(1));
            catalogManager.getSampleManager().create(STUDY, sample, QueryOptions.empty(), sessionIdUser);
        }

        if (numMembers > 2) {
            Sample sample = new Sample().setId(sampleNames.get(2));
            catalogManager.getSampleManager().create(STUDY, sample, QueryOptions.empty(), sessionIdUser);
        }

        if (numMembers > 3) {
            Sample sample = new Sample().setId(sampleNames.get(3));
            catalogManager.getSampleManager().create(STUDY, sample, QueryOptions.empty(), sessionIdUser);
        }

        if (numMembers > 4) {
            Sample sample = new Sample().setId(sampleNames.get(4));
            catalogManager.getSampleManager().create(STUDY, sample, QueryOptions.empty(), sessionIdUser);
        }

        Phenotype phenotype1 = new Phenotype("dis1", "Phenotype 1", "HPO");
        Phenotype phenotype2 = new Phenotype("dis2", "Phenotype 2", "HPO");

        List<Disorder> disorders = new ArrayList<>();
        if (numDisorders > 0) {
            Disorder disorder1 = new Disorder("disorder-1", null, null, null, null, null, null);
            disorders.add(disorder1);
        }

        if (numDisorders > 1) {
            Disorder disorder2 = new Disorder("disorder-2", null, null, null, null, null, null);
            disorders.add(disorder2);
        }

        Individual father = null, mother = null;

        father = new Individual().setId(sampleNames.get(0))
                .setPhenotypes(Arrays.asList(phenotype1))
                .setSex(SexOntologyTermAnnotation.initMale())
                .setLifeStatus(IndividualProperty.LifeStatus.ALIVE);
        if (numDisorders > 0) {
            father.setDisorders(Collections.singletonList(disorders.get(0)));
        }

        if (numMembers > 1) {
            mother = new Individual().setId(sampleNames.get(1))
                    .setPhenotypes(Arrays.asList(phenotype2))
                    .setSex(SexOntologyTermAnnotation.initFemale())
                    .setLifeStatus(IndividualProperty.LifeStatus.ALIVE);
        }

//        // We create a new father and mother with the same information to mimic the behaviour of the webservices. Otherwise, we would be
//        // ingesting references to exactly the same object and this test would not work exactly the same way.
        List<Individual> members = new ArrayList<>();
        List<String> memberIds = new ArrayList<>();
        Individual relFather, relMother = null, relChild1 = null, relChild2 = null, relChild3 = null;
        relFather = new Individual().setId(sampleNames.get(0)).setPhenotypes(Arrays.asList(phenotype1));
        members.add(father);
        memberIds.add(relFather.getId());
        if (numMembers > 1) {
            relMother = new Individual().setId(sampleNames.get(1)).setPhenotypes(Arrays.asList(phenotype2));
            members.add(mother);
            memberIds.add(relMother.getId());
        }

        if (numMembers > 2) {
            relChild1 = new Individual().setId(sampleNames.get(2))
                    .setPhenotypes(Arrays.asList(phenotype1, phenotype2))
                    .setFather(father)
                    .setMother(mother)
                    .setSex(SexOntologyTermAnnotation.initMale())
                    .setLifeStatus(IndividualProperty.LifeStatus.ALIVE)
                    .setParentalConsanguinity(true);
            members.add(relChild1);
            memberIds.add(relChild1.getId());
        }

        if (numMembers > 3) {
            relChild2 = new Individual().setId(sampleNames.get(3))
                    .setPhenotypes(Arrays.asList(phenotype1))
                    .setFather(father)
                    .setMother(mother)
                    .setSex(SexOntologyTermAnnotation.initFemale())
                    .setLifeStatus(IndividualProperty.LifeStatus.ALIVE)
                    .setParentalConsanguinity(true);
            if (numDisorders > 0) {
                relChild2.setDisorders(Collections.singletonList(disorders.get(0)));
            }
            members.add(relChild2);
            memberIds.add(relChild2.getId());
        }

        if (numMembers > 4) {
            relChild3 = new Individual().setId(sampleNames.get(4))
                    .setPhenotypes(Arrays.asList(phenotype1))
                    .setFather(father)
                    .setMother(mother)
                    .setSex(SexOntologyTermAnnotation.initFemale())
                    .setLifeStatus(IndividualProperty.LifeStatus.DECEASED)
                    .setParentalConsanguinity(true);
            if (numDisorders > 1) {
                relChild3.setDisorders(Collections.singletonList(disorders.get(1)));
            }
            members.add(relChild3);
            memberIds.add(relChild3.getId());
        }

        Family family = new Family(familyName, familyName, null, null, members, "", numMembers, Collections.emptyList(),
                Collections.emptyMap());

        OpenCGAResult<Family> familyOpenCGAResult = familyManager.create(STUDY, family, null, INCLUDE_RESULT, sessionIdUser);

        catalogManager.getIndividualManager().update(STUDY, relFather.getId(),
                new IndividualUpdateParams().setSamples(Collections.singletonList(new SampleReferenceParam().setId(sampleNames.get(0)))),
                QueryOptions.empty(), sessionIdUser);

        if (numMembers > 1) {
            catalogManager.getIndividualManager().update(STUDY, relMother.getId(),
                    new IndividualUpdateParams().setSamples(Collections.singletonList(new SampleReferenceParam().setId(sampleNames
                            .get(1)))), QueryOptions.empty(), sessionIdUser);
        }

        if (numMembers > 2) {
            catalogManager.getIndividualManager().update(STUDY, relChild1.getId(),
                    new IndividualUpdateParams().setSamples(Collections.singletonList(new SampleReferenceParam().setId(sampleNames
                            .get(2)))), QueryOptions.empty(), sessionIdUser);
        }

        if (numMembers > 3) {
            catalogManager.getIndividualManager().update(STUDY, relChild1.getId(),
                    new IndividualUpdateParams().setSamples(Collections.singletonList(new SampleReferenceParam().setId(sampleNames
                            .get(3)))), QueryOptions.empty(), sessionIdUser);
        }

        if (numMembers > 4) {
            catalogManager.getIndividualManager().update(STUDY, relChild1.getId(),
                    new IndividualUpdateParams().setSamples(Collections.singletonList(new SampleReferenceParam().setId(sampleNames
                            .get(4)))), QueryOptions.empty(), sessionIdUser);
        }

        return familyOpenCGAResult;
    }

    private DataResult<Family> createThreeGenerationFamily(String familyName, boolean withDisorder) throws CatalogException {
        List<String> sampleNames = Arrays.asList("granma-" + familyName, "pa-" + familyName, "ma-" + familyName, "child-" + familyName);
        int numMembers = sampleNames.size();
        for (String sampleName : sampleNames) {
            catalogManager.getSampleManager().create(STUDY, new Sample().setId(sampleName), QueryOptions.empty(), sessionIdUser);
        }

        Phenotype phenotype1 = new Phenotype("dis1", "Phenotype 1", "HPO");
        Phenotype phenotype2 = new Phenotype("dis2", "Phenotype 2", "HPO");

        Disorder disorder1 = new Disorder("disorder-1", null, null, null, null, null, null);
        Disorder disorder2 = new Disorder("disorder-2", null, null, null, null, null, null);

        Individual granma  = new Individual().setId(sampleNames.get(0))
                .setPhenotypes(Arrays.asList(phenotype1))
                .setSex(SexOntologyTermAnnotation.initFemale())
                .setLifeStatus(IndividualProperty.LifeStatus.ALIVE);
        if (withDisorder) {
            granma.setDisorders(Collections.singletonList(disorder2));
        }

        Individual father = new Individual().setId(sampleNames.get(1))
                .setPhenotypes(Arrays.asList(phenotype1))
                .setMother(granma)
                .setSex(SexOntologyTermAnnotation.initMale())
                .setLifeStatus(IndividualProperty.LifeStatus.ALIVE);
        if (withDisorder) {
            father.setDisorders(Collections.singletonList(disorder1));
        }

        Individual mother = new Individual().setId(sampleNames.get(2))
                .setPhenotypes(Arrays.asList(phenotype2))
                .setSex(SexOntologyTermAnnotation.initFemale())
                .setLifeStatus(IndividualProperty.LifeStatus.ALIVE);

        Individual child = new Individual().setId(sampleNames.get(3))
                .setPhenotypes(Arrays.asList(phenotype2))
                .setFather(father)
                .setMother(mother)
                .setSex(SexOntologyTermAnnotation.initMale())
                .setLifeStatus(IndividualProperty.LifeStatus.ALIVE)
                .setParentalConsanguinity(true);
        if (withDisorder) {
            child.setDisorders(Collections.singletonList(disorder2));
        }

        List<Individual> members = Arrays.asList(granma, father, mother, child);
        Family family = new Family(familyName, familyName, null, null, members, "", numMembers, Collections.emptyList(), Collections.emptyMap());
        OpenCGAResult<Family> familyOpenCGAResult = familyManager.create(STUDY, family, null, INCLUDE_RESULT, sessionIdUser);

        return familyOpenCGAResult;
    }

    private static DataResult<Family> create2Member2GenerationDummyFamily(String familyName, String fatherSample, String childSample)
            throws CatalogException {
        int numMembers = 2;
        Sample sample = new Sample().setId(fatherSample);
        catalogManager.getSampleManager().create(STUDY, sample, QueryOptions.empty(), sessionIdUser);

        sample = new Sample().setId(childSample);
        catalogManager.getSampleManager().create(STUDY, sample, QueryOptions.empty(), sessionIdUser);

        Phenotype phenotype1 = new Phenotype("dis1", "Phenotype 1", "HPO");
        Phenotype phenotype2 = new Phenotype("dis2", "Phenotype 2", "HPO");

        List<Disorder> disorders = new ArrayList<>();
        Disorder disorder1 = new Disorder("disorder-1", null, null, null, null, null, null);
        disorders.add(disorder1);


        Individual father = new Individual().setId(fatherSample)
                .setPhenotypes(Arrays.asList(phenotype1))
                .setSex(SexOntologyTermAnnotation.initMale())
                .setLifeStatus(IndividualProperty.LifeStatus.ALIVE)
                .setDisorders(Collections.singletonList(disorders.get(0)));

        Individual mother = null;

        // We create a new father and mother with the same information to mimic the behaviour of the webservices. Otherwise, we would be
        // ingesting references to exactly the same object and this test would not work exactly the same way.
        List<Individual> members = new ArrayList<>();
        List<String> memberIds = new ArrayList<>();
        Individual relFather = new Individual().setId(fatherSample).setPhenotypes(Arrays.asList(phenotype1));
        members.add(father);
        memberIds.add(relFather.getId());
        Individual relChild1 = new Individual().setId(childSample)
                .setPhenotypes(Arrays.asList(phenotype1, phenotype2))
                .setFather(father)
                .setMother(mother)
                .setSex(SexOntologyTermAnnotation.initMale())
                .setLifeStatus(IndividualProperty.LifeStatus.ALIVE)
                .setParentalConsanguinity(true);
        members.add(relChild1);
        memberIds.add(relChild1.getId());

        Family family = new Family(familyName, familyName, null, null, members, "", numMembers, Collections.emptyList(), Collections.emptyMap());

        OpenCGAResult<Family> familyOpenCGAResult = familyManager.create(STUDY, family, null, INCLUDE_RESULT, sessionIdUser);

        catalogManager.getIndividualManager().update(STUDY, relFather.getId(),
                new IndividualUpdateParams().setSamples(Collections.singletonList(new SampleReferenceParam().setId(fatherSample))),
                QueryOptions.empty(), sessionIdUser);


        catalogManager.getIndividualManager().update(STUDY, relChild1.getId(),
                new IndividualUpdateParams().setSamples(Collections.singletonList(new SampleReferenceParam().setId(childSample))),
                QueryOptions.empty(), sessionIdUser);

        return familyOpenCGAResult;
    }
}
