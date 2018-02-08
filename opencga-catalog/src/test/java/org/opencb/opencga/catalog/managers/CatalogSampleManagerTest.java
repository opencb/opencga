package org.opencb.opencga.catalog.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.test.GenericTest;
import org.opencb.commons.utils.StringUtils;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.CatalogAnnotationsValidatorTest;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.AclParams;
import org.opencb.opencga.core.models.acls.permissions.IndividualAclEntry;
import org.opencb.opencga.core.models.acls.permissions.SampleAclEntry;
import org.opencb.opencga.core.models.summaries.FeatureCount;
import org.opencb.opencga.core.models.summaries.VariableSetSummary;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.opencb.opencga.catalog.db.api.SampleDBAdaptor.QueryParams.ANNOTATION;

public class CatalogSampleManagerTest extends GenericTest {

    public final static String PASSWORD = "asdf";
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public CatalogManagerExternalResource catalogManagerResource = new CatalogManagerExternalResource();

    protected CatalogManager catalogManager;
    protected String sessionIdUser;
    protected String sessionIdUser2;
    protected String sessionIdUser3;
    private File testFolder;
    private long project1;
    private long project2;
    private long studyId;
    private long studyId2;
    private long s_1;
    private long s_2;
    private long s_3;
    private long s_4;
    private long s_5;
    private long s_6;
    private long s_7;
    private long s_8;
    private long s_9;


    @Before
    public void setUp() throws IOException, CatalogException {
        catalogManager = catalogManagerResource.getCatalogManager();
        setUpCatalogManager(catalogManager);
    }

    public void setUpCatalogManager(CatalogManager catalogManager) throws IOException, CatalogException {

        catalogManager.getUserManager().create("user", "User Name", "mail@ebi.ac.uk", PASSWORD, "", null, Account.FULL, null, null);
        catalogManager.getUserManager().create("user2", "User2 Name", "mail2@ebi.ac.uk", PASSWORD, "", null, Account.FULL, null, null);
        catalogManager.getUserManager().create("user3", "User3 Name", "user.2@e.mail", PASSWORD, "ACME", null, Account.FULL, null, null);

        sessionIdUser = catalogManager.getUserManager().login("user", PASSWORD);
        sessionIdUser2 = catalogManager.getUserManager().login("user2", PASSWORD);
        sessionIdUser3 = catalogManager.getUserManager().login("user3", PASSWORD);

        project1 = catalogManager.getProjectManager().create("Project about some genomes", "1000G", "", "ACME", "Homo sapiens",
                null, null, "GRCh38", new QueryOptions(), sessionIdUser).first().getId();
        project2 = catalogManager.getProjectManager().create("Project Management Project", "pmp", "life art intelligent system", "myorg",
                "Homo sapiens", null, null, "GRCh38", new QueryOptions(), sessionIdUser2).first().getId();
        Project project3 = catalogManager.getProjectManager().create("project 1", "p1", "", "", "Homo sapiens",
                null, null, "GRCh38", new QueryOptions(), sessionIdUser3).first();

        studyId = catalogManager.getStudyManager().create(String.valueOf(project1), "Phase 1", "phase1", Study.Type.TRIO, null, "Done",
                null, null, null, null, null, null, null, null, sessionIdUser).first().getId();
        studyId2 = catalogManager.getStudyManager().create(String.valueOf(project1), "Phase 3", "phase3", Study.Type.CASE_CONTROL, null,
                "d", null, null, null, null, null, null, null, null, sessionIdUser).first().getId();
        catalogManager.getStudyManager().create(String.valueOf(project2), "Study 1", "s1", Study.Type.CONTROL_SET, null, "", null, null,
                null, null, null, null, null, null, sessionIdUser2).first().getId();

        catalogManager.getFileManager().createFolder(Long.toString(studyId2), Paths.get("data/test/folder/").toString(), null, true,
                null, QueryOptions.empty(), sessionIdUser);

        catalogManager.getFileManager().createFolder(Long.toString(studyId), Paths.get("analysis/").toString(), null, true, null,
                QueryOptions.empty(), sessionIdUser);
        catalogManager.getFileManager().createFolder(Long.toString(studyId2), Paths.get("analysis/").toString(), null, true, null,
                QueryOptions.empty(), sessionIdUser);

        testFolder = catalogManager.getFileManager().createFolder(Long.toString(studyId), Paths.get("data/test/folder/").toString(),
                null, true, null, QueryOptions.empty(), sessionIdUser).first();
        ObjectMap attributes = new ObjectMap();
        attributes.put("field", "value");
        attributes.put("numValue", 5);
        catalogManager.getFileManager().update(testFolder.getId(), new ObjectMap("attributes", attributes), new QueryOptions(),
                sessionIdUser);

        QueryResult<File> queryResult2 = catalogManager.getFileManager().create(Long.toString(studyId), File.Type.FILE, File.Format.PLAIN, File.Bioformat.NONE, testFolder.getPath() + "test_1K.txt.gz", null, "", new File.FileStatus(File.FileStatus.STAGE), 0, -1, null, -1, null, null, false, null, null, sessionIdUser);

        new FileUtils(catalogManager).upload(new ByteArrayInputStream(StringUtils.randomString(1000).getBytes()), queryResult2.first(), sessionIdUser, false, false, true);

        File fileTest1k = catalogManager.getFileManager().get(queryResult2.first().getId(), null, sessionIdUser).first();
        attributes = new ObjectMap();
        attributes.put("field", "value");
        attributes.put("name", "fileTest1k");
        attributes.put("numValue", "10");
        attributes.put("boolean", false);
        catalogManager.getFileManager().update(fileTest1k.getId(), new ObjectMap("attributes", attributes), new QueryOptions(),
                sessionIdUser);

        QueryResult<File> queryResult1 = catalogManager.getFileManager().create(Long.toString(studyId), File.Type.FILE, File.Format.PLAIN, File.Bioformat.DATAMATRIX_EXPRESSION, testFolder.getPath() + "test_0.5K.txt", null, "", new File.FileStatus(File.FileStatus.STAGE), 0, -1, null, -1, null, null, false, null, null, sessionIdUser);
        new FileUtils(catalogManager).upload(new ByteArrayInputStream(StringUtils.randomString(500).getBytes()), queryResult1.first(), sessionIdUser, false, false, true);
        File fileTest05k = catalogManager.getFileManager().get(queryResult1.first().getId(), null, sessionIdUser).first();
        attributes = new ObjectMap();
        attributes.put("field", "valuable");
        attributes.put("name", "fileTest05k");
        attributes.put("numValue", 5);
        attributes.put("boolean", true);
        catalogManager.getFileManager().update(fileTest05k.getId(), new ObjectMap("attributes", attributes), new QueryOptions(),
                sessionIdUser);

        QueryResult<File> queryResult = catalogManager.getFileManager().create(Long.toString(studyId), File.Type.FILE, File.Format.IMAGE, File.Bioformat.NONE, testFolder.getPath() + "test_0.1K.png", null, "", new File.FileStatus(File.FileStatus.STAGE), 0, -1, null, -1, null, null, false, null, null, sessionIdUser);
        new FileUtils(catalogManager).upload(new ByteArrayInputStream(StringUtils.randomString(100).getBytes()), queryResult.first(), sessionIdUser, false, false, true);
        File test01k = catalogManager.getFileManager().get(queryResult.first().getId(), null, sessionIdUser).first();
        attributes = new ObjectMap();
        attributes.put("field", "other");
        attributes.put("name", "test01k");
        attributes.put("numValue", 50);
        attributes.put("nested", new ObjectMap("num1", 45).append("num2", 33).append("text", "HelloWorld"));
        catalogManager.getFileManager().update(test01k.getId(), new ObjectMap("attributes", attributes), new QueryOptions(), sessionIdUser);

        Set<Variable> variables = new HashSet<>();
        variables.addAll(Arrays.asList(
                new Variable("NAME", "", "", Variable.VariableType.TEXT, "", true, false, Collections.<String>emptyList(), 0, "", "", null,
                        Collections.<String, Object>emptyMap()),
                new Variable("AGE", "", "", Variable.VariableType.DOUBLE, null, true, false, Collections.singletonList("0:130"), 1, "", "",
                        null, Collections.<String, Object>emptyMap()),
                new Variable("HEIGHT", "", "", Variable.VariableType.DOUBLE, "1.5", false, false, Collections.singletonList("0:"), 2, "",
                        "", null, Collections.<String, Object>emptyMap()),
                new Variable("ALIVE", "", "", Variable.VariableType.BOOLEAN, "", true, false, Collections.<String>emptyList(), 3, "", "",
                        null, Collections.<String, Object>emptyMap()),
                new Variable("PHEN", "", "", Variable.VariableType.CATEGORICAL, "", true, false, Arrays.asList("CASE", "CONTROL"), 4, "", "",
                        null, Collections.<String, Object>emptyMap()),
                new Variable("EXTRA", "", "", Variable.VariableType.TEXT, "", false, false, Collections.emptyList(), 5, "", "", null,
                        Collections.<String, Object>emptyMap())
        ));
        VariableSet vs = catalogManager.getStudyManager().createVariableSet(studyId, "vs", true, false, "", null, variables, sessionIdUser).first();

        s_1 = catalogManager.getSampleManager().create(Long.toString(studyId), new Sample().setName("s_1"), new QueryOptions(),
                sessionIdUser).first().getId();
        s_2 = catalogManager.getSampleManager().create(Long.toString(studyId), new Sample().setName("s_2"), new QueryOptions(),
                sessionIdUser).first().getId();
        s_3 = catalogManager.getSampleManager().create(Long.toString(studyId), new Sample().setName("s_3"), new QueryOptions(),
                sessionIdUser).first().getId();
        s_4 = catalogManager.getSampleManager().create(Long.toString(studyId), new Sample().setName("s_4"), new QueryOptions(),
                sessionIdUser).first().getId();
        s_5 = catalogManager.getSampleManager().create(Long.toString(studyId), new Sample().setName("s_5"), new QueryOptions(),
                sessionIdUser).first().getId();
        s_6 = catalogManager.getSampleManager().create(Long.toString(studyId), new Sample().setName("s_6"), new QueryOptions(),
                sessionIdUser).first().getId();
        s_7 = catalogManager.getSampleManager().create(Long.toString(studyId), new Sample().setName("s_7"), new QueryOptions(),
                sessionIdUser).first().getId();
        s_8 = catalogManager.getSampleManager().create(Long.toString(studyId), new Sample().setName("s_8"), new QueryOptions(),
                sessionIdUser).first().getId();
        s_9 = catalogManager.getSampleManager().create(Long.toString(studyId), new Sample().setName("s_9"), new QueryOptions(),
                sessionIdUser).first().getId();

        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_1), null, Long.toString(vs.getId()), "annot1",
                new ObjectMap("NAME", "s_1").append("AGE", 6).append("ALIVE", true).append("PHEN", "CONTROL"), sessionIdUser);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_2), null, Long.toString(vs.getId()), "annot1",
                new ObjectMap("NAME", "s_2").append("AGE", 10).append("ALIVE", false).append("PHEN", "CASE"), sessionIdUser);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_3), null, Long.toString(vs.getId()), "annot1",
                new ObjectMap("NAME", "s_3").append("AGE", 15).append("ALIVE", true).append("PHEN", "CONTROL"), sessionIdUser);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_4), null, Long.toString(vs.getId()), "annot1",
                new ObjectMap("NAME", "s_4").append("AGE", 22).append("ALIVE", false).append("PHEN", "CONTROL"), sessionIdUser);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_5), null, Long.toString(vs.getId()), "annot1",
                new ObjectMap("NAME", "s_5").append("AGE", 29).append("ALIVE", true).append("PHEN", "CASE"), sessionIdUser);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_6), null, Long.toString(vs.getId()), "annot2",
                new ObjectMap("NAME", "s_6").append("AGE", 38).append("ALIVE", true).append("PHEN", "CONTROL"), sessionIdUser);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_7), null, Long.toString(vs.getId()), "annot2",
                new ObjectMap("NAME", "s_7").append("AGE", 46).append("ALIVE", false).append("PHEN", "CASE"), sessionIdUser);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_8), null, Long.toString(vs.getId()), "annot2",
                new ObjectMap("NAME", "s_8").append("AGE", 72).append("ALIVE", true).append("PHEN", "CONTROL"), sessionIdUser);


        catalogManager.getFileManager().update(test01k.getId(), new ObjectMap(FileDBAdaptor.QueryParams.SAMPLES.key(),
                Arrays.asList(s_1, s_2, s_3, s_4, s_5)), new QueryOptions(), sessionIdUser);

    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testSampleVersioning() throws CatalogException {
        Query query = new Query(ProjectDBAdaptor.QueryParams.USER_ID.key(), "user");
        long projectId = catalogManager.getProjectManager().get(query, null, sessionIdUser).first().getId();
        long studyId = catalogManager.getStudyManager().get(new Query(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectId), null,
                sessionIdUser).first().getId();

        catalogManager.getSampleManager().create(Long.toString(studyId),
                new Sample().setName("testSample").setDescription("description"), null, sessionIdUser);
        catalogManager.getSampleManager().update(String.valueOf(studyId), "testSample", new ObjectMap(),
                new QueryOptions(Constants.INCREMENT_VERSION, true), sessionIdUser);
        catalogManager.getSampleManager().update(String.valueOf(studyId), "testSample", new ObjectMap(),
                new QueryOptions(Constants.INCREMENT_VERSION, true), sessionIdUser);

        catalogManager.getProjectManager().incrementRelease(String.valueOf(projectId), sessionIdUser);
        // We create something to have a gap in the release
        catalogManager.getSampleManager().create(Long.toString(studyId), new Sample().setName("dummy"), null, sessionIdUser);

        catalogManager.getProjectManager().incrementRelease(String.valueOf(projectId), sessionIdUser);
        catalogManager.getSampleManager().update(String.valueOf(studyId), "testSample", new ObjectMap(),
                new QueryOptions(Constants.INCREMENT_VERSION, true), sessionIdUser);

        catalogManager.getSampleManager().update(String.valueOf(studyId), "testSample", new ObjectMap("description", "new description"),
                null, sessionIdUser);

        // We want the whole history of the sample
        query = new Query()
                .append(SampleDBAdaptor.QueryParams.NAME.key(), "testSample")
                .append(Constants.ALL_VERSIONS, true);
        QueryResult<Sample> sampleQueryResult = catalogManager.getSampleManager().get(String.valueOf(studyId), query, null, sessionIdUser);
        assertEquals(4, sampleQueryResult.getNumResults());
        assertEquals("description", sampleQueryResult.getResult().get(0).getDescription());
        assertEquals("description", sampleQueryResult.getResult().get(1).getDescription());
        assertEquals("description", sampleQueryResult.getResult().get(2).getDescription());
        assertEquals("new description", sampleQueryResult.getResult().get(3).getDescription());

        // We want the last version of release 1
        query = new Query()
                .append(SampleDBAdaptor.QueryParams.NAME.key(), "testSample")
                .append(SampleDBAdaptor.QueryParams.SNAPSHOT.key(), 1);
        sampleQueryResult = catalogManager.getSampleManager().get(String.valueOf(studyId), query, null, sessionIdUser);
        assertEquals(1, sampleQueryResult.getNumResults());
        assertEquals(3, sampleQueryResult.first().getVersion());

        // We want the last version of release 2 (must be the same of release 1)
        query = new Query()
                .append(SampleDBAdaptor.QueryParams.NAME.key(), "testSample")
                .append(SampleDBAdaptor.QueryParams.SNAPSHOT.key(), 2);
        sampleQueryResult = catalogManager.getSampleManager().get(String.valueOf(studyId), query, null, sessionIdUser);
        assertEquals(1, sampleQueryResult.getNumResults());
        assertEquals(3, sampleQueryResult.first().getVersion());

        // We want the last version of the sample
        query = new Query()
                .append(SampleDBAdaptor.QueryParams.NAME.key(), "testSample");
        sampleQueryResult = catalogManager.getSampleManager().get(String.valueOf(studyId), query, null, sessionIdUser);
        assertEquals(1, sampleQueryResult.getNumResults());
        assertEquals(4, sampleQueryResult.first().getVersion());

        // We want the version 2 of the sample
        query = new Query()
                .append(SampleDBAdaptor.QueryParams.NAME.key(), "testSample")
                .append(SampleDBAdaptor.QueryParams.VERSION.key(), 2);
        sampleQueryResult = catalogManager.getSampleManager().get(String.valueOf(studyId), query, null, sessionIdUser);
        assertEquals(1, sampleQueryResult.getNumResults());
        assertEquals(2, sampleQueryResult.first().getVersion());

        // We want the version 1 of the sample
        query = new Query()
                .append(SampleDBAdaptor.QueryParams.NAME.key(), "testSample")
                .append(SampleDBAdaptor.QueryParams.VERSION.key(), 1);
        sampleQueryResult = catalogManager.getSampleManager().get(String.valueOf(studyId), query, null, sessionIdUser);
        assertEquals(1, sampleQueryResult.getNumResults());
        assertEquals(1, sampleQueryResult.first().getVersion());
    }

    @Test
    public void testCreateSample() throws CatalogException {
        Query query = new Query(ProjectDBAdaptor.QueryParams.USER_ID.key(), "user");
        long projectId = catalogManager.getProjectManager().get(query, null, sessionIdUser).first().getId();
        long studyId = catalogManager.getStudyManager().get(new Query(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectId), null, sessionIdUser).first().getId();

        QueryResult<Sample> sampleQueryResult = catalogManager.getSampleManager().create(Long.toString(studyId), "HG007", "IMDb", "",
                null, false, null, new HashMap<>(), null, null, sessionIdUser);
        System.out.println("sampleQueryResult = " + sampleQueryResult);
    }

    @Test
    public void testUpdateSampleStats() throws CatalogException {
        Query query = new Query(ProjectDBAdaptor.QueryParams.USER_ID.key(), "user");
        long projectId = catalogManager.getProjectManager().get(query, null, sessionIdUser).first().getId();
        long studyId = catalogManager.getStudyManager().get(new Query(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectId), null, sessionIdUser).first().getId();

        catalogManager.getSampleManager().create(Long.toString(studyId), "HG007", "IMDb", "", null, false, null, new HashMap<>(), null,
                null, sessionIdUser);
        QueryResult<Sample> update = catalogManager.getSampleManager().update(Long.toString(studyId), "HG007", new ObjectMap
                (SampleDBAdaptor.QueryParams.STATS.key(), new ObjectMap("one", "two")), new QueryOptions(), sessionIdUser);
        assertEquals(1, update.first().getStats().size());
        assertTrue(update.first().getStats().containsKey("one"));
        assertEquals("two", update.first().getStats().get("one"));

        update = catalogManager.getSampleManager().update(Long.toString(studyId), "HG007",
                new ObjectMap(SampleDBAdaptor.QueryParams.STATS.key(), new ObjectMap("two", "three")), new QueryOptions(), sessionIdUser);
        assertEquals(2, update.first().getStats().size());
//        assertTrue(update.first().getStats().containsKey("two"));
//        assertEquals("three", update.first().getStats().get("two"));
    }

    @Test
    public void testCreateSampleWithDotInName() throws CatalogException {
        Query query = new Query(ProjectDBAdaptor.QueryParams.USER_ID.key(), "user");
        long projectId = catalogManager.getProjectManager().get(query, null, sessionIdUser).first().getId();
        long studyId = catalogManager.getStudyManager().get(new Query(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectId), null, sessionIdUser).first().getId();

        String name = "HG007.sample";
        QueryResult<Sample> sampleQueryResult = catalogManager.getSampleManager().create(Long.toString(studyId), name, "IMDb", "", null,
                false, null, new HashMap<>(), null, null, sessionIdUser);

        assertEquals(name, sampleQueryResult.first().getName());
    }

    @Test
    public void testAnnotate() throws CatalogException {
        long studyId = catalogManager.getStudyManager().getId("user", "1000G:phase1");
        Study study = catalogManager.getStudyManager().get(String.valueOf((Long) studyId), null, sessionIdUser).first();

        Set<Variable> variables = new HashSet<>();
        variables.add(new Variable("NAME", "", Variable.VariableType.TEXT, "", true, false, Collections.<String>emptyList(), 0, "", "",
                null, Collections.<String, Object>emptyMap()));
        variables.add(new Variable("AGE", "", Variable.VariableType.INTEGER, "", false, false, Collections.<String>emptyList(), 0, "", "",
                null, Collections.<String, Object>emptyMap()));
        variables.add(new Variable("HEIGHT", "", Variable.VariableType.DOUBLE, "", false, false, Collections.<String>emptyList(), 0, "",
                "", null, Collections.<String, Object>emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(study.getId(), "vs1", false, false, "", null, variables, sessionIdUser).first();

        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("NAME", "Joe");
        annotations.put("AGE", 25);
        annotations.put("HEIGHT", 180);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_1), Long.toString(studyId), vs1.getName(),"annotation1",
                annotations, sessionIdUser);

        QueryResult<Sample> sampleQueryResult = catalogManager.getSampleManager().get(s_1,
                new QueryOptions(QueryOptions.INCLUDE, Constants.ANNOTATION_SET_NAME + ".annotation1"), sessionIdUser);
        assertEquals(1, sampleQueryResult.getNumResults());
        assertEquals(1, sampleQueryResult.first().getAnnotationSets().size());

//        QueryResult<AnnotationSet> annotationSetQueryResult = catalogManager.getSampleManager().getAnnotationSet(Long.toString(s_1),
//                Long.toString(studyId), "annotation1", sessionIdUser);
//        assertEquals(1, annotationSetQueryResult.getNumResults());
        Map<String, Object> map = sampleQueryResult.first().getAnnotationSets().get(0).getAnnotations();
        assertEquals(3, map.size());
        assertEquals("Joe", map.get("NAME"));
        assertEquals(25, map.get("AGE"));
        assertEquals(180.0, map.get("HEIGHT"));
    }

    @Test
    public void searchSamples() throws CatalogException {
        long studyId = catalogManager.getStudyManager().getId("user", "1000G:phase1");

        catalogManager.getStudyManager().createGroup(Long.toString(studyId), "myGroup", "user2,user3", sessionIdUser);
        catalogManager.getStudyManager().createGroup(Long.toString(studyId), "myGroup2", "user2,user3", sessionIdUser);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(Long.toString(studyId)), "@myGroup", new Study.StudyAclParams("", AclParams.Action.SET, null), sessionIdUser);

        catalogManager.getSampleManager().updateAcl(Long.toString(studyId), Arrays.asList("s_1"), "@myGroup", new Sample.SampleAclParams("VIEW", AclParams.Action.SET, null, null, null), sessionIdUser);

        QueryResult<Sample> search = catalogManager.getSampleManager().search(Long.toString(studyId), new Query(), new QueryOptions(),
                sessionIdUser2);
        assertEquals(1, search.getNumResults());
    }

    @Test
    public void testSearchAnnotation() throws CatalogException {
        long studyId = catalogManager.getStudyManager().getId("user", "1000G:phase1");
        Study study = catalogManager.getStudyManager().get(String.valueOf((Long) studyId), null, sessionIdUser).first();

        Set<Variable> variables = new HashSet<>();
        variables.add(new Variable("var_name", "", Variable.VariableType.TEXT, "", true, false, Collections.<String>emptyList(), 0,
                "",
                "",
                null, Collections.<String, Object>emptyMap()));
        variables.add(new Variable("AGE", "", Variable.VariableType.INTEGER, "", false, false, Collections.<String>emptyList(), 0, "", "",
                null, Collections.<String, Object>emptyMap()));
        variables.add(new Variable("HEIGHT", "", Variable.VariableType.DOUBLE, "", false, false, Collections.<String>emptyList(), 0, "",
                "", null, Collections.<String, Object>emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(study.getId(), "vs1", false, false, "", null, variables, sessionIdUser).first();

        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("var_name", "Joe");
        annotations.put("AGE", 25);
        annotations.put("HEIGHT", 180);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_1), Long.toString(studyId), vs1.getName(),"annotation1",
                annotations, sessionIdUser);

        // TODO: issue-772
//        QueryResult<AnnotationSet> annotQueryResult = catalogManager.getSampleManager().searchAnnotationSet(Long.toString(s_1),
//                Long.toString(studyId), vs1.getName(), "var_name=Joe;AGE=25", sessionIdUser);
//        assertEquals(1, annotQueryResult.getNumResults());
//
//        annotQueryResult = catalogManager.getSampleManager().searchAnnotationSet(Long.toString(s_1),
//                Long.toString(studyId), vs1.getName(), "var_name=Joe;AGE=23", sessionIdUser);
//        assertEquals(0, annotQueryResult.getNumResults());
    }

    @Test
    public void testAnnotateMulti() throws CatalogException {
        long studyId = catalogManager.getStudyManager().getId("user", "1000G:phase1");
        Study study = catalogManager.getStudyManager().get(String.valueOf((Long) studyId), null, sessionIdUser).first();
        long sampleId = catalogManager.getSampleManager().create(Long.toString(study.getId()), "SAMPLE_1", "", "", null, false, null, new
                HashMap<>(), null, new QueryOptions(), sessionIdUser).first()
                .getId();

        Set<Variable> variables = new HashSet<>();
        variables.add(new Variable("NAME", "", Variable.VariableType.TEXT, "", true, false, Collections.<String>emptyList(), 0, "", "",
                null, Collections.<String, Object>emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(study.getId(), "vs1", false, false, "", null, variables, sessionIdUser).first();


        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("NAME", "Luke");
        QueryResult<AnnotationSet> annotationSetQueryResult = catalogManager.getSampleManager().createAnnotationSet(Long.toString
                (sampleId), null, Long.toString(vs1.getId()), "annotation1", annotations, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations = new HashMap<>();
        annotations.put("NAME", "Lucas");
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(sampleId), null, Long.toString(vs1.getId()), "annotation2", annotations, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        assertEquals(2, catalogManager.getSampleManager().get(sampleId, null, sessionIdUser).first().getAnnotationSets().size());
    }

    @Test
    public void testAnnotateUnique() throws CatalogException {
        long studyId = catalogManager.getStudyManager().getId("user", "1000G:phase1");
        Study study = catalogManager.getStudyManager().get(String.valueOf((Long) studyId), null, sessionIdUser).first();
        long sampleId = catalogManager.getSampleManager().create(Long.toString(study.getId()), "SAMPLE_1", "", "", null, false, null, new
                HashMap<>(), null, new QueryOptions(), sessionIdUser).first()
                .getId();

        Set<Variable> variables = new HashSet<>();
        variables.add(new Variable("NAME", "", Variable.VariableType.TEXT, "", true, false, Collections.<String>emptyList(), 0, "", "",
                null, Collections.<String, Object>emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(study.getId(), "vs1", true, false, "", null, variables, sessionIdUser).first();


        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("NAME", "Luke");
        QueryResult<AnnotationSet> annotationSetQueryResult = catalogManager.getSampleManager().createAnnotationSet(Long.toString
                (sampleId), null, Long.toString(vs1.getId()), "annotation1", annotations, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("NAME", "Lucas");
        thrown.expect(CatalogException.class);
        thrown.expectMessage("unique");
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(sampleId), null, Long.toString(vs1.getId()), "annotation2", annotations, sessionIdUser);
    }

    @Test
    public void testAnnotateIndividualUnique() throws CatalogException {
        long studyId = catalogManager.getStudyManager().getId("user", "1000G:phase1");
        Study study = catalogManager.getStudyManager().get(String.valueOf((Long) studyId), null, sessionIdUser).first();
        long individualId = catalogManager.getIndividualManager().create(study.getId(), "INDIVIDUAL_1", "", (long) -1, (long) -1,
                Individual.Sex.UNKNOWN, "", "", "", "", "", Individual.KaryotypicSex.UNKNOWN, Individual.LifeStatus.UNKNOWN, Individual
                        .AffectationStatus.UNKNOWN, new QueryOptions(), sessionIdUser).first().getId();

        Set<Variable> variables = new HashSet<>();
        variables.add(new Variable("NAME", "", Variable.VariableType.TEXT, "", true, false, Collections.<String>emptyList(), 0, "", "",
                null, Collections.<String, Object>emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(study.getId(), "vs1", true, false, "", null, variables, sessionIdUser).first();


        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("NAME", "Luke");
        QueryResult<AnnotationSet> annotationSetQueryResult = catalogManager.getIndividualManager().createAnnotationSet(
                Long.toString(individualId), null, Long.toString(vs1.getId()), "annotation1", annotations, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("NAME", "Lucas");
        thrown.expect(CatalogException.class);
        thrown.expectMessage("unique");
        catalogManager.getIndividualManager().createAnnotationSet(Long.toString(individualId), null, Long.toString(vs1.getId()),
                "annotation2", annotations, sessionIdUser);
    }

    @Test
    public void testAnnotateIncorrectType() throws CatalogException {
        long studyId = catalogManager.getStudyManager().getId("user", "1000G:phase1");
        Study study = catalogManager.getStudyManager().get(String.valueOf((Long) studyId), null, sessionIdUser).first();
        long sampleId = catalogManager.getSampleManager().create(Long.toString(study.getId()), "SAMPLE_1", "", "", null, false, null, new
                HashMap<>(), null, new QueryOptions(), sessionIdUser).first()
                .getId();

        Set<Variable> variables = new HashSet<>();
        variables.add(new Variable("NUM", "", Variable.VariableType.DOUBLE, "", true, false, null, 0, "", "", null, Collections.<String,
                Object>emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(study.getId(), "vs1", false, false, "", null, variables, sessionIdUser).first();


        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("NUM", "5");
        QueryResult<AnnotationSet> annotationSetQueryResult = catalogManager.getSampleManager().createAnnotationSet(Long.toString
                (sampleId), null, Long.toString(vs1.getId()), "annotation1", annotations, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("NUM", "6.8");
        annotationSetQueryResult = catalogManager.getSampleManager().createAnnotationSet(Long.toString(sampleId), null, Long.toString
                (vs1.getId()), "annotation2", annotations, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("NUM", "five polong five");
        thrown.expect(CatalogException.class);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(sampleId), null, Long.toString(vs1.getId()), "annotation3", annotations, sessionIdUser);
    }

    @Test
    public void testAnnotateRange() throws CatalogException {
        long studyId = catalogManager.getStudyManager().getId("user", "1000G:phase1");
        Study study = catalogManager.getStudyManager().get(String.valueOf((Long) studyId), null, sessionIdUser).first();
        long sampleId = catalogManager.getSampleManager().create(Long.toString(study.getId()), "SAMPLE_1", "", "", null, false, null, new HashMap<>(), null, new QueryOptions(), sessionIdUser).first()
                .getId();

        Set<Variable> variables = new HashSet<>();
        variables.add(new Variable("RANGE_NUM", "", Variable.VariableType.DOUBLE, "", true, false, Arrays.asList("1:14", "16:22", "50:")
                , 0, "", "", null, Collections.<String, Object>emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(study.getId(), "vs1", false, false, "", null, variables, sessionIdUser).first();

        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("RANGE_NUM", "1");  // 1:14
        QueryResult<AnnotationSet> annotationSetQueryResult = catalogManager.getSampleManager().createAnnotationSet(Long.toString(sampleId), null, Long.toString(vs1.getId()), "annotation1", annotations, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("RANGE_NUM", "14"); // 1:14
        annotationSetQueryResult = catalogManager.getSampleManager().createAnnotationSet(Long.toString(sampleId), null, Long.toString(vs1.getId()), "annotation2", annotations, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("RANGE_NUM", "20");  // 16:20
        annotationSetQueryResult = catalogManager.getSampleManager().createAnnotationSet(Long.toString(sampleId), null, Long.toString(vs1.getId()), "annotation3", annotations, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("RANGE_NUM", "100000"); // 50:
        annotationSetQueryResult = catalogManager.getSampleManager().createAnnotationSet(Long.toString(sampleId), null, Long.toString(vs1.getId()), "annotation4", annotations, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("RANGE_NUM", "14.1");
        thrown.expect(CatalogException.class);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(sampleId), null, Long.toString(vs1.getId()), "annotation5", annotations, sessionIdUser);
    }

    @Test
    public void testAnnotateCategorical() throws CatalogException {
        long studyId = catalogManager.getStudyManager().getId("user", "1000G:phase1");
        Study study = catalogManager.getStudyManager().get(String.valueOf((Long) studyId), null, sessionIdUser).first();
        long sampleId = catalogManager.getSampleManager().create(Long.toString(study.getId()), "SAMPLE_1", "", "", null, false, null, new HashMap<>(), null, new QueryOptions(), sessionIdUser).first()
                .getId();

        Set<Variable> variables = new HashSet<>();
        variables.add(new Variable("COOL_NAME", "", Variable.VariableType.CATEGORICAL, "", true, false, Arrays.asList("LUKE", "LEIA",
                "VADER", "YODA"), 0, "", "", null, Collections.<String, Object>emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(study.getId(), "vs1", false, false, "", null, variables, sessionIdUser).first();

        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("COOL_NAME", "LUKE");
        QueryResult<AnnotationSet> annotationSetQueryResult = catalogManager.getSampleManager().createAnnotationSet(Long.toString(sampleId), null, Long.toString(vs1.getId()), "annotation1", annotations, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("COOL_NAME", "LEIA");
        annotationSetQueryResult = catalogManager.getSampleManager().createAnnotationSet(Long.toString(sampleId), null, Long.toString(vs1.getId()), "annotation2", annotations, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("COOL_NAME", "VADER");
        annotationSetQueryResult = catalogManager.getSampleManager().createAnnotationSet(Long.toString(sampleId), null, Long.toString
                (vs1.getId()), "annotation3", annotations, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("COOL_NAME", "YODA");
        annotationSetQueryResult = catalogManager.getSampleManager().createAnnotationSet(Long.toString(sampleId), null, Long.toString
                (vs1.getId()), "annotation4", annotations, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("COOL_NAME", "SPOCK");
        thrown.expect(CatalogException.class);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(sampleId), null, Long.toString(vs1.getId()), "annotation5", annotations, sessionIdUser);
    }

    @Test
    public void testAnnotateNested() throws CatalogException {
        long studyId = catalogManager.getStudyManager().getId("user", "1000G:phase1");
        Study study = catalogManager.getStudyManager().get(String.valueOf((Long) studyId), null, sessionIdUser).first();
        long sampleId1 = catalogManager.getSampleManager().create(Long.toString(study.getId()), new Sample().setName("SAMPLE_1"),
                new QueryOptions(), sessionIdUser).first().getId();
        long sampleId2 = catalogManager.getSampleManager().create(Long.toString(study.getId()), new Sample().setName("SAMPLE_2"),
                new QueryOptions(), sessionIdUser).first().getId();

        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(study.getId(), "vs1", false, false, "", null,
                Collections.singleton(CatalogAnnotationsValidatorTest.nestedObject), sessionIdUser).first();

        QueryResult<AnnotationSet> annotationSetQueryResult;
        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("nestedObject", new ObjectMap()
                .append("stringList", Arrays.asList("li", "lu"))
                .append("object", new ObjectMap()
                        .append("string", "my value")
                        .append("numberList", Arrays.asList(2, 3, 4))));
        annotationSetQueryResult = catalogManager.getSampleManager().createAnnotationSet(Long.toString(sampleId1), null,
                Long.toString(vs1.getId()), "annotation1", annotations, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("nestedObject", new ObjectMap()
                .append("stringList", Arrays.asList("lo", "lu"))
                .append("object", new ObjectMap()
                        .append("string", "stringValue")
                        .append("numberList", Arrays.asList(3, 4, 5))));
        annotationSetQueryResult = catalogManager.getSampleManager().createAnnotationSet(Long.toString(sampleId2), null,
                Long.toString(vs1.getId()), "annotation1", annotations, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        List<Sample> samples;
        Query query = new Query(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":nestedObject.stringList=li");
        samples = catalogManager.getSampleManager().get(studyId, query, null, sessionIdUser).getResult();
        assertEquals(1, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":nestedObject.stringList=lo");
        samples = catalogManager.getSampleManager().get(studyId, query, null, sessionIdUser).getResult();
        assertEquals(1, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":nestedObject.stringList=LL");
        samples = catalogManager.getSampleManager().get(studyId, query, null, sessionIdUser).getResult();
        assertEquals(0, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":nestedObject.stringList=lo,li,LL");
        samples = catalogManager.getSampleManager().get(studyId, query, null, sessionIdUser).getResult();
        assertEquals(2, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":nestedObject.object.string=my value");
        samples = catalogManager.getSampleManager().get(studyId, query, null, sessionIdUser).getResult();
        assertEquals(1, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":nestedObject.stringList=lo,lu,LL;" + vs1.getId()
                        + ":nestedObject.object.string=my value");
        samples = catalogManager.getSampleManager().get(studyId, query, null, sessionIdUser).getResult();
        assertEquals(1, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":nestedObject.stringList=lo,lu,LL;" + vs1.getId()
                + ":nestedObject.object.numberList=7");
        samples = catalogManager.getSampleManager().get(studyId, query, null, sessionIdUser).getResult();
        assertEquals(0, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":nestedObject.stringList=lo,lu,LL;"
                + vs1.getId() + ":nestedObject.object.numberList=3");
        samples = catalogManager.getSampleManager().get(studyId, query, null, sessionIdUser).getResult();
        assertEquals(2, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":nestedObject.stringList=lo,lu,LL;" + vs1.getId()
                + ":nestedObject.object.numberList=5;" + vs1.getId() + ":nestedObject.object.string=stringValue");
        samples = catalogManager.getSampleManager().get(studyId, query, null, sessionIdUser).getResult();
        assertEquals(1, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":nestedObject.stringList=lo,lu,LL;" + vs1.getId()
                + ":nestedObject.object.numberList=2,5");
        samples = catalogManager.getSampleManager().get(studyId, query, null, sessionIdUser).getResult();
        assertEquals(2, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":nestedObject.stringList=lo,lu,LL;" + vs1.getId()
                + ":nestedObject.object.numberList=0");
        samples = catalogManager.getSampleManager().get(studyId, query, null, sessionIdUser).getResult();
        assertEquals(0, samples.size());


        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":unexisting=lo,lu,LL");
        thrown.expect(CatalogException.class);
        thrown.expectMessage("does not exist");
        catalogManager.getSampleManager().get(studyId, query, null, sessionIdUser).getResult();
    }

//    @Test
//    public void testQuerySampleAnnotationFail1() throws CatalogException {
//        Query query = new Query();
//        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ":nestedObject.stringList", "lo,lu,LL");
//
//        thrown.expect(CatalogDBException.class);
//        thrown.expectMessage("annotation:nestedObject does not exist");
//        QueryResult<Sample> search = catalogManager.getSampleManager().search(Long.toString(studyId), query, null, sessionIdUser);
//        catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
//    }

//    @Test
//    public void testQuerySampleAnnotationFail2() throws CatalogException {
//        Query query = new Query();
//        query.put(CatalogSampleDBAdaptor.QueryParams.ANNOTATION.key(), "nestedObject.stringList:lo,lu,LL");
//
//        thrown.expect(CatalogDBException.class);
//        thrown.expectMessage("Wrong annotation query");
//        catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
//    }

    @Test
    public void testIteratorSamples() throws CatalogException {
        long studyId = catalogManager.getStudyManager().getId("user", "1000G:phase1");

        Query query = new Query();

        DBIterator<Sample> iterator = catalogManager.getSampleManager().iterator(studyId, query, null, sessionIdUser);
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        assertEquals(9, count);
    }

    @Test
    public void testQuerySamples() throws CatalogException {
        long studyId = catalogManager.getStudyManager().getId("user", "1000G:phase1");
        Study study = catalogManager.getStudyManager().get(String.valueOf((Long) studyId), null, sessionIdUser).first();

        VariableSet variableSet = study.getVariableSets().get(0);

        List<Sample> samples;
        Query query = new Query();

        samples = catalogManager.getSampleManager().get(studyId, query, null, sessionIdUser).getResult();
        assertEquals(9, samples.size());

        query = new Query(ANNOTATION.key(), Constants.VARIABLE_SET + "=" + variableSet.getId());
        samples = catalogManager.getSampleManager().get(studyId, query, null, sessionIdUser).getResult();
        assertEquals(8, samples.size());

        query = new Query(ANNOTATION.key(), Constants.ANNOTATION_SET_NAME + "=annot2");
        samples = catalogManager.getSampleManager().get(studyId, query, null, sessionIdUser).getResult();
        assertEquals(3, samples.size());

        query = new Query(ANNOTATION.key(), Constants.ANNOTATION_SET_NAME + "=noExist");
        samples = catalogManager.getSampleManager().get(studyId, query, null, sessionIdUser).getResult();
        assertEquals(0, samples.size());

        query = new Query(ANNOTATION.key(), variableSet.getId() + ":NAME=s_1,s_2,s_3");
        samples = catalogManager.getSampleManager().get(studyId, query, null, sessionIdUser).getResult();
        assertEquals(3, samples.size());

        query = new Query(ANNOTATION.key(), variableSet.getId() + ":AGE>30;" + Constants.VARIABLE_SET + "=" + variableSet.getId());
        samples = catalogManager.getSampleManager().get(studyId, query, null, sessionIdUser).getResult();
        assertEquals(3, samples.size());

        query = new Query(ANNOTATION.key(), variableSet.getId() + ":AGE>30;" + Constants.VARIABLE_SET + "=" + variableSet.getId());
        samples = catalogManager.getSampleManager().get(studyId, query, null, sessionIdUser).getResult();
        assertEquals(3, samples.size());

        query = new Query(ANNOTATION.key(), variableSet.getId() + ":AGE>30;" + variableSet.getId() + ":ALIVE=true;"
                + Constants.VARIABLE_SET + "=" + variableSet.getId());
        samples = catalogManager.getSampleManager().get(studyId, query, null, sessionIdUser).getResult();
        assertEquals(2, samples.size());
    }

    @Test
    public void testUpdateAnnotation() throws CatalogException, JsonProcessingException {

        String userId = catalogManager.getUserManager().getUserId(sessionIdUser);
        long studyId = catalogManager.getStudyManager().getId(userId, "user@1000G:phase1");
        Study study = catalogManager.getStudyManager().get(String.valueOf((Long) studyId), QueryOptions.empty(), sessionIdUser).first();

        Sample sample = catalogManager.getSampleManager().get(s_1, null, sessionIdUser).first();
        AnnotationSet annotationSet = sample.getAnnotationSets().get(0);

        Individual ind = new Individual()
                .setName("INDIVIDUAL_1")
                .setSex(Individual.Sex.UNKNOWN);
        ind.setAnnotationSets(Arrays.asList(annotationSet));
        ind = catalogManager.getIndividualManager().create(Long.toString(study.getId()), ind, QueryOptions.empty(), sessionIdUser).first();

        ObjectMapper jsonObjectMapper = new ObjectMapper();

        // First update
        ObjectMap annotations = new ObjectMap()
                .append("NAME", "SAMPLE1")
                .append("AGE", 38)
                .append("EXTRA", "extra");
        annotationSet.setAnnotations(annotations);

        ObjectMap updateAnnotation = new ObjectMap()
                // Update the annotation values
                .append(SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key(), Arrays.asList(
                        new ObjectMap(jsonObjectMapper.writeValueAsString(annotationSet))
                ))
                // Delete the annotation made for the variable HEIGHT
                .append(Constants.DELETE_ANNOTATION, annotationSet.getName() + ":HEIGHT");

        // Update annotation set
        catalogManager.getIndividualManager().update(String.valueOf(studyId), String.valueOf(ind.getId()), updateAnnotation,
                new QueryOptions(), sessionIdUser);
        catalogManager.getSampleManager().update(String.valueOf(studyId), String.valueOf(s_1), updateAnnotation,
                new QueryOptions(), sessionIdUser);

        Consumer<AnnotationSet> check = as -> {
            Map<String, Object> auxAnnotations = as.getAnnotations();

            assertEquals(5, auxAnnotations.size());
            assertEquals("SAMPLE1", auxAnnotations.get("NAME"));
            assertEquals(38.0, auxAnnotations.get("AGE"));
            assertEquals("extra", auxAnnotations.get("EXTRA"));
        };

        sample = catalogManager.getSampleManager().get(s_1, null, sessionIdUser).first();
        ind = catalogManager.getIndividualManager().get(null, String.valueOf((Long) ind.getId()), null, sessionIdUser).first();
        check.accept(sample.getAnnotationSets().get(0));
        check.accept(ind.getAnnotationSets().get(0));

        // Call again to the update to check that nothing changed
        catalogManager.getIndividualManager().update(String.valueOf(studyId), String.valueOf(ind.getId()), updateAnnotation,
                new QueryOptions(), sessionIdUser);
        check.accept(ind.getAnnotationSets().get(0));

        annotations = new ObjectMap()
                .append("NAME", "SAMPLE 1");
        annotationSet.setAnnotations(annotations);
        updateAnnotation = new ObjectMap()
                // Update the annotation values
                .append(SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key(), Arrays.asList(
                        new ObjectMap(jsonObjectMapper.writeValueAsString(annotationSet))
                ))
                // Delete the annotation made for the variable HEIGHT
                .append(Constants.DELETE_ANNOTATION, annotationSet.getName() + ":EXTRA");
        catalogManager.getIndividualManager().update(String.valueOf(studyId), String.valueOf(ind.getId()), updateAnnotation,
                new QueryOptions(), sessionIdUser);
        catalogManager.getSampleManager().update(String.valueOf(studyId), String.valueOf(s_1), updateAnnotation,
                new QueryOptions(), sessionIdUser);

        check = as -> {
            Map<String, Object> auxAnnotations = as.getAnnotations();

            assertEquals(4, auxAnnotations.size());
            assertEquals("SAMPLE 1", auxAnnotations.get("NAME"));
            assertEquals(false, auxAnnotations.containsKey("EXTRA"));
        };

        sample = catalogManager.getSampleManager().get(s_1, null, sessionIdUser).first();
        ind = catalogManager.getIndividualManager().get(null, String.valueOf((Long) ind.getId()), null, sessionIdUser).first();
        check.accept(sample.getAnnotationSets().get(0));
        check.accept(ind.getAnnotationSets().get(0));
    }

    @Test
    public void testUpdateAnnotationFail() throws CatalogException {
        Sample sample = catalogManager.getSampleManager().get(s_1, null, sessionIdUser).first();
        AnnotationSet annotationSet = sample.getAnnotationSets().get(0);

        // Delete required annotation
        ObjectMap updateAnnotation = new ObjectMap(Constants.DELETE_ANNOTATION, annotationSet.getName() + ":NAME");

        thrown.expect(CatalogException.class); //Can not delete required fields
        thrown.expectMessage("required annotation");
        catalogManager.getSampleManager().update(String.valueOf(studyId), String.valueOf(s_1), updateAnnotation, new QueryOptions(),
                sessionIdUser);
    }

    @Test
    public void getVariableSetSummary() throws CatalogException {

        String userId = catalogManager.getUserManager().getUserId(sessionIdUser);
        long studyId = catalogManager.getStudyManager().getId(userId, "user@1000G:phase1");
        Study study = catalogManager.getStudyManager().get(String.valueOf((Long) studyId), null, sessionIdUser).first();

        long variableSetId = study.getVariableSets().get(0).getId();

        QueryResult<VariableSetSummary> variableSetSummary = catalogManager.getStudyManager()
                .getVariableSetSummary(Long.toString(studyId), Long.toString(variableSetId), sessionIdUser);

        assertEquals(1, variableSetSummary.getNumResults());
        VariableSetSummary summary = variableSetSummary.first();

        assertEquals(5, summary.getSamples().size());

        // PHEN
        int i;
        for (i = 0; i < summary.getSamples().size(); i++) {
            if ("PHEN".equals(summary.getSamples().get(i).getName())) {
                break;
            }
        }
        List<FeatureCount> annotations = summary.getSamples().get(i).getAnnotations();
        assertEquals("PHEN", summary.getSamples().get(i).getName());
        assertEquals(2, annotations.size());

        for (i = 0; i < annotations.size(); i++) {
            if ("CONTROL".equals(annotations.get(i).getName())) {
                break;
            }
        }
        assertEquals("CONTROL", annotations.get(i).getName());
        assertEquals(5, annotations.get(i).getCount());

        for (i = 0; i < annotations.size(); i++) {
            if ("CASE".equals(annotations.get(i).getName())) {
                break;
            }
        }
        assertEquals("CASE", annotations.get(i).getName());
        assertEquals(3, annotations.get(i).getCount());

    }

    @Test
    public void testModifySample() throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionIdUser);
        long studyId = catalogManager.getStudyManager().getId(userId, "user@1000G:phase1");
        long sampleId1 = catalogManager.getSampleManager()
                .create("user@1000G:phase1", "SAMPLE_1", "", "", null, false, null, new HashMap<>(), null, new QueryOptions(),
                        sessionIdUser).first().getId();
        long individualId = catalogManager.getIndividualManager().create(studyId, "Individual1", "", (long) 0, (long) 0, Individual.Sex
                        .MALE, "", "", "", "", "", Individual.KaryotypicSex.UNKNOWN, Individual.LifeStatus.UNKNOWN, Individual.AffectationStatus.UNKNOWN,
                new QueryOptions(), sessionIdUser).first().getId();

        Sample sample = catalogManager.getSampleManager()
                .update(sampleId1, new ObjectMap(SampleDBAdaptor.QueryParams.INDIVIDUAL.key(), individualId),
                        new QueryOptions("lazy", false), sessionIdUser).first();

        assertEquals(individualId, sample.getIndividual().getId());
    }

    @Test
    public void getSharedProject() throws CatalogException, IOException {
        catalogManager.getUserManager().create("dummy", "dummy", "asd@asd.asd", "dummy", "", 50000L,
                Account.GUEST, QueryOptions.empty(), null);
        catalogManager.getStudyManager().updateGroup(String.valueOf(studyId), "@members", new GroupParams("dummy",
                GroupParams.Action.ADD), sessionIdUser);

        String token = catalogManager.getUserManager().login("dummy", "dummy");
        QueryResult<Project> queryResult = catalogManager.getProjectManager().getSharedProjects("dummy", QueryOptions.empty(), token);
        assertEquals(1, queryResult.getNumResults());

        catalogManager.getStudyManager().updateGroup(String.valueOf(studyId), "@members", new GroupParams("*", GroupParams.Action.ADD),
                sessionIdUser);
        queryResult = catalogManager.getProjectManager().getSharedProjects("*", QueryOptions.empty(), null);
        assertEquals(1, queryResult.getNumResults());
    }

    @Test
    public void smartResolutorStudyAliasFromAnonymousUser() throws CatalogException {
        catalogManager.getStudyManager().updateGroup(String.valueOf(studyId), "@members", new GroupParams("*", GroupParams.Action.ADD),
                sessionIdUser);
        long id = catalogManager.getStudyManager().getId("*", "1000G:phase1");
        assertTrue(id > 0);
    }

    @Test
    public void testCreateSampleWithIndividual() throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionIdUser);
        long studyId = catalogManager.getStudyManager().getId(userId, "user@1000G:phase1");
        long individualId = catalogManager.getIndividualManager().create(studyId, "Individual1", "", (long) 0, (long) 0, Individual.Sex
                        .MALE, "", "", "", "", "", Individual.KaryotypicSex.UNKNOWN, Individual.LifeStatus.UNKNOWN, Individual.AffectationStatus.UNKNOWN,
                new QueryOptions(), sessionIdUser).first().getId();
        long sampleId1 = catalogManager.getSampleManager()
                .create("user@1000G:phase1", "SAMPLE_1", "", "", null, false, new Individual().setName(String.valueOf(individualId)), new HashMap<>(), null,
                        new QueryOptions(), sessionIdUser).first().getId();

        QueryResult<Individual> individualQueryResult = catalogManager.getIndividualManager().get(String.valueOf(studyId),
                String.valueOf(individualId), QueryOptions.empty(), sessionIdUser);
        assertEquals(sampleId1, individualQueryResult.first().getSamples().get(0).getId());

        // Create sample linking to individual based on the individual name
        long sampleId2 = catalogManager.getSampleManager()
                .create("user@1000G:phase1", "SAMPLE_2", "", "", null, false, new Individual().setName("Individual1"), new HashMap<>(), null,
                        new QueryOptions(), sessionIdUser).first().getId();
        individualQueryResult = catalogManager.getIndividualManager().get(String.valueOf(studyId),
                String.valueOf(individualId), QueryOptions.empty(), sessionIdUser);
        assertEquals(2, individualQueryResult.first().getSamples().size());
        assertTrue(individualQueryResult.first().getSamples().stream().map(Sample::getId).collect(Collectors.toSet()).containsAll(
                Arrays.asList(sampleId1, sampleId2)
        ));
    }

    @Test
    public void testModifySampleBadIndividual() throws CatalogException {
        long sampleId1 = catalogManager.getSampleManager().create("user@1000G:phase1", "SAMPLE_1", "", "", null, false, null, new HashMap<>(), null, new
                QueryOptions(), sessionIdUser).first().getId();

        thrown.expect(CatalogDBException.class);
        catalogManager.getSampleManager()
                .update(sampleId1, new ObjectMap(SampleDBAdaptor.QueryParams.INDIVIDUAL.key(), 4), null, sessionIdUser);
    }

    @Test
    public void testDeleteSample() throws CatalogException, IOException {
        long studyId = catalogManager.getStudyManager().getId("user", "1000G:phase1");
        long sampleId = catalogManager.getSampleManager().create(Long.toString(studyId), "SAMPLE_1", "", "", null, false, null, new HashMap<>(), null, new QueryOptions(), sessionIdUser).first().getId();

        List<QueryResult<Sample>> queryResult = catalogManager.getSampleManager()
                .delete(Long.toString(studyId), Long.toString(sampleId), new QueryOptions(), sessionIdUser);
        assertEquals(sampleId, queryResult.get(0).first().getId());

        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.ID.key(), sampleId)
                .append(SampleDBAdaptor.QueryParams.STATUS_NAME.key(), Status.DELETED);

        QueryResult<Sample> sampleQueryResult = catalogManager.getSampleManager().get(studyId, query, new QueryOptions(), sessionIdUser);
//        QueryResult<Sample> sample = catalogManager.getSample(sampleId, new QueryOptions(), sessionIdUser);
        assertEquals(1, sampleQueryResult.getNumResults());
        assertTrue(sampleQueryResult.first().getName().contains(".DELETED"));
    }

    @Test
    public void testAssignPermissionsWithPropagationAndNoIndividual() throws CatalogException {
        Query query = new Query(ProjectDBAdaptor.QueryParams.USER_ID.key(), "user");
        long projectId = catalogManager.getProjectManager().get(query, null, sessionIdUser).first().getId();
        long studyId = catalogManager.getStudyManager().get(new Query(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectId), null,
                sessionIdUser).first().getId();

        Sample sample = new Sample().setName("sample");
        catalogManager.getSampleManager().create(Long.toString(studyId), sample, QueryOptions.empty(), sessionIdUser);

        List<QueryResult<SampleAclEntry>> queryResults = catalogManager.getSampleManager().updateAcl(Long.toString(studyId),
                Arrays.asList("sample"), "user2", new Sample.SampleAclParams("VIEW", AclParams.Action.SET, null, null, null, true),
                sessionIdUser);
        assertEquals(1, queryResults.size());
        assertEquals(1, queryResults.get(0).getNumResults());
        assertEquals(1, queryResults.get(0).first().getPermissions().size());
        assertTrue(queryResults.get(0).first().getPermissions().contains(SampleAclEntry.SamplePermissions.VIEW));
    }

    // Two samples, one related to one individual and the other does not have any individual associated
    @Test
    public void testAssignPermissionsWithPropagationWithIndividualAndNoIndividual() throws CatalogException {
        Query query = new Query(ProjectDBAdaptor.QueryParams.USER_ID.key(), "user");
        long projectId = catalogManager.getProjectManager().get(query, null, sessionIdUser).first().getId();
        long studyId = catalogManager.getStudyManager().get(new Query(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectId), null,
                sessionIdUser).first().getId();

        Sample sample = new Sample().setName("sample").setIndividual(new Individual().setName("individual"));
        catalogManager.getSampleManager().create(Long.toString(studyId), sample, QueryOptions.empty(), sessionIdUser);

        Sample sample2 = new Sample().setName("sample2");
        catalogManager.getSampleManager().create(Long.toString(studyId), sample2, QueryOptions.empty(), sessionIdUser);

        List<QueryResult<SampleAclEntry>> queryResults = catalogManager.getSampleManager().updateAcl(Long.toString(studyId),
                Arrays.asList("sample", "sample2"), "user2", new Sample.SampleAclParams("VIEW", AclParams.Action.SET, null, null, null,
                        true), sessionIdUser);
        assertEquals(2, queryResults.size());
        assertEquals(1, queryResults.get(0).getNumResults());
        assertEquals(1, queryResults.get(0).first().getPermissions().size());
        assertTrue(queryResults.get(0).first().getPermissions().contains(SampleAclEntry.SamplePermissions.VIEW));
        assertTrue(queryResults.get(1).first().getPermissions().contains(SampleAclEntry.SamplePermissions.VIEW));

        List<QueryResult<IndividualAclEntry>> individualAcl = catalogManager.getIndividualManager().getAcls(String.valueOf(studyId),
                Arrays.asList("individual"), "user2", false, sessionIdUser);
        assertEquals(1, individualAcl.size());
        assertEquals(1, individualAcl.get(0).getNumResults());
        assertEquals(1, individualAcl.get(0).first().getPermissions().size());
        assertTrue(individualAcl.get(0).first().getPermissions().contains(IndividualAclEntry.IndividualPermissions.VIEW));
    }

}
