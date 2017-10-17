package org.opencb.opencga.catalog.managers;

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
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.models.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.Assert.assertEquals;

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

        catalogManager.getUserManager().create("user", "User Name", "mail@ebi.ac.uk", PASSWORD, "", null, Account.FULL, null);
        catalogManager.getUserManager().create("user2", "User2 Name", "mail2@ebi.ac.uk", PASSWORD, "", null, Account.FULL, null);
        catalogManager.getUserManager().create("user3", "User3 Name", "user.2@e.mail", PASSWORD, "ACME", null, Account.FULL, null);

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
                new Variable("NAME", "", Variable.VariableType.TEXT, "", true, false, Collections.<String>emptyList(), 0, "", "", null,
                        Collections.<String, Object>emptyMap()),
                new Variable("AGE", "", Variable.VariableType.DOUBLE, null, true, false, Collections.singletonList("0:130"), 1, "", "",
                        null, Collections.<String, Object>emptyMap()),
                new Variable("HEIGHT", "", Variable.VariableType.DOUBLE, "1.5", false, false, Collections.singletonList("0:"), 2, "",
                        "", null, Collections.<String, Object>emptyMap()),
                new Variable("ALIVE", "", Variable.VariableType.BOOLEAN, "", true, false, Collections.<String>emptyList(), 3, "", "",
                        null, Collections.<String, Object>emptyMap()),
                new Variable("PHEN", "", Variable.VariableType.CATEGORICAL, "", true, false, Arrays.asList("CASE", "CONTROL"), 4, "", "",
                        null, Collections.<String, Object>emptyMap()),
                new Variable("EXTRA", "", Variable.VariableType.TEXT, "", false, false, Collections.emptyList(), 5, "", "", null,
                        Collections.<String, Object>emptyMap())
        ));
        VariableSet vs = catalogManager.getStudyManager().createVariableSet(studyId, "vs", true, false, "", null, variables, sessionIdUser).first();


        s_1 = catalogManager.getSampleManager().create(Long.toString(studyId), "s_1", "", "", null, false, null, new HashMap<>(), null, new QueryOptions()
                , sessionIdUser).first().getId();
        s_2 = catalogManager.getSampleManager().create(Long.toString(studyId), "s_2", "", "", null, false, null, new HashMap<>(), null, new QueryOptions()
                , sessionIdUser).first().getId();
        s_3 = catalogManager.getSampleManager().create(Long.toString(studyId), "s_3", "", "", null, false, null, new HashMap<>(), null, new QueryOptions()
                , sessionIdUser).first().getId();
        s_4 = catalogManager.getSampleManager().create(Long.toString(studyId), "s_4", "", "", null, false, null, new HashMap<>(), null, new QueryOptions()
                , sessionIdUser).first().getId();
        s_5 = catalogManager.getSampleManager().create(Long.toString(studyId), "s_5", "", "", null, false, null, new HashMap<>(), null, new QueryOptions()
                , sessionIdUser).first().getId();
        s_6 = catalogManager.getSampleManager().create(Long.toString(studyId), "s_6", "", "", null, false, null, new HashMap<>(), null, new QueryOptions()
                , sessionIdUser).first().getId();
        s_7 = catalogManager.getSampleManager().create(Long.toString(studyId), "s_7", "", "", null, false, null, new HashMap<>(), null, new QueryOptions()
                , sessionIdUser).first().getId();
        s_8 = catalogManager.getSampleManager().create(Long.toString(studyId), "s_8", "", "", null, false, null, new HashMap<>(), null, new QueryOptions()
                , sessionIdUser).first().getId();
        s_9 = catalogManager.getSampleManager().create(Long.toString(studyId), "s_9", "", "", null, false, null, new HashMap<>(), null, new QueryOptions()
                , sessionIdUser).first().getId();

        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_1), null, Long.toString(vs.getId()), "annot1", new ObjectMap("NAME", "s_1").append("AGE", 6).append("ALIVE", true)
                .append("PHEN", "CONTROL"), null, sessionIdUser);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_2), null, Long.toString(vs.getId()), "annot1", new ObjectMap("NAME", "s_2").append("AGE", 10).append("ALIVE", false)

                .append("PHEN", "CASE"), null, sessionIdUser);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_3), null, Long.toString(vs.getId()), "annot1", new ObjectMap("NAME", "s_3").append("AGE", 15).append("ALIVE", true)
                .append("PHEN", "CONTROL"), null, sessionIdUser);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_4), null, Long.toString(vs.getId()), "annot1", new ObjectMap("NAME", "s_4").append("AGE", 22).append("ALIVE", false)
                .append("PHEN", "CONTROL"), null, sessionIdUser);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_5), null, Long.toString(vs.getId()), "annot1", new ObjectMap("NAME", "s_5").append("AGE", 29).append("ALIVE", true)
                .append("PHEN", "CASE"), null, sessionIdUser);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_6), null, Long.toString(vs.getId()), "annot2", new ObjectMap("NAME", "s_6").append("AGE", 38).append("ALIVE", true)
                .append("PHEN", "CONTROL"), null, sessionIdUser);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_7), null, Long.toString(vs.getId()), "annot2", new ObjectMap("NAME", "s_7").append("AGE", 46).append("ALIVE", false)
                .append("PHEN", "CASE"), null, sessionIdUser);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_8), null, Long.toString(vs.getId()), "annot2", new ObjectMap("NAME", "s_8").append("AGE", 72).append("ALIVE", true)
                .append("PHEN", "CONTROL"), null, sessionIdUser);


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

}
