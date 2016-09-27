/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.catalog;

import com.mongodb.BasicDBObject;
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
import org.opencb.opencga.catalog.auth.authentication.CatalogAuthenticationManager;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.*;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.summaries.FeatureCount;
import org.opencb.opencga.catalog.models.summaries.VariableSetSummary;
import org.opencb.opencga.catalog.utils.CatalogAnnotationsValidatorTest;

import java.io.*;
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;
import static org.opencb.opencga.catalog.db.api.SampleDBAdaptor.QueryParams.ANNOTATION_SET_NAME;
import static org.opencb.opencga.catalog.db.api.SampleDBAdaptor.QueryParams.VARIABLE_SET_ID;

public class CatalogManagerTest extends GenericTest {

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

    /* TYPE_FILE UTILS */
    public static java.io.File createDebugFile() throws IOException {
        String fileTestName = "/tmp/fileTest " + StringUtils.randomString(5);
        return createDebugFile(fileTestName);
    }

    public static java.io.File createDebugFile(String fileTestName) throws IOException {
        return createDebugFile(fileTestName, 200);
    }

    public static java.io.File createDebugFile(String fileTestName, int lines) throws IOException {
        DataOutputStream os = new DataOutputStream(new FileOutputStream(fileTestName));

        os.writeBytes("Debug file name: " + fileTestName + "\n");
        for (int i = 0; i < 100; i++) {
            os.writeBytes(i + ", ");
        }
        for (int i = 0; i < lines; i++) {
            os.writeBytes(StringUtils.randomString(500));
            os.write('\n');
        }
        os.close();

        return Paths.get(fileTestName).toFile();
    }


    @Before
    public void setUp() throws IOException, CatalogException {
        catalogManager = catalogManagerResource.getCatalogManager();
        setUpCatalogManager(catalogManager);
    }

    public void setUpCatalogManager(CatalogManager catalogManager) throws IOException, CatalogException {

        catalogManager.createUser("user", "User Name", "mail@ebi.ac.uk", PASSWORD, "", null, null);
        catalogManager.createUser("user2", "User2 Name", "mail2@ebi.ac.uk", PASSWORD, "", null, null);
        catalogManager.createUser("user3", "User3 Name", "user.2@e.mail", PASSWORD, "ACME", null, null);

        sessionIdUser = catalogManager.login("user", PASSWORD, "127.0.0.1").first().getString("sessionId");
        sessionIdUser2 = catalogManager.login("user2", PASSWORD, "127.0.0.1").first().getString("sessionId");
        sessionIdUser3 = catalogManager.login("user3", PASSWORD, "127.0.0.1").first().getString("sessionId");

        Project project1 = catalogManager.createProject("Project about some genomes", "1000G", "", "ACME", null, sessionIdUser)
                .first();
        Project project2 = catalogManager.createProject("Project Management Project", "pmp", "life art intelligent system",
                "myorg", null, sessionIdUser2).first();
        Project project3 = catalogManager.createProject("project 1", "p1", "", "", null, sessionIdUser3).first();

        studyId = catalogManager.createStudy(project1.getId(), "Phase 1", "phase1", Study.Type.TRIO, "Done", sessionIdUser).first().getId();
        studyId2 = catalogManager.createStudy(project1.getId(), "Phase 3", "phase3", Study.Type.CASE_CONTROL, "d", sessionIdUser).first().getId();
        catalogManager.createStudy(project2.getId(), "Study 1", "s1", Study.Type.CONTROL_SET, "", sessionIdUser2).first().getId();

        catalogManager.createFolder(studyId2, Paths.get("data/test/folder/"), true, null, sessionIdUser);

        catalogManager.createFolder(studyId, Paths.get("analysis/"), true, null, sessionIdUser);
        catalogManager.createFolder(studyId2, Paths.get("analysis/"), true, null, sessionIdUser);

        testFolder = catalogManager.createFolder(studyId, Paths.get("data/test/folder/"), true, null, sessionIdUser).first();
        ObjectMap attributes = new ObjectMap();
        attributes.put("field", "value");
        attributes.put("numValue", 5);
        catalogManager.modifyFile(testFolder.getId(), new ObjectMap("attributes", attributes), sessionIdUser);

        File fileTest1k = catalogManager.createFile(studyId, File.Format.PLAIN, File.Bioformat.NONE,
                testFolder.getPath() + "test_1K.txt.gz",
                StringUtils.randomString(1000).getBytes(), "", false, sessionIdUser).first();
        attributes = new ObjectMap();
        attributes.put("field", "value");
        attributes.put("name", "fileTest1k");
        attributes.put("numValue", "10");
        attributes.put("boolean", false);
        catalogManager.modifyFile(fileTest1k.getId(), new ObjectMap("attributes", attributes), sessionIdUser);

        File fileTest05k = catalogManager.createFile(studyId, File.Format.PLAIN, File.Bioformat.DATAMATRIX_EXPRESSION,
                testFolder.getPath() + "test_0.5K.txt",
                StringUtils.randomString(500).getBytes(), "", false, sessionIdUser).first();
        attributes = new ObjectMap();
        attributes.put("field", "valuable");
        attributes.put("name", "fileTest05k");
        attributes.put("numValue", 5);
        attributes.put("boolean", true);
        catalogManager.modifyFile(fileTest05k.getId(), new ObjectMap("attributes", attributes), sessionIdUser);

        File test01k = catalogManager.createFile(studyId, File.Format.IMAGE, File.Bioformat.NONE,
                testFolder.getPath() + "test_0.1K.png",
                StringUtils.randomString(100).getBytes(), "", false, sessionIdUser).first();
        attributes = new ObjectMap();
        attributes.put("field", "other");
        attributes.put("name", "test01k");
        attributes.put("numValue", 50);
        attributes.put("nested", new ObjectMap("num1", 45).append("num2", 33).append("text", "HelloWorld"));
        catalogManager.modifyFile(test01k.getId(), new ObjectMap("attributes", attributes), sessionIdUser);

        Set<Variable> variables = new HashSet<>();
        variables.addAll(Arrays.asList(
                new Variable("NAME", "", Variable.VariableType.TEXT, "", true, false, Collections.<String>emptyList(), 0, "", "", null,
                        Collections.<String, Object>emptyMap()),
                new Variable("AGE", "", Variable.VariableType.NUMERIC, null, true, false, Collections.singletonList("0:130"), 1, "", "",
                        null, Collections.<String, Object>emptyMap()),
                new Variable("HEIGHT", "", Variable.VariableType.NUMERIC, "1.5", false, false, Collections.singletonList("0:"), 2, "",
                        "", null, Collections.<String, Object>emptyMap()),
                new Variable("ALIVE", "", Variable.VariableType.BOOLEAN, "", true, false, Collections.<String>emptyList(), 3, "", "",
                        null, Collections.<String, Object>emptyMap()),
                new Variable("PHEN", "", Variable.VariableType.CATEGORICAL, "", true, false, Arrays.asList("CASE", "CONTROL"), 4, "", "",
                        null, Collections.<String, Object>emptyMap()),
                new Variable("EXTRA", "", Variable.VariableType.TEXT, "", false, false, Collections.emptyList(), 5, "", "", null,
                        Collections.<String, Object>emptyMap())
        ));
        VariableSet vs = catalogManager.createVariableSet(studyId, "vs", true, "", null, variables, sessionIdUser).first();

        s_1 = catalogManager.createSample(studyId, "s_1", "", "", null, new QueryOptions(), sessionIdUser).first().getId();
        s_2 = catalogManager.createSample(studyId, "s_2", "", "", null, new QueryOptions(), sessionIdUser).first().getId();
        s_3 = catalogManager.createSample(studyId, "s_3", "", "", null, new QueryOptions(), sessionIdUser).first().getId();
        s_4 = catalogManager.createSample(studyId, "s_4", "", "", null, new QueryOptions(), sessionIdUser).first().getId();
        s_5 = catalogManager.createSample(studyId, "s_5", "", "", null, new QueryOptions(), sessionIdUser).first().getId();
        s_6 = catalogManager.createSample(studyId, "s_6", "", "", null, new QueryOptions(), sessionIdUser).first().getId();
        s_7 = catalogManager.createSample(studyId, "s_7", "", "", null, new QueryOptions(), sessionIdUser).first().getId();
        s_8 = catalogManager.createSample(studyId, "s_8", "", "", null, new QueryOptions(), sessionIdUser).first().getId();
        s_9 = catalogManager.createSample(studyId, "s_9", "", "", null, new QueryOptions(), sessionIdUser).first().getId();

        catalogManager.annotateSample(s_1, "annot1", vs.getId(), new ObjectMap("NAME", "s_1").append("AGE", 6).append("ALIVE", true)
                .append("PHEN", "CONTROL"), null, true, sessionIdUser);
        catalogManager.annotateSample(s_2, "annot1", vs.getId(), new ObjectMap("NAME", "s_2").append("AGE", 10).append("ALIVE", false)
                .append("PHEN", "CASE"), null, true, sessionIdUser);
        catalogManager.annotateSample(s_3, "annot1", vs.getId(), new ObjectMap("NAME", "s_3").append("AGE", 15).append("ALIVE", true)
                .append("PHEN", "CONTROL"), null, true, sessionIdUser);
        catalogManager.annotateSample(s_4, "annot1", vs.getId(), new ObjectMap("NAME", "s_4").append("AGE", 22).append("ALIVE", false)
                .append("PHEN", "CONTROL"), null, true, sessionIdUser);
        catalogManager.annotateSample(s_5, "annot1", vs.getId(), new ObjectMap("NAME", "s_5").append("AGE", 29).append("ALIVE", true)
                .append("PHEN", "CASE"), null, true, sessionIdUser);
        catalogManager.annotateSample(s_6, "annot2", vs.getId(), new ObjectMap("NAME", "s_6").append("AGE", 38).append("ALIVE", true)
                .append("PHEN", "CONTROL"), null, true, sessionIdUser);
        catalogManager.annotateSample(s_7, "annot2", vs.getId(), new ObjectMap("NAME", "s_7").append("AGE", 46).append("ALIVE", false)
                .append("PHEN", "CASE"), null, true, sessionIdUser);
        catalogManager.annotateSample(s_8, "annot2", vs.getId(), new ObjectMap("NAME", "s_8").append("AGE", 72).append("ALIVE", true)
                .append("PHEN", "CONTROL"), null, true, sessionIdUser);


        catalogManager.modifyFile(test01k.getId(), new ObjectMap("sampleIds", Arrays.asList(s_1, s_2, s_3, s_4, s_5)), sessionIdUser);

    }

    @After
    public void tearDown() throws Exception {
        if (sessionIdUser != null) {
            catalogManager.logout("user", sessionIdUser);
        }
        if (sessionIdUser2 != null) {
            catalogManager.logout("user2", sessionIdUser2);
        }
        if (sessionIdUser3 != null) {
            catalogManager.logout("user3", sessionIdUser3);
        }
//        catalogManager.close();
    }

    public CatalogManager getTestCatalogManager() {
        return catalogManager;
    }

    @Test
    public void testAdminUserExists() throws Exception {
        QueryResult<ObjectMap> login = catalogManager.getUserManager().login("admin", "admin", "localhost");
        assertTrue(login.first().getString("sessionId").length() == 40);
    }

    @Test
    public void testCreateExistingUser() throws Exception {
        thrown.expect(CatalogException.class);
        thrown.expectMessage(containsString("already exists"));
        catalogManager.createUser("user", "User Name", "mail@ebi.ac.uk", PASSWORD, "", null, null);
    }

    @Test
    public void testLoginAsAnonymous() throws Exception {
        System.out.println(catalogManager.loginAsAnonymous("127.0.0.1"));
    }

    @Test
    public void testLogin() throws Exception {
        QueryResult<ObjectMap> queryResult = catalogManager.login("user", PASSWORD, "127.0.0.1");
        System.out.println(queryResult.first().toJson());

        thrown.expect(CatalogException.class);
        thrown.expectMessage(allOf(containsString("Bad"), containsString("password")));
        catalogManager.login("user", "fakePassword", "127.0.0.1");
    }

    @Test
    public void testLogoutAnonymous() throws Exception {
        QueryResult<ObjectMap> queryResult = catalogManager.loginAsAnonymous("127.0.0.1");
        catalogManager.logoutAnonymous(queryResult.first().getString("sessionId"));
    }

    @Test
    public void testGetUserInfo() throws CatalogException {
        QueryResult<User> user = catalogManager.getUser("user", null, sessionIdUser);
        System.out.println("user = " + user);
        QueryResult<User> userVoid = catalogManager.getUser("user", user.first().getLastModified(), sessionIdUser);
        System.out.println("userVoid = " + userVoid);
        assertTrue(userVoid.getResult().isEmpty());
        try {
            catalogManager.getUser("user", null, sessionIdUser2);
            fail();
        } catch (CatalogException e) {
            System.out.println(e);
        }
    }

    @Test
    public void testModifyUser() throws CatalogException, InterruptedException {
        ObjectMap params = new ObjectMap();
        String newName = "Changed Name " + StringUtils.randomString(10);
        String newPassword = StringUtils.randomString(10);
        String newEmail = "new@email.ac.uk";

        params.put("name", newName);
        ObjectMap attributes = new ObjectMap("myBoolean", true);
        attributes.put("value", 6);
        attributes.put("object", new BasicDBObject("id", 1234));
        params.put("attributes", attributes);

        User userPre = catalogManager.getUser("user", null, sessionIdUser).first();
        System.out.println("userPre = " + userPre);
        Thread.sleep(10);

        catalogManager.modifyUser("user", params, sessionIdUser);
        catalogManager.changeEmail("user", newEmail, sessionIdUser);
        catalogManager.changePassword("user", PASSWORD, newPassword);

        List<User> userList = catalogManager.getUser("user", userPre.getLastModified(),
                new QueryOptions(QueryOptions.INCLUDE,
                        Arrays.asList(UserDBAdaptor.QueryParams.PASSWORD.key(), UserDBAdaptor.QueryParams.NAME.key(),
                                UserDBAdaptor.QueryParams.EMAIL.key(), UserDBAdaptor.QueryParams.ATTRIBUTES.key())),
                sessionIdUser).getResult();
        if (userList.isEmpty()) {
            fail("Error. LastModified should have changed");
        }
        User userPost = userList.get(0);
        System.out.println("userPost = " + userPost);
        assertTrue(!userPre.getLastModified().equals(userPost.getLastModified()));
        assertEquals(userPost.getName(), newName);
        assertEquals(userPost.getEmail(), newEmail);
        assertEquals(userPost.getPassword(), CatalogAuthenticationManager.cypherPassword(newPassword));
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            assertEquals(userPost.getAttributes().get(entry.getKey()), entry.getValue());
        }

        catalogManager.changePassword("user", newPassword, PASSWORD);

        try {
            params = new ObjectMap();
            params.put("password", "1234321");
            catalogManager.modifyUser("user", params, sessionIdUser);
            fail("Expected exception");
        } catch (CatalogDBException e) {
            System.out.println(e);
        }

        try {
            catalogManager.modifyUser("user", params, sessionIdUser2);
            fail("Expected exception");
        } catch (CatalogException e) {
            System.out.println(e);
        }

    }

    /**
     * Project methods
     * ***************************
     */


    @Test
    public void testCreateAnonymousProject() throws IOException, CatalogException {
        String sessionId = catalogManager.loginAsAnonymous("127.0.0.1").first().getString("sessionId");

        String userId = catalogManager.getUserIdBySessionId(sessionId);

        catalogManager.createProject("Project", "project", "", "", null, sessionId);

        catalogManager.logoutAnonymous(sessionId);

    }

    @Test
    public void testGetAllProjects() throws Exception {
        QueryResult<Project> projects = catalogManager.getAllProjects("user", null, sessionIdUser);
        assertEquals(1, projects.getNumResults());

        projects = catalogManager.getAllProjects("user", null, sessionIdUser2);
        assertEquals(0, projects.getNumResults());
    }

    @Test
    public void testCreateProject() throws Exception {

        String projectAlias = "projectAlias_ASDFASDF";

        catalogManager.createProject("Project", projectAlias, "", "", null, sessionIdUser);

        thrown.expect(CatalogDBException.class);
        thrown.expectMessage(containsString("already exists"));
        catalogManager.createProject("Project", projectAlias, "", "", null, sessionIdUser);
    }

    @Test
    public void testModifyProject() throws CatalogException {
        String newProjectName = "ProjectName " + StringUtils.randomString(10);
        long projectId = catalogManager.getUser("user", null, sessionIdUser).first().getProjects().get(0).getId();

        ObjectMap options = new ObjectMap();
        options.put("name", newProjectName);
        ObjectMap attributes = new ObjectMap("myBoolean", true);
        attributes.put("value", 6);
        attributes.put("object", new BasicDBObject("id", 1234));
        options.put("attributes", attributes);

        catalogManager.modifyProject(projectId, options, sessionIdUser);
        QueryResult<Project> result = catalogManager.getProject(projectId, null, sessionIdUser);
        Project project = result.first();
        System.out.println(result);

        assertEquals(newProjectName, project.getName());
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            assertEquals(project.getAttributes().get(entry.getKey()), entry.getValue());
        }

        options = new ObjectMap();
        options.put("alias", "newProjectAlias");
        catalogManager.modifyProject(projectId, options, sessionIdUser);

        thrown.expect(CatalogException.class);
        thrown.expectMessage("Permission denied");
        catalogManager.modifyProject(projectId, options, sessionIdUser2);

    }

    /**
     * Study methods
     * ***************************
     */

    @Test
    public void testModifyStudy() throws Exception {
        long projectId = catalogManager.getAllProjects("user", null, sessionIdUser).first().getId();
        long studyId = catalogManager.getAllStudiesInProject(projectId, null, sessionIdUser).first().getId();
        String newName = "Phase 1 " + StringUtils.randomString(20);
        String newDescription = StringUtils.randomString(500);

        ObjectMap parameters = new ObjectMap();
        parameters.put("name", newName);
        parameters.put("description", newDescription);
        BasicDBObject attributes = new BasicDBObject("key", "value");
        parameters.put("attributes", attributes);
        catalogManager.modifyStudy(studyId, parameters, sessionIdUser);

        QueryResult<Study> result = catalogManager.getStudy(studyId, sessionIdUser);
        System.out.println(result);
        Study study = result.first();
        assertEquals(study.getName(), newName);
        assertEquals(study.getDescription(), newDescription);
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            assertEquals(study.getAttributes().get(entry.getKey()), entry.getValue());
        }
    }

    @Test
    public void testGetAllStudies() throws CatalogException {
        long projectId = catalogManager.getAllProjects("user", null, sessionIdUser).first().getId();
        catalogManager.createStudy(projectId, "study_1", "study_1", Study.Type.CASE_CONTROL, "creationDate", "description",
                new Status(), null, null, null, null, null, null, null, sessionIdUser);
        catalogManager.createStudy(projectId, "study_2", "study_2", Study.Type.CASE_CONTROL, "creationDate", "description",
                new Status(), null, null, null, null, null, null, null, sessionIdUser);
        catalogManager.createStudy(projectId, "study_3", "study_3", Study.Type.CASE_CONTROL, "creationDate", "description",
                new Status(), null, null, null, null, null, null, null, sessionIdUser);
        long study_4 = catalogManager.createStudy(projectId, "study_4", "study_4", Study.Type.CASE_CONTROL, "creationDate",
                "description", new Status(), null, null, null, null, null, null, null, sessionIdUser).first().getId();

        assertEquals(new HashSet<>(Collections.emptyList()), catalogManager.getAllStudies(new Query(StudyDBAdaptor.QueryParams
                .GROUP_USER_IDS.key(), "user2"), null, sessionIdUser).getResult().stream().map(Study::getAlias)
                .collect(Collectors.toSet()));

        catalogManager.createGroup(Long.toString(study_4), "admins", "user3", sessionIdUser);
        assertEquals(new HashSet<>(Arrays.asList("study_4")), catalogManager.getAllStudies(new Query(StudyDBAdaptor.QueryParams
                .GROUP_USER_IDS.key(), "user3"), null, sessionIdUser).getResult().stream().map(Study::getAlias)
                .collect(Collectors.toSet()));

        assertEquals(new HashSet<>(Arrays.asList("phase1", "phase3", "study_1", "study_2", "study_3", "study_4")), catalogManager
                .getAllStudies(new Query(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectId), null, sessionIdUser)
                .getResult().stream().map(Study::getAlias).collect(Collectors.toSet()));
        assertEquals(new HashSet<>(Arrays.asList("phase1", "phase3", "study_1", "study_2", "study_3", "study_4")), catalogManager
                .getAllStudies(new Query(), null, sessionIdUser).getResult().stream().map(Study::getAlias).collect(Collectors.toSet()));
        assertEquals(new HashSet<>(Arrays.asList("study_1", "study_2", "study_3", "study_4")), catalogManager.getAllStudies(new
                Query(StudyDBAdaptor.QueryParams.ALIAS.key(), "~^study"), null, sessionIdUser).getResult().stream()
                .map(Study::getAlias).collect(Collectors.toSet()));
        assertEquals(Collections.singleton("s1"), catalogManager.getAllStudies(new Query(), null, sessionIdUser2).getResult().stream()
                .map(Study::getAlias).collect(Collectors.toSet()));
    }

    /**
     * Job methods
     * ***************************
     */

    @Test
    public void testCreateJob() throws CatalogException, IOException {
        long projectId = catalogManager.getAllProjects("user", null, sessionIdUser).first().getId();
        long studyId = catalogManager.getAllStudiesInProject(projectId, null, sessionIdUser).first().getId();

        File outDir = catalogManager.createFolder(studyId, Paths.get("jobs", "myJob"), true, null, sessionIdUser).first();

        URI tmpJobOutDir = catalogManager.createJobOutDir(studyId, StringUtils.randomString(5), sessionIdUser);
        catalogManager.createJob(
                studyId, "myJob", "samtool", "description", "", Collections.emptyMap(), "echo \"Hello World!\"", tmpJobOutDir, outDir
                        .getId(),
                Collections.emptyList(), null, new HashMap<>(), null, new Job.JobStatus(Job.JobStatus.PREPARED), 0, 0, null, sessionIdUser);
//                Collections.emptyList(), null, new HashMap<>(), null, new Job.JobStatus(Job.JobStatus.PREPARED), 0, 0, null, sessionIdUser);

        catalogManager.createJob(
                studyId, "myReadyJob", "samtool", "description", "", Collections.emptyMap(), "echo \"Hello World!\"", tmpJobOutDir,
                outDir.getId(),
                Collections.emptyList(), null, new HashMap<>(), null, new Job.JobStatus(Job.JobStatus.READY), 0, 0, null, sessionIdUser);

        catalogManager.createJob(
                studyId, "myQueuedJob", "samtool", "description", "", Collections.emptyMap(), "echo \"Hello World!\"", tmpJobOutDir,
                outDir.getId(),
                Collections.emptyList(), null, new HashMap<>(), null, new Job.JobStatus(Job.JobStatus.QUEUED), 0, 0, null, sessionIdUser);

        catalogManager.createJob(
                studyId, "myErrorJob", "samtool", "description", "", Collections.emptyMap(), "echo \"Hello World!\"", tmpJobOutDir,
                outDir.getId(),
                Collections.emptyList(), null, new HashMap<>(), null, new Job.JobStatus(Job.JobStatus.ERROR), 0, 0, null, sessionIdUser);

        String sessionId = catalogManager.login("admin", "admin", "localhost").first().get("sessionId").toString();
        QueryResult<Job> unfinishedJobs = catalogManager.getUnfinishedJobs(sessionId);
        assertEquals(2, unfinishedJobs.getNumResults());

        QueryResult<Job> allJobs = catalogManager.getAllJobs(studyId, sessionId);
        assertEquals(4, allJobs.getNumResults());
    }

    @Test
    public void testCreateFailJob() throws CatalogException {
        long projectId = catalogManager.getAllProjects("user", null, sessionIdUser).first().getId();
        long studyId = catalogManager.getAllStudiesInProject(projectId, null, sessionIdUser).first().getId();

        URI tmpJobOutDir = catalogManager.createJobOutDir(studyId, StringUtils.randomString(5), sessionIdUser);
        thrown.expect(CatalogException.class);
        catalogManager.createJob(
                studyId, "myErrorJob", "samtool", "description", "", Collections.emptyMap(), "echo \"Hello World!\"", tmpJobOutDir,
                projectId, //Bad outputId
                Collections.emptyList(), null, new HashMap<>(), null, new Job.JobStatus(Job.JobStatus.ERROR), 0, 0, null, sessionIdUser);
    }

    @Test
    public void testGetAllJobs() throws CatalogException {
        long projectId = catalogManager.getAllProjects("user", null, sessionIdUser).first().getId();
        long studyId = catalogManager.getAllStudiesInProject(projectId, null, sessionIdUser).first().getId();
        File outDir = catalogManager.createFolder(studyId, Paths.get("jobs", "myJob"), true, null, sessionIdUser).first();

        URI tmpJobOutDir = catalogManager.createJobOutDir(studyId, StringUtils.randomString(5), sessionIdUser);
        catalogManager.createJob(
                studyId, "myErrorJob", "samtool", "description", "", Collections.emptyMap(), "echo \"Hello World!\"", tmpJobOutDir,
                outDir.getId(),
                Collections.emptyList(), null, new HashMap<>(), null, new Job.JobStatus(Job.JobStatus.ERROR), 0, 0, null, sessionIdUser);

        QueryResult<Job> allJobs = catalogManager.getAllJobs(studyId, sessionIdUser);

        assertEquals(1, allJobs.getNumTotalResults());
        assertEquals(1, allJobs.getNumResults());
    }

    /**
     * VariableSet methods
     * ***************************
     */

    @Test
    public void testCreateVariableSet() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        Study study = catalogManager.getStudy(studyId, sessionIdUser).first();
        long variableSetNum = study.getVariableSets().size();

        Set<Variable> variables = new HashSet<>();
        variables.addAll(Arrays.asList(
                new Variable("NAME", "", Variable.VariableType.TEXT, "", true, false, Collections.<String>emptyList(), 0, "", "", null,
                        Collections.<String, Object>emptyMap()),
                new Variable("AGE", "", Variable.VariableType.NUMERIC, null, true, false, Collections.singletonList("0:99"), 1, "", "",
                        null, Collections.<String, Object>emptyMap()),
                new Variable("HEIGHT", "", Variable.VariableType.NUMERIC, "1.5", false, false, Collections.singletonList("0:"), 2, "",
                        "", null, Collections.<String, Object>emptyMap()),
                new Variable("ALIVE", "", Variable.VariableType.BOOLEAN, "", true, false, Collections.<String>emptyList(), 3, "", "",
                        null, Collections.<String, Object>emptyMap()),
                new Variable("PHEN", "", Variable.VariableType.CATEGORICAL, "", true, false, Arrays.asList("CASE", "CONTROL"), 4, "", "",
                        null, Collections.<String, Object>emptyMap())
        ));
        QueryResult<VariableSet> queryResult = catalogManager.createVariableSet(study.getId(), "vs1", true, "", null, variables,
                sessionIdUser);

        assertEquals(1, queryResult.getResult().size());

        study = catalogManager.getStudy(study.getId(), sessionIdUser).first();
        assertEquals(variableSetNum + 1, study.getVariableSets().size());
    }

    @Test
    public void testCreateRepeatedVariableSet() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        Study study = catalogManager.getStudy(studyId, sessionIdUser).first();

        List<Variable> variables = Arrays.asList(
                new Variable("NAME", "", Variable.VariableType.TEXT, "", true, false, Collections.<String>emptyList(), 0, "", "", null,
                        Collections.<String, Object>emptyMap()),
                new Variable("NAME", "", Variable.VariableType.BOOLEAN, "", true, false, Collections.<String>emptyList(), 3, "", "",
                        null, Collections.<String, Object>emptyMap()),
                new Variable("AGE", "", Variable.VariableType.NUMERIC, null, true, false, Collections.singletonList("0:99"), 1, "", "",
                        null, Collections.<String, Object>emptyMap()),
                new Variable("HEIGHT", "", Variable.VariableType.NUMERIC, "1.5", false, false, Collections.singletonList("0:"), 2, "",
                        "", null, Collections.<String, Object>emptyMap()),
                new Variable("PHEN", "", Variable.VariableType.CATEGORICAL, "", true, false, Arrays.asList("CASE", "CONTROL"), 4, "", "",
                        null, Collections.<String, Object>emptyMap())
        );
        thrown.expect(CatalogException.class);
        catalogManager.createVariableSet(study.getId(), "vs1", true, "", null, variables, sessionIdUser);
    }

    @Test
    public void testDeleteVariableSet() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");

        List<Variable> variables = Arrays.asList(
                new Variable("NAME", "", Variable.VariableType.TEXT, "", true, false, Collections.<String>emptyList(), 0, "", "", null,
                        Collections.<String, Object>emptyMap()),
                new Variable("AGE", "", Variable.VariableType.NUMERIC, null, true, false, Collections.singletonList("0:99"), 1, "", "",
                        null, Collections.<String, Object>emptyMap())
        );
        VariableSet vs1 = catalogManager.createVariableSet(studyId, "vs1", true, "", null, variables, sessionIdUser).first();

        VariableSet vs1_deleted = catalogManager.deleteVariableSet(vs1.getId(), null, sessionIdUser).first();

        assertEquals(vs1.getId(), vs1_deleted.getId());

        thrown.expect(CatalogDBException.class);    //VariableSet does not exist
        catalogManager.getVariableSet(vs1.getId(), null, sessionIdUser);
    }

    @Test
    public void testGetAllVariableSet() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");

        List<Variable> variables = Arrays.asList(
                new Variable("NAME", "", Variable.VariableType.TEXT, "", true, false, Collections.<String>emptyList(), 0, "", "", null,
                        Collections.<String, Object>emptyMap()),
                new Variable("AGE", "", Variable.VariableType.NUMERIC, null, true, false, Collections.singletonList("0:99"), 1, "", "",
                        null, Collections.<String, Object>emptyMap())
        );
        VariableSet vs1 = catalogManager.createVariableSet(studyId, "vs1", true, "Cancer", null, variables, sessionIdUser).first();
        VariableSet vs2 = catalogManager.createVariableSet(studyId, "vs2", true, "Virgo", null, variables, sessionIdUser).first();
        VariableSet vs3 = catalogManager.createVariableSet(studyId, "vs3", true, "Piscis", null, variables, sessionIdUser).first();
        VariableSet vs4 = catalogManager.createVariableSet(studyId, "vs4", true, "Aries", null, variables, sessionIdUser).first();

        long numResults;
        numResults = catalogManager.getAllVariableSet(studyId, new QueryOptions(StudyDBAdaptor.VariableSetParams.NAME.key()
                , "vs1"), sessionIdUser).getNumResults();
        assertEquals(1, numResults);

        numResults = catalogManager.getAllVariableSet(studyId, new QueryOptions(StudyDBAdaptor.VariableSetParams.NAME.key()
                , "vs1,vs2"), sessionIdUser).getNumResults();
        assertEquals(2, numResults);

        numResults = catalogManager.getAllVariableSet(studyId, new QueryOptions(StudyDBAdaptor.VariableSetParams.NAME.key()
                , "VS1"), sessionIdUser).getNumResults();
        assertEquals(0, numResults);

        numResults = catalogManager.getAllVariableSet(studyId, new QueryOptions(StudyDBAdaptor.VariableSetParams.ID.key()
                , vs1.getId() + "," + vs3.getId()), sessionIdUser).getNumResults();
        assertEquals(2, numResults);

    }

    @Test
    public void testDeleteVariableSetInUse() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        long sampleId1 = catalogManager.createSample(studyId, "SAMPLE_1", "", "", null, new QueryOptions(), sessionIdUser).first().getId();
        List<Variable> variables = Arrays.asList(
                new Variable("NAME", "", Variable.VariableType.TEXT, "", true, false, Collections.<String>emptyList(), 0, "", "", null,
                        Collections.<String, Object>emptyMap()),
                new Variable("AGE", "", Variable.VariableType.NUMERIC, null, false, false, Collections.singletonList("0:99"), 1, "", "",
                        null, Collections.<String, Object>emptyMap())
        );
        VariableSet vs1 = catalogManager.createVariableSet(studyId, "vs1", true, "", null, variables, sessionIdUser).first();
        catalogManager.annotateSample(sampleId1, "annotationId", vs1.getId(), Collections.singletonMap("NAME", "LINUS"), null,
                sessionIdUser);

        try {
            catalogManager.deleteVariableSet(vs1.getId(), null, sessionIdUser).first();
        } finally {
            VariableSet variableSet = catalogManager.getVariableSet(vs1.getId(), null, sessionIdUser).first();
            assertEquals(vs1.getId(), variableSet.getId());

            thrown.expect(CatalogDBException.class); //Expect the exception from the try
        }
    }

    /**
     * Sample methods
     * ***************************
     */

    @Test
    public void testCreateSample() throws CatalogException {
        long projectId = catalogManager.getAllProjects("user", null, sessionIdUser).first().getId();
        long studyId = catalogManager.getAllStudiesInProject(projectId, null, sessionIdUser).first().getId();

        QueryResult<Sample> sampleQueryResult = catalogManager.createSample(studyId, "HG007", "IMDb", "", null, null, sessionIdUser);
        System.out.println("sampleQueryResult = " + sampleQueryResult);
    }

    @Test
    public void testAnnotate() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        Study study = catalogManager.getStudy(studyId, sessionIdUser).first();

        Set<Variable> variables = new HashSet<>();
        variables.add(new Variable("NAME", "", Variable.VariableType.TEXT, "", true, false, Collections.<String>emptyList(), 0, "", "",
                null, Collections.<String, Object>emptyMap()));
        variables.add(new Variable("AGE", "", Variable.VariableType.TEXT, "", false, false, Collections.<String>emptyList(), 0, "", "",
                null, Collections.<String, Object>emptyMap()));
        VariableSet vs1 = catalogManager.createVariableSet(study.getId(), "vs1", false, "", null, variables, sessionIdUser).first();


        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("NAME", "Joe");
        annotations.put("AGE", null);
        QueryResult<AnnotationSet> annotationSetQueryResult = catalogManager.annotateSample(s_1, "annotation1", vs1.getId(), annotations,
                null, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());
        Map<String, Object> map = annotationSetQueryResult.first().getAnnotations().stream().collect(Collectors.toMap(Annotation::getName,
                Annotation::getValue));
        assertEquals(1, map.size());
        assertEquals("Joe", map.get("NAME"));

    }

    @Test
    public void testAnnotateMulti() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        Study study = catalogManager.getStudy(studyId, sessionIdUser).first();
        long sampleId = catalogManager.createSample(study.getId(), "SAMPLE_1", "", "", null, new QueryOptions(), sessionIdUser).first()
                .getId();

        Set<Variable> variables = new HashSet<>();
        variables.add(new Variable("NAME", "", Variable.VariableType.TEXT, "", true, false, Collections.<String>emptyList(), 0, "", "",
                null, Collections.<String, Object>emptyMap()));
        VariableSet vs1 = catalogManager.createVariableSet(study.getId(), "vs1", false, "", null, variables, sessionIdUser).first();


        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("NAME", "Luke");
        QueryResult<AnnotationSet> annotationSetQueryResult = catalogManager.annotateSample(sampleId, "annotation1", vs1.getId(),
                annotations, null, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations = new HashMap<>();
        annotations.put("NAME", "Lucas");
        catalogManager.annotateSample(sampleId, "annotation2", vs1.getId(), annotations, null, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        assertEquals(2, catalogManager.getSample(sampleId, null, sessionIdUser).first().getAnnotationSets().size());
    }

    @Test
    public void testAnnotateUnique() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        Study study = catalogManager.getStudy(studyId, sessionIdUser).first();
        long sampleId = catalogManager.createSample(study.getId(), "SAMPLE_1", "", "", null, new QueryOptions(), sessionIdUser).first()
                .getId();

        Set<Variable> variables = new HashSet<>();
        variables.add(new Variable("NAME", "", Variable.VariableType.TEXT, "", true, false, Collections.<String>emptyList(), 0, "", "",
                null, Collections.<String, Object>emptyMap()));
        VariableSet vs1 = catalogManager.createVariableSet(study.getId(), "vs1", true, "", null, variables, sessionIdUser).first();


        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("NAME", "Luke");
        QueryResult<AnnotationSet> annotationSetQueryResult = catalogManager.annotateSample(sampleId, "annotation1", vs1.getId(),
                annotations, null, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("NAME", "Lucas");
        thrown.expect(CatalogException.class);
        thrown.expectMessage("unique");
        catalogManager.annotateSample(sampleId, "annotation2", vs1.getId(), annotations, null, sessionIdUser);
    }

    @Test
    public void testAnnotateIndividualUnique() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        Study study = catalogManager.getStudy(studyId, sessionIdUser).first();
        long individualId = catalogManager.createIndividual(study.getId(), "INDIVIDUAL_1", "", -1, -1, Individual.Sex.UNKNOWN, new
                QueryOptions(), sessionIdUser).first().getId();

        Set<Variable> variables = new HashSet<>();
        variables.add(new Variable("NAME", "", Variable.VariableType.TEXT, "", true, false, Collections.<String>emptyList(), 0, "", "",
                null, Collections.<String, Object>emptyMap()));
        VariableSet vs1 = catalogManager.createVariableSet(study.getId(), "vs1", true, "", null, variables, sessionIdUser).first();


        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("NAME", "Luke");
        QueryResult<AnnotationSet> annotationSetQueryResult = catalogManager.annotateIndividual(individualId, "annotation1", vs1.getId(),
                annotations, null, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("NAME", "Lucas");
        thrown.expect(CatalogException.class);
        thrown.expectMessage("unique");
        catalogManager.annotateIndividual(individualId, "annotation2", vs1.getId(), annotations, null, sessionIdUser);
    }

    @Test
    public void testAnnotateIncorrectType() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        Study study = catalogManager.getStudy(studyId, sessionIdUser).first();
        long sampleId = catalogManager.createSample(study.getId(), "SAMPLE_1", "", "", null, new QueryOptions(), sessionIdUser).first()
                .getId();

        Set<Variable> variables = new HashSet<>();
        variables.add(new Variable("NUM", "", Variable.VariableType.NUMERIC, "", true, false, null, 0, "", "", null, Collections.<String,
                Object>emptyMap()));
        VariableSet vs1 = catalogManager.createVariableSet(study.getId(), "vs1", false, "", null, variables, sessionIdUser).first();


        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("NUM", "5");
        QueryResult<AnnotationSet> annotationSetQueryResult = catalogManager.annotateSample(sampleId, "annotation1", vs1.getId(),
                annotations, null, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("NUM", "6.8");
        annotationSetQueryResult = catalogManager.annotateSample(sampleId, "annotation2", vs1.getId(), annotations, null, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("NUM", "five polong five");
        thrown.expect(CatalogException.class);
        catalogManager.annotateSample(sampleId, "annotation3", vs1.getId(), annotations, null, sessionIdUser);
    }

    @Test
    public void testAnnotateRange() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        Study study = catalogManager.getStudy(studyId, sessionIdUser).first();
        long sampleId = catalogManager.createSample(study.getId(), "SAMPLE_1", "", "", null, new QueryOptions(), sessionIdUser).first()
                .getId();

        Set<Variable> variables = new HashSet<>();
        variables.add(new Variable("RANGE_NUM", "", Variable.VariableType.NUMERIC, "", true, false, Arrays.asList("1:14", "16:22", "50:")
                , 0, "", "", null, Collections.<String, Object>emptyMap()));
        VariableSet vs1 = catalogManager.createVariableSet(study.getId(), "vs1", false, "", null, variables, sessionIdUser).first();

        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("RANGE_NUM", "1");  // 1:14
        QueryResult<AnnotationSet> annotationSetQueryResult = catalogManager.annotateSample(sampleId, "annotation1", vs1.getId(),
                annotations, null, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("RANGE_NUM", "14"); // 1:14
        annotationSetQueryResult = catalogManager.annotateSample(sampleId, "annotation2", vs1.getId(), annotations, null, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("RANGE_NUM", "20");  // 16:20
        annotationSetQueryResult = catalogManager.annotateSample(sampleId, "annotation3", vs1.getId(), annotations, null, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("RANGE_NUM", "100000"); // 50:
        annotationSetQueryResult = catalogManager.annotateSample(sampleId, "annotation4", vs1.getId(), annotations, null, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("RANGE_NUM", "14.1");
        thrown.expect(CatalogException.class);
        catalogManager.annotateSample(sampleId, "annotation5", vs1.getId(), annotations, null, sessionIdUser);
    }

    @Test
    public void testAnnotateCategorical() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        Study study = catalogManager.getStudy(studyId, sessionIdUser).first();
        long sampleId = catalogManager.createSample(study.getId(), "SAMPLE_1", "", "", null, new QueryOptions(), sessionIdUser).first()
                .getId();

        Set<Variable> variables = new HashSet<>();
        variables.add(new Variable("COOL_NAME", "", Variable.VariableType.CATEGORICAL, "", true, false, Arrays.asList("LUKE", "LEIA",
                "VADER", "YODA"), 0, "", "", null, Collections.<String, Object>emptyMap()));
        VariableSet vs1 = catalogManager.createVariableSet(study.getId(), "vs1", false, "", null, variables, sessionIdUser).first();

        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("COOL_NAME", "LUKE");
        QueryResult<AnnotationSet> annotationSetQueryResult = catalogManager.annotateSample(sampleId, "annotation1", vs1.getId(),
                annotations, null, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("COOL_NAME", "LEIA");
        annotationSetQueryResult = catalogManager.annotateSample(sampleId, "annotation2", vs1.getId(), annotations, null, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("COOL_NAME", "VADER");
        annotationSetQueryResult = catalogManager.annotateSample(sampleId, "annotation3", vs1.getId(), annotations, null, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("COOL_NAME", "YODA");
        annotationSetQueryResult = catalogManager.annotateSample(sampleId, "annotation4", vs1.getId(), annotations, null, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("COOL_NAME", "SPOCK");
        thrown.expect(CatalogException.class);
        catalogManager.annotateSample(sampleId, "annotation5", vs1.getId(), annotations, null, sessionIdUser);
    }

    @Test
    public void testAnnotateNested() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        Study study = catalogManager.getStudy(studyId, sessionIdUser).first();
        long sampleId1 = catalogManager.createSample(study.getId(), "SAMPLE_1", "", "", null, new QueryOptions(), sessionIdUser).first()
                .getId();
        long sampleId2 = catalogManager.createSample(study.getId(), "SAMPLE_2", "", "", null, new QueryOptions(), sessionIdUser).first()
                .getId();
        long sampleId3 = catalogManager.createSample(study.getId(), "SAMPLE_3", "", "", null, new QueryOptions(), sessionIdUser).first()
                .getId();
        long sampleId4 = catalogManager.createSample(study.getId(), "SAMPLE_4", "", "", null, new QueryOptions(), sessionIdUser).first()
                .getId();
        long sampleId5 = catalogManager.createSample(study.getId(), "SAMPLE_5", "", "", null, new QueryOptions(), sessionIdUser).first()
                .getId();

        VariableSet vs1 = catalogManager.createVariableSet(study.getId(), "vs1", false, "", null, Collections.singleton
                (CatalogAnnotationsValidatorTest.nestedObject), sessionIdUser).first();

        QueryResult<AnnotationSet> annotationSetQueryResult;
        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("nestedObject", new QueryOptions("stringList", Arrays.asList("li", "lu")).append("object", new ObjectMap
                ("string", "my value").append("numberList", Arrays.asList(2, 3, 4))));
        annotationSetQueryResult = catalogManager.annotateSample(sampleId1, "annotation1", vs1.getId(), annotations, null, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

        annotations.put("nestedObject", new QueryOptions("stringList", Arrays.asList("lo", "lu")).append("object", new ObjectMap
                ("string", "stringValue").append("numberList", Arrays.asList(3, 4, 5))));
        annotationSetQueryResult = catalogManager.annotateSample(sampleId2, "annotation1", vs1.getId(), annotations, null, sessionIdUser);
        assertEquals(1, annotationSetQueryResult.getNumResults());

//        annotations.put("nestedObject", new QueryOptions("stringList", Arrays.asList("li", "lo", "lu")).append("object", new ObjectMap
// ("string", "my value").append("numberList", Arrays.asList(2, 3, 4))));
//        annotationSetQueryResult = catalogManager.annotateSample(sampleId3, "annotation1", vs1.getId(), annotations, null, sessionIdUser);
//        assertEquals(1, annotationSetQueryResult.getNumResults());
//
//        annotations.put("nestedObject", new QueryOptions("stringList", Arrays.asList("li", "lo", "lu")).append("object", new ObjectMap
// ("string", "my value").append("numberList", Arrays.asList(2, 3, 4))));
//        annotationSetQueryResult = catalogManager.annotateSample(sampleId4, "annotation1", vs1.getId(), annotations, null, sessionIdUser);
//        assertEquals(1, annotationSetQueryResult.getNumResults());

        List<Sample> samples;
        Query query = new Query(SampleDBAdaptor.QueryParams.VARIABLE_SET_ID.key(), vs1.getId());
        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.stringList", "li");
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(1, samples.size());

        //query = new Query(CatalogSampleDBAdaptor.QueryParams.VARIABLE_SET_ID.key(), vs1.getId());
        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.stringList", "lo");
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(1, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.stringList", "LL");
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(0, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.stringList", "lo,li,LL");
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(2, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.object.string", "my value");
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(1, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.stringList", "lo,lu,LL");
        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.object.string", "my value");
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(1, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.stringList" , "lo,lu,LL");
        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.object.numberList", "7");
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(0, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.stringList", "lo,lu,LL");
        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.object.numberList", "3");
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(1, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.stringList", "lo,lu,LL");
        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.object.numberList" , "5");
        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.object.string", "stringValue");
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(1, samples.size());

        query.remove(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.object.string");
        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.stringList", "lo,lu,LL");
        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.object.numberList", "2,5");
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(2, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.stringList", "lo,lu,LL");
        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".nestedObject.object.numberList", "0");
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(0, samples.size());


        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ".unexisting", "lo,lu,LL");
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("not found in variableSet");
        catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
    }

    @Test
    public void testQuerySampleAnnotationFail1() throws CatalogException {
        Query query = new Query();
        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ":nestedObject.stringList", "lo,lu,LL");

        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("annotation:nestedObject does not exist");
        catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
    }

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
    public void testQuerySamples() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        Study study = catalogManager.getStudy(studyId, sessionIdUser).first();

        VariableSet variableSet = study.getVariableSets().get(0);

        List<Sample> samples;
        Query query = new Query();

        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(9, samples.size());

        query = new Query(VARIABLE_SET_ID.key(), variableSet.getId());
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(8, samples.size());

        query = new Query(ANNOTATION_SET_NAME.key(), "annot2");
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(3, samples.size());

        query = new Query(ANNOTATION_SET_NAME.key(), "noExist");
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(0, samples.size());

        query = new Query("annotation.NAME", "s_1,s_2,s_3");
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(3, samples.size());

        query = new Query("annotation.AGE", ">30");
        query.append(VARIABLE_SET_ID.key(), variableSet.getId());
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(3, samples.size());

        query = new Query("annotation.AGE", ">30");
        query.append(VARIABLE_SET_ID.key(), variableSet.getId());
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(3, samples.size());

        query = new Query("annotation.AGE", ">30").append("annotation.ALIVE", "true");
        query.append(VARIABLE_SET_ID.key(), variableSet.getId());
        samples = catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
        assertEquals(2, samples.size());
    }

    @Test
    public void testUpdateAnnotation() throws CatalogException {

        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        Study study = catalogManager.getStudy(studyId, sessionIdUser).first();
        Individual individual = catalogManager.createIndividual(study.getId(), "INDIVIDUAL_1", "", -1, -1, Individual.Sex.UNKNOWN, new
                QueryOptions(), sessionIdUser).first();
        Sample sample = catalogManager.getSample(s_1, null, sessionIdUser).first();

        AnnotationSet annotationSet = sample.getAnnotationSets().get(0);
        catalogManager.annotateIndividual(individual.getId(), annotationSet.getName(), annotationSet.getVariableSetId(),
                annotationSet.getAnnotations().stream().collect(Collectors.toMap(Annotation::getName, Annotation::getValue)),
                Collections.emptyMap(), sessionIdUser);

        // First update
        ObjectMap updateAnnotation = new ObjectMap("NAME", "SAMPLE1")
                .append("AGE", 38)
                .append("HEIGHT", null)
                .append("EXTRA", "extra");
        catalogManager.updateSampleAnnotation(s_1, annotationSet.getName(), updateAnnotation, sessionIdUser);
        catalogManager.updateIndividualAnnotation(individual.getId(), annotationSet.getName(), updateAnnotation, sessionIdUser);

        Consumer<AnnotationSet> check = as -> {
            Map<String, Object> annotations = as.getAnnotations().stream()
                    .collect(Collectors.toMap(Annotation::getName, Annotation::getValue));

            assertEquals(6, annotations.size());
            assertEquals("SAMPLE1", annotations.get("NAME"));
            assertEquals(38.0, annotations.get("AGE"));
            assertEquals(1.5, annotations.get("HEIGHT"));   //Default value
            assertEquals("extra", annotations.get("EXTRA"));
        };

        sample = catalogManager.getSample(s_1, null, sessionIdUser).first();
        individual = catalogManager.getIndividual(individual.getId(), null, sessionIdUser).first();
        check.accept(sample.getAnnotationSets().get(0));
        check.accept(individual.getAnnotationSets().get(0));

        updateAnnotation = new ObjectMap("NAME", "SAMPLE 1")
                .append("EXTRA", null);
        catalogManager.updateSampleAnnotation(s_1, annotationSet.getName(), updateAnnotation, sessionIdUser);
        catalogManager.updateIndividualAnnotation(individual.getId(), annotationSet.getName(), updateAnnotation, sessionIdUser);

        check = as -> {
            Map<String, Object> annotations = as.getAnnotations().stream()
                    .collect(Collectors.toMap(Annotation::getName, Annotation::getValue));

            assertEquals(5, annotations.size());
            assertEquals("SAMPLE 1", annotations.get("NAME"));
            assertEquals(false, annotations.containsKey("EXTRA"));
        };

        sample = catalogManager.getSample(s_1, null, sessionIdUser).first();
        individual = catalogManager.getIndividual(individual.getId(), null, sessionIdUser).first();
        check.accept(sample.getAnnotationSets().get(0));
        check.accept(individual.getAnnotationSets().get(0));
    }

    @Test
    public void testUpdateAnnotationFail() throws CatalogException {

        Sample sample = catalogManager.getSample(s_1, null, sessionIdUser).first();
        AnnotationSet annotationSet = sample.getAnnotationSets().get(0);

        thrown.expect(CatalogException.class); //Can not delete required fields
        catalogManager.updateSampleAnnotation(s_1, annotationSet.getName(),
                new ObjectMap("NAME", null), sessionIdUser);

    }

    @Test
    public void getVariableSetSummary() throws CatalogException {

        long studyId = catalogManager.getStudyId("user@1000G:phase1", sessionIdUser);
        Study study = catalogManager.getStudy(studyId, sessionIdUser).first();

        long variableSetId = study.getVariableSets().get(0).getId();

        QueryResult<VariableSetSummary> variableSetSummary = catalogManager.getStudyManager()
                .getVariableSetSummary(variableSetId, sessionIdUser);

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
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        long sampleId1 = catalogManager.createSample(studyId, "SAMPLE_1", "", "", null, new QueryOptions(), sessionIdUser).first().getId();
        long individualId = catalogManager.createIndividual(studyId, "Individual1", "", 0, 0, Individual.Sex.MALE, new QueryOptions(),
                sessionIdUser).first().getId();

        Sample sample = catalogManager.modifySample(sampleId1, new QueryOptions("individualId", individualId), sessionIdUser).first();

        assertEquals(individualId, sample.getIndividualId());
    }

    @Test
    public void testModifySampleBadIndividual() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        long sampleId1 = catalogManager.createSample(studyId, "SAMPLE_1", "", "", null, new QueryOptions(), sessionIdUser).first().getId();

        thrown.expect(CatalogDBException.class);
        catalogManager.modifySample(sampleId1, new QueryOptions("individualId", 4), sessionIdUser);
    }

    @Test
    public void testModifySampleUnknownIndividual() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        long sampleId1 = catalogManager.createSample(studyId, "SAMPLE_1", "", "", null, new QueryOptions(), sessionIdUser).first().getId();

        // It will not modify anything as the individualId is already -1
        thrown.expect(CatalogDBException.class);
        catalogManager.modifySample(sampleId1, new QueryOptions("individualId", -1), sessionIdUser).first();

        Sample sample = catalogManager.modifySample(sampleId1, new QueryOptions("individualId", -2), sessionIdUser).first();
        assertEquals(-2, sample.getIndividualId());
    }

    @Test
    public void testDeleteSample() throws CatalogException, IOException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        long sampleId = catalogManager.createSample(studyId, "SAMPLE_1", "", "", null, new QueryOptions(), sessionIdUser).first().getId();

        List<QueryResult<Sample>> queryResult = catalogManager.getSampleManager()
                .delete(Long.toString(sampleId), new QueryOptions(), sessionIdUser);
        assertEquals(sampleId, queryResult.get(0).first().getId());

        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.ID.key(), sampleId)
                .append(SampleDBAdaptor.QueryParams.STATUS_NAME.key(), Status.DELETED);

        QueryResult<Sample> sampleQueryResult = catalogManager.getSampleManager().get(studyId, query, new QueryOptions(), sessionIdUser);
//        QueryResult<Sample> sample = catalogManager.getSample(sampleId, new QueryOptions(), sessionIdUser);
        assertEquals(1, sampleQueryResult.getNumResults());
        assertTrue(sampleQueryResult.first().getName().contains(".DELETED"));
    }

    /*
     * Cohort methods
     *
     */


    @Test
    public void testCreateCohort() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");

        long sampleId1 = catalogManager.createSample(studyId, "SAMPLE_1", "", "", null, new QueryOptions(), sessionIdUser).first().getId();
        long sampleId2 = catalogManager.createSample(studyId, "SAMPLE_2", "", "", null, new QueryOptions(), sessionIdUser).first().getId();
        long sampleId3 = catalogManager.createSample(studyId, "SAMPLE_3", "", "", null, new QueryOptions(), sessionIdUser).first().getId();
        Cohort myCohort = catalogManager.createCohort(studyId, "MyCohort", Study.Type.FAMILY, "", Arrays.asList(sampleId1, sampleId2,
                sampleId3), null, sessionIdUser).first();

        assertEquals("MyCohort", myCohort.getName());
        assertEquals(3, myCohort.getSamples().size());
        assertTrue(myCohort.getSamples().contains(sampleId1));
        assertTrue(myCohort.getSamples().contains(sampleId2));
        assertTrue(myCohort.getSamples().contains(sampleId3));
    }

    @Test
    public void testGetAllCohorts() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");

        long sampleId1 = catalogManager.createSample(studyId, "SAMPLE_1", "", "", null, new QueryOptions(), sessionIdUser).first().getId();
        long sampleId2 = catalogManager.createSample(studyId, "SAMPLE_2", "", "", null, new QueryOptions(), sessionIdUser).first().getId();
        long sampleId3 = catalogManager.createSample(studyId, "SAMPLE_3", "", "", null, new QueryOptions(), sessionIdUser).first().getId();
        long sampleId4 = catalogManager.createSample(studyId, "SAMPLE_4", "", "", null, new QueryOptions(), sessionIdUser).first().getId();
        long sampleId5 = catalogManager.createSample(studyId, "SAMPLE_5", "", "", null, new QueryOptions(), sessionIdUser).first().getId();
        Cohort myCohort1 = catalogManager.createCohort(studyId, "MyCohort1", Study.Type.FAMILY, "", Arrays.asList(sampleId1, sampleId2,
                sampleId3), null, sessionIdUser).first();
        Cohort myCohort2 = catalogManager.createCohort(studyId, "MyCohort2", Study.Type.FAMILY, "", Arrays.asList(sampleId1, sampleId2,
                sampleId3, sampleId4), null, sessionIdUser).first();
        Cohort myCohort3 = catalogManager.createCohort(studyId, "MyCohort3", Study.Type.CASE_CONTROL, "", Arrays.asList(sampleId3,
                sampleId4), null, sessionIdUser).first();
        catalogManager.createCohort(studyId, "MyCohort4", Study.Type.TRIO, "", Arrays.asList(sampleId5, sampleId3),
                null, sessionIdUser).first();

        long numResults;
        numResults = catalogManager.getAllCohorts(studyId, new Query(CohortDBAdaptor.QueryParams.SAMPLES.key(), sampleId1),
                new QueryOptions(), sessionIdUser).getNumResults();
        assertEquals(2, numResults);

        numResults = catalogManager.getAllCohorts(studyId, new Query(CohortDBAdaptor.QueryParams.SAMPLES.key(),
                sampleId1 + "," + sampleId5), new QueryOptions(), sessionIdUser).getNumResults();
        assertEquals(3, numResults);

//        numResults = catalogManager.getAllCohorts(studyId, new QueryOptions(CatalogSampleDBAdaptor.CohortFilterOption.samples.toString
// (), sampleId3 + "," + sampleId4), sessionIdUser).getNumResults();
//        assertEquals(2, numResults);

        numResults = catalogManager.getAllCohorts(studyId, new Query(CohortDBAdaptor.QueryParams.NAME.key(),
                "MyCohort2"), new QueryOptions(), sessionIdUser).getNumResults();
        assertEquals(1, numResults);

        numResults = catalogManager.getAllCohorts(studyId, new Query(CohortDBAdaptor.QueryParams.NAME.key(),
                "~MyCohort."), new QueryOptions(), sessionIdUser).getNumResults();
        assertEquals(4, numResults);

        numResults = catalogManager.getAllCohorts(studyId, new Query(CohortDBAdaptor.QueryParams.TYPE.key(),
                Study.Type.FAMILY), new QueryOptions(), sessionIdUser).getNumResults();
        assertEquals(2, numResults);

        numResults = catalogManager.getAllCohorts(studyId, new Query(CohortDBAdaptor.QueryParams.TYPE.key(),
                "CASE_CONTROL"), new QueryOptions(), sessionIdUser).getNumResults();
        assertEquals(1, numResults);

        numResults = catalogManager.getAllCohorts(studyId, new Query(CohortDBAdaptor.QueryParams.ID.key(),
                myCohort1.getId() + "," + myCohort2.getId() + "," + myCohort3.getId()), new QueryOptions(), sessionIdUser).getNumResults();
        assertEquals(3, numResults);
    }

    @Test
    public void testCreateCohortFail() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        thrown.expect(CatalogException.class);
        catalogManager.createCohort(studyId, "MyCohort", Study.Type.FAMILY, "", Arrays.asList(23L, 4L, 5L), null, sessionIdUser);
    }

    @Test
    public void testCreateCohortAlreadyExisting() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");

        long sampleId1 = catalogManager.createSample(studyId, "SAMPLE_1", "", "", null, new QueryOptions(), sessionIdUser).first().getId();
        catalogManager.createCohort(studyId, "MyCohort", Study.Type.FAMILY, "", Arrays.asList(sampleId1), null, sessionIdUser).first();


        thrown.expect(CatalogDBException.class);
        catalogManager.createCohort(studyId, "MyCohort", Study.Type.FAMILY, "", Arrays.asList(sampleId1), null, sessionIdUser).first();
    }

    @Test
    public void testUpdateCohort() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");

        long sampleId1 = catalogManager.createSample(studyId, "SAMPLE_1", "", "", null, new QueryOptions(), sessionIdUser).first().getId();
        long sampleId2 = catalogManager.createSample(studyId, "SAMPLE_2", "", "", null, new QueryOptions(), sessionIdUser).first().getId();
        long sampleId3 = catalogManager.createSample(studyId, "SAMPLE_3", "", "", null, new QueryOptions(), sessionIdUser).first().getId();
        long sampleId4 = catalogManager.createSample(studyId, "SAMPLE_4", "", "", null, new QueryOptions(), sessionIdUser).first().getId();
        long sampleId5 = catalogManager.createSample(studyId, "SAMPLE_5", "", "", null, new QueryOptions(), sessionIdUser).first().getId();

        Cohort myCohort = catalogManager.createCohort(studyId, "MyCohort", Study.Type.FAMILY, "", Arrays.asList(sampleId1, sampleId2,
                sampleId3), null, sessionIdUser).first();


        assertEquals("MyCohort", myCohort.getName());
        assertEquals(3, myCohort.getSamples().size());
        assertTrue(myCohort.getSamples().contains(sampleId1));
        assertTrue(myCohort.getSamples().contains(sampleId2));
        assertTrue(myCohort.getSamples().contains(sampleId3));

        Cohort myModifiedCohort = catalogManager.modifyCohort(myCohort.getId(), new ObjectMap("samples", Arrays.asList(sampleId1,
                sampleId3, sampleId4, sampleId5)).append("name", "myModifiedCohort"), new QueryOptions(), sessionIdUser).first();

        assertEquals("myModifiedCohort", myModifiedCohort.getName());
        assertEquals(4, myModifiedCohort.getSamples().size());
        assertTrue(myModifiedCohort.getSamples().contains(sampleId1));
        assertTrue(myModifiedCohort.getSamples().contains(sampleId3));
        assertTrue(myModifiedCohort.getSamples().contains(sampleId4));
        assertTrue(myModifiedCohort.getSamples().contains(sampleId5));
    }

    /*                    */
    /* Test util methods  */
    /*                    */

    @Test
    public void testDeleteCohort() throws CatalogException, IOException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");

        long sampleId1 = catalogManager.createSample(studyId, "SAMPLE_1", "", "", null, new QueryOptions(), sessionIdUser).first().getId();
        long sampleId2 = catalogManager.createSample(studyId, "SAMPLE_2", "", "", null, new QueryOptions(), sessionIdUser).first().getId();
        long sampleId3 = catalogManager.createSample(studyId, "SAMPLE_3", "", "", null, new QueryOptions(), sessionIdUser).first().getId();

        Cohort myCohort = catalogManager.createCohort(studyId, "MyCohort", Study.Type.FAMILY, "", Arrays.asList(sampleId1, sampleId2,
                sampleId3), null, sessionIdUser).first();


        assertEquals("MyCohort", myCohort.getName());
        assertEquals(3, myCohort.getSamples().size());
        assertTrue(myCohort.getSamples().contains(sampleId1));
        assertTrue(myCohort.getSamples().contains(sampleId2));
        assertTrue(myCohort.getSamples().contains(sampleId3));

        Cohort myDeletedCohort = catalogManager.getCohortManager().delete(Long.toString(myCohort.getId()), null, sessionIdUser).get(0)
                .first();

        assertEquals(myCohort.getId(), myDeletedCohort.getId());

        Cohort cohort = catalogManager.getCohort(myCohort.getId(), null, sessionIdUser).first();
        assertEquals(Status.TRASHED, cohort.getStatus().getName());
    }

    /**
     * Individual methods
     * ***************************
     */

    @Test
    public void testAnnotateIndividual() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        Study study = catalogManager.getStudy(studyId, sessionIdUser).first();
        VariableSet variableSet = study.getVariableSets().get(0);

        long individualId1 = catalogManager.createIndividual(studyId, "INDIVIDUAL_1", "", -1, -1, null, new QueryOptions(), sessionIdUser)
                .first().getId();
        long individualId2 = catalogManager.createIndividual(studyId, "INDIVIDUAL_2", "", -1, -1, null, new QueryOptions(), sessionIdUser)
                .first().getId();
        long individualId3 = catalogManager.createIndividual(studyId, "INDIVIDUAL_3", "", -1, -1, null, new QueryOptions(), sessionIdUser)
                .first().getId();

        catalogManager.annotateIndividual(individualId1, "annot1", variableSet.getId(), new ObjectMap("NAME", "INDIVIDUAL_1").append
                ("AGE", 5).append("PHEN", "CASE").append("ALIVE", true), null, sessionIdUser);
        catalogManager.annotateIndividual(individualId2, "annot1", variableSet.getId(), new ObjectMap("NAME", "INDIVIDUAL_2").append
                ("AGE", 15).append("PHEN", "CONTROL").append("ALIVE", true), null, sessionIdUser);
        catalogManager.annotateIndividual(individualId3, "annot1", variableSet.getId(), new ObjectMap("NAME", "INDIVIDUAL_3").append
                ("AGE", 25).append("PHEN", "CASE").append("ALIVE", true), null, sessionIdUser);

        List<String> individuals;
        individuals = catalogManager.getAllIndividuals(studyId, new Query(IndividualDBAdaptor.QueryParams.VARIABLE_SET_ID.key(),
                        variableSet.getId())
                        .append(IndividualDBAdaptor.QueryParams.ANNOTATION.key() + ".NAME", "~^INDIVIDUAL_"),
                null, sessionIdUser).getResult().stream().map(Individual::getName).collect(Collectors.toList());
        assertTrue(individuals.containsAll(Arrays.asList("INDIVIDUAL_1", "INDIVIDUAL_2", "INDIVIDUAL_3")));

        individuals = catalogManager.getAllIndividuals(studyId, new Query(IndividualDBAdaptor.QueryParams.VARIABLE_SET_ID.key(),
                        variableSet.getId())
                        .append(IndividualDBAdaptor.QueryParams.ANNOTATION.key() + ".AGE", ">10"),
                null, sessionIdUser).getResult().stream().map(Individual::getName).collect(Collectors.toList());
        assertTrue(individuals.containsAll(Arrays.asList("INDIVIDUAL_2", "INDIVIDUAL_3")));

        individuals = catalogManager.getAllIndividuals(studyId, new Query(IndividualDBAdaptor.QueryParams.VARIABLE_SET_ID.key(),
                        variableSet.getId())
                        .append(IndividualDBAdaptor.QueryParams.ANNOTATION.key() + ".AGE", ">10")
                        .append(IndividualDBAdaptor.QueryParams.ANNOTATION.key() + ".PHEN", "CASE"),
                null, sessionIdUser).getResult().stream().map(Individual::getName).collect(Collectors.toList());
        assertTrue(individuals.containsAll(Arrays.asList("INDIVIDUAL_3")));

    }
}