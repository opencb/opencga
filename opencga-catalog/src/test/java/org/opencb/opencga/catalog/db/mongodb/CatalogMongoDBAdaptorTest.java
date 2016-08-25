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

package org.opencb.opencga.catalog.db.mongodb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.opencb.commons.test.GenericTest;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.core.config.DataStoreServerAddress;
import org.opencb.datastore.mongodb.MongoDBConfiguration;
import org.opencb.datastore.mongodb.MongoDataStore;
import org.opencb.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.db.api.CatalogStudyDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogUserDBAdaptor;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.core.common.StringUtils;
import org.opencb.opencga.core.common.TimeUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class CatalogMongoDBAdaptorTest extends GenericTest {

    static CatalogMongoDBAdaptor catalogDBAdaptor;

//    @Rule
//    public Timeout globalTimeout = new Timeout(2000); // 200 ms max per method tested

    @Rule
    public ExpectedException thrown = ExpectedException.none();
    static User user1;
    static User user2;
    static User user3;
    CatalogUserDBAdaptor catalogUserDBAdaptor;
    private CatalogStudyDBAdaptor catalogStudyDBAdaptor;

    /**
     * This method is executed one single time beforeClass all the tests. It connects to the MongoDB server.
     *
     * @throws IOException
     * @throws org.opencb.opencga.catalog.exceptions.CatalogDBException
     */
    @Before
    public void before() throws IOException, CatalogDBException {
        InputStream is = CatalogMongoDBAdaptorTest.class.getClassLoader().getResourceAsStream("catalog.properties");
        Properties properties = new Properties();
        properties.load(is);

        DataStoreServerAddress dataStoreServerAddress = new DataStoreServerAddress(
                properties.getProperty(CatalogManager.CATALOG_DB_HOSTS).split(",")[0], 27017);

        MongoDBConfiguration mongoDBConfiguration = MongoDBConfiguration.builder()
                .add("username", properties.getProperty(CatalogManager.CATALOG_DB_USER, ""))
                .add("password", properties.getProperty(CatalogManager.CATALOG_DB_PASSWORD, ""))
                .add("authenticationDatabase", properties.getProperty(CatalogManager.CATALOG_DB_AUTHENTICATION_DB, ""))
                .build();

        String database = properties.getProperty(CatalogManager.CATALOG_DB_DATABASE);
        /**
         * Database is cleared before each execution
         */
//        clearDB(dataStoreServerAddress, mongoCredentials);
        MongoDataStoreManager mongoManager = new MongoDataStoreManager(dataStoreServerAddress.getHost(), dataStoreServerAddress.getPort());
        MongoDataStore db = mongoManager.get(database);
        db.getDb().dropDatabase();

        catalogDBAdaptor = new CatalogMongoDBAdaptor(Collections.singletonList(dataStoreServerAddress), mongoDBConfiguration, database);
        catalogUserDBAdaptor = catalogDBAdaptor.getCatalogUserDBAdaptor();
        catalogStudyDBAdaptor = catalogDBAdaptor.getCatalogStudyDBAdaptor();
        initDefaultCatalogDB();
    }


    @AfterClass
    public static void afterClass() {
        catalogDBAdaptor.close();
    }

//    @Before
    public void initDefaultCatalogDB() throws CatalogDBException {

        assertTrue(!catalogDBAdaptor.isCatalogDBReady());
        catalogDBAdaptor.initializeCatalogDB();

        /**
         * Let's init the database with some basic data to perform each of the tests
         */
        user1 = new User("jcoll", "Jacobo Coll", "jcoll@ebi", "1234", "", User.Role.USER, "", "", 100, 1000, Arrays.<Project>asList(new Project("project", "P1", "", "", ""), new Project("project", "P2", "", "", ""), new Project("project", "P3", "", "", "")),
                Collections.<Tool>emptyList(), Collections.<Session>emptyList(), Collections.<String, Object>emptyMap(),Collections.<String, Object>emptyMap());
        QueryResult createUser = catalogUserDBAdaptor.insertUser(user1, null);
        assertNotNull(createUser.getResult());

        user2 = new User("jmmut", "Jose Miguel", "jmmut@ebi", "1111", "ACME", User.Role.USER, "off");
        createUser = catalogUserDBAdaptor.insertUser(user2, null);
        assertNotNull(createUser.getResult());

        user3 = new User("imedina", "Nacho", "nacho@gmail", "2222", "SPAIN", User.Role.USER, "active", "", 1222, 122222,
                Arrays.asList(new Project(-1, "90 GigaGenomes", "90G", "today", "very long description", "Spain", "", "", 0, Collections.<AclEntry>emptyList(),
                                Arrays.asList(new Study(-1, "Study name", "ph1", Study.Type.CONTROL_SET, "", "", "", "", "", 0, "", null, Collections.<Experiment>emptyList(),
                                                Arrays.asList(
                                                        new File("data/", File.Type.FOLDER, File.Format.PLAIN, File.Bioformat.NONE, "data/", null, null, "", File.Status.READY, 1000),
                                                        new File("file.vcf", File.Type.FILE, File.Format.PLAIN, File.Bioformat.NONE, "data/file.vcf", null, null, "", File.Status.READY, 1000)
                                                ), Collections.<Job>emptyList(), new LinkedList<Sample>(), new LinkedList<Dataset>(), new LinkedList<Cohort>(), new LinkedList<VariableSet>(), null, null, Collections.<String, Object>emptyMap(), Collections.<String, Object>emptyMap()
                                        )
                                ), Collections.<String, Object>emptyMap())
                ),
                Collections.<Tool>emptyList(), Collections.<Session>emptyList(),
                Collections.<String, Object>emptyMap(), Collections.<String, Object>emptyMap());
        createUser = catalogUserDBAdaptor.insertUser(user3, null);
        assertNotNull(createUser.getResult());

        QueryOptions options = new QueryOptions("includeStudies", true);
        options.put("includeFiles", true);
        options.put("includeJobs", true);
        options.put("includeSamples", true);
        user1 = catalogUserDBAdaptor.getUser(CatalogMongoDBAdaptorTest.user1.getId(), options, null).first();
        user2 = catalogUserDBAdaptor.getUser(CatalogMongoDBAdaptorTest.user2.getId(), options, null).first();
        user3 = catalogUserDBAdaptor.getUser(CatalogMongoDBAdaptorTest.user3.getId(), options, null).first();


    }

    @Test
    public void initializeInitializedDB() throws CatalogDBException {
        assertTrue(catalogDBAdaptor.isCatalogDBReady());

        thrown.expect(CatalogDBException.class);
        catalogDBAdaptor.initializeCatalogDB();
    }

    /** **************************
     * User methods
     * ***************************
     */
    @Test
    public void createUserTest() throws CatalogDBException {
        User user = new User("NewUser", "", "", "", "", User.Role.USER, "");
        QueryResult createUser = catalogUserDBAdaptor.insertUser(user, null);
        assertNotSame(0, createUser.getResult().size());

        thrown.expect(CatalogDBException.class);
        catalogUserDBAdaptor.insertUser(user, null);
    }

    @Test
    public void deleteUserTest() throws CatalogDBException {
        User deletable1 = new User("deletable1", "deletable 1", "d1@ebi", "1234", "", User.Role.USER, "");
        QueryResult createUser = catalogUserDBAdaptor.insertUser(deletable1, null);
        assertFalse(createUser.getResult().isEmpty());
        assertNotNull(createUser.first());

        QueryResult deleteUser = catalogUserDBAdaptor.deleteUser(deletable1.getId());
        assertFalse(deleteUser.getResult().isEmpty());
        assertNotNull(deleteUser.first());

        thrown.expect(CatalogDBException.class);
        catalogUserDBAdaptor.deleteUser(deletable1.getId());
    }

    @Test
    public void getUserTest() throws CatalogDBException {
        QueryResult<User> user = catalogUserDBAdaptor.getUser(user1.getId(), null, null);
        assertNotSame(0, user.getResult().size());

        user = catalogUserDBAdaptor.getUser(user3.getId(), null, null);
        assertFalse(user.getResult().isEmpty());
        assertFalse(user.first().getProjects().isEmpty());

        user = catalogUserDBAdaptor.getUser(user3.getId(), new QueryOptions("exclude", Arrays.asList("projects")), null);
        assertNull(user.first().getProjects());

        user = catalogUserDBAdaptor.getUser(user3.getId(), null, user.first().getLastActivity());
        assertTrue(user.getResult().isEmpty());

        thrown.expect(CatalogDBException.class);
        catalogUserDBAdaptor.getUser("NonExistingUser", null, null);
    }

    @Test
    public void loginTest() throws CatalogDBException, IOException {
        String userId = user1.getId();
        Session sessionJCOLL = new Session("127.0.0.1");
        QueryResult<ObjectMap> login = catalogUserDBAdaptor.login(userId, "1234", sessionJCOLL);
        assertEquals(userId, login.first().getString("userId"));

        thrown.expect(CatalogDBException.class);
        catalogUserDBAdaptor.login(userId, "INVALID_PASSWORD", sessionJCOLL);
    }

    @Test
    public void loginTest2() throws CatalogDBException, IOException {
        String userId = user1.getId();
        Session sessionJCOLL = new Session("127.0.0.1");
        QueryResult<ObjectMap> login = catalogUserDBAdaptor.login(userId, "1234", sessionJCOLL);
        assertEquals(userId, login.first().getString("userId"));

        thrown.expect(CatalogDBException.class); //Already logged
        catalogUserDBAdaptor.login(userId, "1234", sessionJCOLL);
    }

    @Test
    public void logoutTest() throws CatalogDBException, IOException {
        String userId = user1.getId();
        Session sessionJCOLL = new Session("127.0.0.1");
        QueryResult<ObjectMap> login = catalogUserDBAdaptor.login(userId, "1234", sessionJCOLL);
        assertEquals(userId, login.first().getString("userId"));

        QueryResult logout = catalogUserDBAdaptor.logout(userId, sessionJCOLL.getId());
        assertEquals(0, logout.getResult().size());

        //thrown.expect(CatalogDBException.class);
        QueryResult falseSession = catalogUserDBAdaptor.logout(userId, "FalseSession");
        assertTrue(falseSession.getWarningMsg() != null && !falseSession.getWarningMsg().isEmpty());
    }

    @Test
    public void getUserIdBySessionId() throws CatalogDBException {
        String userId = user1.getId();

        catalogUserDBAdaptor.login(userId, "1234", new Session("127.0.0.1")); //Having multiple conections
        catalogUserDBAdaptor.login(userId, "1234", new Session("127.0.0.1"));
        catalogUserDBAdaptor.login(userId, "1234", new Session("127.0.0.1"));

        Session sessionJCOLL = new Session("127.0.0.1");
        QueryResult<ObjectMap> login = catalogUserDBAdaptor.login(userId, "1234", sessionJCOLL);
        assertEquals(userId, login.first().getString("userId"));

        assertEquals(user1.getId(), catalogUserDBAdaptor.getUserIdBySessionId(sessionJCOLL.getId()));
        QueryResult logout = catalogUserDBAdaptor.logout(userId, sessionJCOLL.getId());
        assertEquals(0, logout.getResult().size());

        assertEquals("", catalogUserDBAdaptor.getUserIdBySessionId(sessionJCOLL.getId()));
    }

    @Test
    public void changePasswordTest() throws CatalogDBException {
//        System.out.println(catalogUserDBAdaptor.changePassword("jmmut", "1111", "1234"));
//        System.out.println(catalogUserDBAdaptor.changePassword("jmmut", "1234", "1111"));
//        try {
//            System.out.println(catalogUserDBAdaptor.changePassword("jmmut", "BAD_PASSWORD", "asdf"));
//            fail("Expected \"bad password\" exception");
//        } catch (CatalogDBException e) {
//            System.out.println(e);
//        }
        QueryResult queryResult = catalogUserDBAdaptor.changePassword(user2.getId(), user2.getPassword(), "1234");
        assertNotSame(0, queryResult.getResult().size());

        thrown.expect(CatalogDBException.class);
        catalogUserDBAdaptor.changePassword(user2.getId(), "BAD_PASSWORD", "asdf");
    }

    @Test
    public void modifyUserTest() throws CatalogDBException {

        ObjectMap genomeMapsConfig = new ObjectMap("lastPosition" , "4:1222222:1333333");
        genomeMapsConfig.put("otherConf", Arrays.asList(1,2,3,4,5));
        ObjectMap configs = new ObjectMap("genomemaps" , genomeMapsConfig);
        ObjectMap objectMap = new ObjectMap("configs", configs.toJson());
        catalogUserDBAdaptor.modifyUser(user1.getId(), objectMap);

        User user = catalogUserDBAdaptor.getUser(user1.getId(), null, null).first();
        System.out.println(user);
    }

    /**
     * Project methods
     * ***************************
     */
    @Test
    public void createProjectTest() throws CatalogDBException, JsonProcessingException {
        Project p = new Project("Project about some genomes", "1000G", "Today", "Cool", "", "", 1000, "");
        LinkedList<AclEntry> acl = new LinkedList<>();
        acl.push(new AclEntry(user1.getId(), true, false, true, true));
        acl.push(new AclEntry(user2.getId(), false, true, true, true));
        p.setAcl(acl);
        System.out.println(catalogUserDBAdaptor.createProject(user1.getId(), p, null));
        p = new Project("Project about some more genomes", "2000G", "Tomorrow", "Cool", "", "", 3000, "");
        System.out.println(catalogUserDBAdaptor.createProject(user1.getId(), p, null));
        p = new Project("Project management project", "pmp", "yesterday", "it is a system", "", "", 2000, "");
        System.out.println(catalogUserDBAdaptor.createProject(user2.getId(), p, null));
        System.out.println(catalogUserDBAdaptor.createProject(user1.getId(), p, null));

        try {
            System.out.println(catalogUserDBAdaptor.createProject(user1.getId(), p, null));
            fail("Expected \"projectAlias already exists\" exception");
        } catch (CatalogDBException e) {
            System.out.println(e);
        }
    }

    @Test
    public void getProjectIdTest() throws CatalogDBException {
        assertTrue(catalogUserDBAdaptor.getProjectId(user3.getId(), user3.getProjects().get(0).getAlias()) != -1);
        assertTrue(catalogUserDBAdaptor.getProjectId(user3.getId(), "nonExistingProject") == -1);
    }


    @Test
    public void getProjectTest() throws CatalogDBException {
        int projectId = catalogUserDBAdaptor.getProjectId(user3.getId(), user3.getProjects().get(0).getAlias());
        System.out.println("projectId = " + projectId);
        QueryResult<Project> project = catalogUserDBAdaptor.getProject(projectId, null);
        System.out.println(project);
        assertNotNull(project.first());

        thrown.expect(CatalogDBException.class);    //"Expected \"bad id\" exception"
        catalogUserDBAdaptor.getProject(-100, null);
    }

    @Test
    public void deleteProjectTest() throws CatalogDBException {
        Project p = new Project("Project about some more genomes", "2000G", "Tomorrow", "Cool", "", "", 3000, "");
        QueryResult<Project> result = catalogUserDBAdaptor.createProject(user1.getId(), p, null);
        System.out.println(result);
        p = result.first();
        QueryResult<Integer> queryResult = catalogUserDBAdaptor.deleteProject(p.getId());

        System.out.println(queryResult);
        assertTrue(queryResult.first() == 1);
        thrown.expect(CatalogDBException.class);    //Expected "Project not found" exception
        catalogUserDBAdaptor.deleteProject(-1);
    }

    @Test
    public void getAllProjects() throws CatalogDBException {
        QueryResult<Project> allProjects = catalogUserDBAdaptor.getAllProjects(user3.getId(), null);
        System.out.println(allProjects);
        assertTrue(!allProjects.getResult().isEmpty());
    }

    /**
     * cases:
     * ok: correct projectId, correct newName
     * error: non-existent projectId
     * error: newName already used
     * error: newName == oldName
     *
     * @throws CatalogDBException
     */
    @Test
    public void renameProjectTest() throws CatalogDBException {
        Project p1 = catalogUserDBAdaptor.createProject(user1.getId(), new Project("project1", "p1", "Tomorrow", "Cool", "", "", 3000, ""), null).first();
        Project p2 = catalogUserDBAdaptor.createProject(user1.getId(), new Project("project2", "p2", "Tomorrow", "Cool", "", "", 3000, ""), null).first();
        System.out.println(catalogUserDBAdaptor.renameProjectAlias(p1.getId(), "newpmp"));

        try {
            System.out.println(catalogUserDBAdaptor.renameProjectAlias(-1, "falseProject"));
            fail("renamed project with projectId=-1");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }
        try {
            System.out.println(catalogUserDBAdaptor.renameProjectAlias(p1.getId(), p2.getAlias()));
            fail("renamed project with name collision");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }

//        try {
//            System.out.println(catalogUserDBAdaptor.renameProjectAlias(p1.getId(), p1.getAlias()));
//            fail("renamed project to its old name");
//        } catch (CatalogDBException e) {
//            System.out.println("correct exception: " + e);
//        }
    }

    @Test
    public void projectAclTest() throws CatalogDBException {
        int projectId = user3.getProjects().get(0).getId();
        List<AclEntry> acls = catalogUserDBAdaptor.getProjectAcl(projectId, user3.getId()).getResult();
        assertTrue(acls.isEmpty());
        acls = catalogUserDBAdaptor.getProjectAcl(projectId, user2.getId()).getResult();
        assertTrue(acls.isEmpty());
        acls = catalogUserDBAdaptor.getProjectAcl(projectId, "noUser").getResult();
        assertTrue(acls.isEmpty());


        AclEntry granted = new AclEntry("jmmut", true, true, true, false);
        System.out.println(catalogUserDBAdaptor.setProjectAcl(projectId, granted));  // overwrites
        AclEntry jmmut = catalogUserDBAdaptor.getProjectAcl(projectId, "jmmut").first();
        System.out.println(jmmut);
        assertTrue(jmmut.equals(granted));

        granted.setUserId("imedina");
        System.out.println(catalogUserDBAdaptor.setProjectAcl(projectId, granted));  // just pushes
        AclEntry imedina = catalogUserDBAdaptor.getProjectAcl(projectId, "imedina").first();
        System.out.println(imedina);
        assertTrue(imedina.equals(granted));
        try {
            granted.setUserId("noUser");
            catalogUserDBAdaptor.setProjectAcl(projectId, granted);
            fail("error: expected exception");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }
    }


    /**
     * Study methods
     * ***************************
     */
    @Test
    public void createStudyTest() throws CatalogDBException {
        int projectId = catalogUserDBAdaptor.getProjectId(user1.getId(), "P1");
        int projectId2 = catalogUserDBAdaptor.getProjectId(user1.getId(), "P2");

        Study s = new Study("Phase 1", "ph1", Study.Type.CASE_CONTROL, "", "", null);
        System.out.println(catalogStudyDBAdaptor.createStudy(projectId, s, null));
        System.out.println(catalogStudyDBAdaptor.createStudy(projectId2, s, null));
        s = new Study("Phase 3", "ph3", Study.Type.CASE_CONTROL, "", "", null);
        System.out.println(catalogStudyDBAdaptor.createStudy(projectId, s, null));
        s = new Study("Phase 7", "ph7", Study.Type.CASE_CONTROL, "", "", null);
        System.out.println(catalogStudyDBAdaptor.createStudy(projectId, s, null));

        try {
            System.out.println(catalogStudyDBAdaptor.createStudy(projectId, s, null));  //Repeated study
            fail("Expected \"Study alias already exist\" exception");
        } catch (CatalogDBException e) {
            System.out.println(e);
        }
        try {
            System.out.println(catalogStudyDBAdaptor.createStudy(-100, s, null));  //ProjectId not exists
            fail("Expected \"bad project id\" exception");
        } catch (CatalogDBException e) {
            System.out.println(e);
        }
    }

    @Test
    public void deleteStudyTest() throws CatalogDBException {
        int projectId = catalogUserDBAdaptor.getProjectId("jcoll", "P1");
        Study study = catalogStudyDBAdaptor.createStudy(projectId, new Study("Phase 1", "ph1", Study.Type.CASE_CONTROL, "", "", null), null).first();
        QueryResult<Integer> queryResult = catalogStudyDBAdaptor.deleteStudy(study.getId());
        System.out.println(queryResult);
        assertTrue(queryResult.first() == 1);

        assertTrue(catalogStudyDBAdaptor.getStudyId(projectId, study.getAlias()) == -1);
        try {
            catalogStudyDBAdaptor.getStudy(study.getId(), null);
            fail("error: Expected \"Study not found\" exception");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }

        try {
            QueryResult<Integer> queryResult1 = catalogStudyDBAdaptor.deleteStudy(-1);
            fail("error: Expected \"Study not found\" exception");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }
    }

    @Test
    public void getAllStudiesTest() throws CatalogDBException {
        int projectId = user3.getProjects().get(0).getId();
        QueryResult<Study> allStudies = catalogStudyDBAdaptor.getAllStudiesInProject(projectId, null);
        assertTrue(allStudies.getNumResults() != 0);
        System.out.println(allStudies);
        try {
            System.out.println(catalogStudyDBAdaptor.getAllStudiesInProject(-100, null));
            fail("Expected \"bad project id\" exception");
        } catch (CatalogDBException e) {
            System.out.println(e);
        }
    }

    @Test
    public void getStudyTest() throws CatalogDBException, JsonProcessingException {
        int projectId = user3.getProjects().get(0).getId();
        int studyId = catalogStudyDBAdaptor.getStudyId(projectId, "ph1");

        Study study = catalogStudyDBAdaptor.getStudy(studyId, null).first();
        assertNotNull(study);
        assertEquals(studyId, study.getId());
        assertTrue(study.getDiskUsage() != 0);

        study = catalogStudyDBAdaptor.getStudy(studyId, new QueryOptions("include", "projects.studies.diskUsage").append("exclude", null)).first();
        assertTrue(study.getDiskUsage() != 0);

        study = catalogStudyDBAdaptor.getStudy(studyId, new QueryOptions("include", "projects.studies.diskUsage")).first();
        assertTrue(study.getDiskUsage() != 0);

        study = catalogStudyDBAdaptor.getStudy(studyId, new QueryOptions("exclude", "projects.studies.name")).first();
        assertTrue(study.getDiskUsage() != 0);
        assertNull(study.getName());

        study = catalogStudyDBAdaptor.getStudy(studyId, new QueryOptions("exclude", "projects.studies.name").append("include", null)).first();
        assertTrue(study.getDiskUsage() != 0);
        assertNull(study.getName());

        study = catalogStudyDBAdaptor.getStudy(studyId, new QueryOptions("exclude", "projects.studies.diskUsage")).first();
        assertTrue(study.getDiskUsage() == 0);

        study = catalogStudyDBAdaptor.getStudy(studyId, new QueryOptions("exclude", "projects.studies.diskUsage").append("include", null)).first();
        assertTrue(study.getDiskUsage() == 0);

        study = catalogStudyDBAdaptor.getStudy(studyId, new QueryOptions("include", "projects.studies.id")).first();
        assertEquals(studyId, study.getId());
        assertTrue(study.getDiskUsage() == 0);

        study = catalogStudyDBAdaptor.getStudy(studyId, new QueryOptions("include", "projects.studies.id").append("exclude", null)).first();
        assertEquals(studyId, study.getId());
        assertTrue(study.getDiskUsage() == 0);


        thrown.expect(CatalogDBException.class);
        catalogStudyDBAdaptor.getStudy(-100, null);
    }

    @Test
    public void modifyStudyTest() throws CatalogDBException {
        int projectId = user3.getProjects().get(0).getId();
        int studyId = catalogStudyDBAdaptor.getStudyId(projectId, "ph1");

        String newName = "My new name";
        String unexpectedNewAlias = "myNewAlias";
        ObjectMap objectMap = new ObjectMap("name", newName);
        HashMap<String, Object> newAttributes = new HashMap<>();
        newAttributes.put("Value", 1);
        newAttributes.put("Value2", true);
        newAttributes.put("Value3", new ObjectMap("key", "ok"));

        objectMap.put("attributes", new ObjectMap(newAttributes).toJson());
        objectMap.put("alias", unexpectedNewAlias);

        QueryResult<Study> queryResult = catalogStudyDBAdaptor.modifyStudy(studyId, objectMap);
        Study modifiedStudy = queryResult.first();

        Study study = catalogStudyDBAdaptor.getStudy(studyId, null).first();
        assertEquals(study.toString(), modifiedStudy.toString());

        assertTrue(modifiedStudy.getAttributes().containsKey("Value"));
        assertTrue(modifiedStudy.getAttributes().containsKey("Value2"));
        assertTrue(modifiedStudy.getAttributes().containsKey("Value3"));
        for (Map.Entry<String, Object> entry : newAttributes.entrySet()) {
            assertEquals(study.getAttributes().get(entry.getKey()), entry.getValue());
        }

        assertEquals(newName, study.getName());

        assertFalse("ModifyStudy must NO modify the alias ", unexpectedNewAlias.equals(study.getAlias()));
    }

    /**
     * Files methods
     * ***************************
     */
    @Test
    public void createFileToStudyTest() throws CatalogDBException, IOException {
        int studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        assertTrue(studyId >= 0);
        File file;
        file = new File("jobs/", File.Type.FOLDER, File.Format.PLAIN, File.Bioformat.NONE, "jobs/", null, TimeUtils.getTime(), "", File.Status.STAGE, 1000);
        LinkedList<AclEntry> acl = new LinkedList<>();
        acl.push(new AclEntry("jcoll", true, true, true, true));
        acl.push(new AclEntry("jmmut", false, false, true, true));
        file.setAcl(acl);
        System.out.println(catalogDBAdaptor.createFile(studyId, file, null));
        file = new File("file.sam", File.Type.FILE, File.Format.PLAIN, File.Bioformat.ALIGNMENT, "data/file.sam", null, TimeUtils.getTime(), "", File.Status.STAGE, 1000);
        System.out.println(catalogDBAdaptor.createFile(studyId, file, null));
        file = new File("file.bam", File.Type.FILE, File.Format.BINARY, File.Bioformat.ALIGNMENT, "data/file.bam", null, TimeUtils.getTime(), "", File.Status.STAGE, 1000);
        System.out.println(catalogDBAdaptor.createFile(studyId, file, null));
        file = new File("file.vcf", File.Type.FILE, File.Format.PLAIN, File.Bioformat.VARIANT, "data/file2.vcf", null, TimeUtils.getTime(), "", File.Status.STAGE, 1000);

        try {
            System.out.println(catalogDBAdaptor.createFile(-20, file, null));
            fail("Expected \"StudyId not found\" exception");
        } catch (CatalogDBException e) {
            System.out.println(e);
        }

        System.out.println(catalogDBAdaptor.createFile(studyId, file, null));

        try {
            System.out.println(catalogDBAdaptor.createFile(studyId, file, null));
            fail("Expected \"File already exist\" exception");
        } catch (CatalogDBException e) {
            System.out.println(e);
        }
    }



    @Test
    public void getFileTest() throws CatalogDBException {
        File file = user3.getProjects().get(0).getStudies().get(0).getFiles().get(0);
        QueryResult<File> fileQueryResult = catalogDBAdaptor.getFile(file.getId(), null);
        System.out.println(fileQueryResult);
        try {
            System.out.println(catalogDBAdaptor.getFile(-1, null));
            fail("Expected \"FileId not found\" exception");
        } catch (CatalogDBException e) {
            System.out.println(e);
        }
    }

    @Test
    public void getAllFilesTest() throws CatalogDBException {
        int studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        QueryResult<File> allFiles = catalogDBAdaptor.getAllFilesInStudy(studyId, null);
        List<File> files = allFiles.getResult();
        System.out.println(files);
        assertTrue(!files.isEmpty());

        studyId = catalogStudyDBAdaptor.getStudyId(catalogUserDBAdaptor.getProjectId("jcoll", "1000G"), "ph7");
        allFiles = catalogDBAdaptor.getAllFilesInStudy(studyId, null);
        assertTrue(allFiles.getResult().isEmpty());
    }

//    @Test
//    public void setFileStatus() throws CatalogDBException, IOException {
//        int fileId = catalogDBAdaptor.getFileId("jcoll", "1000G", "ph1", "data/file.vcf");
//        System.out.println(catalogDBAdaptor.setFileStatus(fileId, File.Status.STAGE));
//        assertEquals(catalogDBAdaptor.getFile(fileId).first().getStatus(), File.Status.STAGE);
//        System.out.println(catalogDBAdaptor.setFileStatus(fileId, File.Status.UPLOADED));
//        assertEquals(catalogDBAdaptor.getFile(fileId).first().getStatus(), File.Status.UPLOADED);
//        System.out.println(catalogDBAdaptor.setFileStatus(fileId, File.Status.READY));
//        assertEquals(catalogDBAdaptor.getFile(fileId).first().getStatus(), File.Status.READY);
//        try {
//            System.out.println(catalogDBAdaptor.setFileStatus("jcoll", "1000G", "ph1", "data/noExists", File.READY));
//            fail("Expected \"FileId not found\" exception");
//        } catch (CatalogDBException e) {
//            System.out.println(e);
//        }
//    }

    @Test
    public void modifyFileTest() throws CatalogDBException, IOException {
        File file = user3.getProjects().get(0).getStudies().get(0).getFiles().get(0);
        int fileId = file.getId();

        DBObject stats = BasicDBObjectBuilder.start().append("stat1", 1).append("stat2", true).append("stat3", "ok" + StringUtils.randomString(20)).get();

        ObjectMap parameters = new ObjectMap();
        parameters.put("status", File.Status.READY);
        parameters.put("stats", stats);
        System.out.println(catalogDBAdaptor.modifyFile(fileId, parameters));

        file = catalogDBAdaptor.getFile(fileId, null).first();
        assertEquals(file.getStatus(), File.Status.READY);
        assertEquals(file.getStats(), stats);

    }

    @Test
    public void renameFileTest() throws CatalogDBException {
        String newName = "newFile.bam";
        String parentPath = "data/";
        int fileId = catalogDBAdaptor.getFileId(user3.getProjects().get(0).getStudies().get(0).getId(), "data/file.vcf");
        System.out.println(catalogDBAdaptor.renameFile(fileId, parentPath + newName, null));

        File file = catalogDBAdaptor.getFile(fileId, null).first();
        assertEquals(file.getName(), newName);
        assertEquals(file.getPath(), parentPath + newName);

        try {
            catalogDBAdaptor.renameFile(-1, "noFile", null);
            fail("error: expected \"file not found\"exception");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }

        int folderId = catalogDBAdaptor.getFileId(user3.getProjects().get(0).getStudies().get(0).getId(), "data/");
        String folderName = "folderName";
        catalogDBAdaptor.renameFile(folderId, folderName, null);
        assertTrue(catalogDBAdaptor.getFile(fileId, null).first().getPath().equals(folderName + "/" + newName));

    }

    @Test
    public void deleteFileTest() throws CatalogDBException, IOException {
        int fileId = catalogDBAdaptor.getFileId(user3.getProjects().get(0).getStudies().get(0).getId(), "data/file.vcf");
        QueryResult<Integer> delete = catalogDBAdaptor.deleteFile(fileId);
        System.out.println(delete);
        assertTrue(delete.first() == 1);
        try {
            System.out.println(catalogDBAdaptor.deleteFile(catalogDBAdaptor.getFileId(catalogStudyDBAdaptor.getStudyId(catalogUserDBAdaptor.getProjectId("jcoll", "1000G"), "ph1"), "data/noExists")));
            fail("error: Expected \"FileId not found\" exception");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }
    }

    @Test
    public void fileAclsTest() throws CatalogDBException {
        int fileId = catalogDBAdaptor.getFileId(user3.getProjects().get(0).getStudies().get(0).getId(), "data/file.vcf");
        System.out.println(fileId);

        AclEntry granted = new AclEntry("jmmut", true, true, true, false);
        catalogDBAdaptor.setFileAcl(fileId, granted);
        granted.setUserId("imedina");
        catalogDBAdaptor.setFileAcl(fileId, granted);
        try {
            granted.setUserId("noUser");
            catalogDBAdaptor.setFileAcl(fileId, granted);
            fail("error: expected exception");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }

        List<AclEntry> jmmut = catalogDBAdaptor.getFileAcl(fileId, "jmmut").getResult();
        assertTrue(!jmmut.isEmpty());
        System.out.println(jmmut.get(0));
        List<AclEntry> jcoll = catalogDBAdaptor.getFileAcl(fileId, "jcoll").getResult();
        assertTrue(jcoll.isEmpty());
    }

    @Test
    public void createSampleTest() throws Exception {
        int studyId = user3.getProjects().get(0).getStudies().get(0).getId();

        Sample hg0097 = new Sample(0, "HG0097", "1000g", 0, "A description");
        QueryResult<Sample> result = catalogDBAdaptor.getCatalogSampleDBAdaptor().createSample(studyId, hg0097, null);

        assertEquals(hg0097.getName(), result.first().getName());
        assertEquals(hg0097.getDescription(), result.first().getDescription());
        assertTrue(result.first().getId() > 0);
    }

    @Test
    public void deleteSampleTest() throws Exception {
        int studyId = user3.getProjects().get(0).getStudies().get(0).getId();

        Sample hg0097 = new Sample(0, "HG0097", "1000g", 0, "A description");
        QueryResult<Sample> createResult = catalogDBAdaptor.getCatalogSampleDBAdaptor().createSample(studyId, hg0097, null);
        QueryResult<Sample> deleteResult = catalogDBAdaptor.getCatalogSampleDBAdaptor().deleteSample(createResult.first().getId());
        assertEquals(createResult.first().getId(), deleteResult.first().getId());
        assertEquals(1, deleteResult.getNumResults());

        thrown.expect(CatalogDBException.class);
        catalogDBAdaptor.getCatalogSampleDBAdaptor().getSample(deleteResult.first().getId(), null);
    }

    @Test
    public void deleteSampleFail1Test() throws Exception {
        thrown.expect(CatalogDBException.class);
        QueryResult<Sample> deleteResult = catalogDBAdaptor.getCatalogSampleDBAdaptor().deleteSample(55555555);
    }

    @Test
    public void deleteSampleFail2Test() throws Exception {
        int studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        int fileId = catalogDBAdaptor.getFileId(user3.getProjects().get(0).getStudies().get(0).getId(), "data/file.vcf");

        Sample hg0097 = new Sample(0, "HG0097", "1000g", 0, "A description");
        QueryResult<Sample> createResult = catalogDBAdaptor.getCatalogSampleDBAdaptor().createSample(studyId, hg0097, null);
        catalogDBAdaptor.getCatalogFileDBAdaptor().modifyFile(fileId, new ObjectMap("sampleIds", createResult.first().getId()));

        thrown.expect(CatalogDBException.class);
        QueryResult<Sample> deleteResult = catalogDBAdaptor.getCatalogSampleDBAdaptor().deleteSample(createResult.first().getId());
    }

    @Test
    public void deleteSampleFail3Test() throws Exception {
        int studyId = user3.getProjects().get(0).getStudies().get(0).getId();

        Sample hg0097 = new Sample(0, "HG0097", "1000g", 0, "A description");
        QueryResult<Sample> createResult = catalogDBAdaptor.getCatalogSampleDBAdaptor().createSample(studyId, hg0097, null);
        catalogDBAdaptor.getCatalogSampleDBAdaptor().createCohort(studyId, new Cohort("Cohort", Cohort.Type.COLLECTION, "", "", Collections.singletonList(createResult.first().getId()), null));

        thrown.expect(CatalogDBException.class);
        QueryResult<Sample> deleteResult = catalogDBAdaptor.getCatalogSampleDBAdaptor().deleteSample(createResult.first().getId());
    }

    @Test
    public void createMultipleCohorts() throws Exception {
        int studyId = user3.getProjects().get(0).getStudies().get(0).getId();

        AtomicInteger numFailures = new AtomicInteger();
        Function<Integer, String> getCohortName = c -> "Cohort_" + c;
        int numThreads = 10;
        int numCohorts = 10;
        for (int c = 0; c < numCohorts; c++) {
            List<Thread> threads = new LinkedList<>();
            String cohortName = getCohortName.apply(c);
            for (int i = 0; i < numThreads; i++) {
                threads.add(new Thread(() -> {
                    try {
                        catalogDBAdaptor.getCatalogSampleDBAdaptor().createCohort(studyId, new Cohort(cohortName, Cohort.Type.COLLECTION, "", "", Collections.emptyList(), null));
                    } catch (CatalogDBException ignore) {
                        numFailures.incrementAndGet();
                    }
                }));
            }
            threads.parallelStream().forEach(Thread::run);
            threads.parallelStream().forEach((thread) -> {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }


        assertEquals(numCohorts * numThreads - numCohorts, numFailures.intValue());
        Study study = catalogDBAdaptor.getCatalogStudyDBAdaptor().getStudy(studyId, null).first();
        assertEquals(numCohorts, study.getCohorts().size());
        Set<String> names = study.getCohorts().stream().map(Cohort::getName).collect(Collectors.toSet());
        for (int c = 0; c < numCohorts; c++) {
            String cohortName = getCohortName.apply(c);
            names.contains(cohortName);
        }

    }

    /**
     * Job methods
     * ***************************
     */
    @Test
    public void createJobTest() throws CatalogDBException {
        Job job = new Job();
        job.setVisits(0);

        int studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        job.setName("jobName1");
        System.out.println(catalogDBAdaptor.createJob(studyId, job, null));
//        int analysisId = catalogDBAdaptor.getAnalysisId(studyId, "analysis1Alias");

        job.setName("jobName2");
        System.out.println(catalogDBAdaptor.createJob(studyId, job, null));
        try {
            catalogDBAdaptor.createJob(-1, job, null);
            fail("error: expected exception");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }
    }

    @Test
    public void deleteJobTest() throws CatalogDBException {
        int studyId = user3.getProjects().get(0).getStudies().get(0).getId();

        Job job = catalogDBAdaptor.createJob(studyId, new Job("name", user3.getId(), "", "", "", 4, null, Collections.<Integer>emptyList()), null).first();
        int jobId = job.getId();
        QueryResult<Job> queryResult = catalogDBAdaptor.deleteJob(jobId);
        System.out.println(queryResult);
        assertTrue(queryResult.getNumResults() == 1);
        try {
            System.out.println(catalogDBAdaptor.deleteJob(-1));
            fail("error: Expected \"Job not found\" exception");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }
    }

    @Test
    public void getAllJobTest() throws CatalogDBException {
                 int studyId = user3.getProjects().get(0).getStudies().get(0).getId();
//        int analysisId = catalogDBAdaptor.getAnalysisId(studyId, "analysis1Alias");
        QueryResult<Job> allJobs = catalogDBAdaptor.getAllJobsInStudy(studyId, null);
        System.out.println(allJobs);
    }


    @Test
    public void getJobTest() throws CatalogDBException {
        int studyId = user3.getProjects().get(0).getStudies().get(0).getId();

        Job job = catalogDBAdaptor.createJob(studyId, new Job("name", user3.getId(), "", "", "", 4, null, Collections.<Integer>emptyList()), null).first();
        int jobId = job.getId();

        job = catalogDBAdaptor.getJob(jobId, null).first();
        System.out.println(job);

        try {
            catalogDBAdaptor.getJob(-1, null);
            fail("error: expected exception");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }

    }

    @Test
    public void incJobVisits() throws CatalogDBException {
        int studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        Job jobBefore = catalogDBAdaptor.createJob(studyId, new Job("name", user3.getId(), "", "", "", 4, null, Collections.<Integer>emptyList()), null).first();
        int jobId = jobBefore.getId();

        Integer visits = (Integer) catalogDBAdaptor.incJobVisits(jobBefore.getId()).first().get("visits");

        Job jobAfter = catalogDBAdaptor.getJob(jobId, null).first();
        assertTrue(jobBefore.getVisits() == jobAfter.getVisits() - 1);
        assertTrue(visits == jobAfter.getVisits());
    }



    /////////// Other tests
    @Test
    public void replaceDots() {

        DBObject original = new BasicDBObject("o.o", 4).append("4.4", Arrays.asList(1, 3, 4, new BasicDBObject("933.44", "df.sdf"))).append("key", new BasicDBObject("key....k", "value...2.2.2"));
        DBObject o = new BasicDBObject("o.o", 4).append("4.4", Arrays.asList(1, 3, 4, new BasicDBObject("933.44", "df.sdf"))).append("key", new BasicDBObject("key....k", "value...2.2.2"));
        System.out.println(o);

        CatalogMongoDBUtils.replaceDotsInKeys(o);
        System.out.println(o);

        CatalogMongoDBUtils.restoreDotsInKeys(o);
        System.out.println(o);

        Assert.assertEquals(original, o);

    }

}
