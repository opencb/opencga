package org.opencb.opencga.catalog;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.BasicDBObject;
import org.junit.*;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import org.junit.runners.MethodSorters;
import org.opencb.commons.test.GenericTest;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.mongodb.MongoDataStore;
import org.opencb.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.catalog.beans.*;
import org.opencb.opencga.catalog.beans.File;
import org.opencb.opencga.catalog.db.CatalogManagerException;
import org.opencb.opencga.catalog.io.CatalogIOManagerException;
import org.opencb.opencga.lib.common.StringUtils;
import org.opencb.opencga.lib.common.TimeUtils;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@FixMethodOrder(MethodSorters.JVM)
public class CatalogManagerTest extends GenericTest {

    public static final String PASSWORD = "asdf";
    static CatalogManager catalogManager;
    private String sessionIdUser;
    private String sessionIdUser2;
    private String sessionIdUser3;

    @BeforeClass
    public static void init() throws IOException, CatalogIOManagerException, CatalogManagerException {
        InputStream is = CatalogManagerTest.class.getClassLoader().getResourceAsStream("catalog.properties");
        Properties properties = new Properties();
        properties.load(is);

        clearCatalog(properties);

        catalogManager = new CatalogManager(properties);
    }

    @Before
    public void setUp() throws IOException, CatalogIOManagerException {
        List<ObjectMap> result;
        try {
            result = catalogManager.login("user", PASSWORD, "127.0.0.1").getResult();
            sessionIdUser = result.get(0).getString("sessionId");
        } catch (CatalogManagerException | IOException ignore) {
        }
        try {
            result = catalogManager.login("user2", PASSWORD, "127.0.0.1").getResult();
            sessionIdUser2 = result.get(0).getString("sessionId");
        } catch (CatalogManagerException | IOException ignore) {
        }

        try {
            result = catalogManager.login("user3", PASSWORD, "127.0.0.1").getResult();
            sessionIdUser3 = result.get(0).getString("sessionId");
        } catch (CatalogManagerException | IOException ignore) {
        }
    }

    @After
    public void tearDown() throws Exception {
        if(sessionIdUser != null) {
            catalogManager.logout("user", sessionIdUser);
        }
        if(sessionIdUser2 != null) {
            catalogManager.logout("user2", sessionIdUser2);
        }
        if(sessionIdUser3 != null) {
            catalogManager.logout("user3", sessionIdUser3);
        }
    }


    @Test
    public void testCreateUser() throws Exception {
        User user = new User("user", "User Name", "mail@ebi.ac.uk", PASSWORD, "", "", "");
        System.out.println(catalogManager.createUser(user));
        user = new User("user2", "User2 Name", "mail2@ebi.ac.uk", PASSWORD, "", "", "");
        System.out.println(catalogManager.createUser(user));
        System.out.println(catalogManager.createUser("user3", "User3 Name", "email3", PASSWORD, "ACME"));
    }

    @Test
    public void testLoginAsAnonymous() throws Exception {
        System.out.println(catalogManager.loginAsAnonymous("127.0.0.1"));
    }

    @Test
    public void testLogin() throws Exception {
        QueryResult<ObjectMap> queryResult = catalogManager.login("user", PASSWORD, "127.0.0.1");
        System.out.println(queryResult.getResult().get(0).toJson());
        try{
            catalogManager.login("user", "fakePassword", "127.0.0.1");
            fail("Expected 'wrong password' exception");
        } catch (CatalogManagerException e ){
            System.out.println(e.getMessage());
        }
    }


    @Test
    public void testLogoutAnonymous() throws Exception {
        QueryResult<ObjectMap> queryResult = catalogManager.loginAsAnonymous("127.0.0.1");
        catalogManager.logoutAnonymous(queryResult.getResult().get(0).getString("sessionId"));
    }

    @Test
    public void testGetUserInfo() throws CatalogManagerException {
        QueryResult<User> user = catalogManager.getUser("user", null, sessionIdUser);
        System.out.println("user = " + user);
        QueryResult<User> userVoid = catalogManager.getUser("user", user.getResult().get(0).getLastActivity(), sessionIdUser);
        System.out.println("userVoid = " + userVoid);
        assertTrue(userVoid.getResult().isEmpty());
        try {
            catalogManager.getUser("user", null, sessionIdUser2);
            fail();
        } catch (CatalogManagerException e) {
            System.out.println(e);
        }
    }

    @Test
    public void testModifyUser() throws CatalogManagerException, InterruptedException {
        ObjectMap params = new ObjectMap();
        String newName = "Changed Name " + StringUtils.randomString(10);
        String newPassword = StringUtils.randomString(10);
        String newEmail = "new@email.ac.uk";

        params.put("name", newName);
        ObjectMap attributes = new ObjectMap("myBoolean", true);
        attributes.put("value", 6);
        attributes.put("object", new BasicDBObject("id", 1234));
        params.put("attributes", attributes);

        User userPre = catalogManager.getUser("user", null, sessionIdUser).getResult().get(0);
        System.out.println("userPre = " + userPre);
        Thread.sleep(10);

        catalogManager.modifyUser("user", params, sessionIdUser);
        catalogManager.changeEmail("user", newEmail, sessionIdUser);
        catalogManager.changePassword("user", PASSWORD, newPassword, sessionIdUser);

        List<User> userList = catalogManager.getUser("user", userPre.getLastActivity(), sessionIdUser).getResult();
        if(userList.isEmpty()){
            fail("Error. LastActivity should have changed");
        }
        User userPost = userList.get(0);
        System.out.println("userPost = " + userPost);
        assertTrue(!userPre.getLastActivity().equals(userPost.getLastActivity()));
        assertEquals(userPost.getName(), newName);
        assertEquals(userPost.getEmail(), newEmail);
        assertEquals(userPost.getPassword(), newPassword);
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            assertEquals(userPost.getAttributes().get(entry.getKey()), entry.getValue());
        }

        try {
            params = new ObjectMap();
            params.put("password", "1234321");
            catalogManager.modifyUser("user", params, sessionIdUser);
            fail("Expected exception");
        } catch (CatalogManagerException e){
            System.out.println(e);
        }

        try {
            catalogManager.modifyUser("user", params, sessionIdUser2);
            fail("Expected exception");
        } catch (CatalogManagerException e){
            System.out.println(e);
        }

        catalogManager.changePassword("user", newPassword, PASSWORD, sessionIdUser);

    }

    /**
     * Project methods
     * ***************************
     */

    @Test
    public void testCreateProject() throws Exception {
        Project p = new Project("Project about some genomes", "1000G", "Today", "Cool", "", "", 1000, "");
        System.out.println(catalogManager.createProject("user", p.getName(), p.getAlias(), p.getDescription(), p.getOrganization(), sessionIdUser));
        System.out.println(catalogManager.createProject("user3", "Project Management Project", "pmp", "life art intelligent system", "myorg", sessionIdUser3));
    }

    @Test
    public void testCreateAnonymousProject() throws IOException, CatalogIOManagerException, CatalogManagerException {
        String sessionId = catalogManager.loginAsAnonymous("127.0.0.1").getResult().get(0).getString("sessionId");
//        catalogManager.createProject()
          //TODO: Finish test
    }

    @Test
    public void testGetAllProjects() throws Exception {
        System.out.println(catalogManager.getAllProjects("user", sessionIdUser));
        System.out.println(catalogManager.getAllProjects("user", sessionIdUser2));
    }

    @Test
    public void testModifyProject() throws CatalogManagerException {
        String newProjectName = "ProjectName " + StringUtils.randomString(10);
        int projectId = catalogManager.getUser("user", null, sessionIdUser).getResult().get(0).getProjects().get(0).getId();

        ObjectMap options = new ObjectMap();
        options.put("name", newProjectName);
        ObjectMap attributes = new ObjectMap("myBoolean", true);
        attributes.put("value", 6);
        attributes.put("object", new BasicDBObject("id", 1234));
        options.put("attributes", attributes);

        catalogManager.modifyProject(projectId, options, sessionIdUser);
        QueryResult<Project> result = catalogManager.getProject(projectId, sessionIdUser);
        Project project = result.getResult().get(0);
        System.out.println(result);

        assertEquals(newProjectName, project.getName());
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            assertEquals(project.getAttributes().get(entry.getKey()), entry.getValue());
        }

        try {
            options = new ObjectMap();
            options.put("alias", "newProjectAlias");
            catalogManager.modifyProject(projectId, options, sessionIdUser);
            fail("Expected 'Parameter can't be changed' exception");
        } catch (CatalogManagerException e){
            System.out.println(e);
        }

        try {
            catalogManager.modifyProject(projectId, options, sessionIdUser2);
            fail("Expected 'Permission denied' exception");
        } catch (CatalogManagerException e){
            System.out.println(e);
        }

    }

    /**
     * Study methods
     * ***************************
     */

    @Test
    public void testCreateStudy() throws Exception {
        int projectId = catalogManager.getAllProjects("user", sessionIdUser).getResult().get(0).getId();
        System.out.println(catalogManager.createStudy(projectId, "Phase 3", "phase3", Study.StudyType.CASE_CONTROL, "d", sessionIdUser));
        System.out.println(catalogManager.createStudy(projectId, "Phase 1", "phase1", Study.StudyType.TRIO, "Done", sessionIdUser));
    }

    @Test
    public void testModifyStudy() throws Exception {
        int studyId = catalogManager.getAllProjects("user", sessionIdUser).getResult().get(0).getStudies().get(0).getId();
        String newName = "Phase 1 "+ StringUtils.randomString(20);
        String newDescription = StringUtils.randomString(500);

        ObjectMap parameters = new ObjectMap();
        parameters.put("name", newName);
        parameters.put("description", newDescription);
        BasicDBObject attributes = new BasicDBObject("key", "value");
        parameters.put("attributes", attributes);
        catalogManager.modifyStudy(studyId, parameters, sessionIdUser);

        QueryResult<Study> result = catalogManager.getStudy(studyId, sessionIdUser);
        System.out.println(result);
        Study study = result.getResult().get(0);
        assertEquals(study.getName(), newName);
        assertEquals(study.getDescription(), newDescription);
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            assertEquals(study.getAttributes().get(entry.getKey()), entry.getValue());
        }
    }

    /**
     * File methods
     * ***************************
     */

    @Test
    public void testDeleteDataFromStudy() throws Exception {

    }

    @Test
    public void testCreateFolder() throws Exception {
        int projectId = catalogManager.getAllProjects("user", sessionIdUser).getResult().get(0).getId();
        int studyId = catalogManager.getAllStudies(projectId, sessionIdUser).getResult().get(0).getId();
        System.out.println(catalogManager.createFolder(studyId, Paths.get("data", "new", "folder"), true, sessionIdUser));
    }

    @Test
    public void testCreateAndUpload() throws Exception {
        int projectId = catalogManager.getAllProjects("user", sessionIdUser).getResult().get(0).getId();
        int studyId = catalogManager.getAllStudies(projectId, sessionIdUser).getResult().get(0).getId();

        FileInputStream is;
        java.io.File fileTest;

        String fileName = "item." + TimeUtils.getTimeMillis() + ".vcf";
        QueryResult<File> file = catalogManager.createFile(studyId, "txt", "vcf", "data/" + fileName, "description", true, sessionIdUser);

        is = new FileInputStream(fileTest = createDebugFile());
        System.out.println(catalogManager.uploadFile(file.getResult().get(0).getId(), is, sessionIdUser));
        is.close();
        fileTest.delete();

        fileName = "item." + TimeUtils.getTimeMillis() + ".vcf";
        is = new FileInputStream(fileTest = createDebugFile());
        System.out.println(catalogManager.uploadFile(studyId, "txt", "vcf", "data/" + fileName, "description", true, is, sessionIdUser));
        is.close();
        fileTest.delete();

    }

    @Test
    public void testDownloadFile() throws CatalogManagerException, IOException, InterruptedException, CatalogIOManagerException {
        int projectId = catalogManager.getAllProjects("user", sessionIdUser).getResult().get(0).getId();
        int studyId = catalogManager.getAllStudies(projectId, sessionIdUser).getResult().get(0).getId();

        String fileName = "item." + TimeUtils.getTimeMillis() + ".vcf";
        java.io.File fileTest;
        InputStream is = new FileInputStream(fileTest = createDebugFile());
        int fileId = catalogManager.uploadFile(studyId, "txt", "vcf", "data/" + fileName, "description", true, is, sessionIdUser).getResult().get(0).getId();
        is.close();
        fileTest.delete();

        DataInputStream dis = catalogManager.downloadFile(fileId, sessionIdUser);

        System.out.println(fileTest.getAbsolutePath());
        byte[] bytes = new byte[100];
        dis.read(bytes, 0, 100);
        System.out.println(new String(bytes));
//        System.out.println(Bytes.toString(bytes));
    }

    @Test
    public void searchFileTest() throws CatalogManagerException, CatalogIOManagerException, IOException {

        int studyId = catalogManager.getStudyId("user@1000G:phase1");

        QueryOptions options;
        QueryResult<File> result;

        options = new QueryOptions("startsWith", "new");
        result = catalogManager.searchFile(studyId, options, sessionIdUser);
        System.out.println(result);
        assertTrue(result.getNumResults() == 1);

        options = new QueryOptions("directory", "jobs");
        System.out.println(catalogManager.searchFile(studyId, options, sessionIdUser));

        options = new QueryOptions("directory", "data");
        System.out.println(catalogManager.searchFile(studyId, options, sessionIdUser));

        options = new QueryOptions("directory", "data.*");
        System.out.println(catalogManager.searchFile(studyId, options, sessionIdUser));
    }

    @Test
    public void testDeleteFile () throws CatalogManagerException, JsonProcessingException {
        int projectId = catalogManager.getAllProjects("user", sessionIdUser).getResult().get(0).getId();
        int studyId = catalogManager.getAllStudies(projectId, sessionIdUser).getResult().get(0).getId();
        List<File> result = catalogManager.getAllFiles(studyId, sessionIdUser).getResult();
        File file = null;
        for (int i = 0; i < result.size(); i++) {
            if (result.get(i).getType().equals(File.TYPE_FILE)) {
                file = result.get(i);
            }
        }
        if (file != null) {
            catalogManager.deleteFile(file.getId(), sessionIdUser);
        }
    }
    /* TYPE_FILE UTILS */
    private java.io.File createDebugFile() throws IOException {
        String fileTestName = "/tmp/fileTest" + StringUtils.randomString(5);
        DataOutputStream os = new DataOutputStream(new FileOutputStream(fileTestName));

        os.writeBytes("Debug file name: " + fileTestName + "\n");
        for (int i = 0; i < 100; i++) {
            os.writeBytes(i + ", ");
        }
        for (int i = 0; i < 200; i++) {
            os.writeBytes(StringUtils.randomString(500));
        }
        os.close();

        return Paths.get(fileTestName).toFile();
    }

    /**
     * Analysis methods
     * ***************************
     */

//    @Test
//    public void testCreateAnalysis() throws CatalogManagerException, JsonProcessingException {
//        int projectId = catalogManager.getAllProjects("user", sessionIdUser).getResult().get(0).getId();
//        int studyId = catalogManager.getAllStudies(projectId, sessionIdUser).getResult().get(0).getId();
//        Analysis analysis = new Analysis("MyAnalysis", "analysis1", "date", "user", "description");
//
//        System.out.println(catalogManager.createAnalysis(studyId, analysis.getName(), analysis.getAlias(), analysis.getDescription(), sessionIdUser));
//
//        System.out.println(catalogManager.createAnalysis(
//                studyId, "MyAnalysis2", "analysis2", "description", sessionIdUser));
//    }

    /**
     * Job methods
     * ***************************
     */

    @Test
    public void testCreateJob() throws CatalogManagerException, JsonProcessingException, CatalogIOManagerException {
        int projectId = catalogManager.getAllProjects("user", sessionIdUser).getResult().get(0).getId();
        int studyId = catalogManager.getAllStudies(projectId, sessionIdUser).getResult().get(0).getId();
//        int analysisId = catalogManager.getAllAnalysis(studyId, sessionIdUser).getResult().get(0).getId();

        File outDir = catalogManager.createFolder(studyId, Paths.get("jobs", "myJob"), true, sessionIdUser).getResult().get(0);
        Job job = new Job("myFirstJob", "", "samtool", "description", "#rm -rf .*", outDir.getId(), URI.create("file:///tmp"), Collections.<Integer>emptyList());

//        System.out.println(catalogManager.createJob(analysisId, job, sessionIdUser));
        System.out.println(catalogManager.createJob(
                studyId, "mySecondJob", "samtool", "description", "#rm -rf .*", outDir.getId(), outDir.getId(), null, sessionIdUser));
    }

    private static void clearCatalog(Properties properties) throws IOException {
        MongoDataStoreManager mongoManager = new MongoDataStoreManager(properties.getProperty("CATALOG.HOST"), Integer.parseInt(properties.getProperty("CATALOG.PORT")));
        MongoDataStore db = mongoManager.get(properties.getProperty("CATALOG.DATABASE"));
        db.getDb().dropDatabase();

        Path rootdir = Paths.get(URI.create(properties.getProperty("CATALOG.MAIN.ROOTDIR")));
//        Path rootdir = Paths.get(URI.create(properties.getProperty("CATALOG.MAIN.ROOTDIR")));
        deleteFolderTree(rootdir.toFile());
        Files.createDirectory(rootdir);
    }

    public static void deleteFolderTree(java.io.File folder) {
        java.io.File[] files = folder.listFiles();
        if(files!=null) {
            for(java.io.File f: files) {
                if(f.isDirectory()) {
                    deleteFolderTree(f);
                } else {
                    f.delete();
                }
            }
        }
        folder.delete();
    }
}