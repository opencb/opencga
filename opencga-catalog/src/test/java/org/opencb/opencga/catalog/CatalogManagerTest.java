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
import org.junit.*;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import org.junit.rules.ExpectedException;
import org.opencb.commons.test.GenericTest;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.core.config.DataStoreServerAddress;
import org.opencb.datastore.mongodb.MongoDataStore;
import org.opencb.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.catalog.beans.*;
import org.opencb.opencga.catalog.beans.File;
import org.opencb.opencga.catalog.db.CatalogDBException;
import org.opencb.opencga.catalog.io.CatalogIOManagerException;
import org.opencb.opencga.core.common.StringUtils;
import org.opencb.opencga.core.common.TimeUtils;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class CatalogManagerTest extends GenericTest {

    public final String PASSWORD = "asdf";
    CatalogManager catalogManager;
    protected String sessionIdUser;
    protected String sessionIdUser2;
    protected String sessionIdUser3;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void init() throws IOException, CatalogException {


    }

    @Before
    public void setUp() throws IOException, CatalogException {
        InputStream is = CatalogManagerTest.class.getClassLoader().getResourceAsStream("catalog.properties");
        Properties properties = new Properties();
        properties.load(is);

        clearCatalog(properties);

        catalogManager = new CatalogManager(properties);

        catalogManager.createUser("user", "User Name", "mail@ebi.ac.uk", PASSWORD, "", null);
        catalogManager.createUser("user2", "User2 Name", "mail2@ebi.ac.uk", PASSWORD, "", null);
        catalogManager.createUser("user3", "User3 Name", "user.2@e.mail", PASSWORD, "ACME", null);

        sessionIdUser  = catalogManager.login("user",  PASSWORD, "127.0.0.1").first().getString("sessionId");
        sessionIdUser2 = catalogManager.login("user2", PASSWORD, "127.0.0.1").first().getString("sessionId");
        sessionIdUser3 = catalogManager.login("user3", PASSWORD, "127.0.0.1").first().getString("sessionId");

        Project project1 = catalogManager.createProject("user", "Project about some genomes", "1000G", "", "ACME", null, sessionIdUser).first();
        Project project2 = catalogManager.createProject("user2", "Project Management Project", "pmp", "life art intelligent system", "myorg", null, sessionIdUser2).first();
        Project project3 = catalogManager.createProject("user3", "project 1", "p1", "", "", null, sessionIdUser3).first();

        int studyId = catalogManager.createStudy(project1.getId(), "Phase 1", "phase1", Study.Type.TRIO, "Done", sessionIdUser).first().getId();
        int studyId2 = catalogManager.createStudy(project1.getId(), "Phase 3", "phase3", Study.Type.CASE_CONTROL, "d", sessionIdUser).first().getId();
        int studyId3 = catalogManager.createStudy(project2.getId(), "Study 1", "s1", Study.Type.CONTROL_SET, "", sessionIdUser2).first().getId();


        catalogManager.createFolder(studyId, Paths.get("data/test/folder/"), true, null, sessionIdUser);
        catalogManager.createFolder(studyId2, Paths.get("data/test/folder/"), true, null, sessionIdUser);

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
    public void testCreateExistingUser() throws Exception {
        thrown.expect(CatalogException.class);
        catalogManager.createUser("user", "User Name", "mail@ebi.ac.uk", PASSWORD, "", null);
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
        catalogManager.login("user", "fakePassword", "127.0.0.1");
//        fail("Expected 'wrong password' exception");
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
        QueryResult<User> userVoid = catalogManager.getUser("user", user.first().getLastActivity(), sessionIdUser);
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
        catalogManager.changePassword("user", PASSWORD, newPassword, sessionIdUser);

        List<User> userList = catalogManager.getUser("user", userPre.getLastActivity(), new QueryOptions("exclude", Arrays.asList("sessions")), sessionIdUser).getResult();
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

        catalogManager.changePassword("user", newPassword, PASSWORD, sessionIdUser);

        try {
            params = new ObjectMap();
            params.put("password", "1234321");
            catalogManager.modifyUser("user", params, sessionIdUser);
            fail("Expected exception");
        } catch (CatalogDBException e){
            System.out.println(e);
        }

        try {
            catalogManager.modifyUser("user", params, sessionIdUser2);
            fail("Expected exception");
        } catch (CatalogException e){
            System.out.println(e);
        }

    }

    /**
     * Project methods
     * ***************************
     */


    @Test
    public void testCreateAnonymousProject() throws IOException, CatalogIOManagerException, CatalogException {
        String sessionId = catalogManager.loginAsAnonymous("127.0.0.1").first().getString("sessionId");
//        catalogManager.createProject()
          //TODO: Finish test
    }

    @Test
    public void testGetAllProjects() throws Exception {
        System.out.println(catalogManager.getAllProjects("user", null, sessionIdUser));
        System.out.println(catalogManager.getAllProjects("user", null, sessionIdUser2));
    }

    @Test
    public void testModifyProject() throws CatalogException {
        String newProjectName = "ProjectName " + StringUtils.randomString(10);
        int projectId = catalogManager.getUser("user", null, sessionIdUser).first().getProjects().get(0).getId();

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

        try {
            catalogManager.modifyProject(projectId, options, sessionIdUser2);
            fail("Expected 'Permission denied' exception");
        } catch (CatalogDBException e){
            System.out.println(e);
        }

    }

    /**
     * Study methods
     * ***************************
     */

    @Test
    public void testModifyStudy() throws Exception {
        int projectId = catalogManager.getAllProjects("user", null, sessionIdUser).first().getId();
        int studyId = catalogManager.getAllStudies(projectId, null, sessionIdUser).first().getId();
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
        Study study = result.first();
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
        int projectId = catalogManager.getAllProjects("user", null, sessionIdUser).first().getId();
        int studyId = catalogManager.getAllStudies(projectId, null, sessionIdUser).first().getId();
        System.out.println(catalogManager.createFolder(studyId, Paths.get("data", "new", "folder"), true, null, sessionIdUser));
    }

    @Test
    public void testCreateAndUpload() throws Exception {
        int studyId = catalogManager.getStudyId("user@1000G:phase1");
        int studyId2 = catalogManager.getStudyId("user@1000G:phase3");

        CatalogFileUtils catalogFileUtils = new CatalogFileUtils(catalogManager);

        java.io.File fileTest;

        String fileName = "item." + TimeUtils.getTimeMillis() + ".vcf";
        QueryResult<File> fileResult = catalogManager.createFile(studyId, File.Format.PLAIN, File.Bioformat.VARIANT, "data/" + fileName, "description", true, -1, sessionIdUser);

        fileTest = createDebugFile();
        catalogFileUtils.upload(fileTest.toURI(), fileResult.first(), null, sessionIdUser, false, false, true, true);
        assertTrue("File deleted", !fileTest.exists());

        fileName = "item." + TimeUtils.getTimeMillis() + ".vcf";
        fileResult = catalogManager.createFile(studyId, File.Format.PLAIN, File.Bioformat.VARIANT, "data/" + fileName, "description", true, -1, sessionIdUser);
        fileTest = createDebugFile();
        catalogFileUtils.upload(fileTest.toURI(), fileResult.first(), null, sessionIdUser, false, false, false, true);
        assertTrue("File don't deleted", fileTest.exists());
        assertTrue(fileTest.delete());

        fileName = "item." + TimeUtils.getTimeMillis() + ".txt";
        fileResult = catalogManager.createFile(studyId, File.Format.PLAIN, File.Bioformat.NONE, "data/" + fileName,
                StringUtils.randomString(200).getBytes(), "description", true, sessionIdUser);
        assertTrue("", fileResult.first().getStatus() == File.Status.READY);
        assertTrue("", fileResult.first().getDiskUsage() == 200);

        fileName = "item." + TimeUtils.getTimeMillis() + ".vcf";
        fileTest = createDebugFile();
        QueryResult<File> fileQueryResult = catalogManager.createFile(
                studyId2, File.Format.PLAIN, File.Bioformat.VARIANT, "data/deletable/folder/" + fileName, "description", true, -1, sessionIdUser);
        catalogFileUtils.upload(fileTest.toURI(), fileQueryResult.first(), null, sessionIdUser, false, false, true, true);
        assertFalse("File deleted by the upload", fileTest.delete());

        fileName = "item." + TimeUtils.getTimeMillis() + ".vcf";
        fileTest = createDebugFile();
        fileQueryResult = catalogManager.createFile(
                studyId2, File.Format.PLAIN, File.Bioformat.VARIANT, "data/deletable/" + fileName, "description", true, -1, sessionIdUser);
        catalogFileUtils.upload(fileTest.toURI(), fileQueryResult.first(), null, sessionIdUser, false, false, false, true);
        assertTrue(fileTest.delete());

        fileName = "item." + TimeUtils.getTimeMillis() + ".vcf";
        fileTest = createDebugFile();
        fileQueryResult = catalogManager.createFile(
                studyId2, File.Format.PLAIN, File.Bioformat.VARIANT, "" + fileName, "file at root", true, -1, sessionIdUser);
        catalogFileUtils.upload(fileTest.toURI(), fileQueryResult.first(), null, sessionIdUser, false, false, false, true);
        assertTrue(fileTest.delete());

        fileName = "item." + TimeUtils.getTimeMillis() + ".vcf";
        fileTest = createDebugFile();
        long size = Files.size(fileTest.toPath());
        fileQueryResult = catalogManager.createFile(studyId2, File.Format.PLAIN, File.Bioformat.VARIANT, "" + fileName,
                fileTest.toPath(), "file at root", true, sessionIdUser);
        assertTrue("File should be moved", !fileTest.exists());
        assertTrue(fileQueryResult.first().getDiskUsage() == size);
    }

    @Test
    public void testDownloadAndHeadFile() throws CatalogException, IOException, InterruptedException {
        int projectId = catalogManager.getAllProjects("user", null, sessionIdUser).first().getId();
        int studyId = catalogManager.getAllStudies(projectId, null, sessionIdUser).first().getId();
        CatalogFileUtils catalogFileUtils = new CatalogFileUtils(catalogManager);

        String fileName = "item." + TimeUtils.getTimeMillis() + ".vcf";
        java.io.File fileTest;
        InputStream is = new FileInputStream(fileTest = createDebugFile());
        File file = catalogManager.createFile(studyId, File.Format.PLAIN, File.Bioformat.VARIANT, "data/" + fileName, "description", true, -1, sessionIdUser).first();
        catalogFileUtils.upload(is, file, sessionIdUser, false, false, true);
        is.close();


        byte[] bytes = new byte[100];
        byte[] bytesOrig = new byte[100];
        DataInputStream fis = new DataInputStream(new FileInputStream(fileTest));
        DataInputStream dis = catalogManager.downloadFile(file.getId(), sessionIdUser);
        fis.read(bytesOrig, 0, 100);
        dis.read(bytes, 0, 100);
        fis.close();
        dis.close();
        assertArrayEquals(bytesOrig, bytes);


        int offset = 5;
        int limit = 30;
        dis = catalogManager.downloadFile(file.getId(), offset, limit, sessionIdUser);
        fis = new DataInputStream(new FileInputStream(fileTest));
        for (int i = 0; i < offset; i++) {
            fis.readLine();
        }


        String line;
        int lines = 0;
        while ((line = dis.readLine()) != null) {
            lines++;
            System.out.println(line);
            assertEquals(fis.readLine(), line);
        }

        assertEquals(limit-offset, lines);

        fis.close();
        dis.close();
        fileTest.delete();

    }


    @Test
    public void testDownloadFile() throws CatalogException, IOException, InterruptedException {
        int projectId = catalogManager.getAllProjects("user", null, sessionIdUser).first().getId();
        int studyId = catalogManager.getAllStudies(projectId, null, sessionIdUser).first().getId();

        String fileName = "item." + TimeUtils.getTimeMillis() + ".vcf";
        int fileSize = 200;
        byte[] bytesOrig = StringUtils.randomString(fileSize).getBytes();
        File file = catalogManager.createFile(studyId, File.Format.PLAIN, File.Bioformat.NONE, "data/" + fileName,
                bytesOrig, "description", true, sessionIdUser).first();

        DataInputStream dis = catalogManager.downloadFile(file.getId(), sessionIdUser);

        byte[] bytes = new byte[fileSize];
        dis.read(bytes, 0, fileSize);
        assertTrue(Arrays.equals(bytesOrig, bytes));

    }


    @Test
    public void searchFileTest() throws CatalogException, CatalogIOManagerException, IOException {

        int studyId = catalogManager.getStudyId("user@1000G:phase1");

        QueryOptions options;
        QueryResult<File> result;

        options = new QueryOptions("startsWith", "data");
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
    public void testGetFileParent() throws CatalogException, IOException {

        int studyId = catalogManager.getStudyId("user@1000G:phase1");

        int fileId;
        fileId = catalogManager.getFileId("user@1000G:phase1:data/test/folder/");
        System.out.println(catalogManager.getFile(fileId, null, sessionIdUser));
        QueryResult<File> fileParent = catalogManager.getFileParent(fileId, null, sessionIdUser);
        System.out.println(fileParent);


        fileId = catalogManager.getFileId("user@1000G:phase1:data/");
        System.out.println(catalogManager.getFile(fileId, null, sessionIdUser));
        fileParent = catalogManager.getFileParent(fileId, null, sessionIdUser);
        System.out.println(fileParent);

        fileId = catalogManager.getFileId("user@1000G:phase1:");
        System.out.println(catalogManager.getFile(fileId, null, sessionIdUser));
        fileParent = catalogManager.getFileParent(fileId, null, sessionIdUser);
        System.out.println(fileParent);


    }

    @Test
    public void testDeleteFile () throws CatalogException, IOException {

        int projectId = catalogManager.getAllProjects("user", null, sessionIdUser).first().getId();
        int studyId = catalogManager.getAllStudies(projectId, null, sessionIdUser).first().getId();

        List<File> result = catalogManager.getAllFiles(studyId, null, sessionIdUser).getResult();
        File file = null;
        try {
            for (int i = 0; i < result.size(); i++) {
                if (result.get(i).getType().equals(File.Type.FILE)) {
                    file = result.get(i);
                }
            }
            if (file != null) {
                System.out.println("deleteing " + file.getPath());
                catalogManager.deleteFile(file.getId(), sessionIdUser);
            }
        } catch (CatalogException e) {
            System.out.println(e.getMessage());
        }

        int studyId2 = catalogManager.getAllStudies(projectId, null, sessionIdUser).first().getId();
        result = catalogManager.getAllFiles(studyId2, null, sessionIdUser).getResult();

        file = null;

        try {
            for (int i = 0; i < result.size(); i++) {
                if (result.get(i).getType().equals(File.Type.FILE)) {
                    file = result.get(i);
                }
            }
            if (file != null) {
                System.out.println("deleteing " + file.getPath());
                catalogManager.deleteFile(file.getId(), sessionIdUser);
            }
        } catch (CatalogException e) {
            System.out.println(e.getMessage());
        }
    }

    @Test
    public void testDeleteLeafFolder () throws CatalogException, IOException {
        int deletable = catalogManager.getFileId("user@1000G/phase3/data/test/folder/");
        deleteFolderAndCheck(deletable);
    }

    @Test
    public void testDeleteMiddleFolder () throws CatalogException, IOException {
        int deletable = catalogManager.getFileId("user@1000G/phase3/data/");
        deleteFolderAndCheck(deletable);
    }

    @Test
    public void testDeleteRootFolder () throws CatalogException, IOException {
        int deletable = catalogManager.getFileId("user@1000G/phase3/");
        thrown.expect(CatalogException.class);
        deleteFolderAndCheck(deletable);
    }

    private void deleteFolderAndCheck(int deletable) throws CatalogException, IOException {
        List<File> allFilesInFolder;
        catalogManager.deleteFolder(deletable, sessionIdUser);

        File file = catalogManager.getFile(deletable, sessionIdUser).first();
        allFilesInFolder = catalogManager.getAllFilesInFolder(deletable, null, sessionIdUser).getResult();
        allFilesInFolder = catalogManager.searchFile(
                catalogManager.getStudyIdByFileId(deletable),
                new QueryOptions("directory", catalogManager.getFile(deletable, sessionIdUser).first().getPath() + ".*"),
                null, sessionIdUser).getResult();

        assertTrue(file.getStatus() == File.Status.DELETING);
        for (File subFile : allFilesInFolder) {
            assertTrue(subFile.getStatus() == File.Status.DELETING);
        }
    }

    /* TYPE_FILE UTILS */
    static java.io.File createDebugFile() throws IOException {
        String fileTestName = "/tmp/fileTest " + StringUtils.randomString(5);
        DataOutputStream os = new DataOutputStream(new FileOutputStream(fileTestName));

        os.writeBytes("Debug file name: " + fileTestName + "\n");
        for (int i = 0; i < 100; i++) {
            os.writeBytes(i + ", ");
        }
        for (int i = 0; i < 200; i++) {
            os.writeBytes(StringUtils.randomString(500));
            os.write('\n');
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
//        int projectId = catalogManager.getAllProjects("user", sessionIdUser).first().getId();
//        int studyId = catalogManager.getAllStudies(projectId, sessionIdUser).first().getId();
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
    public void testCreateJob() throws CatalogException, IOException {
        int projectId = catalogManager.getAllProjects("user", null, sessionIdUser).first().getId();
        int studyId = catalogManager.getAllStudies(projectId, null, sessionIdUser).first().getId();

        File outDir = catalogManager.createFolder(studyId, Paths.get("jobs", "myJob"), true, null, sessionIdUser).first();

        URI tmpJobOutDir = catalogManager.createJobOutDir(studyId, StringUtils.randomString(5), sessionIdUser);
        catalogManager.createJob(
                studyId, "myJob", "samtool", "description", "echo \"Hello World!\"", tmpJobOutDir, outDir.getId(),
                Collections.emptyList(), new HashMap<>(), null, Job.Status.PREPARED, null, sessionIdUser);

        catalogManager.createJob(
                studyId, "myReadyJob", "samtool", "description", "echo \"Hello World!\"", tmpJobOutDir, outDir.getId(),
                Collections.emptyList(), new HashMap<>(), null, Job.Status.READY, null, sessionIdUser);

        catalogManager.createJob(
                studyId, "myQueuedJob", "samtool", "description", "echo \"Hello World!\"", tmpJobOutDir, outDir.getId(),
                Collections.emptyList(), new HashMap<>(), null, Job.Status.QUEUED, null, sessionIdUser);

        catalogManager.createJob(
                studyId, "myErrorJob", "samtool", "description", "echo \"Hello World!\"", tmpJobOutDir, outDir.getId(),
                Collections.emptyList(), new HashMap<>(), null, Job.Status.ERROR, null, sessionIdUser);

        String sessionId = catalogManager.login("admin", "admin", "localhost").first().get("sessionId").toString();
        QueryResult<Job> unfinishedJobs = catalogManager.getUnfinishedJobs(sessionId);
        assertEquals(2, unfinishedJobs.getNumResults());

        QueryResult<Job> allJobs = catalogManager.getAllJobs(studyId, sessionId);
        assertEquals(4, allJobs.getNumResults());

    }

    /**
     * Sample methods
     * ***************************
     */

    @Test
    public void testCreateSample () throws CatalogException {
        int projectId = catalogManager.getAllProjects("user", null, sessionIdUser).first().getId();
        int studyId = catalogManager.getAllStudies(projectId, null, sessionIdUser).first().getId();

        QueryResult<Sample> sampleQueryResult = catalogManager.createSample(studyId, "HG007", "IMDb", "", null, null, sessionIdUser);
        System.out.println("sampleQueryResult = " + sampleQueryResult);

    }

    @Test
    public void testCreateVariableSet () throws CatalogException {
        int projectId = catalogManager.getAllProjects("user", null, sessionIdUser).first().getId();
        int studyId = catalogManager.getAllStudies(projectId, null, sessionIdUser).first().getId();

        Set<Variable> variables = new HashSet<>();
        variables.addAll(Arrays.asList(
                new Variable("NAME", "", Variable.VariableType.TEXT, "", true, Collections.<String>emptyList(), 0, "", "", Collections.<String, Object>emptyMap()),
                new Variable("AGE", "", Variable.VariableType.NUMERIC, null, true, Collections.singletonList("0:99"), 1, "", "", Collections.<String, Object>emptyMap()),
                new Variable("HEIGHT", "", Variable.VariableType.NUMERIC, "1.5", false, Collections.singletonList("0:"), 2, "", "", Collections.<String, Object>emptyMap()),
                new Variable("ALIVE", "", Variable.VariableType.BOOLEAN, "", true, Collections.<String>emptyList(), 3, "", "", Collections.<String, Object>emptyMap()),
                new Variable("PHEN", "", Variable.VariableType.CATEGORICAL, "", true, Arrays.asList("CASE", "CONTROL"), 4, "", "", Collections.<String, Object>emptyMap())
        ));
        QueryResult<VariableSet> sampleQueryResult = catalogManager.createVariableSet(studyId, "vs1", true, "", null, variables, sessionIdUser);
        System.out.println("sampleQueryResult = " + sampleQueryResult);

    }
//
//    @Test
//    public void testAnnotation () throws CatalogException {
//        testCreateSample();
//        testCreateVariableSet();
//        int projectId = catalogManager.getAllProjects("user", null, sessionIdUser).first().getId();
//        Study study = catalogManager.getAllStudies(projectId, null, sessionIdUser).first();
//        int sampleId = catalogManager.getAllSamples(study.getId(), new QueryOptions(), sessionIdUser).first().getId();
//
//        {
//            HashMap<String, Object> annotations = new HashMap<String, Object>();
//            annotations.put("NAME", "Luke");
//            annotations.put("AGE", "28");
//            annotations.put("HEIGHT", "1.78");
//            annotations.put("ALIVE", "1");
//            annotations.put("PHEN", "CASE");
//            QueryResult<AnnotationSet> annotationSetQueryResult = catalogManager.annotateSample(sampleId, "annotation1", study.getVariableSets().get(0).getId(), annotations, null, sessionIdUser);
//            System.out.println("annotationSetQueryResult = " + annotationSetQueryResult);
//        }
//
//        {
//            HashMap<String, Object> annotations = new HashMap<String, Object>();
//            annotations.put("NAME", "Luke");
//            annotations.put("AGE", "95");
////            annotations.put("HEIGHT", "1.78");
//            annotations.put("ALIVE", "1");
//            annotations.put("PHEN", "CASE");
//            QueryResult<AnnotationSet> annotationSetQueryResult = catalogManager.annotateSample(sampleId, "annotation2", study.getVariableSets().get(0).getId(), annotations, null, sessionIdUser);
//            System.out.println("annotationSetQueryResult = " + annotationSetQueryResult);
//        }
//
//    }


    public static void clearCatalog(Properties properties) throws IOException {
        List<DataStoreServerAddress> dataStoreServerAddresses = new LinkedList<>();
        for (String hostPort : properties.getProperty(CatalogManager.CATALOG_DB_HOSTS, "localhost").split(",")) {
            if (hostPort.contains(":")) {
                String[] split = hostPort.split(":");
                Integer port = Integer.valueOf(split[1]);
                dataStoreServerAddresses.add(new DataStoreServerAddress(split[0], port));
            } else {
                dataStoreServerAddresses.add(new DataStoreServerAddress(hostPort, 27017));
            }
        }
        MongoDataStoreManager mongoManager = new MongoDataStoreManager(dataStoreServerAddresses);
        MongoDataStore db = mongoManager.get(properties.getProperty(CatalogManager.CATALOG_DB_DATABASE));
        db.getDb().dropDatabase();

        Path rootdir = Paths.get(URI.create(properties.getProperty(CatalogManager.CATALOG_MAIN_ROOTDIR)));
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