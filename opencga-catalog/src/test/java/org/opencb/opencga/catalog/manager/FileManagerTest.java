package org.opencb.opencga.catalog.manager;

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
import org.opencb.opencga.catalog.CatalogManagerExternalResource;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.*;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.managers.CatalogFileUtils;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.catalog.managers.api.IFileManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.core.common.TimeUtils;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by pfurio on 24/08/16.
 */
public class FileManagerTest extends GenericTest {

    public final static String PASSWORD = "asdf";
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public CatalogManagerExternalResource catalogManagerResource = new CatalogManagerExternalResource();

    protected CatalogManager catalogManager;
    private IFileManager fileManager;
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
        fileManager = catalogManager.getFileManager();
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


    @Test
    public void testDeleteDataFromStudy() throws Exception {

    }

    @Test
    public void testCreateFileFromUnsharedStudy() throws CatalogException {
        try {
            catalogManager.createFile(studyId, File.Format.UNKNOWN, File.Bioformat.NONE, "data/test/folder/file.txt", "My description",
                    true, -1, sessionIdUser2);
            fail("The file could be created despite not having the proper permissions.");
        } catch (CatalogAuthorizationException e) {
            assertTrue(e.getMessage().contains("Permission denied"));
            assertEquals(0, catalogManager.searchFile(studyId, new Query(FileDBAdaptor.QueryParams.PATH.key(),
                    "data/test/folder/file.txt"), sessionIdUser).getNumResults());
        }
    }

    @Test
    public void testCreateFileFromSharedStudy() throws CatalogException {
        catalogManager.createStudyAcls(Long.toString(studyId), "user2", "", "analyst", sessionIdUser);
        catalogManager.createFile(studyId, File.Format.UNKNOWN, File.Bioformat.NONE, "data/test/folder/file.txt", "My description", true,
                -1, sessionIdUser2);
        assertEquals(1, catalogManager.searchFile(studyId, new Query(FileDBAdaptor.QueryParams.PATH.key(),
                "data/test/folder/file.txt"), sessionIdUser).getNumResults());
    }

    @Test
    public void testLinkFolder() throws CatalogException, IOException {
//        // We will link the same folders that are already created in this study into another folder
        URI uri = Paths.get(catalogManager.getStudyUri(studyId)).resolve("data").toUri();
//        long folderId = catalogManager.searchFile(studyId, new Query(FileDBAdaptor.QueryParams.PATH.key(), "data/"), null,
//                sessionIdUser).first().getId();
//        int numFiles = catalogManager.getAllFilesInFolder(folderId, null, sessionIdUser).getNumResults();
//
//        catalogManager.link(uri, "data/", Long.toString(studyId), new ObjectMap(), sessionIdUser);
//        int numFilesAfterLink = catalogManager.getAllFilesInFolder(folderId, null, sessionIdUser).getNumResults();
//        assertEquals("Linking the same folders should not change the number of files in catalog", numFiles, numFilesAfterLink);

        // Now we try to create it into a folder that does not exist with parents = true
        catalogManager.link(uri, "myDirectory", Long.toString(studyId), new ObjectMap("parents", true), sessionIdUser);
        QueryResult<File> folderQueryResult = catalogManager.searchFile(studyId, new Query()
                        .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                        .append(FileDBAdaptor.QueryParams.PATH.key(), "myDirectory/"),
                null, sessionIdUser);
        assertEquals(1, folderQueryResult.getNumResults());
        assertTrue(!folderQueryResult.first().isExternal());

        folderQueryResult = catalogManager.searchFile(studyId, new Query()
                        .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                        .append(FileDBAdaptor.QueryParams.PATH.key(), "myDirectory/data/"),
                null, sessionIdUser);
        assertEquals(1, folderQueryResult.getNumResults());
        assertTrue(folderQueryResult.first().isExternal());

        folderQueryResult = catalogManager.searchFile(studyId, new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.PATH.key(), "myDirectory/data/test/"), null, sessionIdUser);
        assertEquals(1, folderQueryResult.getNumResults());
        assertTrue(folderQueryResult.first().isExternal());
        folderQueryResult = catalogManager.searchFile(studyId, new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.PATH.key(), "myDirectory/data/test/folder/"), null, sessionIdUser);
        assertEquals(1, folderQueryResult.getNumResults());
        assertTrue(folderQueryResult.first().isExternal());

        // Now we try to create it into a folder that does not exist with parents = false
        thrown.expect(CatalogException.class);
        thrown.expectMessage("already linked");
        catalogManager.link(uri, "myDirectory2", Long.toString(studyId), new ObjectMap(), sessionIdUser);
    }

    @Test
    public void testLinkFolder2() throws CatalogException, IOException {
        // We will link the same folders that are already created in this study into another folder
        URI uri = Paths.get(catalogManager.getStudyUri(studyId)).resolve("data").toUri();

        // Now we try to create it into a folder that does not exist with parents = false
        thrown.expect(CatalogException.class);
        thrown.expectMessage("not exist");
        catalogManager.link(uri, "myDirectory2", Long.toString(studyId), new ObjectMap(), sessionIdUser);
    }


    @Test
    public void testLinkFolder3() throws CatalogException, IOException {
        URI uri = Paths.get(catalogManager.getStudyUri(studyId)).resolve("data").toUri();
        thrown.expect(CatalogException.class);
        thrown.expectMessage("already existed and is not external");
        catalogManager.link(uri, null, Long.toString(studyId), new ObjectMap(), sessionIdUser);

//        // Make sure that the path of the files linked do not start with /
//        Query query = new Query(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
//                .append(FileDBAdaptor.QueryParams.EXTERNAL.key(), true);
//        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.PATH.key());
//        QueryResult<File> fileQueryResult = catalogManager.getFileManager().get(query, queryOptions, sessionIdUser);
//        assertEquals(5, fileQueryResult.getNumResults());
//        for (File file : fileQueryResult.getResult()) {
//            assertTrue(!file.getPath().startsWith("/"));
//        }
    }

    // This test will make sure that we can link several times the same uri into the same path with same results and without crashing
    // However, if we try to link to a different path, we will fail
    @Test
    public void testLinkFolder4() throws CatalogException, IOException {
        URI uri = Paths.get(catalogManager.getStudyUri(studyId)).resolve("data").toUri();
        ObjectMap params = new ObjectMap("parents", true);
        QueryResult<File> allFiles = catalogManager.link(uri, "test/myLinkedFolder/", Long.toString(studyId), params, sessionIdUser);
        assertEquals(6, allFiles.getNumResults());

        QueryResult<File> sameAllFiles = catalogManager.link(uri, "test/myLinkedFolder/", Long.toString(studyId), params, sessionIdUser);
        assertEquals(allFiles.getNumResults(), sameAllFiles.getNumResults());

        List<File> result = allFiles.getResult();
        for (int i = 0; i < result.size(); i++) {
            assertEquals(allFiles.getResult().get(i).getId(), sameAllFiles.getResult().get(i).getId());
            assertEquals(allFiles.getResult().get(i).getPath(), sameAllFiles.getResult().get(i).getPath());
            assertEquals(allFiles.getResult().get(i).getUri(), sameAllFiles.getResult().get(i).getUri());
        }

        thrown.expect(CatalogException.class);
        thrown.expectMessage("already linked");
        catalogManager.link(uri, "data", Long.toString(studyId), new ObjectMap(), sessionIdUser);
    }

    @Test
    public void testUnlinkFolder() throws CatalogException, IOException {
        URI uri = Paths.get(catalogManager.getStudyUri(studyId)).resolve("data").toUri();
        catalogManager.link(uri, "myDirectory", Long.toString(studyId), new ObjectMap("parents", true), sessionIdUser);

        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.PATH.key(), "~myDirectory/*")
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.READY);
        QueryResult<File> fileQueryResultLinked = catalogManager.searchFile(studyId, query, sessionIdUser);

        System.out.println("Number of files/folders linked = " + fileQueryResultLinked.getNumResults());

        // Now we try to unlink them
        catalogManager.unlink("myDirectory/data/", null, sessionIdUser);
        fileQueryResultLinked = catalogManager.searchFile(studyId, query, sessionIdUser);
        assertEquals(1, fileQueryResultLinked.getNumResults());

        query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.PATH.key(), "~myDirectory/*")
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.REMOVED);
        QueryResult<File> fileQueryResultUnlinked = catalogManager.searchFile(studyId, query, sessionIdUser);
        assertEquals(6, fileQueryResultUnlinked.getNumResults());
    }

    @Test
    public void testCreateFolder() throws Exception {
        long projectId = catalogManager.getAllProjects("user2", null, sessionIdUser2).first().getId();
        long studyId = catalogManager.getAllStudiesInProject(projectId, null, sessionIdUser2).first().getId();

        Set<String> paths = catalogManager.getAllFiles(studyId, new Query("type", File.Type.DIRECTORY), new QueryOptions(), sessionIdUser2)
                .getResult().stream().map(File::getPath).collect(Collectors.toSet());
        assertEquals(1, paths.size());
        assertTrue(paths.contains(""));             //root
//        assertTrue(paths.contains("data/"));        //data
//        assertTrue(paths.contains("analysis/"));    //analysis

        Path folderPath = Paths.get("data", "new", "folder");
        File folder = catalogManager.createFolder(studyId, folderPath, true, null, sessionIdUser2).first();

        paths = catalogManager.getAllFiles(studyId, new Query(FileDBAdaptor.QueryParams.TYPE.key(), File.Type.DIRECTORY),
                new QueryOptions(), sessionIdUser2).getResult().stream().map(File::getPath).collect(Collectors.toSet());
        assertEquals(4, paths.size());
        assertTrue(paths.contains("data/new/"));
        assertTrue(paths.contains("data/new/folder/"));

        URI uri = catalogManager.getFileUri(folder);
        assertTrue(catalogManager.getCatalogIOManagerFactory().get(uri).exists(uri));

        folder = catalogManager.createFolder(studyId, Paths.get("WOLOLO"), true, null, sessionIdUser2).first();

        Path myStudy = Files.createDirectory(catalogManagerResource.getOpencgaHome().resolve("myStudy"));
        long id = catalogManager.createStudy(projectId, "name", "alias", Study.Type.CASE_CONTROL, "", "",
                null, null, null, myStudy.toUri(), null, null, null, null, sessionIdUser2).first().getId();
        System.out.println("studyId = " + id);
        folder = catalogManager.createFolder(id, Paths.get("WOLOLO"), true, null, sessionIdUser2).first();
        System.out.println("folder = " + folder);
        System.out.println(catalogManager.getFileUri(folder));

    }

    @Test
    public void testCreateFolderAlreadyExists() throws Exception {
        long projectId = catalogManager.getAllProjects("user2", null, sessionIdUser2).first().getId();
        long studyId = catalogManager.getAllStudiesInProject(projectId, null, sessionIdUser2).first().getId();

        Set<String> paths = catalogManager.getAllFiles(studyId, new Query("type", File.Type.DIRECTORY), new QueryOptions(), sessionIdUser2)
                .getResult().stream().map(File::getPath).collect(Collectors.toSet());
        assertEquals(1, paths.size());
        assertTrue(paths.contains(""));             //root
//        assertTrue(paths.contains("data/"));        //data
//        assertTrue(paths.contains("analysis/"));    //analysis

        Path folderPath = Paths.get("data", "new", "folder");
        File folder = catalogManager.getFileManager().createFolder(studyId, folderPath.toString(), null, true, null, null, sessionIdUser2)
                .first();

        assertNotNull(folder);
        assertTrue(folder.getPath().contains(folderPath.toString()));

        // When creating the same folder, we should not complain and return it directly
        File sameFolder = catalogManager.getFileManager().createFolder(studyId, folderPath.toString(), null, true, null, null,
                sessionIdUser2).first();
        assertNotNull(sameFolder);
        assertEquals(folder.getPath(), sameFolder.getPath());
        assertEquals(folder.getId(), sameFolder.getId());

        // However, a user without create permissions will receive an exception
        thrown.expect(CatalogAuthorizationException.class);
        thrown.expectMessage("Permission denied");
        catalogManager.getFileManager().createFolder(studyId, folderPath.toString(), null, true, null, null, sessionIdUser3);
    }

    @Test
    public void testCreateAndUpload() throws Exception {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        long studyId2 = catalogManager.getStudyId("user@1000G:phase3");

        CatalogFileUtils catalogFileUtils = new CatalogFileUtils(catalogManager);

        java.io.File fileTest;

        String fileName = "item." + TimeUtils.getTimeMillis() + ".vcf";
        QueryResult<File> fileResult = catalogManager.createFile(studyId, File.Format.PLAIN, File.Bioformat.VARIANT, "data/" + fileName,
                "description", true, -1, sessionIdUser);

        fileTest = createDebugFile();
        catalogFileUtils.upload(fileTest.toURI(), fileResult.first(), null, sessionIdUser, false, false, true, true);
        assertTrue("File deleted", !fileTest.exists());

        fileName = "item." + TimeUtils.getTimeMillis() + ".vcf";
        fileResult = catalogManager.createFile(studyId, File.Format.PLAIN, File.Bioformat.VARIANT, "data/" + fileName, "description",
                true, -1, sessionIdUser);
        fileTest = createDebugFile();
        catalogFileUtils.upload(fileTest.toURI(), fileResult.first(), null, sessionIdUser, false, false, false, true);
        assertTrue("File don't deleted", fileTest.exists());
        assertTrue(fileTest.delete());

        fileName = "item." + TimeUtils.getTimeMillis() + ".txt";
        fileResult = catalogManager.createFile(studyId, File.Format.PLAIN, File.Bioformat.NONE, "data/" + fileName,
                StringUtils.randomString(200).getBytes(), "description", true, sessionIdUser);
        assertTrue("", fileResult.first().getStatus().getName().equals(File.FileStatus.READY));
        assertTrue("", fileResult.first().getDiskUsage() == 200);

        fileName = "item." + TimeUtils.getTimeMillis() + ".vcf";
        fileTest = createDebugFile();
        QueryResult<File> fileQueryResult = catalogManager.createFile(
                studyId2, File.Format.PLAIN, File.Bioformat.VARIANT, "data/deletable/folder/" + fileName, "description", true, -1,
                sessionIdUser);
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
                fileTest.toURI(), "file at root", true, sessionIdUser);
        assertTrue("File should be moved", !fileTest.exists());
        assertTrue(fileQueryResult.first().getDiskUsage() == size);
    }

    @Test
    public void testCreateFileInLinkedFolder() throws Exception {
        // Create an empty folder
        Path dir = catalogManagerResource.getOpencgaHome().resolve("folder_to_link");
        Files.createDirectory(dir);
        URI uri = dir.toUri();

        // Link the folder in the root
        catalogManager.link(uri, "", Long.toString(studyId), new ObjectMap(), sessionIdUser);

        File file = catalogManager.createFile(studyId, File.Format.PLAIN, File.Bioformat.NONE, "folder_to_link/file.txt", "", false, -1, sessionIdUser).first();

        assertEquals(uri.resolve("file.txt"), file.getUri());

    }

    @Test
    public void testDownloadAndHeadFile() throws CatalogException, IOException, InterruptedException {
        long projectId = catalogManager.getAllProjects("user", null, sessionIdUser).first().getId();
        long studyId = catalogManager.getAllStudiesInProject(projectId, null, sessionIdUser).first().getId();
        CatalogFileUtils catalogFileUtils = new CatalogFileUtils(catalogManager);

        String fileName = "item." + TimeUtils.getTimeMillis() + ".vcf";
        java.io.File fileTest;
        InputStream is = new FileInputStream(fileTest = createDebugFile());
        File file = catalogManager.createFile(studyId, File.Format.PLAIN, File.Bioformat.VARIANT, "data/" + fileName, "description",
                true, -1, sessionIdUser).first();
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

        assertEquals(limit - offset, lines);

        fis.close();
        dis.close();
        fileTest.delete();

    }

    @Test
    public void testDownloadFile() throws CatalogException, IOException, InterruptedException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");

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
    public void testGetTreeView() throws CatalogException {
        QueryResult<FileTree> fileTree = catalogManager.getFileManager().getTree("user@1000G:phase1:", new Query(), new QueryOptions(),
                5, sessionIdUser);
        assertEquals(7, fileTree.getNumResults());
    }

    @Test
    public void renameFileTest() throws CatalogException, IOException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1", sessionIdUser);
        catalogManager.createFile(studyId, File.Format.PLAIN, File.Bioformat.NONE, "data/file.txt",
                StringUtils.randomString(200).getBytes(), "description", true, sessionIdUser);
        catalogManager.createFile(studyId, File.Format.PLAIN, File.Bioformat.NONE, "data/nested/folder/file2.txt",
                StringUtils.randomString(200).getBytes(), "description", true, sessionIdUser);

        catalogManager.renameFile(catalogManager.getFileId("user@1000G:phase1:data/nested/", sessionIdUser), "nested2", sessionIdUser);
        Set<String> paths = catalogManager.getAllFiles(studyId, new Query(), new QueryOptions(), sessionIdUser).getResult()
                .stream().map(File::getPath).collect(Collectors.toSet());

        assertTrue(paths.contains("data/nested2/"));
        assertFalse(paths.contains("data/nested/"));
        assertTrue(paths.contains("data/nested2/folder/"));
        assertTrue(paths.contains("data/nested2/folder/file2.txt"));
        assertTrue(paths.contains("data/file.txt"));

        catalogManager.renameFile(catalogManager.getFileId("user@1000G:phase1:data/", sessionIdUser), "Data", sessionIdUser);
        paths = catalogManager.getAllFiles(studyId, new Query(), new QueryOptions(), sessionIdUser).getResult()
                .stream().map(File::getPath).collect(Collectors.toSet());

        assertTrue(paths.contains("Data/"));
        assertTrue(paths.contains("Data/file.txt"));
        assertTrue(paths.contains("Data/nested2/"));
        assertTrue(paths.contains("Data/nested2/folder/"));
        assertTrue(paths.contains("Data/nested2/folder/file2.txt"));
    }

    @Test
    public void getFileIdByString() throws CatalogException {
        catalogManager.createStudyAcls(Long.toString(studyId), "user2", "", "analyst", sessionIdUser);
        File file = catalogManager.createFile(studyId, File.Format.UNKNOWN, File.Bioformat.NONE, "data/test/folder/file.txt",
                "My description", true, -1, sessionIdUser2).first();
        long fileId = catalogManager.getFileId(file.getPath(), sessionIdUser);
        assertEquals(file.getId(), fileId);

        fileId = catalogManager.getFileId(Long.toString(file.getId()), sessionIdUser);
        assertEquals(file.getId(), fileId);
    }

    @Test
    public void renameFileEmptyName() throws CatalogException {
        thrown.expect(CatalogParameterException.class);
        thrown.expectMessage(containsString("null or empty"));

        catalogManager.renameFile(catalogManager.getFileId("user@1000G:phase1:data/"), "", sessionIdUser);
    }

    @Test
    public void renameFileSlashInName() throws CatalogException {
        thrown.expect(CatalogParameterException.class);
        catalogManager.renameFile(catalogManager.getFileId("user@1000G:phase1:data/"), "my/folder", sessionIdUser);
    }

    @Test
    public void renameFileAlreadyExists() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1", sessionIdUser);
        catalogManager.getFileManager().createFolder(studyId, "analysis/", new File.FileStatus(), false, "", new QueryOptions(), sessionIdUser);
        thrown.expect(CatalogIOException.class);
        catalogManager.renameFile(catalogManager.getFileId("user@1000G:phase1:data/", sessionIdUser), "analysis", sessionIdUser);
    }

    @Test
    public void searchFileTest() throws CatalogException, IOException {

        long studyId = catalogManager.getStudyId("user@1000G:phase1");

        Query query;
        QueryResult<File> result;

        query = new Query(FileDBAdaptor.QueryParams.NAME.key(), "~data");
        result = catalogManager.searchFile(studyId, query, sessionIdUser);
        assertEquals(1, result.getNumResults());

        //Get all files in data
        query = new Query(FileDBAdaptor.QueryParams.PATH.key(), "~data/[^/]+/?")
                .append(FileDBAdaptor.QueryParams.TYPE.key(),"FILE");
        result = catalogManager.searchFile(studyId, query, sessionIdUser);
        assertEquals(3, result.getNumResults());

        //Folder "jobs" does not exist
        query = new Query(FileDBAdaptor.QueryParams.DIRECTORY.key(), "jobs");
        result = catalogManager.searchFile(studyId, query, sessionIdUser);
        assertEquals(0, result.getNumResults());

        //Get all files in data
        query = new Query(FileDBAdaptor.QueryParams.DIRECTORY.key(), "data/");
        result = catalogManager.searchFile(studyId, query, sessionIdUser);
        assertEquals(1, result.getNumResults());

        //Get all files in data recursively
        query = new Query(FileDBAdaptor.QueryParams.DIRECTORY.key(), "data/.*");
        result = catalogManager.searchFile(studyId, query, sessionIdUser);
        assertEquals(5, result.getNumResults());

        query = new Query(FileDBAdaptor.QueryParams.TYPE.key(), "FILE");
        result = catalogManager.searchFile(studyId, query, sessionIdUser);
        result.getResult().forEach(f -> assertEquals(File.Type.FILE, f.getType()));
        int numFiles = result.getNumResults();
        assertEquals(3, numFiles);

        query = new Query(FileDBAdaptor.QueryParams.TYPE.key(), "DIRECTORY");
        result = catalogManager.searchFile(studyId, query, sessionIdUser);
        result.getResult().forEach(f -> assertEquals(File.Type.DIRECTORY, f.getType()));
        int numFolders = result.getNumResults();
        assertEquals(4, numFolders);

        query = new Query(FileDBAdaptor.QueryParams.PATH.key(), "");
        result = catalogManager.searchFile(studyId, query, sessionIdUser);
        assertEquals(1, result.getNumResults());
        assertEquals(".", result.first().getName());


        query = new Query(FileDBAdaptor.QueryParams.TYPE.key(), "FILE,DIRECTORY");
        result = catalogManager.searchFile(studyId, query, sessionIdUser);
        assertEquals(7, result.getNumResults());
        assertEquals(numFiles + numFolders, result.getNumResults());

        query = new Query("type", "FILE");
        query.put("diskUsage", ">400");
        result = catalogManager.searchFile(studyId, query, sessionIdUser);
        assertEquals(2, result.getNumResults());

        query = new Query("type", "FILE");
        query.put("diskUsage", "<400");
        result = catalogManager.searchFile(studyId, query, sessionIdUser);
        assertEquals(1, result.getNumResults());

        List<Long> sampleIds = catalogManager.getAllSamples(studyId, new Query("name", "s_1,s_3,s_4"), null, sessionIdUser)
                .getResult().stream().map(Sample::getId).collect(Collectors.toList());
        result = catalogManager.searchFile(studyId, new Query("sampleIds", sampleIds), sessionIdUser);
        assertEquals(1, result.getNumResults());

        query = new Query("type", "FILE");
        query.put("format", "PLAIN");
        result = catalogManager.searchFile(studyId, query, sessionIdUser);
        assertEquals(2, result.getNumResults());

        String attributes = FileDBAdaptor.QueryParams.ATTRIBUTES.key();
        String nattributes = FileDBAdaptor.QueryParams.NATTRIBUTES.key();
        String battributes = FileDBAdaptor.QueryParams.BATTRIBUTES.key();
        /*

        interface Searcher {
            QueryResult search(Integer id, Query query);
        }

        BiFunction<Integer, Query, QueryResult> searcher = (s, q) -> catalogManager.searchFile(s, q, sessionIdUser);

        result = searcher.apply(studyId, new Query(attributes + ".nested.text", "~H"));
        */
        result = catalogManager.searchFile(studyId, new Query(attributes + ".nested.text", "~H"), sessionIdUser);
        assertEquals(1, result.getNumResults());
        result = catalogManager.searchFile(studyId, new Query(nattributes + ".nested.num1", ">0"), sessionIdUser);
        assertEquals(1, result.getNumResults());
        result = catalogManager.searchFile(studyId, new Query(attributes + ".nested.num1", ">0"), sessionIdUser);
        assertEquals(0, result.getNumResults());

        result = catalogManager.searchFile(studyId, new Query(attributes + ".nested.num1", "notANumber"), sessionIdUser);
        assertEquals(0, result.getNumResults());

        result = catalogManager.searchFile(studyId, new Query(attributes + ".field", "~val"), sessionIdUser);
        assertEquals(3, result.getNumResults());

        result = catalogManager.searchFile(studyId, new Query("attributes.field", "~val"), sessionIdUser);
        assertEquals(3, result.getNumResults());

        result = catalogManager.searchFile(studyId, new Query(attributes + ".field", "=~val"), sessionIdUser);
        assertEquals(3, result.getNumResults());

        result = catalogManager.searchFile(studyId, new Query(attributes + ".field", "~val"), sessionIdUser);
        assertEquals(3, result.getNumResults());

        result = catalogManager.searchFile(studyId, new Query(attributes + ".field", "value"), sessionIdUser);
        assertEquals(2, result.getNumResults());

        result = catalogManager.searchFile(studyId, new Query(attributes + ".field", "other"), sessionIdUser);
        assertEquals(1, result.getNumResults());

        result = catalogManager.searchFile(studyId, new Query("nattributes.numValue", ">=5"), sessionIdUser);
        assertEquals(3, result.getNumResults());

        result = catalogManager.searchFile(studyId, new Query("nattributes.numValue", ">4,<6"), sessionIdUser);
        assertEquals(3, result.getNumResults());

        result = catalogManager.searchFile(studyId, new Query(nattributes + ".numValue", "==5"), sessionIdUser);
        assertEquals(2, result.getNumResults());

        result = catalogManager.searchFile(studyId, new Query(nattributes + ".numValue", "==5.0"), sessionIdUser);
        assertEquals(2, result.getNumResults());

        result = catalogManager.searchFile(studyId, new Query(nattributes + ".numValue", "=5.0"), sessionIdUser);
        assertEquals(2, result.getNumResults());

        result = catalogManager.searchFile(studyId, new Query(nattributes + ".numValue", "5.0"), sessionIdUser);
        assertEquals(2, result.getNumResults());

        result = catalogManager.searchFile(studyId, new Query(nattributes + ".numValue", ">5"), sessionIdUser);
        assertEquals(1, result.getNumResults());

        result = catalogManager.searchFile(studyId, new Query(nattributes + ".numValue", ">4"), sessionIdUser);
        assertEquals(3, result.getNumResults());

        result = catalogManager.searchFile(studyId, new Query(nattributes + ".numValue", "<6"), sessionIdUser);
        assertEquals(2, result.getNumResults());

        result = catalogManager.searchFile(studyId, new Query(nattributes + ".numValue", "<=5"), sessionIdUser);
        assertEquals(2, result.getNumResults());

        result = catalogManager.searchFile(studyId, new Query(nattributes + ".numValue", "<5"), sessionIdUser);
        assertEquals(0, result.getNumResults());

        result = catalogManager.searchFile(studyId, new Query(nattributes + ".numValue", "<2"), sessionIdUser);
        assertEquals(0, result.getNumResults());

        result = catalogManager.searchFile(studyId, new Query(nattributes + ".numValue", "==23"), sessionIdUser);
        assertEquals(0, result.getNumResults());

        result = catalogManager.searchFile(studyId, new Query(attributes + ".numValue", "=~10"), sessionIdUser);
        assertEquals(1, result.getNumResults());

        result = catalogManager.searchFile(studyId, new Query(nattributes + ".numValue", "=10"), sessionIdUser);
        assertEquals(0, result.getNumResults());

        result = catalogManager.searchFile(studyId, new Query(attributes + ".boolean", "true"), sessionIdUser);
        assertEquals(0, result.getNumResults());

        result = catalogManager.searchFile(studyId, new Query(attributes + ".boolean", "=true"), sessionIdUser);
        assertEquals(0, result.getNumResults());

        result = catalogManager.searchFile(studyId, new Query(attributes + ".boolean", "=1"), sessionIdUser);
        assertEquals(0, result.getNumResults());

        result = catalogManager.searchFile(studyId, new Query(battributes + ".boolean", "true"), sessionIdUser);
        assertEquals(1, result.getNumResults());

        result = catalogManager.searchFile(studyId, new Query(battributes + ".boolean", "=true"), sessionIdUser);
        assertEquals(1, result.getNumResults());

        // This has to return not only the ones with the attribute boolean = false, but also all the files that does not contain
        // that attribute at all.
        result = catalogManager.searchFile(studyId, new Query(battributes + ".boolean", "!=true"), sessionIdUser);
        assertEquals(6, result.getNumResults());

        result = catalogManager.searchFile(studyId, new Query(battributes + ".boolean", "=false"), sessionIdUser);
        assertEquals(1, result.getNumResults());

        query = new Query();
        query.append(attributes + ".name", "fileTest1k");
        query.append(attributes + ".field", "value");
        result = catalogManager.searchFile(studyId, query, sessionIdUser);
        assertEquals(1, result.getNumResults());

        query = new Query();
        query.append(attributes + ".name", "fileTest1k");
        query.append(attributes + ".field", "value");
        query.append(attributes + ".numValue", Arrays.asList(8, 9, 10));   //Searching as String. numValue = "10"
        result = catalogManager.searchFile(studyId, query, sessionIdUser);
        assertEquals(1, result.getNumResults());

    }

    @Test
    public void testSearchFileBoolean() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");

        Query query;
        QueryResult<File> result;
        FileDBAdaptor.QueryParams battributes = FileDBAdaptor.QueryParams.BATTRIBUTES;

        query = new Query(battributes.key() + ".boolean", "true");       //boolean in [true]
        result = catalogManager.searchFile(studyId, query, sessionIdUser);
        assertEquals(1, result.getNumResults());

        query = new Query(battributes.key() + ".boolean", "false");      //boolean in [false]
        result = catalogManager.searchFile(studyId, query, sessionIdUser);
        assertEquals(1, result.getNumResults());

        query = new Query(battributes.key() + ".boolean", "!=false");    //boolean in [null, true]
        query.put("type", "FILE");
        result = catalogManager.searchFile(studyId, query, sessionIdUser);
        assertEquals(2, result.getNumResults());

        query = new Query(battributes.key() + ".boolean", "!=true");     //boolean in [null, false]
        query.put("type", "FILE");
        result = catalogManager.searchFile(studyId, query, sessionIdUser);
        assertEquals(2, result.getNumResults());
    }

    @Test
    public void testSearchFileFail1() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        thrown.expect(CatalogDBException.class);
        catalogManager.searchFile(studyId, new Query(FileDBAdaptor.QueryParams.NATTRIBUTES.key() + ".numValue",
                "==NotANumber"), sessionIdUser);
    }

    @Test
    public void testSearchFileFail2() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        thrown.expect(CatalogDBException.class);
        catalogManager.searchFile(studyId, new Query("badFilter", "badFilter"), sessionIdUser);
    }

    @Test
    public void testSearchFileFail3() throws CatalogException {
        long studyId = catalogManager.getStudyId("user@1000G:phase1");
        thrown.expect(CatalogDBException.class);
        catalogManager.searchFile(studyId, new Query("id", "~5"), sessionIdUser); //Bad operator
    }

    @Test
    public void testGetFileParent() throws CatalogException, IOException {

        long fileId;
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
    public void testGetFileParents1() throws CatalogException {
        long fileId;
        QueryResult<File> fileParents;

        fileId = catalogManager.getFileId("user@1000G:phase1:data/test/folder/");
        fileParents = catalogManager.getFileParents(fileId, null, sessionIdUser);

        assertEquals(4, fileParents.getNumResults());
        assertEquals("", fileParents.getResult().get(0).getPath());
        assertEquals("data/", fileParents.getResult().get(1).getPath());
        assertEquals("data/test/", fileParents.getResult().get(2).getPath());
        assertEquals("data/test/folder/", fileParents.getResult().get(3).getPath());
    }

    @Test
    public void testGetFileParents2() throws CatalogException {
        long fileId;
        QueryResult<File> fileParents;

        fileId = catalogManager.getFileId("user@1000G:phase1:data/test/folder/test_1K.txt.gz");
        fileParents = catalogManager.getFileParents(fileId, null, sessionIdUser);

        assertEquals(5, fileParents.getNumResults());
        assertEquals("", fileParents.getResult().get(0).getPath());
        assertEquals("data/", fileParents.getResult().get(1).getPath());
        assertEquals("data/test/", fileParents.getResult().get(2).getPath());
        assertEquals("data/test/folder/", fileParents.getResult().get(3).getPath());
        assertEquals("data/test/folder/test_1K.txt.gz", fileParents.getResult().get(4).getPath());
    }

    @Test
    public void testGetFileParents3() throws CatalogException {
        long fileId;
        QueryResult<File> fileParents;

        fileId = catalogManager.getFileId("user@1000G:phase1:data/test/");
        fileParents = catalogManager.getFileParents(fileId,
                new QueryOptions("include", "projects.studies.files.path,projects.studies.files.id"),
                sessionIdUser);

        assertEquals(3, fileParents.getNumResults());
        assertEquals("", fileParents.getResult().get(0).getPath());
        assertEquals("data/", fileParents.getResult().get(1).getPath());
        assertEquals("data/test/", fileParents.getResult().get(2).getPath());

        fileParents.getResult().forEach(f -> {
            assertNull(f.getName());
            assertNotNull(f.getPath());
            assertTrue(f.getId() != 0);
        });

    }

    // Try to delete files/folders whose status is STAGED, MISSING...
    @Test
    public void testDelete1() throws CatalogException, IOException {
        long projectId = catalogManager.getAllProjects("user", null, sessionIdUser).first().getId();
        long studyId = catalogManager.getAllStudiesInProject(projectId, null, sessionIdUser).first().getId();

        String filePath = "data/";
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.PATH.key(), filePath);
        QueryResult<File> fileQueryResult = catalogManager.searchFile(studyId, query, sessionIdUser);

        // Change the status to MISSING
        ObjectMap objectMap = new ObjectMap(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.MISSING);
        catalogManager.modifyFile(fileQueryResult.first().getId(), objectMap, sessionIdUser);

        try {
            catalogManager.getFileManager().delete(Long.toString(fileQueryResult.first().getId()), null, sessionIdUser);
            fail("The call should prohibit deleting a folder in status missing");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("cannot be deleted"));
        }

        // Change the status to STAGED
        objectMap = new ObjectMap(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.STAGE);
        catalogManager.modifyFile(fileQueryResult.first().getId(), objectMap, sessionIdUser);

        try {
            catalogManager.getFileManager().delete(Long.toString(fileQueryResult.first().getId()), null, sessionIdUser);
            fail("The call should prohibit deleting a folder in status staged");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("cannot be deleted"));
        }
    }

    // It will try to delete a folder in status ready
    @Test
    public void testDelete2() throws CatalogException, IOException {
        long projectId = catalogManager.getAllProjects("user", null, sessionIdUser).first().getId();
        long studyId = catalogManager.getAllStudiesInProject(projectId, null, sessionIdUser).first().getId();

        String filePath = "data/";
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.PATH.key(), filePath);
        File file = catalogManager.searchFile(studyId, query, sessionIdUser).first();

        // We look for all the files and folders that fall within that folder
        query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.PATH.key(), "~^" + filePath + "*")
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.READY);
        int numResults = catalogManager.searchFile(studyId, query, sessionIdUser).getNumResults();
        assertEquals(6, numResults);

        // We delete it
        catalogManager.getFileManager().delete(Long.toString(file.getId()), null, sessionIdUser);

        // The files should have been moved to trashed status
        numResults = catalogManager.searchFile(studyId, query, sessionIdUser).getNumResults();
        assertEquals(0, numResults);

        query.put(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.TRASHED);
        numResults = catalogManager.searchFile(studyId, query, sessionIdUser).getNumResults();
        assertEquals(6, numResults);
    }

    // It will try to delete a folder in status ready and skip the trash
    @Test
    public void testDelete3() throws CatalogException, IOException {
        long projectId = catalogManager.getAllProjects("user", null, sessionIdUser).first().getId();
        long studyId = catalogManager.getAllStudiesInProject(projectId, null, sessionIdUser).first().getId();

        String filePath = "data/";
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.PATH.key(), filePath);
        File file = catalogManager.searchFile(studyId, query, sessionIdUser).first();

        // We look for all the files and folders that fall within that folder
        query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.PATH.key(), "~^" + filePath + "*")
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.READY);
        int numResults = catalogManager.searchFile(studyId, query, sessionIdUser).getNumResults();
        assertEquals(6, numResults);

        // We delete it
        QueryOptions queryOptions = new QueryOptions(FileManager.SKIP_TRASH, true);
        catalogManager.getFileManager().delete(Long.toString(file.getId()), queryOptions, sessionIdUser);

        // The files should have been moved to trashed status
        numResults = catalogManager.searchFile(studyId, query, sessionIdUser).getNumResults();
        assertEquals(0, numResults);

        query.put(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.PENDING_DELETE);
        numResults = catalogManager.searchFile(studyId, query, sessionIdUser).getNumResults();
        assertEquals(6, numResults);
    }

    @Test
    public void testDeleteFile() throws CatalogException, IOException {

        long projectId = catalogManager.getAllProjects("user", null, sessionIdUser).first().getId();
        long studyId = catalogManager.getAllStudiesInProject(projectId, null, sessionIdUser).first().getId();

        List<File> result = catalogManager.getAllFiles(studyId, new Query(FileDBAdaptor.QueryParams.TYPE.key(), "FILE"),
                new QueryOptions(), sessionIdUser).getResult();
        for (File file : result) {
            catalogManager.getFileManager().delete(Long.toString(file.getId()), null, sessionIdUser);
        }
        CatalogFileUtils catalogFileUtils = new CatalogFileUtils(catalogManager);
        catalogManager.getAllFiles(studyId, new Query(FileDBAdaptor.QueryParams.TYPE.key(), "FILE"), new QueryOptions(),
                sessionIdUser).getResult().forEach(f -> {
            assertEquals(f.getStatus().getName(), File.FileStatus.TRASHED);
            assertTrue(f.getName().startsWith(".deleted"));
        });

        long studyId2 = catalogManager.getAllStudiesInProject(projectId, null, sessionIdUser).getResult().get(1).getId();
        result = catalogManager.getAllFiles(studyId2, new Query(FileDBAdaptor.QueryParams.TYPE.key(), "FILE"), new QueryOptions(),
                sessionIdUser).getResult();
        for (File file : result) {
            catalogManager.getFileManager().delete(Long.toString(file.getId()), null, sessionIdUser);
        }
        catalogManager.getAllFiles(studyId, new Query(FileDBAdaptor.QueryParams.TYPE.key(), "FILE"), new QueryOptions(),
                sessionIdUser).getResult().forEach(f -> {
            assertEquals(f.getStatus().getName(), File.FileStatus.TRASHED);
            assertTrue(f.getName().startsWith(".deleted"));
        });

    }

    @Test
    public void testDeleteLeafFolder() throws CatalogException, IOException {
        long deletable = catalogManager.getFileId("user@1000G:phase3:/data/test/folder/", sessionIdUser);
        deleteFolderAndCheck(deletable);
    }

    @Test
    public void testDeleteMiddleFolder() throws CatalogException, IOException {
        long deletable = catalogManager.getFileId("user@1000G:phase3:/data/", sessionIdUser);
        deleteFolderAndCheck(deletable);
    }

    @Test
    public void testDeleteRootFolder() throws CatalogException, IOException {
        long deletable = catalogManager.getFileId("user@1000G:phase3:/", sessionIdUser);
        thrown.expect(CatalogException.class);
        deleteFolderAndCheck(deletable);
    }

    @Test
    public void deleteFolderTest() throws CatalogException, IOException {
        List<File> folderFiles = new LinkedList<>();
        long studyId = catalogManager.getStudyId("user@1000G:phase3", sessionIdUser);
        File folder = catalogManager.createFolder(studyId, Paths.get("folder"), false, null, sessionIdUser).first();
        folderFiles.add(catalogManager.createFile(studyId, File.Format.PLAIN, File.Bioformat.NONE, "folder/my.txt", StringUtils
                .randomString(200).getBytes(), "", true, sessionIdUser).first());
        folderFiles.add(catalogManager.createFile(studyId, File.Format.PLAIN, File.Bioformat.NONE, "folder/my2.txt", StringUtils
                .randomString(200).getBytes(), "", true, sessionIdUser).first());
        folderFiles.add(catalogManager.createFile(studyId, File.Format.PLAIN, File.Bioformat.NONE, "folder/my3.txt", StringUtils
                .randomString(200).getBytes(), "", true, sessionIdUser).first());
        folderFiles.add(catalogManager.createFile(studyId, File.Format.PLAIN, File.Bioformat.NONE, "folder/subfolder/my4.txt",
                StringUtils.randomString(200).getBytes(), "", true, sessionIdUser).first());
        folderFiles.add(catalogManager.createFile(studyId, File.Format.PLAIN, File.Bioformat.NONE, "folder/subfolder/my5.txt",
                StringUtils.randomString(200).getBytes(), "", true, sessionIdUser).first());
        folderFiles.add(catalogManager.createFile(studyId, File.Format.PLAIN, File.Bioformat.NONE, "folder/subfolder/subsubfolder/my6" +
                ".txt", StringUtils.randomString(200).getBytes(), "", true, sessionIdUser).first());

        CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(catalogManager.getFileUri(folder));
        for (File file : folderFiles) {
            assertTrue(ioManager.exists(catalogManager.getFileUri(file)));
        }

        File stagedFile = catalogManager.createFile(studyId, File.Type.FILE, File.Format.PLAIN, File.Bioformat.NONE,
                "folder/subfolder/subsubfolder/my_staged.txt", null, null, new File.FileStatus(File.FileStatus.STAGE), 0, -1, null,
                -1, null, null, true, null, sessionIdUser).first();

        thrown.expect(CatalogException.class);
        try {
            catalogManager.getFileManager().delete(Long.toString(folder.getId()), null, sessionIdUser);
        } finally {
            assertEquals("Folder name should not be modified", folder.getPath(), catalogManager.getFile(folder.getId(), sessionIdUser)
                    .first().getPath());
            assertTrue(ioManager.exists(catalogManager.getFileUri(catalogManager.getFile(folder.getId(), sessionIdUser).first())));
            for (File file : folderFiles) {
                assertEquals("File name should not be modified", file.getPath(), catalogManager.getFile(file.getId(), sessionIdUser)
                        .first().getPath());
                URI fileUri = catalogManager.getFileUri(catalogManager.getFile(file.getId(), sessionIdUser).first());
                assertTrue("File uri: " + fileUri + " should exist", ioManager.exists(fileUri));
            }
        }
    }

    @Test
    public void getAllFilesInFolder() throws CatalogException {
        long fileId = catalogManager.getFileId("user@1000G:phase1:/data/test/folder/", sessionIdUser);
        List<File> allFilesInFolder = catalogManager.getAllFilesInFolder(fileId, null, sessionIdUser).getResult();
        assertEquals(3, allFilesInFolder.size());
    }

    private void deleteFolderAndCheck(long deletable) throws CatalogException, IOException {
        List<File> allFilesInFolder;
        catalogManager.getFileManager().delete(Long.toString(deletable), null, sessionIdUser);

        long studyIdByFileId = catalogManager.getFileManager().getStudyId(deletable);

        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyIdByFileId)
                .append(FileDBAdaptor.QueryParams.ID.key(), deletable)
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.TRASHED);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.PATH.key());
        QueryResult<File> fileQueryResult = catalogManager.getFileManager().get(query, options, sessionIdUser);
        assertEquals(1, fileQueryResult.getNumResults());

//        allFilesInFolder = catalogManager.getAllFilesInFolder(deletable, null, sessionIdUser).getResult();
        query = new Query()
                .append(FileDBAdaptor.QueryParams.DIRECTORY.key(), fileQueryResult.first().getPath() + ".*")
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.TRASHED);
        allFilesInFolder = catalogManager.searchFile(studyIdByFileId, query, null, sessionIdUser).getResult();

        for (File subFile : allFilesInFolder) {
            assertTrue(subFile.getStatus().getName().equals(File.FileStatus.TRASHED));
        }
    }

    @Test
    public void testUpdateIndexStatus() throws CatalogException {
        long studyId = catalogManager.getStudyManager().getId("user", "user@1000G:phase1");
        QueryResult<File> fileResult = fileManager.create(studyId, File.Type.FILE, File.Format.VCF, File.Bioformat.VARIANT, "data/test.vcf", "", "description", new File.FileStatus(File.FileStatus.STAGE), 0, -1, Collections.emptyList(), -1,
                Collections.emptyMap(), Collections.emptyMap(), true, new QueryOptions(), sessionIdUser);

        fileManager.updateFileIndexStatus(fileResult.first(), FileIndex.IndexStatus.TRANSFORMED, sessionIdUser);
        QueryResult<File> read = fileManager.get(fileResult.first().getId(), new QueryOptions(), sessionIdUser);
        assertEquals(FileIndex.IndexStatus.TRANSFORMED, read.first().getIndex().getStatus().getName());
    }
}
