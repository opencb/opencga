/*
 * Copyright 2015-2017 OpenCB
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
import org.opencb.commons.datastore.core.result.WriteResult;
import org.opencb.commons.test.GenericTest;
import org.opencb.commons.utils.StringUtils;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.*;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.acls.AclParams;
import org.opencb.opencga.core.models.acls.permissions.FileAclEntry;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;

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
    private FileManager fileManager;
    protected String sessionIdUser;
    protected String sessionIdUser2;
    protected String sessionIdUser3;
    private File testFolder;
    private long projectId;
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
    public java.io.File createDebugFile() throws IOException {
        String fileTestName = catalogManagerResource.getOpencgaHome()
                .resolve("fileTest " + StringUtils.randomString(5)).toAbsolutePath().toString();
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

        catalogManager.getUserManager().create("user", "User Name", "mail@ebi.ac.uk", PASSWORD, "", null, Account.FULL, null, null);
        catalogManager.getUserManager().create("user2", "User2 Name", "mail2@ebi.ac.uk", PASSWORD, "", null, Account.FULL, null, null);
        catalogManager.getUserManager().create("user3", "User3 Name", "user.2@e.mail", PASSWORD, "ACME", null, Account.FULL, null, null);

        sessionIdUser = catalogManager.getUserManager().login("user", PASSWORD);
        sessionIdUser2 = catalogManager.getUserManager().login("user2", PASSWORD);
        sessionIdUser3 = catalogManager.getUserManager().login("user3", PASSWORD);

        projectId = catalogManager.getProjectManager().create("Project about some genomes", "1000G", "", "ACME", "Homo sapiens", null, null, "GRCh38", new QueryOptions(), sessionIdUser).first().getUid();
        Project project2 = catalogManager.getProjectManager().create("Project Management Project", "pmp", "life art intelligent system", "myorg", "Homo sapiens", null, null, "GRCh38", new QueryOptions(), sessionIdUser2).first();
        Project project3 = catalogManager.getProjectManager().create("project 1", "p1", "", "", "Homo sapiens", null, null, "GRCh38", new QueryOptions(), sessionIdUser3).first();


        studyId = catalogManager.getStudyManager().create(String.valueOf(projectId), "phase1", "Phase 1", Study.Type.TRIO, null, "Done",
                null, null, null, null, null, null, null, null, sessionIdUser).first().getUid();
        studyId2 = catalogManager.getStudyManager().create(String.valueOf(projectId), "phase3", "Phase 3", Study.Type.CASE_CONTROL, null,
                "d", null, null, null, null, null, null, null, null, sessionIdUser).first().getUid();
        catalogManager.getStudyManager().create(String.valueOf(project2.getUid()), "s1", "Study 1", Study.Type.CONTROL_SET, null, "",
                null, null, null, null, null, null, null, null, sessionIdUser2).first().getUid();

        catalogManager.getFileManager().createFolder(Long.toString(studyId2), Paths.get("data/test/folder/").toString(), null, true, null, QueryOptions.empty(), sessionIdUser);


        testFolder = catalogManager.getFileManager().createFolder(Long.toString(studyId), Paths.get("data/test/folder/").toString(), null, true, null, QueryOptions.empty(), sessionIdUser).first();
        ObjectMap attributes = new ObjectMap();
        attributes.put("field", "value");
        attributes.put("numValue", 5);
        catalogManager.getFileManager().update(testFolder.getUid(), new ObjectMap("attributes", attributes), new QueryOptions(), sessionIdUser);

        QueryResult<File> queryResult2 = catalogManager.getFileManager().create(Long.toString(studyId), File.Type.FILE, File.Format.PLAIN, File.Bioformat.NONE, testFolder.getPath() + "test_1K.txt.gz", null, "", new File.FileStatus(File.FileStatus.STAGE), 0, -1, null, -1, null, null, false, null, null, sessionIdUser);

        new FileUtils(catalogManager).upload(new ByteArrayInputStream(StringUtils.randomString(1000).getBytes()), queryResult2.first(), sessionIdUser, false, false, true);


        File fileTest1k = catalogManager.getFileManager().get(queryResult2.first().getUid(), null, sessionIdUser).first();
        attributes = new ObjectMap();
        attributes.put("field", "value");
        attributes.put("name", "fileTest1k");
        attributes.put("numValue", "10");
        attributes.put("boolean", false);
        catalogManager.getFileManager().update(fileTest1k.getUid(), new ObjectMap("attributes", attributes), new QueryOptions(), sessionIdUser);

        QueryResult<File> queryResult1 = catalogManager.getFileManager().create(Long.toString(studyId), File.Type.FILE, File.Format.PLAIN, File.Bioformat.DATAMATRIX_EXPRESSION, testFolder.getPath() + "test_0.5K.txt", null, "", new File.FileStatus(File.FileStatus.STAGE), 0, -1, null, -1, null, null, false, null, null, sessionIdUser);
        new FileUtils(catalogManager).upload(new ByteArrayInputStream(StringUtils.randomString(500).getBytes()), queryResult1.first(), sessionIdUser, false, false, true);
        File fileTest05k = catalogManager.getFileManager().get(queryResult1.first().getUid(), null, sessionIdUser).first();
        attributes = new ObjectMap();
        attributes.put("field", "valuable");
        attributes.put("name", "fileTest05k");
        attributes.put("numValue", 5);
        attributes.put("boolean", true);
        catalogManager.getFileManager().update(fileTest05k.getUid(), new ObjectMap("attributes", attributes), new QueryOptions(), sessionIdUser);

        QueryResult<File> queryResult = catalogManager.getFileManager().create(Long.toString(studyId), File.Type.FILE, File.Format.IMAGE, File.Bioformat.NONE, testFolder.getPath() + "test_0.1K.png", null, "", new File.FileStatus(File.FileStatus.STAGE), 0, -1, null, -1, null, null, false, null, null, sessionIdUser);
        new FileUtils(catalogManager).upload(new ByteArrayInputStream(StringUtils.randomString(100).getBytes()), queryResult.first(), sessionIdUser, false, false, true);
        File test01k = catalogManager.getFileManager().get(queryResult.first().getUid(), null, sessionIdUser).first();
        attributes = new ObjectMap();
        attributes.put("field", "other");
        attributes.put("name", "test01k");
        attributes.put("numValue", 50);
        attributes.put("nested", new ObjectMap("num1", 45).append("num2", 33).append("text", "HelloWorld"));
        catalogManager.getFileManager().update(test01k.getUid(), new ObjectMap("attributes", attributes), new QueryOptions(), sessionIdUser);

        Set<Variable> variables = new HashSet<>();
        variables.addAll(Arrays.asList(new Variable("NAME", "", Variable.VariableType.TEXT, "", true, false, Collections.<String>emptyList(), 0, "", "", null, Collections.<String, Object>emptyMap()), new Variable("AGE", "", Variable.VariableType.DOUBLE, null, true, false, Collections.singletonList("0:130"), 1, "", "", null, Collections.<String, Object>emptyMap()), new Variable("HEIGHT", "", Variable.VariableType.DOUBLE, "1.5", false, false, Collections.singletonList("0:"), 2, "", "", null, Collections.<String, Object>emptyMap()), new Variable("ALIVE", "", Variable.VariableType.BOOLEAN, "", true, false, Collections.<String>emptyList(), 3, "", "", null, Collections.<String, Object>emptyMap()), new Variable("PHEN", "", Variable.VariableType.CATEGORICAL, "", true, false, Arrays.asList("CASE", "CONTROL"), 4, "", "", null, Collections.<String, Object>emptyMap()), new Variable("EXTRA", "", Variable.VariableType.TEXT, "", false, false, Collections.emptyList(), 5, "", "", null, Collections.<String, Object>emptyMap())));
        VariableSet vs = catalogManager.getStudyManager().createVariableSet(studyId, "vs", true, false, "", null, variables, sessionIdUser).first();


        s_1 = catalogManager.getSampleManager().create(Long.toString(studyId), "s_1", "", "", null, false, null, new HashMap<>(), null, new QueryOptions(), sessionIdUser).first().getUid();
        s_2 = catalogManager.getSampleManager().create(Long.toString(studyId), "s_2", "", "", null, false, null, new HashMap<>(), null, new QueryOptions(), sessionIdUser).first().getUid();
        s_3 = catalogManager.getSampleManager().create(Long.toString(studyId), "s_3", "", "", null, false, null, new HashMap<>(), null, new QueryOptions(), sessionIdUser).first().getUid();
        s_4 = catalogManager.getSampleManager().create(Long.toString(studyId), "s_4", "", "", null, false, null, new HashMap<>(), null, new QueryOptions(), sessionIdUser).first().getUid();
        s_5 = catalogManager.getSampleManager().create(Long.toString(studyId), "s_5", "", "", null, false, null, new HashMap<>(), null, new QueryOptions(), sessionIdUser).first().getUid();
        s_6 = catalogManager.getSampleManager().create(Long.toString(studyId), "s_6", "", "", null, false, null, new HashMap<>(), null, new QueryOptions(), sessionIdUser).first().getUid();
        s_7 = catalogManager.getSampleManager().create(Long.toString(studyId), "s_7", "", "", null, false, null, new HashMap<>(), null, new QueryOptions(), sessionIdUser).first().getUid();
        s_8 = catalogManager.getSampleManager().create(Long.toString(studyId), "s_8", "", "", null, false, null, new HashMap<>(), null, new QueryOptions(), sessionIdUser).first().getUid();
        s_9 = catalogManager.getSampleManager().create(Long.toString(studyId), "s_9", "", "", null, false, null, new HashMap<>(), null, new QueryOptions(), sessionIdUser).first().getUid();

        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_1), null, Long.toString(vs.getUid()), "annot1", new ObjectMap("NAME", "s_1").append("AGE", 6).append("ALIVE", true).append("PHEN", "CONTROL"), sessionIdUser);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_2), null, Long.toString(vs.getUid()), "annot1", new ObjectMap("NAME", "s_2").append("AGE", 10).append("ALIVE", false)

                .append("PHEN", "CASE"), sessionIdUser);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_3), null, Long.toString(vs.getUid()), "annot1", new ObjectMap("NAME", "s_3").append("AGE", 15).append("ALIVE", true).append("PHEN", "CONTROL"), sessionIdUser);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_4), null, Long.toString(vs.getUid()), "annot1", new ObjectMap("NAME", "s_4").append("AGE", 22).append("ALIVE", false)

                .append("PHEN", "CONTROL"), sessionIdUser);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_5), null, Long.toString(vs.getUid()), "annot1", new ObjectMap("NAME", "s_5").append("AGE", 29).append("ALIVE", true).append("PHEN", "CASE"), sessionIdUser);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_6), null, Long.toString(vs.getUid()), "annot2", new ObjectMap("NAME", "s_6").append("AGE", 38).append("ALIVE", true).append("PHEN", "CONTROL"), sessionIdUser);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_7), null, Long.toString(vs.getUid()), "annot2", new ObjectMap("NAME", "s_7").append("AGE", 46).append("ALIVE", false).append("PHEN", "CASE"), sessionIdUser);
        catalogManager.getSampleManager().createAnnotationSet(Long.toString(s_8), null, Long.toString(vs.getUid()), "annot2", new ObjectMap("NAME", "s_8").append("AGE", 72).append("ALIVE", true).append("PHEN", "CONTROL"), sessionIdUser);


        catalogManager.getFileManager().update(test01k.getUid(), new ObjectMap(FileDBAdaptor.QueryParams.SAMPLES.key(), Arrays.asList(s_1, s_2, s_3, s_4, s_5)), new QueryOptions(), sessionIdUser);

    }

    @After
    public void tearDown() throws Exception {
    }

    private QueryResult<File> link(URI uriOrigin, String pathDestiny, String studyIdStr, ObjectMap params, String sessionId)
            throws CatalogException, IOException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyIdStr);
        return fileManager.link(uriOrigin, pathDestiny, studyId, params, sessionId);
    }

    @Test
    public void testDeleteDataFromStudy() throws Exception {

    }

    @Test
    public void testCreateFileFromUnsharedStudy() throws CatalogException {
        try {
            catalogManager.getFileManager().create(Long.toString(studyId), File.Type.FILE, File.Format.UNKNOWN, File.Bioformat.NONE, "data/test/folder/file.txt", null, "My description", null, 0, -1, null, (long) -1, null, null, true, null, null, sessionIdUser2);
            fail("The file could be created despite not having the proper permissions.");
        } catch (CatalogAuthorizationException e) {
            assertEquals(0, catalogManager.getFileManager().get(studyId, new Query(FileDBAdaptor.QueryParams.PATH.key(),
                    "data/test/folder/file.txt"), null, sessionIdUser).getNumResults());
        }
    }

    @Test
    public void testCreateFileFromSharedStudy() throws CatalogException {
        Study.StudyAclParams aclParams = new Study.StudyAclParams("", AclParams.Action.ADD, "analyst");
        catalogManager.getStudyManager().updateAcl(Arrays.asList(Long.toString(studyId)), "user2", aclParams, sessionIdUser).get(0);
        catalogManager.getFileManager().create(Long.toString(studyId), File.Type.FILE, File.Format.UNKNOWN, File.Bioformat.NONE, "data/test/folder/file.txt", null, "My description", null, 0, -1, null, (long) -1, null, null, true, null, null, sessionIdUser2);
        assertEquals(1, catalogManager.getFileManager().get(studyId, new Query(FileDBAdaptor.QueryParams.PATH.key(),
                "data/test/folder/file.txt"), null, sessionIdUser).getNumResults());
    }

    URI getStudyURI() throws CatalogException {
        return catalogManager.getStudyManager().get(String.valueOf(studyId), 
                new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.URI.key()), sessionIdUser).first().getUri();    
    }
    
    @Test
    public void testLinkFolder() throws CatalogException, IOException {
//        // We will link the same folders that are already created in this study into another folder
        URI uri = Paths.get(getStudyURI()).resolve("data").toUri();
//        long folderId = catalogManager.searchFile(studyId, new Query(FileDBAdaptor.QueryParams.PATH.key(), "data/"), null,
//                sessionIdUser).first().getId();
//        int numFiles = catalogManager.getAllFilesInFolder(folderId, null, sessionIdUser).getNumResults();
//
//        catalogManager.link(uri, "data/", Long.toString(studyId), new ObjectMap(), sessionIdUser);
//        int numFilesAfterLink = catalogManager.getAllFilesInFolder(folderId, null, sessionIdUser).getNumResults();
//        assertEquals("Linking the same folders should not change the number of files in catalog", numFiles, numFilesAfterLink);

        // Now we try to create it into a folder that does not exist with parents = true
        link(uri, "myDirectory", Long.toString(studyId), new ObjectMap("parents", true), sessionIdUser);
        QueryResult<File> folderQueryResult = catalogManager.getFileManager().get(studyId, new Query().append(FileDBAdaptor.QueryParams
                .STUDY_UID.key(), studyId).append(FileDBAdaptor.QueryParams.PATH.key(), "myDirectory/"), null, sessionIdUser);
        assertEquals(1, folderQueryResult.getNumResults());
        assertTrue(!folderQueryResult.first().isExternal());

        folderQueryResult = catalogManager.getFileManager().get(studyId, new Query().append(FileDBAdaptor.QueryParams.STUDY_UID.key(),
                studyId).append(FileDBAdaptor.QueryParams.PATH.key(), "myDirectory/data/"), null, sessionIdUser);
        assertEquals(1, folderQueryResult.getNumResults());
        assertTrue(folderQueryResult.first().isExternal());

        folderQueryResult = catalogManager.getFileManager().get(studyId, new Query().append(FileDBAdaptor.QueryParams.STUDY_UID.key(),
                studyId).append(FileDBAdaptor.QueryParams.PATH.key(), "myDirectory/data/test/"), null, sessionIdUser);
        assertEquals(1, folderQueryResult.getNumResults());
        assertTrue(folderQueryResult.first().isExternal());
        folderQueryResult = catalogManager.getFileManager().get(studyId, new Query().append(FileDBAdaptor.QueryParams.STUDY_UID.key(),
                studyId).append(FileDBAdaptor.QueryParams.PATH.key(), "myDirectory/data/test/folder/"), null, sessionIdUser);
        assertEquals(1, folderQueryResult.getNumResults());
        assertTrue(folderQueryResult.first().isExternal());

        // Now we try to create it into a folder that does not exist with parents = false
        thrown.expect(CatalogException.class);
        thrown.expectMessage("already linked");
        link(uri, "myDirectory2", Long.toString(studyId), new ObjectMap(), sessionIdUser);
    }

    @Test
    public void testLinkFolder2() throws CatalogException, IOException {
        // We will link the same folders that are already created in this study into another folder
        URI uri = Paths.get(getStudyURI()).resolve("data").toUri();

        // Now we try to create it into a folder that does not exist with parents = false
        thrown.expect(CatalogException.class);
        thrown.expectMessage("not exist");
        link(uri, "myDirectory2", Long.toString(studyId), new ObjectMap(), sessionIdUser);
    }


    @Test
    public void testLinkFolder3() throws CatalogException, IOException {
        URI uri = Paths.get(getStudyURI()).resolve("data").toUri();
        thrown.expect(CatalogException.class);
        thrown.expectMessage("already existed and is not external");
        link(uri, null, Long.toString(studyId), new ObjectMap(), sessionIdUser);

//        // Make sure that the path of the files linked do not start with /
//        Query query = new Query(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
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
        URI uri = Paths.get(getStudyURI()).resolve("data").toUri();
        ObjectMap params = new ObjectMap("parents", true);
        QueryResult<File> allFiles = link(uri, "test/myLinkedFolder/", Long.toString(studyId), params, sessionIdUser);
        assertEquals(6, allFiles.getNumResults());

        QueryResult<File> sameAllFiles = link(uri, "test/myLinkedFolder/", Long.toString(studyId), params, sessionIdUser);
        assertEquals(allFiles.getNumResults(), sameAllFiles.getNumResults());

        List<File> result = allFiles.getResult();
        for (int i = 0; i < result.size(); i++) {
            assertEquals(allFiles.getResult().get(i).getUid(), sameAllFiles.getResult().get(i).getUid());
            assertEquals(allFiles.getResult().get(i).getPath(), sameAllFiles.getResult().get(i).getPath());
            assertEquals(allFiles.getResult().get(i).getUri(), sameAllFiles.getResult().get(i).getUri());
        }

        thrown.expect(CatalogException.class);
        thrown.expectMessage("already linked");
        link(uri, "data", Long.toString(studyId), new ObjectMap(), sessionIdUser);
    }

    @Test
    public void testLinkNormalizedUris() throws CatalogException, IOException, URISyntaxException {
        Path path = Paths.get(getStudyURI().resolve("data"));
        URI uri = new URI("file://" + path.toString() + "/../data");
        ObjectMap params = new ObjectMap("parents", true);
        QueryResult<File> allFiles = link(uri, "test/myLinkedFolder/", Long.toString(studyId), params, sessionIdUser);
        assertEquals(6, allFiles.getNumResults());
        for (File file : allFiles.getResult()) {
            assertTrue(file.getUri().isAbsolute());
            assertEquals(file.getUri().normalize(), file.getUri());
        }
    }

    @Test
    public void testLinkNonExistentFile() throws CatalogException, IOException {
        URI uri= Paths.get(getStudyURI().resolve("inexistentData")).toUri();
        ObjectMap params = new ObjectMap("parents", true);
        thrown.expect(CatalogIOException.class);
        thrown.expectMessage("does not exist");
        link(uri, "test/myLinkedFolder/", Long.toString(studyId), params, sessionIdUser);
    }

    // The VCF file that is going to be linked contains names with "." Issue: #570
    @Test
    public void testLinkFile() throws CatalogException, IOException, URISyntaxException {
        URI uri = getClass().getResource("/biofiles/variant-test-file-dot-names.vcf.gz").toURI();
        QueryResult<File> link = fileManager.link(uri, ".", studyId, new ObjectMap(), sessionIdUser);

        assertEquals(4, link.first().getSamples().size());

        List<Long> sampleList = link.first().getSamples().stream().map(Sample::getUid).collect(Collectors.toList());
        Query query = new Query(SampleDBAdaptor.QueryParams.UID.key(), sampleList);
        QueryResult<Sample> sampleQueryResult = catalogManager.getSampleManager().get(String.valueOf(studyId), query, QueryOptions.empty(),
                sessionIdUser);

        assertEquals(4, sampleQueryResult.getNumResults());
        List<String> sampleNames = sampleQueryResult.getResult().stream().map(Sample::getId).collect(Collectors.toList());
        assertTrue(sampleNames.contains("test-name.bam"));
        assertTrue(sampleNames.contains("NA19660"));
        assertTrue(sampleNames.contains("NA19661"));
        assertTrue(sampleNames.contains("NA19685"));
    }

    @Test
    public void stressTestLinkFile() throws Exception {
        URI uri = getClass().getResource("/biofiles/variant-test-file.vcf.gz").toURI();
        AtomicInteger numFailures = new AtomicInteger();
        AtomicInteger numOk = new AtomicInteger();
        int numThreads = 10;
        int numOperations = 250;

        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

        for (int i = 0; i < numOperations; i++) {
            executorService.submit(() -> {
                try {
                    fileManager.link(uri, ".", studyId, new ObjectMap(), sessionIdUser);
                    numOk.incrementAndGet();
                } catch (Exception ignore) {
                    ignore.printStackTrace();
                    numFailures.incrementAndGet();
                }
            });

        }
        executorService.awaitTermination(1, TimeUnit.SECONDS);
        executorService.shutdown();

        int unexecuted = executorService.shutdownNow().size();
        System.out.println("Do not execute " + unexecuted + " tasks!");
        System.out.println("numFailures = " + numFailures);
        System.out.println("numOk.get() = " + numOk.get());

        assertEquals(numOperations, numOk.get());
    }

    @Test
    public void testUnlinkFolder() throws CatalogException, IOException {
        URI uri = Paths.get(getStudyURI()).resolve("data").toUri();
        link(uri, "myDirectory", Long.toString(studyId), new ObjectMap("parents", true), sessionIdUser);

        CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(uri);

        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.PATH.key(), "~myDirectory/*")
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.READY);
        QueryResult<File> fileQueryResultLinked = catalogManager.getFileManager().get(studyId, query, null, sessionIdUser);

        System.out.println("Number of files/folders linked = " + fileQueryResultLinked.getNumResults());

        // Now we try to unlink them
        catalogManager.getFileManager().unlink(Long.toString(studyId), "myDirectory/data/", sessionIdUser);
        fileQueryResultLinked = catalogManager.getFileManager().get(studyId, query, null, sessionIdUser);
        assertEquals(1, fileQueryResultLinked.getNumResults());

        query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.PATH.key(), "~myDirectory/*")
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.REMOVED);
        QueryResult<File> fileQueryResultUnlinked = catalogManager.getFileManager().get(studyId, query, null, sessionIdUser);
        assertEquals(6, fileQueryResultUnlinked.getNumResults());

        String myPath = "myDirectory/data" + AbstractManager.INTERNAL_DELIMITER + "REMOVED";
        for (File file : fileQueryResultUnlinked.getResult()) {
            assertTrue("File name should have been modified", file.getPath().contains(myPath));
            assertEquals("Status should be to REMOVED", File.FileStatus.REMOVED, file.getStatus().getName());
            assertEquals("Name should not have changed", file.getName(), file.getName());
            assertTrue("File uri: " + file.getUri() + " should exist", ioManager.exists(file.getUri()));
        }
    }

    @Test
    public void testUnlinkFile() throws CatalogException, IOException {
        URI uri = Paths.get(getStudyURI()).resolve("data").toUri();
        link(uri, "myDirectory", Long.toString(studyId), new ObjectMap("parents", true), sessionIdUser);

        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.PATH.key(), "~myDirectory/*")
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.READY);
        QueryResult<File> fileQueryResultLinked = catalogManager.getFileManager().get(studyId, query, null, sessionIdUser);

        System.out.println("Number of files/folders linked = " + fileQueryResultLinked.getNumResults());

        // Now we try to unlink the file
        catalogManager.getFileManager().unlink(Long.toString(studyId), "myDirectory/data/test/folder/test_0.5K.txt", sessionIdUser);
        fileQueryResultLinked = catalogManager.getFileManager().get(35L, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, fileQueryResultLinked.getNumResults());
        assertTrue(fileQueryResultLinked.first().getPath().contains(AbstractManager.INTERNAL_DELIMITER + "REMOVED"));
        assertEquals(fileQueryResultLinked.first().getPath().indexOf(AbstractManager.INTERNAL_DELIMITER + "REMOVED"),
                fileQueryResultLinked.first().getPath().lastIndexOf(AbstractManager.INTERNAL_DELIMITER + "REMOVED"));

        fileQueryResultLinked = catalogManager.getFileManager().get(35L, QueryOptions.empty(), sessionIdUser);
        // We check REMOVED is only contained once in the path
        assertEquals(fileQueryResultLinked.first().getPath().indexOf(AbstractManager.INTERNAL_DELIMITER + "REMOVED"),
                fileQueryResultLinked.first().getPath().lastIndexOf(AbstractManager.INTERNAL_DELIMITER + "REMOVED"));

        // We send the unlink command again
        thrown.expect(CatalogException.class);
        thrown.expectMessage("already deleted or unlinked");
        catalogManager.getFileManager().unlink(Long.toString(studyId), "35", sessionIdUser);
    }

    @Test
    public void testCreateFile() throws CatalogException, IOException {
        Query query = new Query(ProjectDBAdaptor.QueryParams.USER_ID.key(), "user2");
        long projectId = catalogManager.getProjectManager().get(query, null, sessionIdUser2).first().getUid();
        Study study = catalogManager.getStudyManager().get(new Query(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectId), null,
                sessionIdUser2).first();

        String content = "This is the content\tof the file";
        try {
            fileManager.create(Long.toString(study.getUid()), File.Type.FILE, File.Format.UNKNOWN, File.Bioformat.UNKNOWN,
                    "data/test/myTest/myFile.txt", null, null, new File.FileStatus(File.FileStatus.READY), 0, -1, null, -1,
                    null, null,
                    false, "This is the content\tof the file", null, sessionIdUser2);
            fail("An error should be raised because parents is false");
        } catch (CatalogException e) {
            System.out.println("Correct");
        }

        QueryResult<File> fileQueryResult = fileManager.create(Long.toString(study.getUid()), File.Type.FILE, File.Format.UNKNOWN,
                File.Bioformat.UNKNOWN, "data/test/myTest/myFile.txt", null, null,
                new File.FileStatus(File.FileStatus.READY), 0, -1, null, -1, null, null, true, content, null, sessionIdUser2);
        CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(fileQueryResult.first().getUri());
        assertTrue(ioManager.exists(fileQueryResult.first().getUri()));

        DataInputStream fileObject = ioManager.getFileObject(fileQueryResult.first().getUri(), -1, -1);
        assertEquals(content, fileObject.readLine());
    }

    @Test
    public void testCreateFolder() throws Exception {
        Query query = new Query(ProjectDBAdaptor.QueryParams.USER_ID.key(), "user2");
        long projectId = catalogManager.getProjectManager().get(query, null, sessionIdUser2).first().getUid();
        long studyId = catalogManager.getStudyManager().get(new Query(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectId), null, sessionIdUser2).first().getUid();


        Set<String> paths = catalogManager.getFileManager().get(studyId, new Query("type", File.Type.DIRECTORY), new QueryOptions(), sessionIdUser2)
                .getResult().stream().map(File::getPath).collect(Collectors.toSet());
        assertEquals(1, paths.size());
        assertTrue(paths.contains(""));             //root
//        assertTrue(paths.contains("data/"));        //data
//        assertTrue(paths.contains("analysis/"));    //analysis

        Path folderPath = Paths.get("data", "new", "folder");
        File folder = catalogManager.getFileManager().createFolder(Long.toString(studyId), folderPath.toString(), null, true, null,
                QueryOptions.empty(), sessionIdUser2).first();
        System.out.println(folder);
        CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(folder.getUri());
        assertTrue(ioManager.exists(folder.getUri()));

        paths = catalogManager.getFileManager().get(studyId, new Query(FileDBAdaptor.QueryParams.TYPE.key(), File.Type.DIRECTORY), new
                QueryOptions(), sessionIdUser2).getResult().stream().map(File::getPath).collect(Collectors.toSet());
        assertEquals(4, paths.size());
        assertTrue(paths.contains("data/new/"));
        assertTrue(paths.contains("data/new/folder/"));

        URI uri = catalogManager.getFileManager().getUri(folder);
        assertTrue(catalogManager.getCatalogIOManagerFactory().get(uri).exists(uri));

        folder = catalogManager.getFileManager().createFolder(Long.toString(studyId), Paths.get("WOLOLO").toString(), null, true,
                null, QueryOptions.empty(), sessionIdUser2).first();

        Path myStudy = Files.createDirectory(catalogManagerResource.getOpencgaHome().resolve("myStudy"));
        long id = catalogManager.getStudyManager().create(String.valueOf(projectId), "alias", "name", Study.Type.CASE_CONTROL, "", "",
                null, null, null, myStudy.toUri(), null, null, null, null, sessionIdUser2).first().getUid();
        System.out.println("studyId = " + id);
        folder = catalogManager.getFileManager().createFolder(Long.toString(id), Paths.get("WOLOLO").toString(), null, true, null,
                QueryOptions.empty(), sessionIdUser2).first();
        System.out.println("folder = " + folder);
        System.out.println(catalogManager.getFileManager().getUri(folder));

    }

    @Test
    public void testCreateFolderAlreadyExists() throws Exception {
        Query query = new Query(ProjectDBAdaptor.QueryParams.USER_ID.key(), "user2");
        long projectId = catalogManager.getProjectManager().get(query, null, sessionIdUser2).first().getUid();
        long studyId = catalogManager.getStudyManager().get(new Query(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectId), null, sessionIdUser2).first().getUid();

        Set<String> paths = catalogManager.getFileManager().get(studyId, new Query("type", File.Type.DIRECTORY), new QueryOptions(), sessionIdUser2)
                .getResult().stream().map(File::getPath).collect(Collectors.toSet());
        assertEquals(1, paths.size());
        assertTrue(paths.contains(""));             //root
//        assertTrue(paths.contains("data/"));        //data
//        assertTrue(paths.contains("analysis/"));    //analysis

        Path folderPath = Paths.get("data", "new", "folder");
        File folder = catalogManager.getFileManager().createFolder(Long.toString(studyId), folderPath.toString(), null, true, null, null,
                sessionIdUser2).first();

        assertNotNull(folder);
        assertTrue(folder.getPath().contains(folderPath.toString()));

        // When creating the same folder, we should not complain and return it directly
        File sameFolder = catalogManager.getFileManager().createFolder(Long.toString(studyId), folderPath.toString(), null, true,
                null, null, sessionIdUser2).first();
        assertNotNull(sameFolder);
        assertEquals(folder.getPath(), sameFolder.getPath());
        assertEquals(folder.getUid(), sameFolder.getUid());

        // However, a user without create permissions will receive an exception
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFileManager().createFolder(Long.toString(studyId), folderPath.toString(), null, true, null, null,
                sessionIdUser3);
    }

    @Test
    public void testCreateAndUpload() throws Exception {
        long studyId = catalogManager.getStudyManager().getId("user", "1000G:phase1");
        long studyId2 = catalogManager.getStudyManager().getId("user", "1000G:phase3");

        FileUtils catalogFileUtils = new FileUtils(catalogManager);

        java.io.File fileTest;

        String fileName = "item." + TimeUtils.getTimeMillis() + ".vcf";
        QueryResult<File> fileResult = catalogManager.getFileManager().create(Long.toString(studyId), File.Type.FILE, File.Format.PLAIN,
                File.Bioformat.VARIANT, "data/" + fileName, null, "description", null, 0, -1, null, (long) -1, null, null, true, null,
                null, sessionIdUser);

        fileTest = createDebugFile();
        catalogFileUtils.upload(fileTest.toURI(), fileResult.first(), null, sessionIdUser, false, false, true, true);
        assertTrue("File deleted", !fileTest.exists());

        fileName = "item." + TimeUtils.getTimeMillis() + ".vcf";
        fileResult = catalogManager.getFileManager().create(Long.toString(studyId), File.Type.FILE, File.Format.PLAIN, File.Bioformat
                .VARIANT, "data/" + fileName, null, "description", null, 0, -1, null, (long) -1, null, null, true, null, null, sessionIdUser);
        fileTest = createDebugFile();
        catalogFileUtils.upload(fileTest.toURI(), fileResult.first(), null, sessionIdUser, false, false, false, true);
        assertTrue("File don't deleted", fileTest.exists());
        assertTrue(fileTest.delete());

        fileName = "item." + TimeUtils.getTimeMillis() + ".txt";
        QueryResult<File> queryResult = catalogManager.getFileManager().create(Long.toString(studyId), File.Type.FILE, File.Format.PLAIN, File.Bioformat.NONE, "data/" + fileName, null, "description", new File.FileStatus(File.FileStatus.STAGE), 0, -1, null, -1, null, null, true, null, null, sessionIdUser);
        new FileUtils(catalogManager).upload(new ByteArrayInputStream(StringUtils.randomString(200).getBytes()), queryResult.first(), sessionIdUser, false, false, true);
        fileResult = catalogManager.getFileManager().get(queryResult.first().getUid(), null, sessionIdUser);
        assertTrue("", fileResult.first().getStatus().getName().equals(File.FileStatus.READY));
        assertTrue("", fileResult.first().getSize() == 200);

        fileName = "item." + TimeUtils.getTimeMillis() + ".vcf";
        fileTest = createDebugFile();
        QueryResult<File> fileQueryResult = catalogManager.getFileManager().create(Long.toString(studyId2), File.Type.FILE, File.Format
                .PLAIN, File.Bioformat.VARIANT, "data/deletable/folder/" + fileName, null, "description", null, 0, -1, null, (long) -1,
                null, null, true, null, null, sessionIdUser);
        catalogFileUtils.upload(fileTest.toURI(), fileQueryResult.first(), null, sessionIdUser, false, false, true, true);
        assertFalse("File deleted by the upload", fileTest.delete());

        fileName = "item." + TimeUtils.getTimeMillis() + ".vcf";
        fileTest = createDebugFile();
        fileQueryResult = catalogManager.getFileManager().create(Long.toString(studyId2), File.Type.FILE, File.Format.PLAIN, File
                .Bioformat.VARIANT, "data/deletable/" + fileName, null, "description", null, 0, -1, null, (long) -1, null, null, true, null, null, sessionIdUser);
        catalogFileUtils.upload(fileTest.toURI(), fileQueryResult.first(), null, sessionIdUser, false, false, false, true);
        assertTrue(fileTest.delete());

        fileName = "item." + TimeUtils.getTimeMillis() + ".vcf";
        fileTest = createDebugFile();
        fileQueryResult = catalogManager.getFileManager().create(Long.toString(studyId2), File.Type.FILE, File.Format.PLAIN, File
                .Bioformat.VARIANT, "" + fileName, null, "file at root", null, 0, -1, null, (long) -1, null, null, true, null, null, sessionIdUser);
        catalogFileUtils.upload(fileTest.toURI(), fileQueryResult.first(), null, sessionIdUser, false, false, false, true);
        assertTrue(fileTest.delete());

        fileName = "item." + TimeUtils.getTimeMillis() + ".vcf";
        fileTest = createDebugFile();
        long size = Files.size(fileTest.toPath());
        QueryResult<File> queryResult1 = catalogManager.getFileManager().create(Long.toString(studyId2), File.Type.FILE, File.Format.PLAIN, File.Bioformat.VARIANT, "" + fileName, null, "file at root", new File.FileStatus(File.FileStatus.STAGE), 0, -1, null, -1, null, null, true, null, null, sessionIdUser);

        new FileUtils(catalogManager).upload(fileTest.toURI(), queryResult1.first(), null, sessionIdUser, false, false, true, true, Long.MAX_VALUE);

        fileQueryResult = catalogManager.getFileManager().get(queryResult1.first().getUid(), null, sessionIdUser);
        assertTrue("File should be moved", !fileTest.exists());
        assertTrue(fileQueryResult.first().getSize() == size);
    }

    @Test
    public void testCreateFileInLinkedFolder() throws Exception {
        // Create an empty folder
        Path dir = catalogManagerResource.getOpencgaHome().resolve("folder_to_link");
        Files.createDirectory(dir);
        URI uri = dir.toUri();

        // Link the folder in the root
        link(uri, "", Long.toString(studyId), new ObjectMap(), sessionIdUser);

        File file = catalogManager.getFileManager().create(Long.toString(studyId), File.Type.FILE, File.Format.PLAIN, File.Bioformat
                .NONE, "folder_to_link/file.txt", null, "", null, 0, -1, null, (long) -1, null, null, false, null, null, sessionIdUser).first();

        assertEquals(uri.resolve("file.txt"), file.getUri());

    }

    @Test
    public void testDownloadAndHeadFile() throws CatalogException, IOException, InterruptedException {
        FileUtils catalogFileUtils = new FileUtils(catalogManager);

        String fileName = "item." + TimeUtils.getTimeMillis() + ".vcf";
        java.io.File fileTest;
        InputStream is = new FileInputStream(fileTest = createDebugFile());
        File file = catalogManager.getFileManager().create(Long.toString(studyId), File.Type.FILE, File.Format.PLAIN, File.Bioformat
                .VARIANT, "data/" + fileName, null, "description", null, 0, -1, null, (long) -1, null, null, true, null, null, sessionIdUser).first();
        catalogFileUtils.upload(is, file, sessionIdUser, false, false, true);
        is.close();


        byte[] bytes = new byte[100];
        byte[] bytesOrig = new byte[100];
        DataInputStream fis = new DataInputStream(new FileInputStream(fileTest));
        DataInputStream dis = catalogManager.getFileManager().download(file.getUid(), -1, -1, null, sessionIdUser);
        fis.read(bytesOrig, 0, 100);
        dis.read(bytes, 0, 100);
        fis.close();
        dis.close();
        assertArrayEquals(bytesOrig, bytes);


        int offset = 5;
        int limit = 30;
        dis = catalogManager.getFileManager().download(file.getUid(), offset, limit, null, sessionIdUser);
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
        long studyId = catalogManager.getStudyManager().getId("user", "1000G:phase1");

        String fileName = "item." + TimeUtils.getTimeMillis() + ".vcf";
        int fileSize = 200;
        byte[] bytesOrig = StringUtils.randomString(fileSize).getBytes();
        QueryResult<File> queryResult = catalogManager.getFileManager().create(Long.toString(studyId), File.Type.FILE, File.Format.PLAIN, File.Bioformat.NONE, "data/" + fileName, null, "description", new File.FileStatus(File.FileStatus.STAGE), 0, -1, null, -1, null, null, true, null, null, sessionIdUser);
        new FileUtils(catalogManager).upload(new ByteArrayInputStream(bytesOrig), queryResult.first(), sessionIdUser, false, false, true);
        File file = catalogManager.getFileManager().get(queryResult.first().getUid(), null, sessionIdUser).first();

        DataInputStream dis = catalogManager.getFileManager().download(file.getUid(), -1, -1, null, sessionIdUser);

        byte[] bytes = new byte[fileSize];
        dis.read(bytes, 0, fileSize);
        assertTrue(Arrays.equals(bytesOrig, bytes));

    }

    @Test
    public void testGetTreeView() throws CatalogException {
        QueryResult<FileTree> fileTree = catalogManager.getFileManager().getTree("/", "user@1000G:phase1", new Query(), new QueryOptions(),
                5, sessionIdUser);
        assertEquals(7, fileTree.getNumResults());
    }

    @Test
    public void testGetTreeViewMoreThanOneFile() throws CatalogException {

        // Create a new study so more than one file will be found under the root /. However, it should be able to consider the study given
        // properly
        catalogManager.getStudyManager().create(String.valueOf(projectId), "phase2", "Phase 2", Study.Type.TRIO, null, "Done", null,
                null, null, null, null, null, null, null, sessionIdUser).first().getUid();

        QueryResult<FileTree> fileTree = catalogManager.getFileManager().getTree("/", "user@1000G:phase1", new Query(), new QueryOptions(),
                5, sessionIdUser);
        assertEquals(7, fileTree.getNumResults());

        fileTree = catalogManager.getFileManager().getTree(".", "user@1000G:phase2", new Query(), new QueryOptions(), 5, sessionIdUser);
        assertEquals(1, fileTree.getNumResults());
    }

    @Test
    public void renameFileTest() throws CatalogException, IOException {
        String userId = catalogManager.getUserManager().getUserId(sessionIdUser);
        long studyId = catalogManager.getStudyManager().getId(userId, "user@1000G:phase1");
        QueryResult<File> queryResult1 = catalogManager.getFileManager().create(Long.toString(studyId), File.Type.FILE, File.Format.PLAIN, File.Bioformat.NONE, "data/file.txt", null, "description", new File.FileStatus(File.FileStatus.STAGE), 0, -1, null, -1, null, null, true, null, null, sessionIdUser);
        new FileUtils(catalogManager).upload(new ByteArrayInputStream(StringUtils.randomString(200).getBytes()), queryResult1.first(), sessionIdUser, false, false, true);
        catalogManager.getFileManager().get(queryResult1.first().getUid(), null, sessionIdUser);
        QueryResult<File> queryResult = catalogManager.getFileManager().create(Long.toString(studyId), File.Type.FILE, File.Format.PLAIN, File.Bioformat.NONE, "data/nested/folder/file2.txt", null, "description", new File.FileStatus(File.FileStatus.STAGE), 0, -1, null, -1, null, null, true, null, null, sessionIdUser);
        new FileUtils(catalogManager).upload(new ByteArrayInputStream(StringUtils.randomString(200).getBytes()), queryResult.first(), sessionIdUser, false, false, true);
        catalogManager.getFileManager().get(queryResult.first().getUid(), null, sessionIdUser);

        catalogManager.getFileManager().rename(catalogManager.getFileManager().getUid("data/nested/", "user@1000G:phase1", sessionIdUser)
                        .getResourceId(), "nested2",
                sessionIdUser);
        Set<String> paths = catalogManager.getFileManager().get(studyId, new Query(), new QueryOptions(), sessionIdUser).getResult()
                .stream().map(File::getPath).collect(Collectors.toSet());

        assertTrue(paths.contains("data/nested2/"));
        assertFalse(paths.contains("data/nested/"));
        assertTrue(paths.contains("data/nested2/folder/"));
        assertTrue(paths.contains("data/nested2/folder/file2.txt"));
        assertTrue(paths.contains("data/file.txt"));

        catalogManager.getFileManager().rename(catalogManager.getFileManager().getUid("data/", "user@1000G:phase1", sessionIdUser).getResourceId(), "Data",
                sessionIdUser);
        paths = catalogManager.getFileManager().get(studyId, new Query(), new QueryOptions(), sessionIdUser).getResult()
                .stream().map(File::getPath).collect(Collectors.toSet());

        assertTrue(paths.contains("Data/"));
        assertTrue(paths.contains("Data/file.txt"));
        assertTrue(paths.contains("Data/nested2/"));
        assertTrue(paths.contains("Data/nested2/folder/"));
        assertTrue(paths.contains("Data/nested2/folder/file2.txt"));
    }

    @Test
    public void getFileIdByString() throws CatalogException {
        Study.StudyAclParams aclParams = new Study.StudyAclParams("", AclParams.Action.ADD, "analyst");
        catalogManager.getStudyManager().updateAcl(Arrays.asList(Long.toString(studyId)), "user2", aclParams, sessionIdUser).get(0);
        File file = catalogManager.getFileManager().create(Long.toString(studyId), File.Type.FILE, File.Format.UNKNOWN, File.Bioformat
                .NONE, "data/test/folder/file.txt", null, "My description", null, 0, -1, null, (long) -1, null, null, true, null, null, sessionIdUser2).first();
        long fileId = catalogManager.getFileManager().getUid(file.getPath(), Long.toString(studyId), sessionIdUser).getResourceId();
        assertEquals(file.getUid(), fileId);

        fileId = catalogManager.getFileManager().getUid(Long.toString(file.getUid()), Long.toString(studyId), sessionIdUser).getResourceId();
        assertEquals(file.getUid(), fileId);

        fileId = catalogManager.getFileManager().getUid("/", Long.toString(studyId), sessionIdUser).getResourceId();
        System.out.println(fileId);
    }

    @Test
    public void renameFileEmptyName() throws CatalogException {
        thrown.expect(CatalogParameterException.class);
        thrown.expectMessage(containsString("null or empty"));

        long fileId = catalogManager.getFileManager().getUid("data/", "user@1000G:phase1", sessionIdUser).getResourceId();
        catalogManager.getFileManager().rename(fileId, "", sessionIdUser);
    }

    @Test
    public void renameFileSlashInName() throws CatalogException {
        thrown.expect(CatalogParameterException.class);
        long fileId = catalogManager.getFileManager().getUid("data/", "user@1000G:phase1", sessionIdUser).getResourceId();
        catalogManager.getFileManager().rename(fileId, "my/folder", sessionIdUser);
    }

    @Test
    public void renameFileAlreadyExists() throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionIdUser);
        long studyId = catalogManager.getStudyManager().getId(userId, "user@1000G:phase1");
        catalogManager.getFileManager().createFolder(Long.toString(studyId), "analysis/", new File.FileStatus(), false, "",
                new QueryOptions(), sessionIdUser);
        thrown.expect(CatalogIOException.class);
        catalogManager.getFileManager().rename(catalogManager.getFileManager().getUid("data/", "user@1000G:phase1", sessionIdUser).getResourceId(), "analysis",
                sessionIdUser);
    }

    @Test
    public void searchFileTest() throws CatalogException, IOException {

        long studyId = catalogManager.getStudyManager().getId("user", "1000G:phase1");

        Query query;
        QueryResult<File> result;

        query = new Query(FileDBAdaptor.QueryParams.NAME.key(), "~data");
        result = catalogManager.getFileManager().get(studyId, query, null, sessionIdUser);
        assertEquals(1, result.getNumResults());

        //Get all files in data
        query = new Query(FileDBAdaptor.QueryParams.PATH.key(), "~data/[^/]+/?")
                .append(FileDBAdaptor.QueryParams.TYPE.key(),"FILE");
        result = catalogManager.getFileManager().get(studyId, query, null, sessionIdUser);
        assertEquals(3, result.getNumResults());

        //Folder "jobs" does not exist
        query = new Query(FileDBAdaptor.QueryParams.DIRECTORY.key(), "jobs");
        result = catalogManager.getFileManager().get(studyId, query, null, sessionIdUser);
        assertEquals(0, result.getNumResults());

        //Get all files in data
        query = new Query(FileDBAdaptor.QueryParams.DIRECTORY.key(), "data/");
        result = catalogManager.getFileManager().get(studyId, query, null, sessionIdUser);
        assertEquals(1, result.getNumResults());

        //Get all files in data recursively
        query = new Query(FileDBAdaptor.QueryParams.DIRECTORY.key(), "data/.*");
        result = catalogManager.getFileManager().get(studyId, query, null, sessionIdUser);
        assertEquals(5, result.getNumResults());

        query = new Query(FileDBAdaptor.QueryParams.TYPE.key(), "FILE");
        result = catalogManager.getFileManager().get(studyId, query, null, sessionIdUser);
        result.getResult().forEach(f -> assertEquals(File.Type.FILE, f.getType()));
        int numFiles = result.getNumResults();
        assertEquals(3, numFiles);

        query = new Query(FileDBAdaptor.QueryParams.TYPE.key(), "DIRECTORY");
        result = catalogManager.getFileManager().get(studyId, query, null, sessionIdUser);
        result.getResult().forEach(f -> assertEquals(File.Type.DIRECTORY, f.getType()));
        int numFolders = result.getNumResults();
        assertEquals(4, numFolders);

        query = new Query(FileDBAdaptor.QueryParams.PATH.key(), "");
        result = catalogManager.getFileManager().get(studyId, query, null, sessionIdUser);
        assertEquals(1, result.getNumResults());
        assertEquals(".", result.first().getName());


        query = new Query(FileDBAdaptor.QueryParams.TYPE.key(), "FILE,DIRECTORY");
        result = catalogManager.getFileManager().get(studyId, query, null, sessionIdUser);
        assertEquals(7, result.getNumResults());
        assertEquals(numFiles + numFolders, result.getNumResults());

        query = new Query("type", "FILE");
        query.put("size", ">400");
        result = catalogManager.getFileManager().get(studyId, query, null, sessionIdUser);
        assertEquals(2, result.getNumResults());

        query = new Query("type", "FILE");
        query.put("size", "<400");
        result = catalogManager.getFileManager().get(studyId, query, null, sessionIdUser);
        assertEquals(1, result.getNumResults());

        List<Long> sampleIds = catalogManager.getSampleManager().get(studyId, new Query("name", "s_1,s_3,s_4"), null, sessionIdUser)
                .getResult().stream().map(Sample::getUid).collect(Collectors.toList());
        result = catalogManager.getFileManager().get(studyId, new Query(FileDBAdaptor.QueryParams.SAMPLES.key(), sampleIds), null,
                sessionIdUser);
        assertEquals(1, result.getNumResults());

        query = new Query("type", "FILE");
        query.put("format", "PLAIN");
        result = catalogManager.getFileManager().get(studyId, query, null, sessionIdUser);
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
        result = catalogManager.getFileManager().get(studyId, new Query(attributes + ".nested.text", "~H"), null, sessionIdUser);
        assertEquals(1, result.getNumResults());
        result = catalogManager.getFileManager().get(studyId, new Query(nattributes + ".nested.num1", ">0"), null, sessionIdUser);
        assertEquals(1, result.getNumResults());
        result = catalogManager.getFileManager().get(studyId, new Query(attributes + ".nested.num1", ">0"), null, sessionIdUser);
        assertEquals(0, result.getNumResults());

        result = catalogManager.getFileManager().get(studyId, new Query(attributes + ".nested.num1", "notANumber"), null, sessionIdUser);
        assertEquals(0, result.getNumResults());

        result = catalogManager.getFileManager().get(studyId, new Query(attributes + ".field", "~val"), null, sessionIdUser);
        assertEquals(3, result.getNumResults());

        result = catalogManager.getFileManager().get(studyId, new Query("attributes.field", "~val"), null, sessionIdUser);
        assertEquals(3, result.getNumResults());

        result = catalogManager.getFileManager().get(studyId, new Query(attributes + ".field", "=~val"), null, sessionIdUser);
        assertEquals(3, result.getNumResults());

        result = catalogManager.getFileManager().get(studyId, new Query(attributes + ".field", "~val"), null, sessionIdUser);
        assertEquals(3, result.getNumResults());

        result = catalogManager.getFileManager().get(studyId, new Query(attributes + ".field", "value"), null, sessionIdUser);
        assertEquals(2, result.getNumResults());

        result = catalogManager.getFileManager().get(studyId, new Query(attributes + ".field", "other"), null, sessionIdUser);
        assertEquals(1, result.getNumResults());

        result = catalogManager.getFileManager().get(studyId, new Query("nattributes.numValue", ">=5"), null, sessionIdUser);
        assertEquals(3, result.getNumResults());

        result = catalogManager.getFileManager().get(studyId, new Query("nattributes.numValue", ">4,<6"), null, sessionIdUser);
        assertEquals(3, result.getNumResults());

        result = catalogManager.getFileManager().get(studyId, new Query(nattributes + ".numValue", "==5"), null, sessionIdUser);
        assertEquals(2, result.getNumResults());

        result = catalogManager.getFileManager().get(studyId, new Query(nattributes + ".numValue", "==5.0"), null, sessionIdUser);
        assertEquals(2, result.getNumResults());

        result = catalogManager.getFileManager().get(studyId, new Query(nattributes + ".numValue", "=5.0"), null, sessionIdUser);
        assertEquals(2, result.getNumResults());

        result = catalogManager.getFileManager().get(studyId, new Query(nattributes + ".numValue", "5.0"), null, sessionIdUser);
        assertEquals(2, result.getNumResults());

        result = catalogManager.getFileManager().get(studyId, new Query(nattributes + ".numValue", ">5"), null, sessionIdUser);
        assertEquals(1, result.getNumResults());

        result = catalogManager.getFileManager().get(studyId, new Query(nattributes + ".numValue", ">4"), null, sessionIdUser);
        assertEquals(3, result.getNumResults());

        result = catalogManager.getFileManager().get(studyId, new Query(nattributes + ".numValue", "<6"), null, sessionIdUser);
        assertEquals(2, result.getNumResults());

        result = catalogManager.getFileManager().get(studyId, new Query(nattributes + ".numValue", "<=5"), null, sessionIdUser);
        assertEquals(2, result.getNumResults());

        result = catalogManager.getFileManager().get(studyId, new Query(nattributes + ".numValue", "<5"), null, sessionIdUser);
        assertEquals(0, result.getNumResults());

        result = catalogManager.getFileManager().get(studyId, new Query(nattributes + ".numValue", "<2"), null, sessionIdUser);
        assertEquals(0, result.getNumResults());

        result = catalogManager.getFileManager().get(studyId, new Query(nattributes + ".numValue", "==23"), null, sessionIdUser);
        assertEquals(0, result.getNumResults());

        result = catalogManager.getFileManager().get(studyId, new Query(attributes + ".numValue", "=~10"), null, sessionIdUser);
        assertEquals(1, result.getNumResults());

        result = catalogManager.getFileManager().get(studyId, new Query(nattributes + ".numValue", "=10"), null, sessionIdUser);
        assertEquals(0, result.getNumResults());

        result = catalogManager.getFileManager().get(studyId, new Query(attributes + ".boolean", "true"), null, sessionIdUser);
        assertEquals(0, result.getNumResults());

        result = catalogManager.getFileManager().get(studyId, new Query(attributes + ".boolean", "=true"), null, sessionIdUser);
        assertEquals(0, result.getNumResults());

        result = catalogManager.getFileManager().get(studyId, new Query(attributes + ".boolean", "=1"), null, sessionIdUser);
        assertEquals(0, result.getNumResults());

        result = catalogManager.getFileManager().get(studyId, new Query(battributes + ".boolean", "true"), null, sessionIdUser);
        assertEquals(1, result.getNumResults());

        result = catalogManager.getFileManager().get(studyId, new Query(battributes + ".boolean", "=true"), null, sessionIdUser);
        assertEquals(1, result.getNumResults());

        // This has to return not only the ones with the attribute boolean = false, but also all the files that does not contain
        // that attribute at all.
        result = catalogManager.getFileManager().get(studyId, new Query(battributes + ".boolean", "!=true"), null, sessionIdUser);
        assertEquals(6, result.getNumResults());

        result = catalogManager.getFileManager().get(studyId, new Query(battributes + ".boolean", "=false"), null, sessionIdUser);
        assertEquals(1, result.getNumResults());

        query = new Query();
        query.append(attributes + ".name", "fileTest1k");
        query.append(attributes + ".field", "value");
        result = catalogManager.getFileManager().get(studyId, query, null, sessionIdUser);
        assertEquals(1, result.getNumResults());

        query = new Query();
        query.append(attributes + ".name", "fileTest1k");
        query.append(attributes + ".field", "value");
        query.append(attributes + ".numValue", Arrays.asList(8, 9, 10));   //Searching as String. numValue = "10"
        result = catalogManager.getFileManager().get(studyId, query, null, sessionIdUser);
        assertEquals(1, result.getNumResults());

    }

    @Test
    public void testSearchFileBoolean() throws CatalogException {
        long studyId = catalogManager.getStudyManager().getId("user", "1000G:phase1");

        Query query;
        QueryResult<File> result;
        FileDBAdaptor.QueryParams battributes = FileDBAdaptor.QueryParams.BATTRIBUTES;

        query = new Query(battributes.key() + ".boolean", "true");       //boolean in [true]
        result = catalogManager.getFileManager().get(studyId, query, null, sessionIdUser);
        assertEquals(1, result.getNumResults());

        query = new Query(battributes.key() + ".boolean", "false");      //boolean in [false]
        result = catalogManager.getFileManager().get(studyId, query, null, sessionIdUser);
        assertEquals(1, result.getNumResults());

        query = new Query(battributes.key() + ".boolean", "!=false");    //boolean in [null, true]
        query.put("type", "FILE");
        result = catalogManager.getFileManager().get(studyId, query, null, sessionIdUser);
        assertEquals(2, result.getNumResults());

        query = new Query(battributes.key() + ".boolean", "!=true");     //boolean in [null, false]
        query.put("type", "FILE");
        result = catalogManager.getFileManager().get(studyId, query, null, sessionIdUser);
        assertEquals(2, result.getNumResults());
    }

    @Test
    public void testSearchFileFail1() throws CatalogException {
        long studyId = catalogManager.getStudyManager().getId("user", "1000G:phase1");
        thrown.expect(CatalogDBException.class);
        catalogManager.getFileManager().get(studyId, new Query(FileDBAdaptor.QueryParams.NATTRIBUTES.key() + ".numValue",
                "==NotANumber"), null, sessionIdUser);
    }

    @Test
    public void testSearchFileFail3() throws CatalogException {
        long studyId = catalogManager.getStudyManager().getId("user", "1000G:phase1");
        thrown.expect(CatalogDBException.class);
        catalogManager.getFileManager().get(studyId, new Query("id", "~5"), null, sessionIdUser);
    }

    @Test
    public void testGetFileParent() throws CatalogException, IOException {

        long fileId;
        fileId = catalogManager.getFileManager().getUid("data/test/folder/", "user@1000G:phase1", sessionIdUser).getResourceId();
        QueryResult<File> fileParent = catalogManager.getFileManager().getParent(fileId, null, sessionIdUser);
        assertEquals("data/test/", fileParent.first().getPath());

        fileId = catalogManager.getFileManager().getUid("data/", "user@1000G:phase1", sessionIdUser).getResourceId();
        fileParent = catalogManager.getFileManager().getParent(fileId, null, sessionIdUser);
        assertEquals("", fileParent.first().getPath());
        assertEquals(".", fileParent.first().getName());

        fileParent = catalogManager.getFileManager().getParent(fileParent.first().getUid(), null, sessionIdUser);
        assertEquals("", fileParent.first().getPath());
        assertEquals(".", fileParent.first().getName());
    }

    @Test
    public void testGetFileParents1() throws CatalogException {
        long fileId;
        QueryResult<File> fileParents;

        fileId = catalogManager.getFileManager().getUid("data/test/folder/", "user@1000G:phase1", sessionIdUser).getResourceId();
        fileParents = catalogManager.getFileManager().getParents(fileId, null, sessionIdUser);

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

        fileId = catalogManager.getFileManager().getUid("data/test/folder/test_1K.txt.gz", "user@1000G:phase1", sessionIdUser)
                .getResourceId();
        fileParents = catalogManager.getFileManager().getParents(fileId, null, sessionIdUser);

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

        fileId = catalogManager.getFileManager().getUid("data/test/", "user@1000G:phase1", sessionIdUser).getResourceId();
        fileParents = catalogManager.getFileManager().getParents(fileId, new QueryOptions("include", "projects.studies.files.path," +
                "projects.studies.files.id"), sessionIdUser);

        assertEquals(3, fileParents.getNumResults());
        assertEquals("", fileParents.getResult().get(0).getPath());
        assertEquals("data/", fileParents.getResult().get(1).getPath());
        assertEquals("data/test/", fileParents.getResult().get(2).getPath());

        fileParents.getResult().forEach(f -> {
            assertNull(f.getName());
            assertNotNull(f.getPath());
            assertTrue(f.getUid() != 0);
        });

    }

    // Try to delete files/folders whose status is STAGED, MISSING...
    @Test
    public void testDelete1() throws CatalogException, IOException {
        String filePath = "data/";
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.PATH.key(), filePath);
        QueryResult<File> fileQueryResult = catalogManager.getFileManager().get(studyId, query, null, sessionIdUser);

        // Change the status to MISSING
        catalogManager.getFileManager()
                .setStatus(Long.toString(fileQueryResult.first().getUid()), File.FileStatus.MISSING, null, sessionIdUser);

        WriteResult deleteResult = catalogManager.getFileManager().delete(String.valueOf(studyId),
                new Query(FileDBAdaptor.QueryParams.UID.key(), fileQueryResult.first().getUid()), null, sessionIdUser);
        assertEquals(1, deleteResult.getNumMatches());
        assertEquals(0, deleteResult.getNumModified());
        assertTrue(deleteResult.getFailed().get(0).getMessage().contains("Cannot delete"));

        // Change the status to STAGED
        catalogManager.getFileManager()
                .setStatus(Long.toString(fileQueryResult.first().getUid()), File.FileStatus.STAGE, null, sessionIdUser);

        deleteResult = catalogManager.getFileManager().delete(String.valueOf(studyId), new Query(FileDBAdaptor.QueryParams.UID.key(),
                fileQueryResult.first().getUid()), null, sessionIdUser);
        assertEquals(1, deleteResult.getNumMatches());
        assertEquals(0, deleteResult.getNumModified());
        assertTrue(deleteResult.getFailed().get(0).getMessage().contains("Cannot delete"));

        // Change the status to READY
        catalogManager.getFileManager()
                .setStatus(Long.toString(fileQueryResult.first().getUid()), File.FileStatus.READY, null, sessionIdUser);

        deleteResult = catalogManager.getFileManager().delete(String.valueOf(studyId), new Query(FileDBAdaptor.QueryParams.UID.key(),
                fileQueryResult.first().getUid()), null, sessionIdUser);
        assertEquals(6, deleteResult.getNumMatches());
        assertEquals(6, deleteResult.getNumModified());
    }

    // It will try to delete a folder in status ready
    @Test
    public void testDelete2() throws CatalogException, IOException {
        String filePath = "data/";
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.PATH.key(), filePath);
        File file = catalogManager.getFileManager().get(studyId, query, null, sessionIdUser).first();

        // We look for all the files and folders that fall within that folder
        query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.PATH.key(), "~^" + filePath + "*")
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.READY);
        int numResults = catalogManager.getFileManager().get(studyId, query, null, sessionIdUser).getNumResults();
        assertEquals(6, numResults);

        // We delete it
        catalogManager.getFileManager().delete(Long.toString(studyId), new Query(FileDBAdaptor.QueryParams.UID.key(), file.getUid()), null,
                sessionIdUser);

        // The files should have been moved to trashed status
        numResults = catalogManager.getFileManager().get(studyId, query, null, sessionIdUser).getNumResults();
        assertEquals(0, numResults);

        query.put(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.TRASHED);
        numResults = catalogManager.getFileManager().get(studyId, query, null, sessionIdUser).getNumResults();
        assertEquals(6, numResults);
    }

    // It will try to delete a folder in status ready and skip the trash
    @Test
    public void testDelete3() throws CatalogException, IOException {
        String filePath = "data/";
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.PATH.key(), filePath);
        File file = catalogManager.getFileManager().get(studyId, query, null, sessionIdUser).first();

        // We look for all the files and folders that fall within that folder
        query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.PATH.key(), "~^" + filePath + "*")
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.READY);
        int numResults = catalogManager.getFileManager().get(studyId, query, null, sessionIdUser).getNumResults();
        assertEquals(6, numResults);

        // We delete it
        QueryOptions queryOptions = new QueryOptions(FileManager.SKIP_TRASH, true);
        catalogManager.getFileManager().delete(String.valueOf(studyId), new Query(FileDBAdaptor.QueryParams.UID.key(), file.getUid()),
                queryOptions, sessionIdUser);

        // The files should have been moved to trashed status
        numResults = catalogManager.getFileManager().get(studyId, query, null, sessionIdUser).getNumResults();
        assertEquals(0, numResults);

        query.put(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.PENDING_DELETE);
        numResults = catalogManager.getFileManager().get(studyId, query, null, sessionIdUser).getNumResults();
        assertEquals(6, numResults);
    }

    @Test
    public void testDeleteFile() throws CatalogException, IOException {
        List<File> result = catalogManager.getFileManager().get(studyId, new Query(FileDBAdaptor.QueryParams.TYPE.key(), "FILE"), new QueryOptions(), sessionIdUser).getResult();
        for (File file : result) {
            catalogManager.getFileManager().delete(String.valueOf(studyId), new Query(FileDBAdaptor.QueryParams.UID.key(), file.getUid()),
                    null, sessionIdUser);
        }
//        CatalogFileUtils catalogFileUtils = new CatalogFileUtils(catalogManager);
        catalogManager.getFileManager().get(studyId, new Query(FileDBAdaptor.QueryParams.TYPE.key(), "FILE"), new QueryOptions(), sessionIdUser).getResult().forEach(f -> {
            assertEquals(f.getStatus().getName(), File.FileStatus.TRASHED);
            assertTrue(f.getName().startsWith(".deleted"));
        });

        result = catalogManager.getFileManager().get(studyId2, new Query(FileDBAdaptor.QueryParams.TYPE.key(), "FILE"), new QueryOptions(), sessionIdUser).getResult();
        for (File file : result) {
            catalogManager.getFileManager().delete(String.valueOf(studyId2), new Query(FileDBAdaptor.QueryParams.UID.key(), file.getUid()),
                    null, sessionIdUser);
        }
        catalogManager.getFileManager().get(studyId, new Query(FileDBAdaptor.QueryParams.TYPE.key(), "FILE"), new QueryOptions(), sessionIdUser).getResult().forEach(f -> {
            assertEquals(f.getStatus().getName(), File.FileStatus.TRASHED);
            assertTrue(f.getName().startsWith(".deleted"));
        });

    }

    @Test
    public void testDeleteLeafFolder() throws CatalogException, IOException {
        long deletable = catalogManager.getFileManager().getUid("/data/test/folder/", "user@1000G:phase3", sessionIdUser).getResourceId();
        deleteFolderAndCheck(deletable);
    }

    @Test
    public void testDeleteMiddleFolder() throws CatalogException, IOException {
        long deletable = catalogManager.getFileManager().getUid("/data/", "user@1000G:phase3", sessionIdUser).getResourceId();
        deleteFolderAndCheck(deletable);
    }

    @Test
    public void testDeleteRootFolder() throws CatalogException, IOException {
        long deletable = catalogManager.getFileManager().getUid("/", "user@1000G:phase3", sessionIdUser).getResourceId();

        long studyIdByFileId = catalogManager.getFileManager().getStudyId(deletable);
        WriteResult result = catalogManager.getFileManager().delete(String.valueOf(studyIdByFileId), new Query(FileDBAdaptor.QueryParams
                .UID.key(), deletable), null, sessionIdUser);

        assertEquals(1, result.getNumMatches());
        assertEquals(0, result.getNumModified());
        assertEquals("Root directories cannot be deleted", result.getFailed().get(0).getMessage());
    }

    // Cannot delete staged files
    @Test
    public void deleteFolderTest() throws CatalogException, IOException {
        List<File> folderFiles = new LinkedList<>();
        String userId = catalogManager.getUserManager().getUserId(sessionIdUser);
        long studyId = catalogManager.getStudyManager().getId(userId, "user@1000G:phase3");
        File folder = createBasicDirectoryFileTestEnvironment(folderFiles, studyId);

        CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(catalogManager.getFileManager().getUri(folder));
        for (File file : folderFiles) {
            assertTrue(ioManager.exists(catalogManager.getFileManager().getUri(file)));
        }

        catalogManager.getFileManager().create(Long.toString(studyId), File.Type.FILE, File.Format.PLAIN, File.Bioformat.NONE,
                "folder/subfolder/subsubfolder/my_staged.txt", null, null, new File.FileStatus(File.FileStatus.STAGE), (long) 0, (long)
                        -1, null, (long) -1, null, null, true, null, null, sessionIdUser).first();


        WriteResult deleteResult = catalogManager.getFileManager().delete(String.valueOf(studyId),
                new Query(FileDBAdaptor.QueryParams.UID.key(), folder.getUid()), null, sessionIdUser);
        assertEquals(0, deleteResult.getNumModified());

        File fileTmp = catalogManager.getFileManager().get(folder.getUid(), null, sessionIdUser).first();
        assertEquals("Folder name should not be modified", folder.getPath(), fileTmp.getPath());
        assertTrue(ioManager.exists(fileTmp.getUri()));

        for (File file : folderFiles) {
            fileTmp = catalogManager.getFileManager().get(file.getUid(), null, sessionIdUser).first();
            assertEquals("File name should not be modified", file.getPath(), fileTmp.getPath());
            assertTrue("File uri: " + fileTmp.getUri() + " should exist", ioManager.exists(fileTmp.getUri()));
        }

    }

    // Deleted folders should be all put to TRASHED
    @Test
    public void deleteFolderTest2() throws CatalogException, IOException {
        List<File> folderFiles = new LinkedList<>();
        String userId = catalogManager.getUserManager().getUserId(sessionIdUser);
        long studyId = catalogManager.getStudyManager().getId(userId, "user@1000G:phase3");
        File folder = createBasicDirectoryFileTestEnvironment(folderFiles, studyId);

        CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(catalogManager.getFileManager().getUri(folder));
        for (File file : folderFiles) {
            assertTrue(ioManager.exists(catalogManager.getFileManager().getUri(file)));
        }

        catalogManager.getFileManager().delete(String.valueOf(studyId), new Query(FileDBAdaptor.QueryParams.UID.key(), folder.getUid()),
                null, sessionIdUser);

        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.UID.key(), folder.getUid())
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.TRASHED);
        File fileTmp = catalogManager.getFileManager().get(studyId, query, QueryOptions.empty(), sessionIdUser).first();

        assertEquals("Folder name should not be modified", folder.getPath(), fileTmp.getPath());
        assertEquals("Status should be to TRASHED", File.FileStatus.TRASHED, fileTmp.getStatus().getName());
        assertEquals("Name should not have changed", folder.getName(), fileTmp.getName());
        assertTrue(ioManager.exists(fileTmp.getUri()));

        for (File file : folderFiles) {
            query.put(FileDBAdaptor.QueryParams.UID.key(), file.getUid());
            fileTmp = fileManager.get(String.valueOf(studyId), query, QueryOptions.empty(), sessionIdUser).first();
            assertEquals("Folder name should not be modified", file.getPath(), fileTmp.getPath());
            assertEquals("Status should be to TRASHED", File.FileStatus.TRASHED, fileTmp.getStatus().getName());
            assertEquals("Name should not have changed", file.getName(), fileTmp.getName());
            assertTrue("File uri: " + fileTmp.getUri() + " should exist", ioManager.exists(fileTmp.getUri()));
        }
    }

    // READY -> PENDING_DELETE
    @Test
    public void deleteFolderTest3() throws CatalogException, IOException {
        List<File> folderFiles = new LinkedList<>();
        String userId = catalogManager.getUserManager().getUserId(sessionIdUser);
        long studyId = catalogManager.getStudyManager().getId(userId, "user@1000G:phase3");
        File folder = createBasicDirectoryFileTestEnvironment(folderFiles, studyId);

        CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(catalogManager.getFileManager().getUri(folder));
        for (File file : folderFiles) {
            assertTrue(ioManager.exists(catalogManager.getFileManager().getUri(file)));
        }

        catalogManager.getFileManager().delete(String.valueOf(studyId), new Query(FileDBAdaptor.QueryParams.UID.key(), folder.getUid()),
                new ObjectMap(FileManager.SKIP_TRASH, true), sessionIdUser);
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.UID.key(), folder.getUid())
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.PENDING_DELETE);
        File fileTmp = fileManager.get(String.valueOf(studyId), query, QueryOptions.empty(), sessionIdUser).first();

        String myPath = Paths.get(folder.getPath()) + AbstractManager.INTERNAL_DELIMITER + "DELETED";

        assertTrue("Folder name should have been modified", fileTmp.getPath().contains(myPath));
        assertEquals("Status should be to PENDING_DELETE", File.FileStatus.PENDING_DELETE, fileTmp.getStatus().getName());
        assertEquals("Name should not have changed", folder.getName(), fileTmp.getName());
        assertTrue(ioManager.exists(fileTmp.getUri()));

        for (File file : folderFiles) {
            query.put(FileDBAdaptor.QueryParams.UID.key(), file.getUid());
            fileTmp = fileManager.get(String.valueOf(studyId), query, QueryOptions.empty(), sessionIdUser).first();
            assertTrue("Folder name should have been modified", fileTmp.getPath().contains(myPath));
            assertEquals("Status should be to PENDING_DELETE", File.FileStatus.PENDING_DELETE, fileTmp.getStatus().getName());
            assertEquals("Name should not have changed", file.getName(), fileTmp.getName());
            assertTrue("File uri: " + fileTmp.getUri() + " should exist", ioManager.exists(fileTmp.getUri()));
        }
    }

    // READY -> PENDING_DELETE -> DELETED
    @Test
    public void deleteFolderTest4() throws CatalogException, IOException {
        List<File> folderFiles = new LinkedList<>();
        String userId = catalogManager.getUserManager().getUserId(sessionIdUser);
        long studyId = catalogManager.getStudyManager().getId(userId, "user@1000G:phase3");
        File folder = createBasicDirectoryFileTestEnvironment(folderFiles, studyId);

        CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(catalogManager.getFileManager().getUri(folder));
        for (File file : folderFiles) {
            assertTrue(ioManager.exists(catalogManager.getFileManager().getUri(file)));
        }

        ObjectMap params = new ObjectMap()
                .append(FileManager.SKIP_TRASH, true)
                .append(FileManager.FORCE_DELETE, true);
        // We now delete and they should be passed to PENDING_DELETE (test deleteFolderTest3)
        catalogManager.getFileManager().delete(String.valueOf(studyId), new Query(FileDBAdaptor.QueryParams.UID.key(), folder.getUid()),
                params, sessionIdUser);

        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.UID.key(), folder.getUid())
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.DELETED);
        File fileTmp = fileManager.get(String.valueOf(studyId), query, QueryOptions.empty(), sessionIdUser).first();

        String myPath = Paths.get(folder.getPath()) + AbstractManager.INTERNAL_DELIMITER + "DELETED";

        assertTrue("Folder name should have been modified", fileTmp.getPath().contains(myPath));
        assertEquals("Status should be DELETED", File.FileStatus.DELETED, fileTmp.getStatus().getName());
        assertEquals("Name should not have changed", folder.getName(), fileTmp.getName());
        assertTrue(!ioManager.exists(fileTmp.getUri()));

        for (File file : folderFiles) {
            query.put(FileDBAdaptor.QueryParams.UID.key(), file.getUid());
            fileTmp = fileManager.get(String.valueOf(studyId), query, QueryOptions.empty(), sessionIdUser).first();
            assertTrue("Folder name should have been modified", fileTmp.getPath().contains(myPath));
            assertEquals("Status should be to DELETED", File.FileStatus.DELETED, fileTmp.getStatus().getName());
            assertEquals("Name should not have changed", file.getName(), fileTmp.getName());
            assertTrue("File uri: " + fileTmp.getUri() + " should not exist", !ioManager.exists(fileTmp.getUri()));
        }
    }

    // READY -> DELETED
    @Test
    public void deleteFolderTest5() throws CatalogException, IOException {
        List<File> folderFiles = new LinkedList<>();
        String userId = catalogManager.getUserManager().getUserId(sessionIdUser);
        long studyId = catalogManager.getStudyManager().getId(userId, "user@1000G:phase3");
        File folder = createBasicDirectoryFileTestEnvironment(folderFiles, studyId);

        CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(catalogManager.getFileManager().getUri(folder));
        for (File file : folderFiles) {
            assertTrue(ioManager.exists(catalogManager.getFileManager().getUri(file)));
        }

        ObjectMap params = new ObjectMap()
                .append(FileManager.SKIP_TRASH, true)
                .append(FileManager.FORCE_DELETE, true);
        catalogManager.getFileManager().delete(String.valueOf(studyId), new Query(FileDBAdaptor.QueryParams.UID.key(), folder.getUid()),
                params, sessionIdUser);
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.UID.key(), folder.getUid())
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.DELETED);
        File fileTmp = fileManager.get(String.valueOf(studyId), query, QueryOptions.empty(), sessionIdUser).first();

        String myPath = Paths.get(folder.getPath()) + AbstractManager.INTERNAL_DELIMITER + "DELETED";

        assertTrue("Folder name should have been modified", fileTmp.getPath().contains(myPath));
        assertEquals("Status should be to DELETED", File.FileStatus.DELETED, fileTmp.getStatus().getName());
        assertEquals("Name should not have changed", folder.getName(), fileTmp.getName());
        assertTrue(!ioManager.exists(fileTmp.getUri()));

        for (File file : folderFiles) {
            query.put(FileDBAdaptor.QueryParams.UID.key(), file.getUid());
            fileTmp = fileManager.get(String.valueOf(studyId), query, QueryOptions.empty(), sessionIdUser).first();
            assertTrue("Folder name should have been modified", fileTmp.getPath().contains(myPath));
            assertEquals("Status should be to DELETED", File.FileStatus.DELETED, fileTmp.getStatus().getName());
            assertEquals("Name should not have changed", file.getName(), fileTmp.getName());
            assertTrue("File uri: " + fileTmp.getUri() + " should not exist", !ioManager.exists(fileTmp.getUri()));
        }
    }

    private File createBasicDirectoryFileTestEnvironment(List<File> folderFiles, long studyId) throws CatalogException, IOException {
        File folder = catalogManager.getFileManager().createFolder(Long.toString(studyId), Paths.get("folder").toString(), null, false,
                null, QueryOptions.empty(), sessionIdUser).first();
        QueryResult<File> queryResult5 = catalogManager.getFileManager().create(Long.toString(studyId), File.Type.FILE, File.Format.PLAIN, File.Bioformat.NONE, "folder/my.txt", null, "", new File.FileStatus(File.FileStatus.STAGE), 0, -1, null, -1, null, null, true, null, null, sessionIdUser);
        new FileUtils(catalogManager).upload(new ByteArrayInputStream(StringUtils
                .randomString(200).getBytes()), queryResult5.first(), sessionIdUser, false, false, true);
        folderFiles.add(catalogManager.getFileManager().get(queryResult5.first().getUid(), null, sessionIdUser).first());
        QueryResult<File> queryResult4 = catalogManager.getFileManager().create(Long.toString(studyId), File.Type.FILE, File.Format.PLAIN, File.Bioformat.NONE, "folder/my2.txt", null, "", new File.FileStatus(File.FileStatus.STAGE), 0, -1, null, -1, null, null, true, null, null, sessionIdUser);
        new FileUtils(catalogManager).upload(new ByteArrayInputStream(StringUtils
                .randomString(200).getBytes()), queryResult4.first(), sessionIdUser, false, false, true);
        folderFiles.add(catalogManager.getFileManager().get(queryResult4.first().getUid(), null, sessionIdUser).first());
        QueryResult<File> queryResult3 = catalogManager.getFileManager().create(Long.toString(studyId), File.Type.FILE, File.Format.PLAIN, File.Bioformat.NONE, "folder/my3.txt", null, "", new File.FileStatus(File.FileStatus.STAGE), 0, -1, null, -1, null, null, true, null, null, sessionIdUser);
        new FileUtils(catalogManager).upload(new ByteArrayInputStream(StringUtils
                .randomString(200).getBytes()), queryResult3.first(), sessionIdUser, false, false, true);
        folderFiles.add(catalogManager.getFileManager().get(queryResult3.first().getUid(), null, sessionIdUser).first());
        QueryResult<File> queryResult2 = catalogManager.getFileManager().create(Long.toString(studyId), File.Type.FILE, File.Format.PLAIN, File.Bioformat.NONE, "folder/subfolder/my4.txt", null, "", new File.FileStatus(File.FileStatus.STAGE), 0, -1, null, -1, null, null, true, null, null, sessionIdUser);
        new FileUtils(catalogManager).upload(new ByteArrayInputStream(StringUtils.randomString(200).getBytes()), queryResult2.first(), sessionIdUser, false, false, true);
        folderFiles.add(catalogManager.getFileManager().get(queryResult2.first().getUid(), null, sessionIdUser).first());
        QueryResult<File> queryResult1 = catalogManager.getFileManager().create(Long.toString(studyId), File.Type.FILE, File.Format.PLAIN, File.Bioformat.NONE, "folder/subfolder/my5.txt", null, "", new File.FileStatus(File.FileStatus.STAGE), 0, -1, null, -1, null, null, true, null, null, sessionIdUser);
        new FileUtils(catalogManager).upload(new ByteArrayInputStream(StringUtils.randomString(200).getBytes()), queryResult1.first(), sessionIdUser, false, false, true);
        folderFiles.add(catalogManager.getFileManager().get(queryResult1.first().getUid(), null, sessionIdUser).first());
        QueryResult<File> queryResult = catalogManager.getFileManager().create(Long.toString(studyId), File.Type.FILE, File.Format.PLAIN, File.Bioformat.NONE, "folder/subfolder/subsubfolder/my6" +
                ".txt", null, "", new File.FileStatus(File.FileStatus.STAGE), 0, -1, null, -1, null, null, true, null, null, sessionIdUser);
        new FileUtils(catalogManager).upload(new ByteArrayInputStream(StringUtils.randomString(200).getBytes()), queryResult.first(), sessionIdUser, false, false, true);
        folderFiles.add(catalogManager.getFileManager().get(queryResult.first().getUid(), null, sessionIdUser).first());
        return folder;
    }

    @Test
    public void sendFolderToTrash() {

    }

    @Test
    public void getAllFilesInFolder() throws CatalogException {
        List<File> allFilesInFolder = catalogManager.getFileManager().getFilesFromFolder("/data/test/folder/", "user@1000G:phase1", null,
                sessionIdUser).getResult();
        assertEquals(3, allFilesInFolder.size());
    }

    private void deleteFolderAndCheck(long deletable) throws CatalogException, IOException {
        List<File> allFilesInFolder;
        long studyIdByFileId = catalogManager.getFileManager().getStudyId(deletable);

        catalogManager.getFileManager().delete(String.valueOf(studyIdByFileId), new Query(FileDBAdaptor.QueryParams.UID.key(), deletable),
                null, sessionIdUser);

        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.UID.key(), deletable)
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.TRASHED);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.PATH.key());
        QueryResult<File> fileQueryResult = catalogManager.getFileManager().get(String.valueOf(studyIdByFileId), query, options,
                sessionIdUser);
        assertEquals(1, fileQueryResult.getNumResults());

//        allFilesInFolder = catalogManager.getAllFilesInFolder(deletable, null, sessionIdUser).getResult();
        query = new Query()
                .append(FileDBAdaptor.QueryParams.DIRECTORY.key(), fileQueryResult.first().getPath() + ".*")
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.TRASHED);
        allFilesInFolder = catalogManager.getFileManager().get(studyIdByFileId, query, null, sessionIdUser).getResult();

        for (File subFile : allFilesInFolder) {
            assertTrue(subFile.getStatus().getName().equals(File.FileStatus.TRASHED));
        }
    }

    @Test
    public void assignPermissionsRecursively() throws Exception {
        Path folderPath = Paths.get("data", "new", "folder");
        catalogManager.getFileManager().createFolder(Long.toString(studyId), folderPath.toString(), null, true, null,
                QueryOptions.empty(), sessionIdUser).first();

        Path filePath = Paths.get("data", "file1.txt");
        fileManager.create(Long.toString(studyId), File.Type.FILE, File.Format.UNKNOWN, File.Bioformat.UNKNOWN, filePath.toString(),
                "", "", new File.FileStatus(), 10, -1, null, -1, null, null, true, "My content", null, sessionIdUser);

        Study.StudyAclParams aclParams = new Study.StudyAclParams("", AclParams.Action.ADD, null);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(Long.toString(studyId)), "user2", aclParams, sessionIdUser).get(0);
        List<QueryResult<FileAclEntry>> queryResults = fileManager.updateAcl(Long.toString(studyId), Arrays.asList("data/new/",
                filePath.toString()), "user2", new File.FileAclParams("VIEW", AclParams.Action.SET, null), sessionIdUser);

        assertEquals(3, queryResults.size());
        for (QueryResult<FileAclEntry> queryResult : queryResults) {
            assertEquals("user2", queryResult.getResult().get(0).getMember());
            assertEquals(1, queryResult.getResult().get(0).getPermissions().size());
            assertEquals(FileAclEntry.FilePermissions.VIEW, queryResult.getResult().get(0).getPermissions().iterator().next());
        }
    }

    @Test
    public void testUpdateIndexStatus() throws CatalogException {
        long studyId = catalogManager.getStudyManager().getId("user", "user@1000G:phase1");
        QueryResult<File> fileResult = fileManager.create(Long.toString(studyId), File.Type.FILE, File.Format.VCF,
                File.Bioformat.VARIANT, "data/test.vcf", "", "description", new File.FileStatus(File.FileStatus.STAGE), 0, -1, Collections.emptyList(), -1, Collections.emptyMap(), Collections.emptyMap(), true, null, new QueryOptions(), sessionIdUser);

        fileManager.updateFileIndexStatus(fileResult.first(), FileIndex.IndexStatus.TRANSFORMED, null, sessionIdUser);
        QueryResult<File> read = fileManager.get(fileResult.first().getUid(), new QueryOptions(), sessionIdUser);
        assertEquals(FileIndex.IndexStatus.TRANSFORMED, read.first().getIndex().getStatus().getName());
    }

    @Test
    public void testIndex() throws Exception {
        URI uri = getClass().getResource("/biofiles/variant-test-file.vcf.gz").toURI();
        File file = fileManager.link(uri, "", studyId, null, sessionIdUser).first();
        Job job = fileManager.index(Collections.singletonList(file.getName()), String.valueOf(studyId), "VCF", null, sessionIdUser).first();
        assertEquals(file.getUid(), job.getInput().get(0).getUid());
    }

    @Test
    public void testIndexFromAvro() throws Exception {
        URI uri = getClass().getResource("/biofiles/variant-test-file.vcf.gz").toURI();
        File file = fileManager.link(uri, "data", studyId, null, sessionIdUser).first();
        fileManager.create(Long.toString(studyId), File.Type.FILE, File.Format.AVRO, null, "data/variant-test-file.vcf.gz.variants.avro.gz", "", "description", new File.FileStatus(File.FileStatus.READY), 0, -1, Collections.emptyList(), -1, Collections.emptyMap(), Collections.emptyMap(), true, "asdf", new QueryOptions(), sessionIdUser);
        fileManager.link(getClass().getResource("/biofiles/variant-test-file.vcf.gz.file.json.gz").toURI(), "data", studyId, null, sessionIdUser).first();

        Job job = fileManager.index(Collections.singletonList("variant-test-file.vcf.gz.variants.avro.gz"), String.valueOf(studyId), "VCF", null, sessionIdUser).first();
        assertEquals(file.getUid(), job.getInput().get(0).getUid());
    }

    @Test
    public void testIndexFromAvroIncomplete() throws Exception {
        URI uri = getClass().getResource("/biofiles/variant-test-file.vcf.gz").toURI();
        File file = fileManager.link(uri, "data", studyId, null, sessionIdUser).first();
        fileManager.create(Long.toString(studyId), File.Type.FILE, File.Format.AVRO, null, "data/variant-test-file.vcf.gz.variants.avro.gz", "", "description", new File.FileStatus(File.FileStatus.READY), 0, -1, Collections.emptyList(), -1, Collections.emptyMap(), Collections.emptyMap(), true, "asdf", new QueryOptions(), sessionIdUser);
//        fileManager.link(getClass().getResource("/biofiles/variant-test-file.vcf.gz.file.json.gz").toURI(), "data", studyId, null, sessionIdUser).first();


        thrown.expect(CatalogException.class);
        thrown.expectMessage("The file variant-test-file.vcf.gz.variants.avro.gz is not a VCF file.");
        fileManager.index(Collections.singletonList("variant-test-file.vcf.gz.variants.avro.gz"), String.valueOf(studyId), "VCF", null, sessionIdUser).first();
    }
}
