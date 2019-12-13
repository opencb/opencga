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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.StringUtils;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.*;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.models.update.FileUpdateParams;
import org.opencb.opencga.catalog.models.update.SampleUpdateParams;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.AclParams;
import org.opencb.opencga.core.models.acls.permissions.FileAclEntry;
import org.opencb.opencga.core.results.OpenCGAResult;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
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
public class FileManagerTest extends AbstractManagerTest {

    private FileManager fileManager;

    @Before
    public void setUp() throws IOException, CatalogException {
        super.setUp();
        fileManager = catalogManager.getFileManager();
    }

    private DataResult<File> link(URI uriOrigin, String pathDestiny, String studyIdStr, ObjectMap params, String sessionId)
            throws CatalogException {
        return fileManager.link(studyIdStr, uriOrigin, pathDestiny, params, sessionId);
    }

    @Test
    public void testCreateFileFromUnsharedStudy() throws CatalogException {
        try {
            fileManager.create(studyFqn, File.Type.FILE, File.Format.UNKNOWN, File.Bioformat.NONE,
                    "data/test/folder/file.txt", "My description", null, 0, null, -1, null, null, true, null, null, sessionIdUser2);
            fail("The file could be created despite not having the proper permissions.");
        } catch (CatalogAuthorizationException e) {
            assertEquals(0, fileManager.search(studyFqn, new Query(FileDBAdaptor.QueryParams.PATH.key(),
                    "data/test/folder/file.txt"), null, sessionIdUser).getNumResults());
        }
    }

    @Test
    public void testCreateFileFromSharedStudy() throws CatalogException {
        Study.StudyAclParams aclParams = new Study.StudyAclParams("", AclParams.Action.ADD, "analyst");
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), "user2", aclParams, sessionIdUser);
        fileManager.create(studyFqn, File.Type.FILE, File.Format.UNKNOWN, File.Bioformat.NONE,
                "data/test/folder/file.txt", "My description", null, 0, null, (long) -1, null, null, true, "blabla", null, sessionIdUser2);
        assertEquals(1, fileManager.search(studyFqn, new Query(FileDBAdaptor.QueryParams.PATH.key(),
                "data/test/folder/file.txt"), null, sessionIdUser).getNumResults());
    }

    URI getStudyURI() throws CatalogException {
        return catalogManager.getStudyManager().get(studyFqn,
                new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.URI.key()), sessionIdUser).first().getUri();    
    }

    @Test
    public void testLinkCram() throws CatalogException, IOException {
        String reference = getClass().getResource("/biofiles/cram/hg19mini.fasta").getFile();
        File referenceFile = fileManager.link(studyFqn, Paths.get(reference).toUri(), "", null, sessionIdUser).first();
        assertEquals(File.Format.FASTA, referenceFile.getFormat());
        assertEquals(File.Bioformat.REFERENCE_GENOME, referenceFile.getBioformat());

        File.RelatedFile relatedFile = new File.RelatedFile(new File().setId("hg19mini.fasta"),
                File.RelatedFile.Relation.REFERENCE_GENOME);
        String cramFile = getClass().getResource("/biofiles/cram/cram_with_crai_index.cram").getFile();
        DataResult<File> link = fileManager.link(studyFqn, Paths.get(cramFile).toUri(), "",
                new ObjectMap("relatedFiles", Collections.singletonList(relatedFile)), sessionIdUser);
        assertTrue(!link.first().getAttributes().isEmpty());
        assertNotNull(link.first().getAttributes().get("alignmentHeader"));
        assertEquals(File.Format.CRAM, link.first().getFormat());
        assertEquals(File.Bioformat.ALIGNMENT, link.first().getBioformat());
        assertEquals(referenceFile.getId(), link.first().getRelatedFiles().get(0).getFile().getId());
        assertEquals(File.RelatedFile.Relation.REFERENCE_GENOME, link.first().getRelatedFiles().get(0).getRelation());
    }

    @Test
    public void testLinkFolder() throws CatalogException, IOException {
//        // We will link the same folders that are already created in this study into another folder
        URI uri = createExternalDummyData().toUri();
//        long folderId = catalogManager.searchFile(studyUid, new Query(FileDBAdaptor.QueryParams.PATH.key(), "data/"), null,
//                sessionIdUser).first().getId();
//        int numFiles = catalogManager.getAllFilesInFolder(folderId, null, sessionIdUser).getNumResults();
//
//        catalogManager.link(uri, "data/", studyFqn, new ObjectMap(), sessionIdUser);
//        int numFilesAfterLink = catalogManager.getAllFilesInFolder(folderId, null, sessionIdUser).getNumResults();
//        assertEquals("Linking the same folders should not change the number of files in catalog", numFiles, numFilesAfterLink);

        // Now we try to create it into a folder that does not exist with parents = true
        link(uri, "myDirectory", studyFqn, new ObjectMap("parents", true), sessionIdUser);
        DataResult<File> folderDataResult = fileManager.search(studyFqn, new Query()
                .append(FileDBAdaptor.QueryParams.PATH.key(), "myDirectory/"), null, sessionIdUser);
        assertEquals(1, folderDataResult.getNumResults());
        assertTrue(!folderDataResult.first().isExternal());

        folderDataResult = fileManager.search(studyFqn, new Query(FileDBAdaptor.QueryParams.PATH.key(),
                "myDirectory/A/"), null, sessionIdUser);
        assertEquals(1, folderDataResult.getNumResults());
        assertTrue(folderDataResult.first().isExternal());

        folderDataResult = fileManager.search(studyFqn, new Query(FileDBAdaptor.QueryParams.PATH.key(),
                "myDirectory/A/C/D/"), null, sessionIdUser);
        assertEquals(1, folderDataResult.getNumResults());
        assertTrue(folderDataResult.first().isExternal());
        folderDataResult = fileManager.search(studyFqn, new Query(FileDBAdaptor.QueryParams.PATH.key(),
                "myDirectory/A/B/"), null, sessionIdUser);
        assertEquals(1, folderDataResult.getNumResults());
        assertTrue(folderDataResult.first().isExternal());

        // Now we try to create it into a folder that does not exist with parents = false
        thrown.expect(CatalogException.class);
        thrown.expectMessage("already linked");
        link(uri, "myDirectory2", studyFqn, new ObjectMap(), sessionIdUser);
    }

    @Test
    public void testLinkFolder2() throws CatalogException, IOException {
        // We will link the same folders that are already created in this study into another folder
        URI uri = Paths.get(getStudyURI()).resolve("data").toUri();

        // Now we try to create it into a folder that does not exist with parents = false
        thrown.expect(CatalogException.class);
        thrown.expectMessage("not exist");
        link(uri, "myDirectory2", studyFqn, new ObjectMap(), sessionIdUser);
    }


    @Test
    public void testLinkFolder3() throws CatalogException, IOException {
        URI uri = Paths.get(getStudyURI()).resolve("data").toUri();
        thrown.expect(CatalogException.class);
        thrown.expectMessage("already existed and is not external");
        link(uri, null, studyFqn, new ObjectMap(), sessionIdUser);

//        // Make sure that the path of the files linked do not start with /
//        Query query = new Query(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
//                .append(FileDBAdaptor.QueryParams.EXTERNAL.key(), true);
//        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.PATH.key());
//        DataResult<File> fileDataResult = fileManager.get(query, queryOptions, sessionIdUser);
//        assertEquals(5, fileDataResult.getNumResults());
//        for (File file : fileDataResult.getResults()) {
//            assertTrue(!file.getPath().startsWith("/"));
//        }
    }

    // This test will make sure that we can link several times the same uri into the same path with same results and without crashing
    // However, if we try to link to a different path, we will fail
    @Test
    public void testLinkFolder4() throws CatalogException, IOException {
        URI uri = Paths.get(getStudyURI()).resolve("data").toUri();
        ObjectMap params = new ObjectMap("parents", true);
        DataResult<File> allFiles = link(uri, "test/myLinkedFolder/", studyFqn, params, sessionIdUser);
        assertEquals(6, allFiles.getNumResults());

        DataResult<File> sameAllFiles = link(uri, "test/myLinkedFolder/", studyFqn, params, sessionIdUser);
        assertEquals(allFiles.getNumResults(), sameAllFiles.getNumResults());

        List<File> result = allFiles.getResults();
        for (int i = 0; i < result.size(); i++) {
            assertEquals(allFiles.getResults().get(i).getUid(), sameAllFiles.getResults().get(i).getUid());
            assertEquals(allFiles.getResults().get(i).getPath(), sameAllFiles.getResults().get(i).getPath());
            assertEquals(allFiles.getResults().get(i).getUri(), sameAllFiles.getResults().get(i).getUri());
        }

        thrown.expect(CatalogException.class);
        thrown.expectMessage("already linked");
        link(uri, "data", studyFqn, new ObjectMap(), sessionIdUser);
    }

    @Test
    public void testLinkNormalizedUris() throws CatalogException, IOException, URISyntaxException {
        Path path = createExternalDummyData();
        URI uri = UriUtils.createUri(path.toString() + "/../A");
        ObjectMap params = new ObjectMap("parents", true);
        DataResult<File> allFiles = link(uri, "test/myLinkedFolder/", studyFqn, params, sessionIdUser);
        assertEquals(6, allFiles.getNumResults());
        for (File file : allFiles.getResults()) {
            assertTrue(file.getUri().isAbsolute());
            assertEquals(file.getUri().normalize(), file.getUri());
        }
    }

    private Path createExternalDummyData() throws CatalogIOException {
        Path jUnitDir = Paths.get(catalogManager.getConfiguration().getWorkspace()).getParent();

        CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().getDefault();
        ioManager.createDirectory(jUnitDir.resolve("A").resolve("B").toUri(), true);
        ioManager.createDirectory(jUnitDir.resolve("A").resolve("C").resolve("D").toUri(), true);
        ioManager.createFile(jUnitDir.resolve("A").resolve("C").resolve("file1.txt").toUri(),
                new ByteArrayInputStream("blablabla".getBytes()));
        ioManager.createFile(jUnitDir.resolve("A").resolve("C").resolve("D").resolve("file3.txt").toUri(),
                new ByteArrayInputStream("blablabla".getBytes()));

        return jUnitDir.resolve("A");
    }

    @Test
    public void testLinkNonExistentFile() throws CatalogException, IOException {
        URI uri= Paths.get(getStudyURI().resolve("inexistentData")).toUri();
        ObjectMap params = new ObjectMap("parents", true);
        thrown.expect(CatalogException.class);
        thrown.expectMessage("does not exist");
        link(uri, "test/myLinkedFolder/", studyFqn, params, sessionIdUser);
    }

    // The VCF file that is going to be linked contains names with "." Issue: #570
    @Test
    public void testLinkFile() throws CatalogException, IOException, URISyntaxException {
        URI uri = getClass().getResource("/biofiles/variant-test-file-dot-names.vcf.gz").toURI();
        DataResult<File> link = fileManager.link(studyFqn, uri, ".", new ObjectMap(), sessionIdUser);

        assertEquals(4, link.first().getSamples().size());

        List<Long> sampleList = link.first().getSamples().stream().map(Sample::getUid).collect(Collectors.toList());
        Query query = new Query(SampleDBAdaptor.QueryParams.UID.key(), sampleList);
        DataResult<Sample> sampleDataResult = catalogManager.getSampleManager().search(studyFqn, query, QueryOptions.empty(), sessionIdUser);

        assertEquals(4, sampleDataResult.getNumResults());
        List<String> sampleNames = sampleDataResult.getResults().stream().map(Sample::getId).collect(Collectors.toList());
        assertTrue(sampleNames.contains("test-name.bam"));
        assertTrue(sampleNames.contains("NA19660"));
        assertTrue(sampleNames.contains("NA19661"));
        assertTrue(sampleNames.contains("NA19685"));
    }

    @Test
    public void testFileHooks() throws CatalogException, IOException, URISyntaxException {
        URI uri = getClass().getResource("/biofiles/variant-test-file-dot-names.vcf.gz").toURI();
        DataResult<File> link = fileManager.link(studyFqn, uri, ".", new ObjectMap(), sessionIdUser);

        assertEquals(2, link.first().getTags().size());
        assertTrue(link.first().getTags().containsAll(Arrays.asList("VCF", "FILE")));
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
                    fileManager.link(studyFqn, uri, ".", new ObjectMap(), sessionIdUser);
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
        URI uri = createExternalDummyData().toUri();
        link(uri, "myDirectory", studyFqn, new ObjectMap("parents", true), sessionIdUser);

        CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(uri);

        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(FileDBAdaptor.QueryParams.PATH.key(), "~myDirectory/*")
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.READY);
        DataResult<File> fileDataResultLinked = fileManager.search(studyFqn, query, null, sessionIdUser);

        System.out.println("Number of files/folders linked = " + fileDataResultLinked.getNumResults());

        // We set to PENDING DELETE the subdirectory that will be unlinked
        Query updateQuery = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(FileDBAdaptor.QueryParams.PATH.key(), "~myDirectory/A/*")
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.READY);
        setToPendingDelete(studyFqn, updateQuery);

        // Now we try to unlink them
        fileManager.unlink(studyFqn, "myDirectory/A/", sessionIdUser);
        fileDataResultLinked = fileManager.search(studyFqn, query, null, sessionIdUser);
        assertEquals(1, fileDataResultLinked.getNumResults());

        query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(FileDBAdaptor.QueryParams.PATH.key(), "~myDirectory/*")
                .append(FileDBAdaptor.QueryParams.DELETED.key(), true);
        DataResult<File> fileDataResultUnlinked = fileManager.search(studyFqn, query, null, sessionIdUser);
        assertEquals(6, fileDataResultUnlinked.getNumResults());

        for (File file : fileDataResultUnlinked.getResults()) {
            assertEquals("Status should be to REMOVED", File.FileStatus.REMOVED, file.getStatus().getName());
            assertEquals("Name should not have changed", file.getName(), file.getName());
            assertTrue("File uri: " + file.getUri() + " should exist", ioManager.exists(file.getUri()));
        }
    }

    @Test
    public void testUnlinkFile() throws CatalogException, IOException {
        URI uri = Paths.get(getStudyURI()).resolve("data").toUri();
        link(uri, "myDirectory", studyFqn, new ObjectMap("parents", true), sessionIdUser);

        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.PATH.key(), "~myDirectory/*")
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.READY);
        DataResult<File> fileDataResultLinked = fileManager.search(studyFqn, query, null, sessionIdUser);

        int numberLinkedFiles = fileDataResultLinked.getNumResults();
        System.out.println("Number of files/folders linked = " + numberLinkedFiles);

        Query unlinkQuery = new Query(FileDBAdaptor.QueryParams.PATH.key(), "myDirectory/data/test/folder/test_0.5K.txt");

        setToPendingDelete(studyFqn, unlinkQuery);

        // Now we try to unlink the file
        fileManager.unlink(studyFqn, "myDirectory/data/test/folder/test_0.5K.txt", sessionIdUser);
        fileDataResultLinked = fileManager.search(studyFqn, unlinkQuery, QueryOptions.empty(), sessionIdUser);
        assertEquals(0, fileDataResultLinked.getNumResults());

        unlinkQuery.put(FileDBAdaptor.QueryParams.DELETED.key(), true);
        fileDataResultLinked = fileManager.search(studyFqn, unlinkQuery, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, fileDataResultLinked.getNumResults());
        assertEquals(File.FileStatus.REMOVED, fileDataResultLinked.first().getStatus().getName());

        // Check the other root linked files/folders have not been touched
        fileDataResultLinked = fileManager.search(studyFqn, query, QueryOptions.empty(), sessionIdUser);
        assertEquals(numberLinkedFiles - 1, fileDataResultLinked.getNumResults());

        // We send the unlink command again
        thrown.expect(CatalogException.class);
        thrown.expectMessage("not found");
        fileManager.unlink(studyFqn, "myDirectory/data/test/folder/test_0.5K.txt", sessionIdUser);
    }

    @Test
    public void testCreateFile() throws CatalogException, IOException {
        String content = "This is the content\tof the file";
        try {
            fileManager.create(studyFqn3, File.Type.FILE, File.Format.UNKNOWN, File.Bioformat.UNKNOWN,
                    "data/test/myTest/myFile.txt", null, new File.FileStatus(File.FileStatus.READY), 0, null, -1,
                    null, null, false, "This is the content\tof the file", null, sessionIdUser2);
            fail("An error should be raised because parents is false");
        } catch (CatalogException e) {
            System.out.println("Correct");
        }

        DataResult<File> fileDataResult = fileManager.create(studyFqn3, File.Type.FILE, File.Format.UNKNOWN, File.Bioformat.UNKNOWN,
                "data/test/myTest/myFile.txt", null, new File.FileStatus(File.FileStatus.READY), 0, null, -1, null, null, true,
                content, null, sessionIdUser2);
        CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(fileDataResult.first().getUri());
        assertTrue(ioManager.exists(fileDataResult.first().getUri()));

        DataInputStream fileObject = ioManager.getFileObject(fileDataResult.first().getUri(), -1, -1);
        assertEquals(content, fileObject.readLine());
    }

    @Test
    public void testCreateFolder() throws Exception {
        Query query = new Query(StudyDBAdaptor.QueryParams.OWNER.key(), "user2");
        Study study = catalogManager.getStudyManager().get(query, QueryOptions.empty(), sessionIdUser2).first();
        Set<String> paths = fileManager.search(study.getFqn(), new Query("type", File.Type.DIRECTORY), new
                QueryOptions(), sessionIdUser2)
                .getResults().stream().map(File::getPath).collect(Collectors.toSet());
        assertEquals(2, paths.size());
        assertTrue(paths.contains(""));             //root
        assertTrue(paths.contains("JOBS/"));        //JOBS
//        assertTrue(paths.contains("analysis/"));    //analysis

        Path folderPath = Paths.get("data", "new", "folder");
        File folder = fileManager.createFolder(study.getFqn(), folderPath.toString(), null, true, null,
                QueryOptions.empty(), sessionIdUser2).first();
        System.out.println(folder);
        CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(folder.getUri());
        assertTrue(!ioManager.exists(folder.getUri()));

        paths = fileManager.search(study.getFqn(), new Query(FileDBAdaptor.QueryParams.TYPE.key(), File.Type
                .DIRECTORY), new QueryOptions(), sessionIdUser2).getResults().stream().map(File::getPath).collect(Collectors.toSet());
        assertEquals(5, paths.size());
        assertTrue(paths.contains("data/new/"));
        assertTrue(paths.contains("data/new/folder/"));

        URI uri = fileManager.getUri(folder);
        assertTrue(!catalogManager.getCatalogIOManagerFactory().get(uri).exists(uri));

        fileManager.createFolder(study.getFqn(), Paths.get("WOLOLO").toString(), null, true, null, QueryOptions.empty(),
                sessionIdUser2);

        Path myStudy = Files.createDirectory(catalogManagerResource.getOpencgaHome().resolve("myStudy"));
        String newStudy = catalogManager.getStudyManager().create(project2, "alias", null, "name", Study.Type.CASE_CONTROL, "", "", null, null, null, myStudy.toUri(), null, null, null, null, sessionIdUser2).first().getFqn();

        folder = fileManager.createFolder(newStudy, Paths.get("WOLOLO").toString(), null, true, null,
                QueryOptions.empty(), sessionIdUser2).first();
        assertTrue(!ioManager.exists(folder.getUri()));
    }

    @Test
    public void testCreateFolderAlreadyExists() throws Exception {
        Set<String> paths = fileManager.search(studyFqn3, new Query("type", File.Type.DIRECTORY), new QueryOptions(), sessionIdUser2).getResults().stream().map(File::getPath).collect(Collectors.toSet());
        assertEquals(2, paths.size());
        assertTrue(paths.contains(""));             //root
//        assertTrue(paths.contains("data/"));        //data
//        assertTrue(paths.contains("analysis/"));    //analysis

        Path folderPath = Paths.get("data", "new", "folder");
        File folder = fileManager.createFolder(studyFqn3, folderPath.toString(), null, true, null, null,
                sessionIdUser2).first();

        assertNotNull(folder);
        assertTrue(folder.getPath().contains(folderPath.toString()));

        // When creating the same folder, we should not complain and return it directly
        File sameFolder = fileManager.createFolder(studyFqn3, folderPath.toString(), null, true, null, null, sessionIdUser2).first();
        assertNotNull(sameFolder);
        assertEquals(folder.getPath(), sameFolder.getPath());
        assertEquals(folder.getUid(), sameFolder.getUid());

        // However, a user without create permissions will receive an exception
        thrown.expect(CatalogAuthorizationException.class);
        fileManager.createFolder(studyFqn3, folderPath.toString(), null, true, null, null,
                sessionIdUser3);
    }

    @Test
    public void testAnnotationWrongEntity() throws CatalogException, JsonProcessingException {
        List<Variable> variables = new ArrayList<>();
        variables.add(new Variable("var_name", "", "", Variable.VariableType.TEXT, "", true, false, Collections.emptyList(), 0, "", "",
                null, Collections.emptyMap()));
        variables.add(new Variable("AGE", "", "", Variable.VariableType.INTEGER, "", false, false, Collections.emptyList(), 0, "", "",
                null, Collections.emptyMap()));
        variables.add(new Variable("HEIGHT", "", "", Variable.VariableType.DOUBLE, "", false, false, Collections.emptyList(), 0, "",
                "", null, Collections.emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", false, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), sessionIdUser).first();

        ObjectMap annotations = new ObjectMap()
                .append("var_name", "Joe")
                .append("AGE", 25)
                .append("HEIGHT", 180);
        AnnotationSet annotationSet = new AnnotationSet("annotation1", vs1.getId(), annotations);

        FileUpdateParams updateParams = new FileUpdateParams().setAnnotationSets(Collections.singletonList(annotationSet));

        thrown.expect(CatalogException.class);
        thrown.expectMessage("intended only for");
        fileManager.update(studyFqn, "data/", updateParams, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void testAnnotationForAnyEntity() throws CatalogException, JsonProcessingException {
        List<Variable> variables = new ArrayList<>();
        variables.add(new Variable("var_name", "", "", Variable.VariableType.TEXT, "", true, false, Collections.emptyList(), 0, "", "",
                null, Collections.emptyMap()));
        variables.add(new Variable("AGE", "", "", Variable.VariableType.INTEGER, "", false, false, Collections.emptyList(), 0, "", "",
                null, Collections.emptyMap()));
        variables.add(new Variable("HEIGHT", "", "", Variable.VariableType.DOUBLE, "", false, false, Collections.emptyList(), 0, "",
                "", null, Collections.emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", false, false, "", null, variables,
                null, sessionIdUser).first();

        ObjectMap annotations = new ObjectMap()
                .append("var_name", "Joe")
                .append("AGE", 25)
                .append("HEIGHT", 180);
        AnnotationSet annotationSet = new AnnotationSet("annotation1", vs1.getId(), annotations);

        FileUpdateParams updateParams = new FileUpdateParams().setAnnotationSets(Collections.singletonList(annotationSet));

        DataResult<File> updateResult = fileManager.update(studyFqn, "data/", updateParams, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, updateResult.getNumUpdated());

        File file = fileManager.get(studyFqn, "data/", QueryOptions.empty(), sessionIdUser).first();
        assertEquals(1, file.getAnnotationSets().size());
    }

    @Test
    public void testAnnotations() throws CatalogException, JsonProcessingException {
        List<Variable> variables = new ArrayList<>();
        variables.add(new Variable("var_name", "", "", Variable.VariableType.TEXT, "", true, false, Collections.emptyList(), 0, "", "",
                null, Collections.emptyMap()));
        variables.add(new Variable("AGE", "", "", Variable.VariableType.INTEGER, "", false, false, Collections.emptyList(), 0, "", "",
                null, Collections.emptyMap()));
        variables.add(new Variable("HEIGHT", "", "", Variable.VariableType.DOUBLE, "", false, false, Collections.emptyList(), 0, "",
                "", null, Collections.emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", false, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.FILE), sessionIdUser).first();

        ObjectMap annotations = new ObjectMap()
                .append("var_name", "Joe")
                .append("AGE", 25)
                .append("HEIGHT", 180);
        AnnotationSet annotationSet = new AnnotationSet("annotation1", vs1.getId(), annotations);
        AnnotationSet annotationSet1 = new AnnotationSet("annotation2", vs1.getId(), annotations);

        FileUpdateParams updateParams = new FileUpdateParams().setAnnotationSets(Arrays.asList(annotationSet, annotationSet1));
        DataResult<File> updateResult = fileManager.update(studyFqn, "data/", updateParams, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, updateResult.getNumUpdated());

        File file = fileManager.get(studyFqn, "data/", QueryOptions.empty(), sessionIdUser).first();
        assertEquals(2, file.getAnnotationSets().size());
    }

    @Test
    public void testUpdateSamples() throws CatalogException {
        // Update the same sample twice to the file
        FileUpdateParams updateParams = new FileUpdateParams().setSamples(Arrays.asList("s_1", "s_1", "s_2", "s_1"));
        DataResult<File> updateResult = fileManager.update(studyFqn, "test_1K.txt.gz", updateParams, null, sessionIdUser);
        assertEquals(1, updateResult.getNumUpdated());

        File file = fileManager.get(studyFqn, "test_1K.txt.gz", QueryOptions.empty(), sessionIdUser).first();
        assertEquals(2, file.getSamples().size());
        assertTrue(file.getSamples().stream().map(Sample::getId).collect(Collectors.toSet()).containsAll(Arrays.asList("s_1", "s_2")));
    }

    @Test
    public void testCreate() throws Exception {
        String fileName = "item." + TimeUtils.getTimeMillis() + ".vcf";
        DataResult<File> fileResult = fileManager.create(studyFqn, File.Type.FILE, File.Format.PLAIN, File.Bioformat.VARIANT,
                "data/" + fileName, "description", null, 0, null, (long) -1, null, null, true, getDummyVCFContent(), null, sessionIdUser);
        assertEquals(3, fileResult.first().getSamples().size());

        fileName = "item." + TimeUtils.getTimeMillis() + ".vcf";
        fileManager.create(studyFqn, File.Type.FILE, File.Format.PLAIN, File.Bioformat.VARIANT, "data/" + fileName, "description", null, 0,
                null, (long) -1, null, null, true, getDummyVCFContent(), null, sessionIdUser);

        fileName = "item." + TimeUtils.getTimeMillis() + ".txt";
        DataResult<File> queryResult = fileManager.create(studyFqn, new File().setPath("data/" + fileName), false,
                StringUtils.randomString(200), null, sessionIdUser);
        assertEquals(File.FileStatus.READY, queryResult.first().getStatus().getName());
        assertEquals(200, queryResult.first().getSize());

        fileManager.create(studyFqn, File.Type.FILE, File.Format.PLAIN, File.Bioformat.NONE, "data/deletable/folder/item." + TimeUtils.getTimeMillis() + ".txt",
                "description", null, 0, null, (long) -1, null, null, true, createRandomString(200), null, sessionIdUser);

        fileManager.create(studyFqn2, File.Type.FILE, File.Format.PLAIN, File.Bioformat.NONE, "data/deletable/item." + TimeUtils.getTimeMillis() + ".txt",
                "description", null, 0, null, (long) -1, null, null, true, createRandomString(200), null, sessionIdUser);

        fileManager.create(studyFqn2, File.Type.FILE, File.Format.PLAIN, File.Bioformat.NONE, "item." + TimeUtils.getTimeMillis() + ".txt",
                "file at root", null, 0, null, (long) -1, null, null, true, createRandomString(200), null, sessionIdUser);

        fileName =  "item." + TimeUtils.getTimeMillis() + ".txt";
        fileManager.create(studyFqn2, File.Type.FILE, File.Format.PLAIN, File.Bioformat.NONE, fileName,
                "file at root", null, 0, null, (long) -1, null, null, true, createRandomString(200), null, sessionIdUser);

        DataResult<File> fileDataResult = fileManager.get(studyFqn2, fileName, null, sessionIdUser);
        assertTrue(fileDataResult.first().getSize() > 0);
    }

    @Test
    public void testCreateFileInLinkedFolder() throws Exception {
        // Create an empty folder
        Path dir = catalogManagerResource.getOpencgaHome().resolve("folder_to_link");
        Files.createDirectory(dir);
        URI uri = dir.toUri();

        // Link the folder in the root
        link(uri, "", studyFqn, new ObjectMap(), sessionIdUser);

        File file = fileManager.create(studyFqn, new File(File.Type.FILE, File.Format.PLAIN, File.Bioformat.NONE, "folder_to_link/file.txt",
                "", null, 0, null, (long) -1, null, null, null), false, "bla bla", null, sessionIdUser).first();

        assertEquals(uri.resolve("file.txt"), file.getUri());
    }

    @Test
    public void testDownloadAndHeadFile() throws CatalogException, IOException, InterruptedException {
        String fileName = "item." + TimeUtils.getTimeMillis() + ".vcf";

        File file = fileManager.create(studyFqn, File.Type.FILE, File.Format.PLAIN, File.Bioformat.VARIANT, "data/" + fileName,
                "description", null, 0, null, (long) -1, null, null, true, getDummyVCFContent(), null, sessionIdUser).first();

        byte[] bytes = new byte[100];
        byte[] bytesOrig = new byte[100];
        DataInputStream fis = new DataInputStream(new FileInputStream(file.getUri().getPath()));
        DataInputStream dis = fileManager.download(studyFqn, file.getPath(), -1, -1, sessionIdUser);
        fis.read(bytesOrig, 0, 100);
        dis.read(bytes, 0, 100);
        fis.close();
        dis.close();
        assertArrayEquals(bytesOrig, bytes);

        int offset = 1;
        int limit = 10;
        dis = fileManager.download(studyFqn, file.getPath(), offset, limit, sessionIdUser);
        fis = new DataInputStream(new FileInputStream(file.getUri().getPath()));
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
    }

    @Test
    public void testDownloadFile() throws CatalogException, IOException, URISyntaxException {
        URI sourceUri = getClass().getResource("/biofiles/variant-test-file.vcf.gz").toURI();
        OpenCGAResult<File> fileResult = fileManager.link(studyFqn, sourceUri, "data/", new ObjectMap("parents", true), sessionIdUser);

        DataInputStream dis = fileManager.download(studyFqn, fileResult.first().getPath(), -1, -1, sessionIdUser);

        byte[] bytes = new byte[(int) fileResult.first().getSize()];
        dis.read(bytes, 0, (int) fileResult.first().getSize());
        assertTrue(Arrays.equals(Files.readAllBytes(Paths.get(sourceUri)), bytes));
    }

    @Test
    public void testGetTreeView() throws CatalogException {
        DataResult<FileTree> fileTree = fileManager.getTree(studyFqn, "/", new Query(), new QueryOptions(),
                5, sessionIdUser);
        assertEquals(8, fileTree.getNumResults());
    }

    @Test
    public void testGetTreeViewMoreThanOneFile() throws CatalogException {

        // Create a new study so more than one file will be found under the root /. However, it should be able to consider the study given
        // properly
        catalogManager.getStudyManager().create(project1, "phase2", null, "Phase 2", Study.Type.TRIO, null, "Done", null, null, null, null, null, null, null, null, sessionIdUser).first().getUid();

        DataResult<FileTree> fileTree = fileManager.getTree(studyFqn, "/", new Query(), new QueryOptions(),
                5, sessionIdUser);
        assertEquals(8, fileTree.getNumResults());

        fileTree = fileManager.getTree("user@1000G:phase2", ".", new Query(), new QueryOptions(), 5, sessionIdUser);
        assertEquals(2, fileTree.getNumResults());
    }

    @Test
    public void renameFileTest() throws CatalogException {
        DataResult<File> queryResult1 = fileManager.create(studyFqn, new File().setPath("data/file.txt"), true,
                StringUtils.randomString(200), null, sessionIdUser);
        assertEquals(1, queryResult1.getNumResults());

        DataResult<File> queryResult = fileManager.create(studyFqn, new File().setPath("data/nested/folder/file2.txt"),
                true, StringUtils.randomString(200), null, sessionIdUser);
        assertEquals(1, queryResult.getNumResults());

        fileManager.rename(studyFqn, "data/nested/", "nested2", sessionIdUser);
        Set<String> paths = fileManager.search(studyFqn, new Query(), new QueryOptions(), sessionIdUser)
                .getResults()
                .stream().map(File::getPath).collect(Collectors.toSet());

        assertTrue(paths.contains("data/nested2/"));
        assertFalse(paths.contains("data/nested/"));
        assertTrue(paths.contains("data/nested2/folder/"));
        assertTrue(paths.contains("data/nested2/folder/file2.txt"));
        assertTrue(paths.contains("data/file.txt"));

        fileManager.rename(studyFqn, "data/", "Data", sessionIdUser);
        paths = fileManager.search(studyFqn, new Query(), new QueryOptions(), sessionIdUser).getResults()
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
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), "user2", aclParams, sessionIdUser);
        File file = fileManager.create(studyFqn, File.Type.FILE, File.Format.UNKNOWN, File.Bioformat.NONE,
                "data/test/folder/file.txt", "My description", null, 0, null, (long) -1, null, null, true, "blabla", null,
                sessionIdUser2).first();
        long fileId = fileManager.get(studyFqn, file.getPath(), FileManager.INCLUDE_FILE_IDS, sessionIdUser).first().getUid();
        assertEquals(file.getUid(), fileId);

        fileId = fileManager.get(studyFqn, file.getPath(), FileManager.INCLUDE_FILE_IDS, sessionIdUser).first().getUid();
        assertEquals(file.getUid(), fileId);

        fileId = fileManager.get(studyFqn, "/", FileManager.INCLUDE_FILE_IDS, sessionIdUser).first().getUid();
        System.out.println(fileId);
    }

    @Test
    public void renameFileEmptyName() throws CatalogException {
        thrown.expect(CatalogParameterException.class);
        thrown.expectMessage(containsString("null or empty"));
        fileManager.rename(studyFqn, "data/", "", sessionIdUser);
    }

    @Test
    public void renameFileSlashInName() throws CatalogException {
        thrown.expect(CatalogParameterException.class);
        fileManager.rename(studyFqn, "data/", "my/folder", sessionIdUser);
    }

    @Test
    public void renameFileAlreadyExists() throws CatalogException {
        fileManager.createFolder(studyFqn, "analysis/", new File.FileStatus(), false, "", new QueryOptions(),
                sessionIdUser);
        thrown.expect(CatalogException.class);
        thrown.expectMessage("already exists");
        fileManager.rename(studyFqn, "data/", "analysis", sessionIdUser);
    }

    @Test
    public void searchFileTest() throws CatalogException {
        Query query;
        DataResult<File> result;

        // Look for a file and folder
        DataResult<File> queryResults = fileManager.get(studyFqn, Arrays.asList("data/", "data/test/folder/test_1K.txt.gz"),
                new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(FileDBAdaptor.QueryParams.NAME.key())), sessionIdUser);
        assertEquals(2, queryResults.getNumResults());
        assertTrue("Name not included", queryResults.getResults().stream().map(File::getName)
                .filter(org.apache.commons.lang3.StringUtils::isNotEmpty)
                .collect(Collectors.toList()).size() == 2);

        query = new Query(FileDBAdaptor.QueryParams.NAME.key(), "~data");
        result = fileManager.search(studyFqn, query, null, sessionIdUser);
        assertEquals(1, result.getNumResults());

        query = new Query(FileDBAdaptor.QueryParams.NAME.key(), "~txt.gz$");
        result = fileManager.search(studyFqn, query, null, sessionIdUser);
        assertEquals(1, result.getNumResults());

        //Get all files in data
        query = new Query(FileDBAdaptor.QueryParams.PATH.key(), "~data/[^/]+/?")
                .append(FileDBAdaptor.QueryParams.TYPE.key(),"FILE");
        result = fileManager.search(studyFqn, query, null, sessionIdUser);
        assertEquals(3, result.getNumResults());

        //Folder "jobs" does not exist
        query = new Query(FileDBAdaptor.QueryParams.DIRECTORY.key(), "jobs");
        result = fileManager.search(studyFqn, query, null, sessionIdUser);
        assertEquals(0, result.getNumResults());

        //Get all files in data
        query = new Query(FileDBAdaptor.QueryParams.DIRECTORY.key(), "data/");
        result = fileManager.search(studyFqn, query, null, sessionIdUser);
        assertEquals(1, result.getNumResults());

        //Get all files in data recursively
        query = new Query(FileDBAdaptor.QueryParams.DIRECTORY.key(), "data/.*");
        result = fileManager.search(studyFqn, query, null, sessionIdUser);
        assertEquals(5, result.getNumResults());

        query = new Query(FileDBAdaptor.QueryParams.TYPE.key(), "FILE");
        result = fileManager.search(studyFqn, query, null, sessionIdUser);
        result.getResults().forEach(f -> assertEquals(File.Type.FILE, f.getType()));
        int numFiles = result.getNumResults();
        assertEquals(3, numFiles);

        query = new Query(FileDBAdaptor.QueryParams.TYPE.key(), "DIRECTORY");
        result = fileManager.search(studyFqn, query, null, sessionIdUser);
        result.getResults().forEach(f -> assertEquals(File.Type.DIRECTORY, f.getType()));
        int numFolders = result.getNumResults();
        assertEquals(5, numFolders);

        query = new Query(FileDBAdaptor.QueryParams.PATH.key(), "");
        result = fileManager.search(studyFqn, query, null, sessionIdUser);
        assertEquals(1, result.getNumResults());
        assertEquals(".", result.first().getName());


        query = new Query(FileDBAdaptor.QueryParams.TYPE.key(), "FILE,DIRECTORY");
        result = fileManager.search(studyFqn, query, null, sessionIdUser);
        assertEquals(8, result.getNumResults());
        assertEquals(numFiles + numFolders, result.getNumResults());

        query = new Query("type", "FILE");
        query.put("size", ">400");
        result = fileManager.search(studyFqn, query, null, sessionIdUser);
        assertEquals(2, result.getNumResults());

        query = new Query("type", "FILE");
        query.put("size", "<400");
        result = fileManager.search(studyFqn, query, null, sessionIdUser);
        assertEquals(1, result.getNumResults());

        List<String> sampleIds = catalogManager.getSampleManager().search(studyFqn, new Query(SampleDBAdaptor.QueryParams.ID.key(), "s_1,s_3,s_4"), null, sessionIdUser).getResults()
                .stream()
                .map(Sample::getId)
                .collect(Collectors.toList());
        result = fileManager.search(studyFqn, new Query(FileDBAdaptor.QueryParams.SAMPLES.key(), sampleIds), null, sessionIdUser);
        assertEquals(1, result.getNumResults());

        query = new Query(FileDBAdaptor.QueryParams.TYPE.key(), "FILE");
        query.put(FileDBAdaptor.QueryParams.FORMAT.key(), "PLAIN");
        result = fileManager.search(studyFqn, query, null, sessionIdUser);
        assertEquals(2, result.getNumResults());

        String attributes = FileDBAdaptor.QueryParams.ATTRIBUTES.key();
        String nattributes = FileDBAdaptor.QueryParams.NATTRIBUTES.key();
        String battributes = FileDBAdaptor.QueryParams.BATTRIBUTES.key();
        /*

        interface Searcher {
            DataResult search(Integer id, Query query);
        }

        BiFunction<Integer, Query, DataResult> searcher = (s, q) -> catalogManager.searchFile(s, q, sessionIdUser);

        result = searcher.apply(studyUid, new Query(attributes + ".nested.text", "~H"));
        */
        result = fileManager.search(studyFqn, new Query(attributes + ".nested.text", "~H"), null, sessionIdUser);
        assertEquals(1, result.getNumResults());
        result = fileManager.search(studyFqn, new Query(nattributes + ".nested.num1", ">0"), null, sessionIdUser);
        assertEquals(1, result.getNumResults());
        result = fileManager.search(studyFqn, new Query(attributes + ".nested.num1", ">0"), null, sessionIdUser);
        assertEquals(0, result.getNumResults());

        result = fileManager.search(studyFqn, new Query(attributes + ".nested.num1", "notANumber"), null, sessionIdUser);
        assertEquals(0, result.getNumResults());

        result = fileManager.search(studyFqn, new Query(attributes + ".field", "~val"), null, sessionIdUser);
        assertEquals(3, result.getNumResults());

        result = fileManager.search(studyFqn, new Query("attributes.field", "~val"), null, sessionIdUser);
        assertEquals(3, result.getNumResults());

        result = fileManager.search(studyFqn, new Query(attributes + ".field", "=~val"), null, sessionIdUser);
        assertEquals(3, result.getNumResults());

        result = fileManager.search(studyFqn, new Query(attributes + ".field", "~val"), null, sessionIdUser);
        assertEquals(3, result.getNumResults());

        result = fileManager.search(studyFqn, new Query(attributes + ".field", "value"), null, sessionIdUser);
        assertEquals(2, result.getNumResults());

        result = fileManager.search(studyFqn, new Query(attributes + ".field", "other"), null, sessionIdUser);
        assertEquals(1, result.getNumResults());

        result = fileManager.search(studyFqn, new Query("nattributes.numValue", ">=5"), null, sessionIdUser);
        assertEquals(3, result.getNumResults());

        result = fileManager.search(studyFqn, new Query("nattributes.numValue", ">4,<6"), null, sessionIdUser);
        assertEquals(3, result.getNumResults());

        result = fileManager.search(studyFqn, new Query(nattributes + ".numValue", "==5"), null, sessionIdUser);
        assertEquals(2, result.getNumResults());

        result = fileManager.search(studyFqn, new Query(nattributes + ".numValue", "==5.0"), null, sessionIdUser);
        assertEquals(2, result.getNumResults());

        result = fileManager.search(studyFqn, new Query(nattributes + ".numValue", "=5.0"), null, sessionIdUser);
        assertEquals(2, result.getNumResults());

        result = fileManager.search(studyFqn, new Query(nattributes + ".numValue", "5.0"), null, sessionIdUser);
        assertEquals(2, result.getNumResults());

        result = fileManager.search(studyFqn, new Query(nattributes + ".numValue", ">5"), null, sessionIdUser);
        assertEquals(1, result.getNumResults());

        result = fileManager.search(studyFqn, new Query(nattributes + ".numValue", ">4"), null, sessionIdUser);
        assertEquals(3, result.getNumResults());

        result = fileManager.search(studyFqn, new Query(nattributes + ".numValue", "<6"), null, sessionIdUser);
        assertEquals(2, result.getNumResults());

        result = fileManager.search(studyFqn, new Query(nattributes + ".numValue", "<=5"), null, sessionIdUser);
        assertEquals(2, result.getNumResults());

        result = fileManager.search(studyFqn, new Query(nattributes + ".numValue", "<5"), null, sessionIdUser);
        assertEquals(0, result.getNumResults());

        result = fileManager.search(studyFqn, new Query(nattributes + ".numValue", "<2"), null, sessionIdUser);
        assertEquals(0, result.getNumResults());

        result = fileManager.search(studyFqn, new Query(nattributes + ".numValue", "==23"), null, sessionIdUser);
        assertEquals(0, result.getNumResults());

        result = fileManager.search(studyFqn, new Query(attributes + ".numValue", "=~10"), null, sessionIdUser);
        assertEquals(1, result.getNumResults());

        result = fileManager.search(studyFqn, new Query(nattributes + ".numValue", "=10"), null, sessionIdUser);
        assertEquals(0, result.getNumResults());

        result = fileManager.search(studyFqn, new Query(attributes + ".boolean", "true"), null, sessionIdUser);
        assertEquals(0, result.getNumResults());

        result = fileManager.search(studyFqn, new Query(attributes + ".boolean", "=true"), null, sessionIdUser);
        assertEquals(0, result.getNumResults());

        result = fileManager.search(studyFqn, new Query(attributes + ".boolean", "=1"), null, sessionIdUser);
        assertEquals(0, result.getNumResults());

        result = fileManager.search(studyFqn, new Query(battributes + ".boolean", "true"), null, sessionIdUser);
        assertEquals(1, result.getNumResults());

        result = fileManager.search(studyFqn, new Query(battributes + ".boolean", "=true"), null, sessionIdUser);
        assertEquals(1, result.getNumResults());

        // This has to return not only the ones with the attribute boolean = false, but also all the files that does not contain
        // that attribute at all.
        result = fileManager.search(studyFqn, new Query(battributes + ".boolean", "!=true"), null, sessionIdUser);
        assertEquals(7, result.getNumResults());

        result = fileManager.search(studyFqn, new Query(battributes + ".boolean", "=false"), null, sessionIdUser);
        assertEquals(1, result.getNumResults());

        query = new Query();
        query.append(attributes + ".name", "fileTest1k");
        query.append(attributes + ".field", "value");
        result = fileManager.search(studyFqn, query, null, sessionIdUser);
        assertEquals(1, result.getNumResults());

        query = new Query();
        query.append(attributes + ".name", "fileTest1k");
        query.append(attributes + ".field", "value");
        query.append(attributes + ".numValue", Arrays.asList(8, 9, 10));   //Searching as String. numValue = "10"
        result = fileManager.search(studyFqn, query, null, sessionIdUser);
        assertEquals(1, result.getNumResults());

        QueryOptions options = new QueryOptions(QueryOptions.LIMIT, 2).append(QueryOptions.COUNT, true);
        result = fileManager.search(studyFqn, new Query(), options, sessionIdUser);
        assertEquals(2, result.getNumResults());
        assertEquals(8, result.getNumMatches());

        options = new QueryOptions(QueryOptions.LIMIT, 2);
        result = fileManager.search(studyFqn, new Query(), options, sessionIdUser);
        assertEquals(2, result.getNumResults());
        assertEquals(2, result.getNumMatches());

    }

    @Test
    public void testSearchFileBoolean() throws CatalogException {
        Query query;
        DataResult<File> result;
        FileDBAdaptor.QueryParams battributes = FileDBAdaptor.QueryParams.BATTRIBUTES;

        query = new Query(battributes.key() + ".boolean", "true");       //boolean in [true]
        result = fileManager.search(studyFqn, query, null, sessionIdUser);
        assertEquals(1, result.getNumResults());

        query = new Query(battributes.key() + ".boolean", "false");      //boolean in [false]
        result = fileManager.search(studyFqn, query, null, sessionIdUser);
        assertEquals(1, result.getNumResults());

        query = new Query(battributes.key() + ".boolean", "!=false");    //boolean in [null, true]
        query.put("type", "FILE");
        result = fileManager.search(studyFqn, query, null, sessionIdUser);
        assertEquals(2, result.getNumResults());

        query = new Query(battributes.key() + ".boolean", "!=true");     //boolean in [null, false]
        query.put("type", "FILE");
        result = fileManager.search(studyFqn, query, null, sessionIdUser);
        assertEquals(2, result.getNumResults());
    }

    @Test
    public void testSearchFileFail1() throws CatalogException {
        thrown.expect(CatalogDBException.class);
        fileManager.search(studyFqn, new Query(FileDBAdaptor.QueryParams.NATTRIBUTES.key() + ".numValue", "==NotANumber"), null,
                sessionIdUser);
    }

    @Test
    public void testGetFileParents1() throws CatalogException {
        long fileId;
        DataResult<File> fileParents;

        fileId = fileManager.get(studyFqn, "data/test/folder/", FileManager.INCLUDE_FILE_IDS, sessionIdUser).first().getUid();
        fileParents = fileManager.getParents(fileId, null, sessionIdUser);

        assertEquals(4, fileParents.getNumResults());
        assertEquals("", fileParents.getResults().get(0).getPath());
        assertEquals("data/", fileParents.getResults().get(1).getPath());
        assertEquals("data/test/", fileParents.getResults().get(2).getPath());
        assertEquals("data/test/folder/", fileParents.getResults().get(3).getPath());
    }

    @Test
    public void testGetFileParents2() throws CatalogException {
        long fileId;
        DataResult<File> fileParents;

        fileId = fileManager.get(studyFqn, "data/test/folder/test_1K.txt.gz", FileManager.INCLUDE_FILE_IDS, sessionIdUser)
            .first().getUid();
        fileParents = fileManager.getParents(fileId, null, sessionIdUser);

        assertEquals(5, fileParents.getNumResults());
        assertEquals("", fileParents.getResults().get(0).getPath());
        assertEquals("data/", fileParents.getResults().get(1).getPath());
        assertEquals("data/test/", fileParents.getResults().get(2).getPath());
        assertEquals("data/test/folder/", fileParents.getResults().get(3).getPath());
        assertEquals("data/test/folder/test_1K.txt.gz", fileParents.getResults().get(4).getPath());
    }

    @Test
    public void testGetFileParents3() throws CatalogException {
        long fileId;
        DataResult<File> fileParents;

        fileId = fileManager.get(studyFqn, "data/test/", FileManager.INCLUDE_FILE_IDS, sessionIdUser).first().getUid();
        fileParents = fileManager.getParents(fileId, new QueryOptions("include", "projects.studies.files.path," +
                "projects.studies.files.id"), sessionIdUser);

        assertEquals(3, fileParents.getNumResults());
        assertEquals("", fileParents.getResults().get(0).getPath());
        assertEquals("data/", fileParents.getResults().get(1).getPath());
        assertEquals("data/test/", fileParents.getResults().get(2).getPath());

        fileParents.getResults().forEach(f -> {
            assertNull(f.getName());
            assertNotNull(f.getPath());
            assertTrue(f.getUid() != 0);
        });

    }

    @Test
    public void testGetFileWithSamples() throws CatalogException {
        DataResult<File> fileDataResult = fileManager.get(studyFqn, "data/test/", QueryOptions.empty(),
                sessionIdUser);
        assertEquals(1, fileDataResult.getNumResults());
        assertEquals(0, fileDataResult.first().getSamples().size());

        // Create two samples
        Sample sample1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("sample1"), QueryOptions.empty(),
                sessionIdUser).first();
        Sample sample2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("sample2"), QueryOptions.empty(),
                sessionIdUser).first();

        // Associate the two samples to the file
        fileManager.update(studyFqn, "data/test/", new FileUpdateParams().setSamples(Arrays.asList(sample1.getId(), sample2.getId())),
                QueryOptions.empty(), sessionIdUser);

        // Fetch the file
        fileDataResult = fileManager.get(studyFqn, "data/test/", new QueryOptions(
                QueryOptions.INCLUDE, Arrays.asList(FileDBAdaptor.QueryParams.ID.key(), FileDBAdaptor.QueryParams.SAMPLE_UIDS.key())),
                sessionIdUser);
        assertEquals(1, fileDataResult.getNumResults());
        assertEquals(2, fileDataResult.first().getSamples().size());
        for (Sample sample : fileDataResult.first().getSamples()) {
            assertTrue(sample.getUid() > 0);
            assertTrue(org.apache.commons.lang3.StringUtils.isEmpty(sample.getId()));
        }

        // Update the version of one of the samples
        catalogManager.getSampleManager().update(studyFqn, sample1.getId(), new SampleUpdateParams(),
                new QueryOptions(Constants.INCREMENT_VERSION, true), sessionIdUser);

        // Fetch the file again to see if we get the latest version as expected
        fileDataResult = fileManager.get(studyFqn, "data/test/", QueryOptions.empty(),
                sessionIdUser);
        assertEquals(1, fileDataResult.getNumResults());
        assertEquals(2, fileDataResult.first().getSamples().size());
        for (Sample sample : fileDataResult.first().getSamples()) {
            if (sample.getId().equals(sample1.getId())) {
                assertEquals(2, sample.getVersion());
            } else if (sample.getId().equals(sample2.getId())) {
                assertEquals(1, sample.getVersion());
            } else {
                fail("The sample found is not sample1 or sample2");
            }
        }
    }

    // Try to delete files/folders whose status is STAGED, MISSING...
    @Test
    public void testDelete1() throws CatalogException {
        String filePath = "data/";
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(FileDBAdaptor.QueryParams.PATH.key(), filePath);
        DataResult<File> fileDataResult = fileManager.search(studyFqn, query, null, sessionIdUser);

        // Change the status to MISSING
        FileUpdateParams updateParams = new FileUpdateParams()
                .setStatus(new File.FileStatus(File.FileStatus.MISSING));
        catalogManager.getFileManager().update(studyFqn, filePath, updateParams, null, sessionIdUser);

        try {
            fileManager.delete(studyFqn, new Query(FileDBAdaptor.QueryParams.UID.key(), fileDataResult.first().getUid()), null, sessionIdUser);
            fail("Expected fail. It should not be able to delete");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("Cannot delete"));
        }
        // Change the status to STAGED
        updateParams = new FileUpdateParams()
                .setStatus(new File.FileStatus(File.FileStatus.STAGE));
        catalogManager.getFileManager().update(studyFqn, filePath, updateParams, null, sessionIdUser);

        try {
            fileManager.delete(studyFqn, new Query(FileDBAdaptor.QueryParams.UID.key(), fileDataResult.first().getUid()), null, sessionIdUser);
            fail("Expected fail. It should not be able to delete");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("Cannot delete"));
        }

        // Change the status to READY
        query = new Query(FileDBAdaptor.QueryParams.PATH.key(), "~^" + filePath + "*");
        setToPendingDelete(studyFqn, query);

        DataResult deleteResult = fileManager.delete(studyFqn, new Query(FileDBAdaptor.QueryParams.UID.key(),
                fileDataResult.first().getUid()), null, sessionIdUser);
        assertEquals(6, deleteResult.getNumMatches());
        assertEquals(6, deleteResult.getNumUpdated());
    }

    // It will try to delete a folder in status ready
    @Test
    public void testDelete2() throws CatalogException, IOException {
        String filePath = "data/";
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(FileDBAdaptor.QueryParams.PATH.key(), filePath);
        File file = fileManager.search(studyFqn, query, null, sessionIdUser).first();

        // We look for all the files and folders that fall within that folder
        query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(FileDBAdaptor.QueryParams.PATH.key(), "~^" + filePath + "*")
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.READY);
        int numResults = fileManager.search(studyFqn, query, null, sessionIdUser).getNumResults();
        assertEquals(6, numResults);

        query = new Query(FileDBAdaptor.QueryParams.PATH.key(), "~^" + file.getPath() + "*");
        setToPendingDelete(studyFqn, query);

        // We delete it
        fileManager.delete(studyFqn, new Query(FileDBAdaptor.QueryParams.UID.key(), file.getUid()), null, sessionIdUser);

        // The files should have been moved to trashed status
        OpenCGAResult<File> search = fileManager.search(studyFqn, query, null, sessionIdUser);
        assertEquals(6, search.getNumResults());
        for (File trashedFile : search.getResults()) {
            assertEquals(File.FileStatus.TRASHED, trashedFile.getStatus().getName());
        }
    }

    // It will try to delete a folder in status ready and skip the trash
    @Test
    public void testDelete3() throws CatalogException, IOException {
        String filePath = "data/";
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(FileDBAdaptor.QueryParams.PATH.key(), filePath);
        File file = fileManager.search(studyFqn, query, null, sessionIdUser).first();

        // We look for all the files and folders that fall within that folder
        query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(FileDBAdaptor.QueryParams.PATH.key(), "~^" + filePath + "*")
                .append(FileDBAdaptor.QueryParams.STATUS.key(), File.FileStatus.READY);
        int numResults = fileManager.search(studyFqn, query, null, sessionIdUser).getNumResults();
        assertEquals(6, numResults);

        setToPendingDelete(studyFqn, query);

        // We delete it
        QueryOptions queryOptions = new QueryOptions(Constants.SKIP_TRASH, true);
        fileManager.delete(studyFqn, new Query(FileDBAdaptor.QueryParams.UID.key(), file.getUid()),
                queryOptions, sessionIdUser);

        // The files should have been moved to trashed status
        numResults = fileManager.search(studyFqn, query, null, sessionIdUser).getNumResults();
        assertEquals(0, numResults);

        query.put(FileDBAdaptor.QueryParams.DELETED.key(), true);
        query.put(FileDBAdaptor.QueryParams.STATUS.key(), File.FileStatus.DELETED);
        numResults = fileManager.search(studyFqn, query, null, sessionIdUser).getNumResults();
        assertEquals(6, numResults);
    }

    @Test
    public void testDeleteFile() throws CatalogException, IOException {
        List<File> result = fileManager.search(studyFqn, new Query(FileDBAdaptor.QueryParams.TYPE.key(),
                "FILE"), new QueryOptions(), sessionIdUser).getResults();

        // 1st we set the status to PENDING DELETE.
        setToPendingDelete(studyFqn, new Query(FileDBAdaptor.QueryParams.TYPE.key(), "FILE"));

        for (File file : result) {
            fileManager.delete(studyFqn, new Query(FileDBAdaptor.QueryParams.UID.key(), file.getUid()), null, sessionIdUser);
        }
//        CatalogFileUtils catalogFileUtils = new CatalogFileUtils(catalogManager);
        fileManager.search(studyFqn, new Query(FileDBAdaptor.QueryParams.TYPE.key(), "FILE"), new QueryOptions(), sessionIdUser).getResults().forEach(f -> {
                    assertEquals(f.getStatus().getName(), File.FileStatus.TRASHED);
        });

        result = fileManager.search(studyFqn2, new Query(FileDBAdaptor.QueryParams.TYPE.key(), "FILE"), new QueryOptions(), sessionIdUser).getResults();
        // 1st we set the status to PENDING DELETE.
        setToPendingDelete(studyFqn2, new Query(FileDBAdaptor.QueryParams.TYPE.key(), "FILE"));
        for (File file : result) {
            fileManager.delete(studyFqn2, new Query(FileDBAdaptor.QueryParams.UID.key(), file.getUid()), null, sessionIdUser);
        }
        fileManager.search(studyFqn2, new Query(FileDBAdaptor.QueryParams.TYPE.key(), "FILE"), new QueryOptions(), sessionIdUser).getResults().forEach(f -> {
                    assertEquals(f.getStatus().getName(), File.FileStatus.TRASHED);
        });
    }

    @Test
    public void testDeleteLeafFolder() throws CatalogException, IOException {
        File deletable = fileManager.get(studyFqn, "/data/test/folder/", QueryOptions.empty(), sessionIdUser).first();
        deleteFolderAndCheck(deletable);
    }

    @Test
    public void testDeleteMiddleFolder() throws CatalogException, IOException {
        File deletable = fileManager.get(studyFqn, "/data/", QueryOptions.empty(), sessionIdUser).first();
        deleteFolderAndCheck(deletable);
    }

    @Test
    public void testDeleteRootFolder() throws CatalogException {
        File deletable = fileManager.get(studyFqn, "/", QueryOptions.empty(), sessionIdUser).first();

        thrown.expect(CatalogException.class);
        thrown.expectMessage("Root directories cannot be deleted");
        fileManager.delete(studyFqn, new Query(FileDBAdaptor.QueryParams.PATH.key(), deletable.getPath()), null, sessionIdUser);
    }

    // Cannot delete staged files
    @Test
    public void deleteFolderTest() throws CatalogException {
        List<File> folderFiles = new LinkedList<>();

        File folder = createBasicDirectoryFileTestEnvironment(folderFiles);

        CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(fileManager.getUri(folder));
        for (File file : folderFiles) {
            assertTrue(ioManager.exists(fileManager.getUri(file)));
        }

        fileManager.create(studyFqn, new File(File.Type.FILE, File.Format.PLAIN, File.Bioformat.NONE,
                "folder/subfolder/subsubfolder/my_staged.txt", null, new File.FileStatus(File.FileStatus.STAGE), (long) 0, null, (long) -1,
                null, null, null), true, "bla bla", null, sessionIdUser).first();

        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.PATH.key(), "~^" + folder.getPath() + "*")
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.READY);
        setToPendingDelete(studyFqn, query);

        try {
            fileManager.delete(studyFqn, new Query(FileDBAdaptor.QueryParams.UID.key(), folder.getUid()), null, sessionIdUser);
            fail("Delete should fail because it cannot delete files in STAGE status");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("STAGE"));
        }

        File fileTmp = fileManager.get(studyFqn, folder.getPath(), null, sessionIdUser).first();
        assertEquals("Folder name should not be modified", folder.getPath(), fileTmp.getPath());
        assertTrue(ioManager.exists(fileTmp.getUri()));

        for (File file : folderFiles) {
            fileTmp = fileManager.get(studyFqn, file.getPath(), null, sessionIdUser).first();
            assertEquals("File name should not be modified", file.getPath(), fileTmp.getPath());
            assertTrue("File uri: " + fileTmp.getUri() + " should exist", ioManager.exists(fileTmp.getUri()));
        }

    }

    // Deleted folders should be all put to TRASHED
    @Test
    public void deleteFolderTest2() throws CatalogException {
        List<File> folderFiles = new LinkedList<>();

        File folder = createBasicDirectoryFileTestEnvironment(folderFiles);

        CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(fileManager.getUri(folder));
        for (File file : folderFiles) {
            assertTrue(ioManager.exists(fileManager.getUri(file)));
        }

        // 1st we set the status to PENDING DELETE.
        setToPendingDelete(studyFqn, new Query(FileDBAdaptor.QueryParams.PATH.key(), "~^" + folder.getPath() + "*"));

        // Now we delete the files
        fileManager.delete(studyFqn, new Query(FileDBAdaptor.QueryParams.UID.key(), folder.getUid()), null, sessionIdUser);

        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.UID.key(), folder.getUid());
        File fileTmp = fileManager.search(studyFqn, query, QueryOptions.empty(), sessionIdUser).first();

        assertEquals("Folder name should not be modified", folder.getPath(), fileTmp.getPath());
        assertEquals("Status should be to TRASHED", File.FileStatus.TRASHED, fileTmp.getStatus().getName());
        assertEquals("Name should not have changed", folder.getName(), fileTmp.getName());
        assertTrue(ioManager.exists(fileTmp.getUri()));

        for (File file : folderFiles) {
            query.put(FileDBAdaptor.QueryParams.UID.key(), file.getUid());
            fileTmp = fileManager.search(studyFqn, query, QueryOptions.empty(), sessionIdUser).first();
            assertEquals("Folder name should not be modified", file.getPath(), fileTmp.getPath());
            assertEquals("Status should be to TRASHED", File.FileStatus.TRASHED, fileTmp.getStatus().getName());
            assertEquals("Name should not have changed", file.getName(), fileTmp.getName());
            assertTrue("File uri: " + fileTmp.getUri() + " should exist", ioManager.exists(fileTmp.getUri()));
        }
    }

    // READY -> PENDING_DELETE
    @Test
    public void deleteFolderTest3() throws CatalogException {
        List<File> folderFiles = new LinkedList<>();

        File folder = createBasicDirectoryFileTestEnvironment(folderFiles);

        CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(fileManager.getUri(folder));
        for (File file : folderFiles) {
            assertTrue(ioManager.exists(fileManager.getUri(file)));
        }

        // 1st we set the status to PENDING DELETE.
        setToPendingDelete(studyFqn, new Query(FileDBAdaptor.QueryParams.PATH.key(), "~^" + folder.getPath() + "*"));

        Query query = new Query(FileDBAdaptor.QueryParams.PATH.key(), "~^" + folder.getPath() + "*");
        OpenCGAResult<File> results = fileManager.search(studyFqn, query,
                new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.STATUS.key()), sessionIdUser);
        assertEquals(9, results.getNumResults());
        for (File result : results.getResults()) {
            assertEquals(File.FileStatus.PENDING_DELETE, result.getStatus().getName());
        }
    }

    // READY -> PENDING_DELETE -> DELETED
    @Test
    public void deleteFolderTest4() throws CatalogException {
        List<File> folderFiles = new LinkedList<>();

        File folder = createBasicDirectoryFileTestEnvironment(folderFiles);

        CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(fileManager.getUri(folder));
        for (File file : folderFiles) {
            assertTrue(ioManager.exists(fileManager.getUri(file)));
        }

        // 1st we set the status to PENDING DELETE.
        setToPendingDelete(studyFqn, new Query(FileDBAdaptor.QueryParams.PATH.key(), "~^" + folder.getPath() + "*"));

        // Now we delete the files
        ObjectMap params = new ObjectMap(Constants.SKIP_TRASH, true);
        fileManager.delete(studyFqn, new Query(FileDBAdaptor.QueryParams.UID.key(), folder.getUid()), params, sessionIdUser);

        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.UID.key(), folder.getUid())
                .append(FileDBAdaptor.QueryParams.DELETED.key(), true);
        File fileTmp = fileManager.search(studyFqn, query, QueryOptions.empty(), sessionIdUser).first();

        assertEquals("Folder name should not be modified", folder.getPath(), fileTmp.getPath());
        assertEquals("Status should be to DELETED", File.FileStatus.DELETED, fileTmp.getStatus().getName());
        assertEquals("Name should not have changed", folder.getName(), fileTmp.getName());
        assertFalse(ioManager.exists(fileTmp.getUri()));

        for (File file : folderFiles) {
            query.put(FileDBAdaptor.QueryParams.UID.key(), file.getUid());
            fileTmp = fileManager.search(studyFqn, query, QueryOptions.empty(), sessionIdUser).first();
            assertEquals("Folder name should not be modified", file.getPath(), fileTmp.getPath());
            assertEquals("Status should be to DELETED", File.FileStatus.DELETED, fileTmp.getStatus().getName());
            assertEquals("Name should not have changed", file.getName(), fileTmp.getName());
            assertFalse("File uri: " + fileTmp.getUri() + " should not exist", ioManager.exists(fileTmp.getUri()));
        }
    }

    private File createBasicDirectoryFileTestEnvironment(List<File> folderFiles) throws CatalogException {
        File folder = fileManager.createFolder(studyFqn, Paths.get("folder").toString(), null, false,
                null, QueryOptions.empty(), sessionIdUser).first();
        folderFiles.add(
             fileManager.create(studyFqn, new File().setPath("folder/my.txt"), false, StringUtils.randomString(200),
                     null, sessionIdUser).first()
        );
        folderFiles.add(
                fileManager.create(studyFqn, new File().setPath("folder/my2.txt"), false, StringUtils.randomString(200),
                        null, sessionIdUser).first()
        );
        folderFiles.add(
                fileManager.create(studyFqn, new File().setPath("folder/my3.txt"), false, StringUtils.randomString(200),
                        null, sessionIdUser).first()
        );
        folderFiles.add(
                fileManager.create(studyFqn, new File().setPath("folder/subfolder/my4.txt"), true,
                        StringUtils.randomString(200), null, sessionIdUser).first()
        );
        folderFiles.add(
                fileManager.create(studyFqn, new File().setPath("folder/subfolder/my5.txt"), false,
                        StringUtils.randomString(200), null, sessionIdUser).first()
        );
        folderFiles.add(
                fileManager.create(studyFqn, new File().setPath("folder/subfolder/subsubfolder/my6.txt"), true,
                        StringUtils.randomString(200), null, sessionIdUser).first()
        );
        return folder;
    }

    @Test
    public void sendFolderToTrash() {

    }

    @Test
    public void getAllFilesInFolder() throws CatalogException {
        List<File> allFilesInFolder = fileManager.getFilesFromFolder("/data/test/folder/", studyFqn, null,
                sessionIdUser).getResults();
        assertEquals(3, allFilesInFolder.size());
    }

    private void deleteFolderAndCheck(File deletable) throws CatalogException {
        List<File> allFilesInFolder;
        Study study = fileManager.getStudy(deletable, sessionIdUser);

        // 1st, we set the status to PENDING_DELETE
        Query query = new Query(FileDBAdaptor.QueryParams.PATH.key(), "~^" + deletable.getPath() + "*");
        setToPendingDelete(study.getFqn(), query);

        fileManager.delete(study.getFqn(), query, null, sessionIdUser);

        query = new Query()
                .append(FileDBAdaptor.QueryParams.PATH.key(), deletable.getPath())
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.TRASHED);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.PATH.key());
        DataResult<File> fileDataResult = fileManager.search(study.getFqn(), query, options, sessionIdUser);
        assertEquals(1, fileDataResult.getNumResults());

        query = new Query()
                .append(FileDBAdaptor.QueryParams.DIRECTORY.key(), fileDataResult.first().getPath() + ".*")
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.TRASHED);
        allFilesInFolder = fileManager.search(study.getFqn(), query, null, sessionIdUser).getResults();

        for (File subFile : allFilesInFolder) {
            assertTrue(subFile.getStatus().getName().equals(File.FileStatus.TRASHED));
        }
    }

    private void setToPendingDelete(String study, Query query) throws CatalogException {
        FileUpdateParams updateParams = new FileUpdateParams()
                .setStatus(new File.FileStatus(File.FileStatus.PENDING_DELETE));
        fileManager.update(study, query, updateParams, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void assignPermissionsRecursively() throws Exception {
        Path folderPath = Paths.get("data", "new", "folder");
        fileManager.createFolder(studyFqn, folderPath.toString(), null, true, null,
                QueryOptions.empty(), sessionIdUser).first();

        Path filePath = Paths.get("data", "file1.txt");
        fileManager.create(studyFqn, File.Type.FILE, File.Format.UNKNOWN, File.Bioformat.UNKNOWN, filePath.toString(),
                "", new File.FileStatus(), 10, null, -1, null, null, true, "My content", null, sessionIdUser);

        DataResult<Map<String, List<String>>> dataResult = fileManager.updateAcl(studyFqn, Arrays.asList("data/new/",
                filePath.toString()), "user2", new File.FileAclParams("VIEW", AclParams.Action.SET, null), sessionIdUser);

        assertEquals(3, dataResult.getNumResults());
        for (Map<String, List<String>> result : dataResult.getResults()) {
            assertEquals(1, result.get("user2").size());
            assertEquals(FileAclEntry.FilePermissions.VIEW.name(), result.get("user2").iterator().next());
        }
    }

    @Test
    public void testUpdateIndexStatus() throws CatalogException, URISyntaxException, IOException {
        Path path = Paths.get(fileManager.get(studyFqn, ".", FileManager.INCLUDE_FILE_URI, sessionIdUser).first().getUri());
        Path sourcePath = Paths.get(getClass().getResource("/biofiles/variant-test-file.vcf.gz").toURI());
        Files.copy(sourcePath, path.resolve("data/" + sourcePath.getFileName()));
        DataResult<File> fileResult = fileManager.create(studyFqn, File.Type.FILE, File.Format.VCF,
                File.Bioformat.VARIANT, "data/variant-test-file.vcf.gz", "description", new File.FileStatus(File.FileStatus.STAGE), 0,
                Collections.emptyList(), -1, Collections.emptyMap(), null, true, null, new QueryOptions(), sessionIdUser);

        fileManager.updateFileIndexStatus(fileResult.first(), FileIndex.IndexStatus.TRANSFORMED, null, sessionIdUser);
        DataResult<File> read = fileManager.get(studyFqn, fileResult.first().getPath(), new QueryOptions(), sessionIdUser);
        assertEquals(FileIndex.IndexStatus.TRANSFORMED, read.first().getIndex().getStatus().getName());
    }

//    @Test
//    public void testIndex() throws Exception {
//        URI uri = getClass().getResource("/biofiles/variant-test-file.vcf.gz").toURI();
//        File file = fileManager.link(studyFqn, uri, "", null, sessionIdUser).first();
//        assertEquals(4, file.getSamples().size());
//        assertEquals(File.Format.VCF, file.getFormat());
//        assertEquals(File.Bioformat.VARIANT, file.getBioformat());
//
//        Job job = fileManager.index(studyFqn, Collections.singletonList(file.getName()), "VCF", null, sessionIdUser).first();
//        assertEquals(file.getUid(), job.getInput().get(0).getUid());
//    }
//
//    @Test
//    public void testIndexFromAvro() throws Exception {
//        URI uri = getClass().getResource("/biofiles/variant-test-file.vcf.gz").toURI();
//        File file = fileManager.link(studyFqn, uri, "data", null, sessionIdUser).first();
//        fileManager.create(studyFqn, File.Type.FILE, File.Format.AVRO, null, "data/variant-test-file.vcf.gz.variants.avro.gz",
//                "description", new File.FileStatus(File.FileStatus.READY), 0, Collections.emptyList(), -1, Collections.emptyMap(), Collections.emptyMap(), true, "asdf", new QueryOptions(), sessionIdUser);
//        fileManager.link(studyFqn, getClass().getResource("/biofiles/variant-test-file.vcf.gz.file.json.gz").toURI(), "data", null,
//                sessionIdUser).first();
//
//        Job job = fileManager.index(studyFqn, Collections.singletonList("variant-test-file.vcf.gz.variants.avro.gz"), "VCF", null,
//                sessionIdUser).first();
//        assertEquals(file.getUid(), job.getInput().get(0).getUid());
//    }
//
//    @Test
//    public void testIndexFromAvroIncomplete() throws Exception {
//        URI uri = getClass().getResource("/biofiles/variant-test-file.vcf.gz").toURI();
//        File file = fileManager.link(studyFqn, uri, "data", null, sessionIdUser).first();
//        fileManager.create(studyFqn, File.Type.FILE, File.Format.AVRO, null, "data/variant-test-file.vcf.gz.variants.avro.gz",
//                "description", new File.FileStatus(File.FileStatus.READY), 0, Collections.emptyList(), -1, Collections.emptyMap(),
//                Collections.emptyMap(), true, "asdf", new QueryOptions(), sessionIdUser);
////        fileManager.link(getClass().getResource("/biofiles/variant-test-file.vcf.gz.file.json.gz").toURI(), "data", studyUid, null, s
////        essionIdUser).first();
//
//
//        thrown.expect(CatalogException.class);
//        thrown.expectMessage("The file variant-test-file.vcf.gz.variants.avro.gz is not a VCF file.");
//        fileManager.index(studyFqn, Collections.singletonList("variant-test-file.vcf.gz.variants.avro.gz"), "VCF", null, sessionIdUser).first();
//    }

//    @Test
//    public void testMassiveUpdateFileAcl() throws CatalogException {
//        List<String> fileIdList = new ArrayList<>();
//
//        // Create 2000 files
//        for (int i = 0; i < 10000; i++) {
//            fileIdList.add(String.valueOf(fileManager.createFile("user@1000G:phase1", "file_" + i + ".txt", "", false,
//                    "File " + i, sessionIdUser).first().getId()));
//        }
//
//        StopWatch watch = StopWatch.createStarted();
//        // Assign VIEW permissions to all those files
//        fileManager.updateAcl("user@1000G:phase1", fileIdList, "*,user2,user3", new File.FileAclParams("VIEW",
//                AclParams.Action.SET, null), sessionIdUser);
//        System.out.println("Time: " + watch.getTime(TimeUnit.MILLISECONDS) + " milliseconds");
//        System.out.println("Time: " + watch.getTime(TimeUnit.SECONDS) + " seconds");
//
//        watch.reset();
//        watch.start();
//        // Assign VIEW permissions to all those files
//        fileManager.updateAcl("user@1000G:phase1", fileIdList, "*,user2,user3", new File.FileAclParams("VIEW",
//                AclParams.Action.SET, null), sessionIdUser);
//        System.out.println("Time: " + watch.getTime(TimeUnit.MILLISECONDS) + " milliseconds");
//        System.out.println("Time: " + watch.getTime(TimeUnit.SECONDS) + " seconds");
//
//        watch.reset();
//        watch.start();
//        // Assign VIEW permissions to all those files
//        fileManager.updateAcl("user@1000G:phase1", fileIdList, "*,user2,user3", new File.FileAclParams("VIEW",
//                AclParams.Action.SET, null), sessionIdUser);
//        System.out.println("Time: " + watch.getTime(TimeUnit.MILLISECONDS) + " milliseconds");
//        System.out.println("Time: " + watch.getTime(TimeUnit.SECONDS) + " seconds");
//
//        watch.reset();
//        watch.start();
//        // Assign VIEW permissions to all those files
//        fileManager.updateAcl("user@1000G:phase1", fileIdList, "*,user2,user3", new File.FileAclParams("VIEW",
//                AclParams.Action.SET, null), sessionIdUser);
//        System.out.println("Time: " + watch.getTime(TimeUnit.MILLISECONDS) + " milliseconds");
//        System.out.println("Time: " + watch.getTime(TimeUnit.SECONDS) + " seconds");
//
//        watch.reset();
//        watch.start();
//        // Assign VIEW permissions to all those files
//        fileManager.updateAcl("user@1000G:phase1", fileIdList, "*,user2,user3", new File.FileAclParams("VIEW",
//                AclParams.Action.SET, null), sessionIdUser);
//        System.out.println("Time: " + watch.getTime(TimeUnit.MILLISECONDS) + " milliseconds");
//        System.out.println("Time: " + watch.getTime(TimeUnit.SECONDS) + " seconds");
//    }
}
