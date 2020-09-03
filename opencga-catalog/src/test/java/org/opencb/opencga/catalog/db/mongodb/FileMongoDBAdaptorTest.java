/*
 * Copyright 2015-2020 OpenCB
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

import org.apache.commons.lang3.RandomStringUtils;
import org.bson.Document;
import org.junit.Test;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.models.common.ResourceReference;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileAclEntry;
import org.opencb.opencga.core.models.file.FileInternal;
import org.opencb.opencga.core.models.file.FileStatus;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleInternal;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Created by pfurio on 3/2/16.
 */
public class FileMongoDBAdaptorTest extends MongoDBAdaptorTest {

    @Test
    public void createFileToStudyTest() throws CatalogException {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getUid();
        assertTrue(studyId >= 0);
        File file;
        file = new File("jobs/", File.Type.DIRECTORY, File.Format.PLAIN, File.Bioformat.NONE, "jobs/", null, "",
                FileInternal.initialize(), 1000, 1);
        LinkedList<FileAclEntry> acl = new LinkedList<>();
        acl.push(new FileAclEntry("jcoll", Arrays.asList(FileAclEntry.FilePermissions.VIEW.name(),
                FileAclEntry.FilePermissions.VIEW_CONTENT.name(), FileAclEntry.FilePermissions.VIEW_HEADER.name(),
                FileAclEntry.FilePermissions.DELETE.name())));
        acl.push(new FileAclEntry("jmmut", Collections.emptyList()));
        System.out.println(catalogFileDBAdaptor.insert(studyId, file, null, null));
        file = new File("file.sam", File.Type.FILE, File.Format.PLAIN, File.Bioformat.ALIGNMENT, "data/file.sam", null, "",
                FileInternal.initialize(), 1000, 1);
        System.out.println(catalogFileDBAdaptor.insert(studyId, file, null, null));
        file = new File("file.bam", File.Type.FILE, File.Format.BINARY, File.Bioformat.ALIGNMENT, "data/file.bam", null, "",
                FileInternal.initialize(), 1000, 1);
        System.out.println(catalogFileDBAdaptor.insert(studyId, file, null, null));
        file = new File("file.vcf", File.Type.FILE, File.Format.PLAIN, File.Bioformat.VARIANT, "data/file2.vcf", null, "",
                FileInternal.initialize(), 1000, 1);

        try {
            System.out.println(catalogFileDBAdaptor.insert(-20, file, null, null));
            fail("Expected \"StudyId not found\" exception");
        } catch (CatalogDBException e) {
            System.out.println(e);
        }

        System.out.println(catalogFileDBAdaptor.insert(studyId, file, null, null));

        try {
            System.out.println(catalogFileDBAdaptor.insert(studyId, file, null, null));
            fail("Expected \"File already exist\" exception");
        } catch (CatalogDBException e) {
            System.out.println(e);
        }
    }

    @Test
    public void getFileTest() throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        File file = user3.getProjects().get(0).getStudies().get(0).getFiles().get(0);
        DataResult<File> fileDataResult = catalogFileDBAdaptor.get(file.getUid(), null);
        System.out.println(fileDataResult);
        try {
            System.out.println(catalogFileDBAdaptor.get(-1, null));
            fail("Expected \"FileId not found\" exception");
        } catch (CatalogDBException e) {
            System.out.println(e);
        }
    }

    @Test
    public void getAllFilesStudyNotValidTest() throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("not valid");
        catalogFileDBAdaptor.getAllInStudy(-1, null);
    }

    @Test
    public void getAllFilesStudyNotExistsTest() throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("not exist");
        catalogFileDBAdaptor.getAllInStudy(216544, null);
    }

    @Test
    public void getAllFilesTest() throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getUid();
        DataResult<File> allFiles = catalogFileDBAdaptor.getAllInStudy(studyId, null);
        List<File> files = allFiles.getResults();
        List<File> expectedFiles = user3.getProjects().get(0).getStudies().get(0).getFiles();
        assertEquals(expectedFiles.size(), files.size());
        for (File expectedFile : expectedFiles) {
            boolean found = false;
            for (File fileResult : allFiles.getResults()) {
                if (fileResult.getUid() == expectedFile.getUid())
                    found = true;
            }
            if (!found) {
                throw new CatalogDBException("The file " + expectedFile.getName() + " could not be found.");
            }
        }
    }

//    // Test if the lookup operation works fine
//    @Test
//    public void getFileWithJob() throws CatalogDBException {
//        long studyId = user3.getProjects().get(0).getStudies().get(0).getUid();
//        QueryOptions queryOptions = new QueryOptions();
//
//        // We create a job
//        String jobName = "jobName";
//        String jobDescription = "This is the description of the job";
//        Job myJob = new Job().setName(jobName).setDescription(jobDescription);
//        DataResult<Job> jobInsert = catalogJobDBAdaptor.insert(myJob, studyId, queryOptions);
//
//        // We create a new file giving that job
//        File file = new File().setName("Filename").setPath("data/Filename").setJob(jobInsert.first());
//        DataResult<File> fileInsert = catalogFileDBAdaptor.insert(file, studyId, queryOptions);
//
//        // Get the file
//        DataResult<File> noJobInfoDataResult = catalogFileDBAdaptor.get(fileInsert.first().getUid(), queryOptions);
//        assertNull(noJobInfoDataResult.first().getJob().getName());
//        assertNull(noJobInfoDataResult.first().getJob().getDescription());
//
//        queryOptions.put("lazy", false);
//        DataResult<File> jobInfoDataResult = catalogFileDBAdaptor.get(fileInsert.first().getUid(), queryOptions);
//        assertEquals(jobName, jobInfoDataResult.first().getJob().getName());
//        assertEquals(jobDescription, jobInfoDataResult.first().getJob().getDescription());
//    }

    @Test
    public void modifyFileTest() throws CatalogDBException, IOException, CatalogParameterException, CatalogAuthorizationException {
        File file = user3.getProjects().get(0).getStudies().get(0).getFiles().get(0);
        long fileId = file.getUid();

        Document stats = new Document("stat1", 1).append("stat2", true).append("stat3", "ok" + RandomStringUtils.randomAlphanumeric(20));

        ObjectMap parameters = new ObjectMap();
        parameters.put("status.name", FileStatus.READY);
        parameters.put("stats", stats);
        System.out.println(catalogFileDBAdaptor.update(fileId, parameters, QueryOptions.empty()));

        file = catalogFileDBAdaptor.get(fileId, null).first();
        assertEquals(file.getInternal().getStatus().getName(), FileStatus.READY);
        assertEquals(file.getStats(), stats);

        parameters = new ObjectMap();
        parameters.put("stats", "{}");
        System.out.println(catalogFileDBAdaptor.update(fileId, parameters, QueryOptions.empty()));

        file = catalogFileDBAdaptor.get(fileId, null).first();
        assertEquals(file.getStats(), new LinkedHashMap<String, Object>());
    }

    @Test
    public void renameFileTest() throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        String newName = "newFile.bam";
        String parentPath = "data/";
        long fileId = catalogFileDBAdaptor.getId(user3.getProjects().get(0).getStudies().get(0).getUid(), "data/file.vcf");
        System.out.println(catalogFileDBAdaptor.rename(fileId, parentPath + newName, "", null));

        File file = catalogFileDBAdaptor.get(fileId, null).first();
        assertEquals(file.getName(), newName);
        assertEquals(file.getPath(), parentPath + newName);

        try {
            catalogFileDBAdaptor.rename(-1, "noFile", "", null);
            fail("error: expected \"file not found\"exception");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }

        long folderId = catalogFileDBAdaptor.getId(user3.getProjects().get(0).getStudies().get(0).getUid(), "data/");
        String folderName = "folderName";
        catalogFileDBAdaptor.rename(folderId, folderName, "", null);
        assertTrue(catalogFileDBAdaptor.get(fileId, null).first().getPath().equals(folderName + "/" + newName));
    }

    @Test
    public void includeFields() throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        DataResult<File> fileDataResult = catalogFileDBAdaptor.get(7,
                new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.PATH.key()));
        List<File> files = fileDataResult.getResults();
        assertEquals("Include path does not work.", "data/file.vcf", files.get(0).getPath());
        assertEquals("Include not working.", null, files.get(0).getName());
    }

    @Test
    public void testDistinct() throws Exception {

//        List<String> distinctOwners = catalogFileDBAdaptor.distinct(new Query(), CatalogFileDBAdaptor.QueryParams.OWNER_ID.key()).getResults();
        List<String> distinctTypes = catalogFileDBAdaptor.distinct(new Query(), FileDBAdaptor.QueryParams.TYPE.key()).getResults();
//        assertEquals(Arrays.asList("imedina", "pfurio"), distinctOwners);
        assertEquals(Arrays.asList("DIRECTORY","FILE"), distinctTypes);

        List<Long> pfurioStudies = Arrays.asList(9L, 14L);
        List<String> distinctFormats = catalogFileDBAdaptor.distinct(
                new Query(FileDBAdaptor.QueryParams.STUDY_UID.key(), pfurioStudies),
                FileDBAdaptor.QueryParams.FORMAT.key()).getResults();
        assertTrue(Arrays.asList("UNKNOWN", "COMMA_SEPARATED_VALUES", "BAM").containsAll(distinctFormats));

        distinctFormats = catalogFileDBAdaptor.distinct(new Query(),
                FileDBAdaptor.QueryParams.FORMAT.key()).getResults();
        Collections.sort(distinctFormats);
        List<String> expected = Arrays.asList("PLAIN", "UNKNOWN", "COMMA_SEPARATED_VALUES", "BAM");
        Collections.sort(expected);
        assertEquals(expected, distinctFormats);
    }

    @Test
    public void testRank() throws Exception {
        List<Long> pfurioStudies = Arrays.asList(9L, 14L);
        List<Document> rankedFilesPerDiskUsage = catalogFileDBAdaptor.rank(
                new Query(FileDBAdaptor.QueryParams.STUDY_UID.key(), pfurioStudies),
                FileDBAdaptor.QueryParams.SIZE.key(), 100, false).getResults();

        assertEquals(3, rankedFilesPerDiskUsage.size());
        assertTrue(Arrays.asList(10, 100, 5000)
                .containsAll(rankedFilesPerDiskUsage.stream().map(d -> d.get("_id")).collect(Collectors.toSet())));

        for (Document document : rankedFilesPerDiskUsage) {
            switch (document.getInteger("_id")) {
                case 10:
                case 5000:
                    assertEquals(2, document.get("count"));
                    break;
                case 100:
                    assertEquals(3, document.get("count"));
                    break;
            }
        }
    }

    @Test
    public void testGroupBy() throws Exception {
        List<Long> pfurioStudies = Arrays.asList(9L, 14L);

        List<Document> groupByBioformat = catalogFileDBAdaptor.groupBy(new Query(FileDBAdaptor.QueryParams.STUDY_UID.key(), pfurioStudies),
                FileDBAdaptor.QueryParams.BIOFORMAT.key(), new QueryOptions()).getResults();

        assertTrue(Arrays.asList("ALIGNMENT", "NONE")
                .containsAll(groupByBioformat.stream().map(d -> d.get("_id"))
                        .map(d -> ((Document) d).get(FileDBAdaptor.QueryParams.BIOFORMAT.key())).collect(Collectors.toSet())));
        for (Document document : groupByBioformat) {
            switch (((Document) document.get("_id")).getString(FileDBAdaptor.QueryParams.BIOFORMAT.key())) {
                case "ALIGNMENT":
                    assertTrue(Arrays.asList("m_alignment.bam", "alignment.bam").containsAll(document.getList("items", String.class)));
                    break;
                case "NONE":
                    assertTrue(Arrays.asList("m_file1.txt", "file2.txt", "file1.txt", "data/")
                            .containsAll(document.getList("items", String.class)));
                    break;
            }
        }

        groupByBioformat = catalogFileDBAdaptor.groupBy(new Query(FileDBAdaptor.QueryParams.STUDY_UID.key(), 14), // MINECO study
                FileDBAdaptor.QueryParams.BIOFORMAT.key(), new QueryOptions()).getResults();

        assertTrue(Arrays.asList("ALIGNMENT", "NONE")
                .containsAll(groupByBioformat.stream().map(d -> d.get("_id"))
                        .map(d -> ((Document) d).get(FileDBAdaptor.QueryParams.BIOFORMAT.key())).collect(Collectors.toSet())));
        for (Document document : groupByBioformat) {
            switch (((Document) document.get("_id")).getString(FileDBAdaptor.QueryParams.BIOFORMAT.key())) {
                case "ALIGNMENT":
                    assertTrue(Arrays.asList("m_alignment.bam").containsAll(document.getList("items", String.class)));
                    break;
                case "NONE":
                    assertTrue(Arrays.asList("m_file1.txt", "data/")
                            .containsAll(document.getList("items", String.class)));
                    break;
            }
        }
    }

    @Test
    public void testGroupBy1() throws Exception {

        List<Long> pfurioStudies = Arrays.asList(9L, 14L);
        List<Document> groupByBioformat = catalogFileDBAdaptor.groupBy(
                new Query(FileDBAdaptor.QueryParams.STUDY_UID.key(), pfurioStudies),
                Arrays.asList(FileDBAdaptor.QueryParams.BIOFORMAT.key(), FileDBAdaptor.QueryParams.TYPE.key()),
                new QueryOptions()).getResults();

        assertEquals(3, groupByBioformat.size());
        for (Document document : groupByBioformat) {
            Document d = (Document) document.get("_id");

            switch (d.getString(FileDBAdaptor.QueryParams.BIOFORMAT.key()) + "_" + d.getString(FileDBAdaptor.QueryParams.TYPE.key())) {
                case "ALIGNMENT_FILE":
                    assertTrue(Arrays.asList("m_alignment.bam", "alignment.bam").containsAll(document.getList("items", String.class)));
                    break;
                case "NONE_FILE":
                    assertTrue(Arrays.asList("m_file1.txt", "file2.txt", "file1.txt").containsAll(document.getList("items", String.class)));
                    break;
                case "NONE_FOLDER":
                    assertTrue(Arrays.asList("data/").containsAll(document.getList("items", String.class)));
                    break;
            }
        }
    }

    @Test
    public void testAddSamples() throws Exception {
        long studyUid = user3.getProjects().get(0).getStudies().get(0).getUid();
        new Status();
        catalogDBAdaptor.getCatalogSampleDBAdaptor().insert(studyUid, new Sample().setId("sample1").setInternal(new SampleInternal(new Status())),
                Collections.emptyList(), QueryOptions.empty());
        Sample sample1 = getSample(studyUid, "sample1");
        new Status();
        catalogDBAdaptor.getCatalogSampleDBAdaptor().insert(studyUid, new Sample().setId("sample2").setInternal(new SampleInternal(new Status())),
                Collections.emptyList(), QueryOptions.empty());
        Sample sample2 = getSample(studyUid, "sample2");

        ObjectMap action = new ObjectMap(FileDBAdaptor.QueryParams.SAMPLES.key(), ParamUtils.UpdateAction.ADD);
        QueryOptions options = new QueryOptions(Constants.ACTIONS, action);

        File file = user3.getProjects().get(0).getStudies().get(0).getFiles().get(0);
        catalogFileDBAdaptor.update(file.getUid(), new ObjectMap(FileDBAdaptor.QueryParams.SAMPLES.key(),
                        Arrays.asList(ResourceReference.of(sample1), ResourceReference.of(sample2))), options);

        DataResult<File> fileDataResult = catalogFileDBAdaptor.get(file.getUid(), QueryOptions.empty());
        assertEquals(2, fileDataResult.first().getSamples().size());
        assertTrue(Arrays.asList(sample1.getUid(), sample2.getUid()).containsAll(
                fileDataResult.first().getSamples().stream().map(ResourceReference::getUid).collect(Collectors.toList())));

        catalogDBAdaptor.getCatalogSampleDBAdaptor().insert(studyUid, new Sample().setId("sample3").setInternal(new SampleInternal(new Status())),
                Collections.emptyList(), QueryOptions.empty());
        Sample sample3 = getSample(studyUid, "sample3");
        // Test we avoid duplicities
        catalogFileDBAdaptor.update(file.getUid(), new ObjectMap(FileDBAdaptor.QueryParams.SAMPLES.key(),
                        Arrays.asList(ResourceReference.of(sample1), ResourceReference.of(sample2), ResourceReference.of(sample2),
                                ResourceReference.of(sample3))), options);
        fileDataResult = catalogFileDBAdaptor.get(file.getUid(), QueryOptions.empty());
        assertEquals(3, fileDataResult.first().getSamples().size());
        assertTrue(Arrays.asList(sample1.getUid(), sample2.getUid(), sample3.getUid()).containsAll(
                fileDataResult.first().getSamples().stream().map(ResourceReference::getUid).collect(Collectors.toList())));
    }

    @Test
    public void testRemoveSamples() throws Exception {
        long studyUid = user3.getProjects().get(0).getStudies().get(0).getUid();
        new Status();
        catalogDBAdaptor.getCatalogSampleDBAdaptor().insert(studyUid, new Sample().setId("sample1").setInternal(new SampleInternal(new Status())),
                Collections.emptyList(), QueryOptions.empty());
        Sample sample1 = getSample(studyUid, "sample1");
        new Status();
        catalogDBAdaptor.getCatalogSampleDBAdaptor().insert(studyUid, new Sample().setId("sample2").setInternal(new SampleInternal(new Status())),
                Collections.emptyList(), QueryOptions.empty());
        Sample sample2 = getSample(studyUid, "sample2");
        new Status();
        catalogDBAdaptor.getCatalogSampleDBAdaptor().insert(studyUid, new Sample().setId("sample3").setInternal(new SampleInternal(new Status())),
                Collections.emptyList(), QueryOptions.empty());
        Sample sample3 = getSample(studyUid, "sample3");
        List<File> files = user3.getProjects().get(0).getStudies().get(0).getFiles();
        File file = files.get(0);
        File file2 = files.get(1);
        ObjectMap action = new ObjectMap(FileDBAdaptor.QueryParams.SAMPLES.key(), ParamUtils.UpdateAction.ADD);
        QueryOptions options = new QueryOptions(Constants.ACTIONS, action);

        catalogFileDBAdaptor.update(file.getUid(), new ObjectMap(FileDBAdaptor.QueryParams.SAMPLES.key(),
                        Arrays.asList(ResourceReference.of(sample1), ResourceReference.of(sample2), ResourceReference.of(sample3))),
                options);
        catalogFileDBAdaptor.update(file2.getUid(), new ObjectMap(FileDBAdaptor.QueryParams.SAMPLES.key(),
                        Arrays.asList(ResourceReference.of(sample1), ResourceReference.of(sample2), ResourceReference.of(sample3))),
                options);

        DataResult<File> fileDataResult = catalogFileDBAdaptor.get(file.getUid(), QueryOptions.empty());
        assertEquals(3, fileDataResult.first().getSamples().size());
        assertTrue(Arrays.asList(sample1.getUid(), sample2.getUid(), sample3.getUid())
                .containsAll(fileDataResult.first().getSamples().stream().map(ResourceReference::getUid).collect(Collectors.toList())));

        fileDataResult = catalogFileDBAdaptor.get(file2.getUid(), QueryOptions.empty());
        assertEquals(3, fileDataResult.first().getSamples().size());
        assertTrue(Arrays.asList(sample1.getUid(), sample2.getUid(), sample3.getUid())
                .containsAll(fileDataResult.first().getSamples().stream().map(ResourceReference::getUid).collect(Collectors.toList())));

        catalogFileDBAdaptor.removeSampleReferences(null, studyUid, sample1);
        catalogFileDBAdaptor.removeSampleReferences(null, studyUid, sample3);
        fileDataResult = catalogFileDBAdaptor.get(file.getUid(), QueryOptions.empty());
        assertEquals(1, fileDataResult.first().getSamples().size());
        assertTrue(fileDataResult.first().getSamples().get(0).getUid() == sample2.getUid());

        fileDataResult = catalogFileDBAdaptor.get(file2.getUid(), QueryOptions.empty());
        assertEquals(1, fileDataResult.first().getSamples().size());
        assertTrue(fileDataResult.first().getSamples().get(0).getUid() == sample2.getUid());
    }

    @Test
    public void testGroupByDates() throws Exception {
        List<Long> pfurioStudies = Arrays.asList(9L, 14L);

        List<Document> groupByBioformat = catalogFileDBAdaptor.groupBy(
                new Query(FileDBAdaptor.QueryParams.STUDY_UID.key(), pfurioStudies),
                Arrays.asList(FileDBAdaptor.QueryParams.BIOFORMAT.key(), FileDBAdaptor.QueryParams.TYPE.key(), "day"),
                new QueryOptions()).getResults();

        assertEquals(3, groupByBioformat.size());

        for (int i = 0; i < groupByBioformat.size(); i++) {
            String bioformat = ((Document) groupByBioformat.get(i).get("_id")).getString("bioformat");
            String type = ((Document) groupByBioformat.get(i).get("_id")).getString("type");
            switch (bioformat) {
                case "NONE":
                    switch (type) {
                        case "FILE":
                            assertEquals(5, ((Document) groupByBioformat.get(i).get("_id")).size()); // None - File
                            assertTrue(Arrays.asList("m_file1.txt", "file2.txt", "file1.txt").containsAll(groupByBioformat.get(i).getList("items", String.class)));
                            break;
                        default:
                            assertEquals(5, ((Document) groupByBioformat.get(i).get("_id")).size()); // None - Folder
                            assertEquals(Arrays.asList("data/"), groupByBioformat.get(i).get("items"));
                            break;
                    }
                    break;
                case "ALIGNMENT":
                    assertEquals(5, ((Document) groupByBioformat.get(i).get("_id")).size());
                    assertTrue(Arrays.asList("m_alignment.bam", "alignment.bam").containsAll(groupByBioformat.get(i).getList("items", String.class)));
                    break;
                default:
                    fail("This case should not happen.");
                    break;
            }
        }
    }
}
