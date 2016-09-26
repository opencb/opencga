package org.opencb.opencga.catalog.db.mongodb;

import org.bson.Document;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.StringUtils;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.acls.permissions.FileAclEntry;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Created by pfurio on 3/2/16.
 */
public class CatalogMongoFileDBAdaptorTest extends CatalogMongoDBAdaptorTest {

    @Test
    public void createFileToStudyTest() throws CatalogException, IOException {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        assertTrue(studyId >= 0);
        File file;
        file = new File("jobs/", File.Type.DIRECTORY, File.Format.PLAIN, File.Bioformat.NONE, "jobs/", "",
                new File.FileStatus(File.FileStatus.STAGE), 1000);
        LinkedList<FileAclEntry> acl = new LinkedList<>();
        acl.push(new FileAclEntry("jcoll", Arrays.asList(FileAclEntry.FilePermissions.VIEW.name(),
                FileAclEntry.FilePermissions.VIEW_CONTENT.name(), FileAclEntry.FilePermissions.VIEW_HEADER.name(),
                FileAclEntry.FilePermissions.DELETE.name(), FileAclEntry.FilePermissions.SHARE.name()
                )));
        acl.push(new FileAclEntry("jmmut", Collections.emptyList()));
//        acl.push(new AclEntry("jcoll", true, true, true, true));
//        acl.push(new AclEntry("jmmut", false, false, true, true));
        file.setAcl(acl);
        System.out.println(catalogFileDBAdaptor.insert(file, studyId, null));
        file = new File("file.sam", File.Type.FILE, File.Format.PLAIN, File.Bioformat.ALIGNMENT, "data/file.sam", "",
                new File.FileStatus(File.FileStatus.STAGE), 1000);
        System.out.println(catalogFileDBAdaptor.insert(file, studyId, null));
        file = new File("file.bam", File.Type.FILE, File.Format.BINARY, File.Bioformat.ALIGNMENT, "data/file.bam", "",
                new File.FileStatus(File.FileStatus.STAGE), 1000);
        System.out.println(catalogFileDBAdaptor.insert(file, studyId, null));
        file = new File("file.vcf", File.Type.FILE, File.Format.PLAIN, File.Bioformat.VARIANT, "data/file2.vcf", "",
                new File.FileStatus(File.FileStatus.STAGE), 1000);

        try {
            System.out.println(catalogFileDBAdaptor.insert(file, -20, null));
            fail("Expected \"StudyId not found\" exception");
        } catch (CatalogDBException e) {
            System.out.println(e);
        }

        System.out.println(catalogFileDBAdaptor.insert(file, studyId, null));

        try {
            System.out.println(catalogFileDBAdaptor.insert(file, studyId, null));
            fail("Expected \"File already exist\" exception");
        } catch (CatalogDBException e) {
            System.out.println(e);
        }
    }

    @Test
    public void getFileTest() throws CatalogDBException {
        File file = user3.getProjects().get(0).getStudies().get(0).getFiles().get(0);
        QueryResult<File> fileQueryResult = catalogFileDBAdaptor.get(file.getId(), null);
        System.out.println(fileQueryResult);
        try {
            System.out.println(catalogFileDBAdaptor.get(-1, null));
            fail("Expected \"FileId not found\" exception");
        } catch (CatalogDBException e) {
            System.out.println(e);
        }
    }

    @Test
    public void getAllFilesStudyNotValidTest() throws CatalogDBException {
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("not valid");
        catalogFileDBAdaptor.getAllInStudy(-1, null);
    }

    @Test
    public void getAllFilesStudyNotExistsTest() throws CatalogDBException {
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("not exist");
        catalogFileDBAdaptor.getAllInStudy(216544, null);
    }

    @Test
    public void getAllFilesTest() throws CatalogDBException {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        QueryResult<File> allFiles = catalogFileDBAdaptor.getAllInStudy(studyId, null);
        List<File> files = allFiles.getResult();
        List<File> expectedFiles = user3.getProjects().get(0).getStudies().get(0).getFiles();
        assertEquals(expectedFiles.size(), files.size());
        for (File expectedFile : expectedFiles) {
            boolean found = false;
            for (File fileResult : allFiles.getResult()) {
                if (fileResult.getId() == expectedFile.getId())
                    found = true;
            }
            if (!found) {
                throw new CatalogDBException("The file " + expectedFile.getName() + " could not be found.");
            }
        }
    }

    @Test
    public void modifyFileTest() throws CatalogDBException, IOException {
        File file = user3.getProjects().get(0).getStudies().get(0).getFiles().get(0);
        long fileId = file.getId();

        Document stats = new Document("stat1", 1).append("stat2", true).append("stat3", "ok" + StringUtils.randomString(20));

        ObjectMap parameters = new ObjectMap();
        parameters.put("status.name", File.FileStatus.READY);
        parameters.put("stats", stats);
        System.out.println(catalogFileDBAdaptor.update(fileId, parameters));

        file = catalogFileDBAdaptor.get(fileId, null).first();
        assertEquals(file.getStatus().getName(), File.FileStatus.READY);
        assertEquals(file.getStats(), stats);

        parameters = new ObjectMap();
        parameters.put("stats", "{}");
        System.out.println(catalogFileDBAdaptor.update(fileId, parameters));

        file = catalogFileDBAdaptor.get(fileId, null).first();
        assertEquals(file.getStats(), new LinkedHashMap<String, Object>());
    }

    @Test
    public void renameFileTest() throws CatalogDBException {
        String newName = "newFile.bam";
        String parentPath = "data/";
        long fileId = catalogFileDBAdaptor.getId(user3.getProjects().get(0).getStudies().get(0).getId(), "data/file.vcf");
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

        long folderId = catalogFileDBAdaptor.getId(user3.getProjects().get(0).getStudies().get(0).getId(), "data/");
        String folderName = "folderName";
        catalogFileDBAdaptor.rename(folderId, folderName, "", null);
        assertTrue(catalogFileDBAdaptor.get(fileId, null).first().getPath().equals(folderName + "/" + newName));
    }

    @Test
    public void deleteFileTest() throws CatalogDBException, IOException {
        long fileId = catalogFileDBAdaptor.getId(user3.getProjects().get(0).getStudies().get(0).getId(), "data/file.vcf");
        QueryResult<File> delete = catalogFileDBAdaptor.delete(fileId, new QueryOptions());
        assertTrue(delete.getNumResults() == 1);
        assertEquals(File.FileStatus.TRASHED, delete.first().getStatus().getName());
        try {
            System.out.println(catalogFileDBAdaptor.delete(catalogFileDBAdaptor.getId(catalogStudyDBAdaptor.getId
                    (catalogProjectDBAdaptor.getId("jcoll", "1000G"), "ph1"), "data/noExists"), new QueryOptions()));
            fail("error: Expected \"FileId not found\" exception");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }
    }

    @Test
    public void deleteFileTest2() throws CatalogDBException, IOException {
        long fileId = catalogFileDBAdaptor.getId(user3.getProjects().get(0).getStudies().get(0).getId(), "data/file.vcf");

        // New status after delete
        ObjectMap objectMap = new ObjectMap(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.REMOVED);

        QueryResult<File> delete = catalogFileDBAdaptor.delete(fileId, objectMap, new QueryOptions());
        assertTrue(delete.getNumResults() == 1);
        assertEquals(File.FileStatus.REMOVED, delete.first().getStatus().getName());
        try {
            System.out.println(catalogFileDBAdaptor.delete(catalogFileDBAdaptor.getId(catalogStudyDBAdaptor.getId
                    (catalogProjectDBAdaptor.getId("jcoll", "1000G"), "ph1"), "data/noExists"), new QueryOptions()));
            fail("error: Expected \"FileId not found\" exception");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }
    }
//
//    @Test
//    public void removeFileTest() throws CatalogDBException, IOException {
//        long fileId = catalogFileDBAdaptor.getFileId(user3.getProjects().get(0).getStudies().get(0).getId(), "data/file.vcf");
//        catalogFileDBAdaptor.delete(fileId, new QueryOptions());
//        // Remove after deleted
//        QueryResult<File> remove = catalogFileDBAdaptor.remove(fileId, new QueryOptions());
//        assertTrue(remove.getNumResults() == 1);
//        assertEquals(File.FileStatus.REMOVED, remove.first().getStatus().getName());
//    }
//
//    @Test
//    public void removeFileTest2() throws CatalogDBException, IOException {
//        long fileId = catalogFileDBAdaptor.getFileId(user3.getProjects().get(0).getStudies().get(0).getId(), "data/file.vcf");
//        // Remove after READY
//        QueryResult<File> remove = catalogFileDBAdaptor.remove(fileId, new QueryOptions());
//        assertTrue(remove.getNumResults() == 1);
//        assertEquals(File.FileStatus.REMOVED, remove.first().getStatus().getName());
//    }

    @Test
    public void fileAclsTest() throws CatalogDBException {
        long fileId = catalogFileDBAdaptor.getId(user3.getProjects().get(0).getStudies().get(0).getId(), "data/file.vcf");
        System.out.println(fileId);

        FileAclEntry granted = new FileAclEntry("jmmut", Arrays.asList(FileAclEntry.FilePermissions.VIEW.name(),
                FileAclEntry.FilePermissions.VIEW_CONTENT.name(), FileAclEntry.FilePermissions.VIEW_HEADER.name(),
                FileAclEntry.FilePermissions.DELETE.name(), FileAclEntry.FilePermissions.SHARE.name()
        ));

//        AclEntry granted = new AclEntry("jmmut", true, true, true, false);
        catalogFileDBAdaptor.createAcl(fileId, granted);
        granted.setMember("imedina");
        catalogFileDBAdaptor.createAcl(fileId, granted);
//        try {
//            granted.setMember("noUser");
//            catalogFileDBAdaptor.setFileAcl(fileId, granted, true);
//            fail("error: expected exception");
//        } catch (CatalogDBException e) {
//            System.out.println("correct exception: " + e);
//        }

        List<FileAclEntry> jmmut = catalogFileDBAdaptor.getAcl(fileId, "jmmut").getResult();
        assertTrue(!jmmut.isEmpty());
        System.out.println(jmmut.get(0).getPermissions());
        List<FileAclEntry> jcoll = catalogFileDBAdaptor.getAcl(fileId, "jcoll").getResult();
        assertTrue(jcoll.isEmpty());
    }

    @Test
    public void includeFields() throws CatalogDBException {

        QueryResult<File> fileQueryResult = catalogFileDBAdaptor.get(7,
                new QueryOptions("include", "projects.studies.files.id,projects.studies.files.path"));
        List<File> files = fileQueryResult.getResult();
        assertEquals("Include path does not work.", "data/file.vcf", files.get(0).getPath());
        assertEquals("Include not working.", null, files.get(0).getName());
    }

    @Test
    public void testDistinct() throws Exception {

//        List<String> distinctOwners = catalogFileDBAdaptor.distinct(new Query(), CatalogFileDBAdaptor.QueryParams.OWNER_ID.key()).getResult();
        List<String> distinctTypes = catalogFileDBAdaptor.distinct(new Query(), FileDBAdaptor.QueryParams.TYPE.key()).getResult();
//        assertEquals(Arrays.asList("imedina", "pfurio"), distinctOwners);
        assertEquals(Arrays.asList("DIRECTORY","FILE"), distinctTypes);

        List<Long> pfurioStudies = Arrays.asList(9L, 14L);
        List<String> distinctFormats = catalogFileDBAdaptor.distinct(
                new Query(FileDBAdaptor.QueryParams.STUDY_ID.key(), pfurioStudies),
                FileDBAdaptor.QueryParams.FORMAT.key()).getResult();
        assertEquals(Arrays.asList("UNKNOWN", "COMMA_SEPARATED_VALUES", "BAM"), distinctFormats);

        distinctFormats = catalogFileDBAdaptor.distinct(new Query(),
                FileDBAdaptor.QueryParams.FORMAT.key()).getResult();
        Collections.sort(distinctFormats);
        List<String> expected = Arrays.asList("PLAIN", "UNKNOWN", "COMMA_SEPARATED_VALUES", "BAM");
        Collections.sort(expected);
        assertEquals(expected, distinctFormats);
    }

    @Test
    public void testRank() throws Exception {
        List<Long> pfurioStudies = Arrays.asList(9L, 14L);
        List<Document> rankedFilesPerDiskUsage = catalogFileDBAdaptor.rank(
                new Query(FileDBAdaptor.QueryParams.STUDY_ID.key(), pfurioStudies),
                FileDBAdaptor.QueryParams.DISK_USAGE.key(), 100, false).getResult();

        assertEquals(3, rankedFilesPerDiskUsage.size());

        assertEquals(100, rankedFilesPerDiskUsage.get(0).get("_id"));
        assertEquals(3, rankedFilesPerDiskUsage.get(0).get("count"));

        assertEquals(5000, rankedFilesPerDiskUsage.get(1).get("_id"));
        assertEquals(2, rankedFilesPerDiskUsage.get(1).get("count"));

        assertEquals(10, rankedFilesPerDiskUsage.get(2).get("_id"));
        assertEquals(2, rankedFilesPerDiskUsage.get(2).get("count"));
    }

    @Test
    public void testGroupBy() throws Exception {
        List<Long> pfurioStudies = Arrays.asList(9L, 14L);

        List<Document> groupByBioformat = catalogFileDBAdaptor.groupBy(new Query(FileDBAdaptor.QueryParams.STUDY_ID.key(), pfurioStudies),
                FileDBAdaptor.QueryParams.BIOFORMAT.key(), new QueryOptions()).getResult();

        assertEquals("ALIGNMENT", ((Document) groupByBioformat.get(0).get("_id")).get(FileDBAdaptor.QueryParams.BIOFORMAT.key()));
        assertEquals(Arrays.asList("m_alignment.bam", "alignment.bam"), groupByBioformat.get(0).get("features"));

        assertEquals("NONE", ((Document) groupByBioformat.get(1).get("_id")).get(FileDBAdaptor.QueryParams.BIOFORMAT.key()));
        assertEquals(Arrays.asList("m_file1.txt", "file2.txt", "file1.txt", "data/"), groupByBioformat.get(1).get("features"));

        groupByBioformat = catalogFileDBAdaptor.groupBy(new Query(FileDBAdaptor.QueryParams.STUDY_ID.key(), 14), // MINECO study
                FileDBAdaptor.QueryParams.BIOFORMAT.key(), new QueryOptions()).getResult();

        assertEquals("ALIGNMENT", ((Document) groupByBioformat.get(0).get("_id")).get(FileDBAdaptor.QueryParams.BIOFORMAT.key()));
        assertEquals(Arrays.asList("m_alignment.bam"), groupByBioformat.get(0).get("features"));

        assertEquals("NONE", ((Document) groupByBioformat.get(1).get("_id")).get(FileDBAdaptor.QueryParams.BIOFORMAT.key()));
        assertEquals(Arrays.asList("m_file1.txt", "data/"), groupByBioformat.get(1).get("features"));

    }

    @Test
    public void testGroupBy1() throws Exception {

        List<Long> pfurioStudies = Arrays.asList(9L, 14L);
        List<Document> groupByBioformat = catalogFileDBAdaptor.groupBy(
                new Query(FileDBAdaptor.QueryParams.STUDY_ID.key(), pfurioStudies),
                Arrays.asList(FileDBAdaptor.QueryParams.BIOFORMAT.key(), FileDBAdaptor.QueryParams.TYPE.key()),
                new QueryOptions()).getResult();

        assertEquals(3, groupByBioformat.size());

        assertEquals(2, ((Document) groupByBioformat.get(0).get("_id")).size()); // Alignment - File
        assertEquals(Arrays.asList("m_alignment.bam", "alignment.bam"), groupByBioformat.get(0).get("features"));

        assertEquals(2, ((Document) groupByBioformat.get(1).get("_id")).size()); // None - File
        assertEquals(Arrays.asList("m_file1.txt", "file2.txt", "file1.txt"), groupByBioformat.get(1).get("features"));

        assertEquals(2, ((Document) groupByBioformat.get(2).get("_id")).size()); // None - Folder
        assertEquals(Arrays.asList("data/"), groupByBioformat.get(2).get("features"));

    }

    @Test
    public void testGroupByDates() throws Exception {
        List<Long> pfurioStudies = Arrays.asList(9L, 14L);

        List<Document> groupByBioformat = catalogFileDBAdaptor.groupBy(
                new Query(FileDBAdaptor.QueryParams.STUDY_ID.key(), pfurioStudies),
                Arrays.asList(FileDBAdaptor.QueryParams.BIOFORMAT.key(), FileDBAdaptor.QueryParams.TYPE.key(), "day"),
                new QueryOptions()).getResult();

        assertEquals(3, groupByBioformat.size());

        assertEquals(5, ((Document) groupByBioformat.get(0).get("_id")).size()); // Alignment - File
        assertEquals(Arrays.asList("m_alignment.bam", "alignment.bam"), groupByBioformat.get(0).get("features"));

        assertEquals(5, ((Document) groupByBioformat.get(1).get("_id")).size()); // None - File
        assertEquals(Arrays.asList("m_file1.txt", "file2.txt", "file1.txt"), groupByBioformat.get(1).get("features"));

        assertEquals(5, ((Document) groupByBioformat.get(2).get("_id")).size()); // None - Folder
        assertEquals(Arrays.asList("data/"), groupByBioformat.get(2).get("features"));

    }
}
