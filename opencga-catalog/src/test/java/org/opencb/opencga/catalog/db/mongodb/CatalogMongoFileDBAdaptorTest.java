package org.opencb.opencga.catalog.db.mongodb;

import org.bson.Document;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.StringUtils;
import org.opencb.opencga.catalog.db.api.CatalogFileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.acls.FileAcl;
import org.opencb.opencga.core.common.TimeUtils;

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
        file = new File("jobs/", File.Type.FOLDER, File.Format.PLAIN, File.Bioformat.NONE, "jobs/", null, TimeUtils.getTime(), "",
                new File.FileStatus(File.FileStatus.STAGE), 1000);
        LinkedList<FileAcl> acl = new LinkedList<>();
        acl.push(new FileAcl(Arrays.asList("jcoll"), Arrays.asList(FileAcl.FilePermissions.VIEW.name(),
                FileAcl.FilePermissions.VIEW_CONTENT.name(), FileAcl.FilePermissions.VIEW_HEADER.name(),
                FileAcl.FilePermissions.DELETE.name(), FileAcl.FilePermissions.SHARE.name()
                )));
        acl.push(new FileAcl(Arrays.asList("jmmut"), Collections.emptyList()));
//        acl.push(new AclEntry("jcoll", true, true, true, true));
//        acl.push(new AclEntry("jmmut", false, false, true, true));
        file.setAcls(acl);
        System.out.println(catalogFileDBAdaptor.createFile(studyId, file, null));
        file = new File("file.sam", File.Type.FILE, File.Format.PLAIN, File.Bioformat.ALIGNMENT, "data/file.sam", null, TimeUtils.getTime
                (), "", new File.FileStatus(File.FileStatus.STAGE), 1000);
        System.out.println(catalogFileDBAdaptor.createFile(studyId, file, null));
        file = new File("file.bam", File.Type.FILE, File.Format.BINARY, File.Bioformat.ALIGNMENT, "data/file.bam", null, TimeUtils
                .getTime(), "", new File.FileStatus(File.FileStatus.STAGE), 1000);
        System.out.println(catalogFileDBAdaptor.createFile(studyId, file, null));
        file = new File("file.vcf", File.Type.FILE, File.Format.PLAIN, File.Bioformat.VARIANT, "data/file2.vcf", null, TimeUtils.getTime
                (), "", new File.FileStatus(File.FileStatus.STAGE), 1000);

        try {
            System.out.println(catalogFileDBAdaptor.createFile(-20, file, null));
            fail("Expected \"StudyId not found\" exception");
        } catch (CatalogDBException e) {
            System.out.println(e);
        }

        System.out.println(catalogFileDBAdaptor.createFile(studyId, file, null));

        try {
            System.out.println(catalogFileDBAdaptor.createFile(studyId, file, null));
            fail("Expected \"File already exist\" exception");
        } catch (CatalogDBException e) {
            System.out.println(e);
        }
    }

    @Test
    public void getFileTest() throws CatalogDBException {
        File file = user3.getProjects().get(0).getStudies().get(0).getFiles().get(0);
        QueryResult<File> fileQueryResult = catalogFileDBAdaptor.getFile(file.getId(), null);
        System.out.println(fileQueryResult);
        try {
            System.out.println(catalogFileDBAdaptor.getFile(-1, null));
            fail("Expected \"FileId not found\" exception");
        } catch (CatalogDBException e) {
            System.out.println(e);
        }
    }

    @Test
    public void getAllFilesStudyNotValidTest() throws CatalogDBException {
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("not valid");
        catalogFileDBAdaptor.getAllFilesInStudy(-1, null);
    }

    @Test
    public void getAllFilesStudyNotExistsTest() throws CatalogDBException {
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("not exist");
        catalogFileDBAdaptor.getAllFilesInStudy(216544, null);
    }

    @Test
    public void getAllFilesTest() throws CatalogDBException {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        QueryResult<File> allFiles = catalogFileDBAdaptor.getAllFilesInStudy(studyId, null);
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
        parameters.put("status.status", File.FileStatus.READY);
        parameters.put("stats", stats);
        System.out.println(catalogFileDBAdaptor.update(fileId, parameters));

        file = catalogFileDBAdaptor.getFile(fileId, null).first();
        assertEquals(file.getStatus().getStatus(), File.FileStatus.READY);
        assertEquals(file.getStats(), stats);

        parameters = new ObjectMap();
        parameters.put("stats", "{}");
        System.out.println(catalogFileDBAdaptor.update(fileId, parameters));

        file = catalogFileDBAdaptor.getFile(fileId, null).first();
        assertEquals(file.getStats(), new LinkedHashMap<String, Object>());
    }

    @Test
    public void renameFileTest() throws CatalogDBException {
        String newName = "newFile.bam";
        String parentPath = "data/";
        long fileId = catalogFileDBAdaptor.getFileId(user3.getProjects().get(0).getStudies().get(0).getId(), "data/file.vcf");
        System.out.println(catalogFileDBAdaptor.renameFile(fileId, parentPath + newName, null));

        File file = catalogFileDBAdaptor.getFile(fileId, null).first();
        assertEquals(file.getName(), newName);
        assertEquals(file.getPath(), parentPath + newName);

        try {
            catalogFileDBAdaptor.renameFile(-1, "noFile", null);
            fail("error: expected \"file not found\"exception");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }

        long folderId = catalogFileDBAdaptor.getFileId(user3.getProjects().get(0).getStudies().get(0).getId(), "data/");
        String folderName = "folderName";
        catalogFileDBAdaptor.renameFile(folderId, folderName, null);
        assertTrue(catalogFileDBAdaptor.getFile(fileId, null).first().getPath().equals(folderName + "/" + newName));

    }

    @Test
    public void deleteFileTest() throws CatalogDBException, IOException {
        long fileId = catalogFileDBAdaptor.getFileId(user3.getProjects().get(0).getStudies().get(0).getId(), "data/file.vcf");
        QueryResult<File> delete = catalogFileDBAdaptor.delete(fileId, new QueryOptions());
        System.out.println(delete);
        assertTrue(delete.getNumResults() == 1);
        try {
            System.out.println(catalogFileDBAdaptor.delete(catalogFileDBAdaptor.getFileId(catalogStudyDBAdaptor.getStudyId
                    (catalogProjectDBAdaptor.getProjectId("jcoll", "1000G"), "ph1"), "data/noExists"), new QueryOptions()));
            fail("error: Expected \"FileId not found\" exception");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }
    }

    @Test
    public void fileAclsTest() throws CatalogDBException {
        long fileId = catalogFileDBAdaptor.getFileId(user3.getProjects().get(0).getStudies().get(0).getId(), "data/file.vcf");
        System.out.println(fileId);

        FileAcl granted = new FileAcl(Arrays.asList("jmmut"), Arrays.asList(FileAcl.FilePermissions.VIEW.name(),
                FileAcl.FilePermissions.VIEW_CONTENT.name(), FileAcl.FilePermissions.VIEW_HEADER.name(),
                FileAcl.FilePermissions.DELETE.name(), FileAcl.FilePermissions.SHARE.name()
        ));

//        AclEntry granted = new AclEntry("jmmut", true, true, true, false);
        catalogFileDBAdaptor.setFileAcl(fileId, granted, true);
        granted.setUsers(Arrays.asList("imedina"));
        catalogFileDBAdaptor.setFileAcl(fileId, granted, true);
        try {
            granted.setUsers(Arrays.asList("noUser"));
            catalogFileDBAdaptor.setFileAcl(fileId, granted, true);
            fail("error: expected exception");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }

        List<FileAcl> jmmut = catalogFileDBAdaptor.getFileAcl(fileId, "jmmut").getResult();
        assertTrue(!jmmut.isEmpty());
        System.out.println(jmmut.get(0).getPermissions());
        List<FileAcl> jcoll = catalogFileDBAdaptor.getFileAcl(fileId, "jcoll").getResult();
        assertTrue(jcoll.isEmpty());
    }

    @Test
    public void includeFields() throws CatalogDBException {

        QueryResult<File> fileQueryResult = catalogFileDBAdaptor.getFile(7,
                new QueryOptions("include", "projects.studies.files.id,projects.studies.files.path"));
        List<File> files = fileQueryResult.getResult();
        assertEquals("Include path does not work.", "data/file.vcf", files.get(0).getPath());
        assertEquals("Include not working.", null, files.get(0).getName());
    }

    @Test
    public void testDistinct() throws Exception {

        List<String> distinctOwners = catalogFileDBAdaptor.distinct(new Query(), CatalogFileDBAdaptor.QueryParams.OWNER_ID.key()).getResult();
        List<String> distinctTypes = catalogFileDBAdaptor.distinct(new Query(), CatalogFileDBAdaptor.QueryParams.TYPE.key()).getResult();
        assertEquals(Arrays.asList("imedina", "pfurio"), distinctOwners);
        assertEquals(Arrays.asList("FOLDER","FILE"), distinctTypes);

        List<String> distinctFormats = catalogFileDBAdaptor.distinct(new Query(CatalogFileDBAdaptor.QueryParams.OWNER_ID.key(), "pfurio"),
                CatalogFileDBAdaptor.QueryParams.FORMAT.key()).getResult();
        assertEquals(Arrays.asList("UNKNOWN", "COMMA_SEPARATED_VALUES", "BAM"), distinctFormats);

        distinctFormats = catalogFileDBAdaptor.distinct(new Query(),
                CatalogFileDBAdaptor.QueryParams.FORMAT.key()).getResult();
        Collections.sort(distinctFormats);
        List<String> expected = Arrays.asList("PLAIN", "UNKNOWN", "COMMA_SEPARATED_VALUES", "BAM");
        Collections.sort(expected);
        assertEquals(expected, distinctFormats);
    }

    @Test
    public void testRank() throws Exception {
        List<Document> rankedFilesPerDiskUsage = catalogFileDBAdaptor.rank(
                new Query(CatalogFileDBAdaptor.QueryParams.OWNER_ID.key(), "pfurio"),
                CatalogFileDBAdaptor.QueryParams.DISK_USAGE.key(), 100, false).getResult();

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

        List<Document> groupByBioformat = catalogFileDBAdaptor.groupBy(new Query(CatalogFileDBAdaptor.QueryParams.OWNER_ID.key(), "pfurio"),
                CatalogFileDBAdaptor.QueryParams.BIOFORMAT.key(), new QueryOptions()).getResult();

        assertEquals("ALIGNMENT", groupByBioformat.get(0).get("_id"));
        assertEquals(Arrays.asList("m_alignment.bam", "alignment.bam"), groupByBioformat.get(0).get("features"));

        assertEquals("NONE", groupByBioformat.get(1).get("_id"));
        assertEquals(Arrays.asList("m_file1.txt", "file2.txt", "file1.txt", "data/"), groupByBioformat.get(1).get("features"));

        groupByBioformat = catalogFileDBAdaptor.groupBy(new Query(CatalogFileDBAdaptor.QueryParams.OWNER_ID.key(), "pfurio")
                .append(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key(), 14), // MINECO study
                CatalogFileDBAdaptor.QueryParams.BIOFORMAT.key(), new QueryOptions()).getResult();

        assertEquals("ALIGNMENT", groupByBioformat.get(0).get("_id"));
        assertEquals(Arrays.asList("m_alignment.bam"), groupByBioformat.get(0).get("features"));

        assertEquals("NONE", groupByBioformat.get(1).get("_id"));
        assertEquals(Arrays.asList("m_file1.txt", "data/"), groupByBioformat.get(1).get("features"));

    }

    @Test
    public void testGroupBy1() throws Exception {

        List<Document> groupByBioformat = catalogFileDBAdaptor.groupBy(new Query(CatalogFileDBAdaptor.QueryParams.OWNER_ID.key(), "pfurio"),
                Arrays.asList(CatalogFileDBAdaptor.QueryParams.BIOFORMAT.key(), CatalogFileDBAdaptor.QueryParams.TYPE.key()),
                new QueryOptions()).getResult();

        assertEquals(3, groupByBioformat.size());

        assertEquals(2, ((Document) groupByBioformat.get(0).get("_id")).size()); // Alignment - File
        assertEquals(Arrays.asList("m_alignment.bam", "alignment.bam"), groupByBioformat.get(0).get("features"));

        assertEquals(2, ((Document) groupByBioformat.get(1).get("_id")).size()); // None - File
        assertEquals(Arrays.asList("m_file1.txt", "file2.txt", "file1.txt"), groupByBioformat.get(1).get("features"));

        assertEquals(2, ((Document) groupByBioformat.get(2).get("_id")).size()); // None - Folder
        assertEquals(Arrays.asList("data/"), groupByBioformat.get(2).get("features"));

    }
}
