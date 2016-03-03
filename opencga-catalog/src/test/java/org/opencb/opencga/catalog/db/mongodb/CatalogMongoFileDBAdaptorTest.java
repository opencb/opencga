package org.opencb.opencga.catalog.db.mongodb;

import org.bson.Document;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.AclEntry;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.core.common.StringUtils;
import org.opencb.opencga.core.common.TimeUtils;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by pfurio on 3/2/16.
 */
public class CatalogMongoFileDBAdaptorTest extends CatalogMongoDBAdaptorTest {

    @Test
    public void createFileToStudyTest() throws CatalogDBException, IOException {
        int studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        assertTrue(studyId >= 0);
        File file;
        file = new File("jobs/", File.Type.FOLDER, File.Format.PLAIN, File.Bioformat.NONE, "jobs/", null, TimeUtils.getTime(), "", File
                .Status.STAGE, 1000);
        LinkedList<AclEntry> acl = new LinkedList<>();
        acl.push(new AclEntry("jcoll", true, true, true, true));
        acl.push(new AclEntry("jmmut", false, false, true, true));
        file.setAcl(acl);
        System.out.println(catalogFileDBAdaptor.createFile(studyId, file, null));
        file = new File("file.sam", File.Type.FILE, File.Format.PLAIN, File.Bioformat.ALIGNMENT, "data/file.sam", null, TimeUtils.getTime
                (), "", File.Status.STAGE, 1000);
        System.out.println(catalogFileDBAdaptor.createFile(studyId, file, null));
        file = new File("file.bam", File.Type.FILE, File.Format.BINARY, File.Bioformat.ALIGNMENT, "data/file.bam", null, TimeUtils
                .getTime(), "", File.Status.STAGE, 1000);
        System.out.println(catalogFileDBAdaptor.createFile(studyId, file, null));
        file = new File("file.vcf", File.Type.FILE, File.Format.PLAIN, File.Bioformat.VARIANT, "data/file2.vcf", null, TimeUtils.getTime
                (), "", File.Status.STAGE, 1000);

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
    public void getAllFilesTest() throws CatalogDBException {
        int studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        QueryResult<File> allFiles = catalogFileDBAdaptor.getAllFilesInStudy(studyId, null);
        List<File> files = allFiles.getResult();
        System.out.println(files);
        assertTrue(!files.isEmpty());

        studyId = catalogStudyDBAdaptor.getStudyId(catalogProjectDBAdaptor.getProjectId("jcoll", "1000G"), "ph7");
        allFiles = catalogFileDBAdaptor.getAllFilesInStudy(studyId, null);
        assertTrue(allFiles.getResult().isEmpty());
    }

    @Test
    public void modifyFileTest() throws CatalogDBException, IOException {
        File file = user3.getProjects().get(0).getStudies().get(0).getFiles().get(0);
        int fileId = file.getId();

        Document stats = new Document("stat1", 1).append("stat2", true).append("stat3", "ok" + StringUtils
                .randomString(20));

        ObjectMap parameters = new ObjectMap();
        parameters.put("status", File.Status.READY);
        parameters.put("stats", stats);
        System.out.println(catalogFileDBAdaptor.update(fileId, parameters));

        file = catalogFileDBAdaptor.getFile(fileId, null).first();
        assertEquals(file.getStatus(), File.Status.READY);
        assertEquals(file.getStats(), stats);

    }

    @Test
    public void renameFileTest() throws CatalogDBException {
        String newName = "newFile.bam";
        String parentPath = "data/";
        int fileId = catalogFileDBAdaptor.getFileId(user3.getProjects().get(0).getStudies().get(0).getId(), "data/file.vcf");
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

        int folderId = catalogFileDBAdaptor.getFileId(user3.getProjects().get(0).getStudies().get(0).getId(), "data/");
        String folderName = "folderName";
        catalogFileDBAdaptor.renameFile(folderId, folderName, null);
        assertTrue(catalogFileDBAdaptor.getFile(fileId, null).first().getPath().equals(folderName + "/" + newName));

    }

    @Test
    public void deleteFileTest() throws CatalogDBException, IOException {
        int fileId = catalogFileDBAdaptor.getFileId(user3.getProjects().get(0).getStudies().get(0).getId(), "data/file.vcf");
        QueryResult<File> delete = catalogFileDBAdaptor.delete(fileId);
        System.out.println(delete);
        assertTrue(delete.getNumResults() == 1);
        try {
            System.out.println(catalogFileDBAdaptor.delete(catalogFileDBAdaptor.getFileId(catalogStudyDBAdaptor.getStudyId
                    (catalogProjectDBAdaptor.getProjectId("jcoll", "1000G"), "ph1"), "data/noExists")));
            fail("error: Expected \"FileId not found\" exception");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }
    }

    @Test
    public void fileAclsTest() throws CatalogDBException {
        int fileId = catalogFileDBAdaptor.getFileId(user3.getProjects().get(0).getStudies().get(0).getId(), "data/file.vcf");
        System.out.println(fileId);

        AclEntry granted = new AclEntry("jmmut", true, true, true, false);
        catalogFileDBAdaptor.setFileAcl(fileId, granted);
        granted.setUserId("imedina");
        catalogFileDBAdaptor.setFileAcl(fileId, granted);
        try {
            granted.setUserId("noUser");
            catalogFileDBAdaptor.setFileAcl(fileId, granted);
            fail("error: expected exception");
        } catch (CatalogDBException e) {
            System.out.println("correct exception: " + e);
        }

        List<AclEntry> jmmut = catalogFileDBAdaptor.getFileAcl(fileId, "jmmut").getResult();
        assertTrue(!jmmut.isEmpty());
        System.out.println(jmmut.get(0));
        List<AclEntry> jcoll = catalogFileDBAdaptor.getFileAcl(fileId, "jcoll").getResult();
        assertTrue(jcoll.isEmpty());
    }

}
