package org.opencb.opencga.catalog.utils;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogManagerExternalResource;
import org.opencb.opencga.catalog.CatalogManagerTest;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Project;
import org.opencb.opencga.catalog.models.Status;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.core.common.IOUtils;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class FileScannerTest {
    @Rule
    public CatalogManagerExternalResource catalogManagerExternalResource = new CatalogManagerExternalResource();

    public static final String PASSWORD = "asdf";
    private CatalogManager catalogManager;
    private String sessionIdUser;
    private File folder;
    private Study study;
    private Project project;
    private Path directory;

    @Before
    public void setUp() throws IOException, CatalogException {
        catalogManager = catalogManagerExternalResource.getCatalogManager();

        catalogManager.createUser("user", "User Name", "mail@ebi.ac.uk", PASSWORD, "", null, null);
        sessionIdUser = catalogManager.login("user", PASSWORD, "127.0.0.1").first().getString("sessionId");
        project = catalogManager.createProject("Project about some genomes", "1000G", "", "ACME", null, sessionIdUser).first();
        study = catalogManager.createStudy(project.getId(), "Phase 1", "phase1", Study.Type.TRIO, "Done", sessionIdUser).first();
        folder = catalogManager.createFolder(study.getId(), Paths.get("data/test/folder/"), true, null, sessionIdUser).first();

        directory = catalogManagerExternalResource.getOpencgaHome().resolve("catalog_scan_test_folder").toAbsolutePath();
        if (directory.toFile().exists()) {
            IOUtils.deleteDirectory(directory);
        }
        Files.createDirectory(directory);
    }

    @Test
    public void testScan() throws IOException, CatalogException {

        Files.createDirectory(directory.resolve("subfolder"));
        Files.createDirectory(directory.resolve("subfolder/subsubfolder"));
        CatalogManagerTest.createDebugFile(directory.resolve("file1.txt").toString());
        CatalogManagerTest.createDebugFile(directory.resolve("file2.txt").toString());
        CatalogManagerTest.createDebugFile(directory.resolve("file3.txt").toString());
        CatalogManagerTest.createDebugFile(directory.resolve("subfolder/file1.txt").toString());
        CatalogManagerTest.createDebugFile(directory.resolve("subfolder/file2.txt").toString());
        CatalogManagerTest.createDebugFile(directory.resolve("subfolder/file3.txt").toString());
        CatalogManagerTest.createDebugFile(directory.resolve("subfolder/subsubfolder/file1.txt").toString());
        CatalogManagerTest.createDebugFile(directory.resolve("subfolder/subsubfolder/file2.txt").toString());
        CatalogManagerTest.createDebugFile(directory.resolve("subfolder/subsubfolder/file3.txt").toString());

        FileScanner fileScanner = new FileScanner(catalogManager);
        List<File> files = fileScanner.scan(folder, directory.toUri(), FileScanner.FileScannerPolicy.DELETE, true, true, sessionIdUser);

        assertEquals(9, files.size());
        files.forEach((File file) -> assertTrue(file.getAttributes().containsKey("checksum")));

    }

    @Test
    public void testDeleteExisting() throws IOException, CatalogException {

        File file = catalogManager.createFile(study.getId(), File.Format.PLAIN, File.Bioformat.NONE, folder.getPath() + "file1.txt",
                CatalogManagerTest.createDebugFile().toURI(), "", false, sessionIdUser).first();

        CatalogManagerTest.createDebugFile(directory.resolve("file1.txt").toString());
        List<File> files = new FileScanner(catalogManager).scan(folder, directory.toUri(), FileScanner.FileScannerPolicy.DELETE, false,
                true, sessionIdUser);

        files.forEach((File f) -> assertFalse(f.getAttributes().containsKey("checksum")));
        assertEquals(File.FileStatus.DELETED, getFile(file.getId()).getStatus().getName());
    }

    public File getFile(long id) throws CatalogException {
        return catalogManager.searchFile(study.getId(),
                new Query(FileDBAdaptor.QueryParams.ID.key(), id)
                        .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), Status.DELETED + "," + Status.TRASHED + "," + Status.READY), sessionIdUser)
                .first();
    }

    @Test
    public void testDeleteTrashed() throws IOException, CatalogException {
        File file = catalogManager.createFile(study.getId(), File.Format.PLAIN, File.Bioformat.NONE, folder.getPath() + "file1.txt",
                CatalogManagerTest.createDebugFile().toURI(), "", false, sessionIdUser).first();

        catalogManager.getFileManager().delete(Long.toString(file.getId()), new QueryOptions(), sessionIdUser);

        QueryResult<File> fileQueryResult = catalogManager.getFileManager().get(new Query()
                        .append(FileDBAdaptor.QueryParams.ID.key(), file.getId())
                        .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), study.getId())
                        .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), "!=EMPTY"),
                new QueryOptions(), sessionIdUser);
        file = fileQueryResult.first();
        assertEquals(File.FileStatus.TRASHED, file.getStatus().getName());

//        Files.delete(Paths.get(catalogManager.getFileUri(file)));
//        List<File> files = new FileScanner(catalogManager).checkStudyFiles(study, false, sessionIdUser);
//
//        file = getFile(file.getId());
//        assertEquals(File.FileStatus.TRASHED, file.getStatus().getName());
//        assertEquals(1, files.size());
//        assertEquals(file.getId(), files.get(0).getId());
    }

    @Test
    public void testReplaceExisting() throws IOException, CatalogException {
        CatalogManagerTest.createDebugFile(directory.resolve("file1.txt").toString());
        Files.createDirectory(directory.resolve("s/"));
        CatalogManagerTest.createDebugFile(directory.resolve("s/file2.txt").toString());

        File file = catalogManager.createFile(study.getId(), File.Format.PLAIN, File.Bioformat.NONE, folder.getPath() + "file1.txt",
                CatalogManagerTest.createDebugFile().toURI(), "", false, sessionIdUser).first();

        catalogManager.createFile(study.getId(), File.Format.PLAIN, File.Bioformat.NONE, folder.getPath() + "s/file2.txt",
                CatalogManagerTest.createDebugFile().toURI(), "", true, sessionIdUser).first();

        FileScanner fileScanner = new FileScanner(catalogManager);
        fileScanner.scan(folder, directory.toUri(), FileScanner.FileScannerPolicy.REPLACE, true, true, sessionIdUser);

        File replacedFile = catalogManager.getFile(file.getId(), sessionIdUser).first();
        assertEquals(File.FileStatus.READY, replacedFile.getStatus().getName());
        assertEquals(file.getId(), replacedFile.getId());
        assertFalse(replacedFile.getAttributes().get("checksum").equals(file.getAttributes().get("checksum")));
    }

    @Test
    public void testRegisterFiles() throws IOException, CatalogException {
        Path file1 = directory.resolve("file1.txt");
        Path file2 = directory.resolve("s/file2.txt");
        Path folder = directory.resolve("s/");
        Path file3 = directory.resolve("file3.txt");

        CatalogManagerTest.createDebugFile(file1.toString());
        Files.createDirectory(folder);
        CatalogManagerTest.createDebugFile(file2.toString());
        CatalogManagerTest.createDebugFile(file3.toString());

        List<Path> filePaths = new ArrayList<>(2);
        filePaths.add(file1);
        filePaths.add(file2);
        FileScanner fileScanner = new FileScanner(catalogManager);
//        List<File> files = fileScanner.registerFiles(this.folder, filePaths, FileScanner.FileScannerPolicy.DELETE, true, false, sessionIdUser);
        Predicate<URI> uriPredicate = uri -> uri.getPath().endsWith("file1.txt") || uri.getPath().endsWith("file2.txt");
        List<File> files = fileScanner.scan(this.folder, directory.toUri(), FileScanner.FileScannerPolicy.DELETE, true, false, uriPredicate, -1, sessionIdUser);

        assertEquals(2, files.size());
        for (File file : files) {
            assertTrue(Paths.get(file.getUri()).toFile().exists());
        }
        for (Path filePath : filePaths) {
            assertTrue(filePath.toFile().exists());
        }
        assertTrue(file3.toFile().exists());
    }


    @Test
    public void testScanStudyURI() throws IOException, CatalogException {

        CatalogManagerTest.createDebugFile(directory.resolve("file1.txt").toString());

        FileScanner fileScanner = new FileScanner(catalogManager);
        List<File> files = fileScanner.scan(folder, directory.toUri(), FileScanner.FileScannerPolicy.REPLACE, true, true, sessionIdUser);
        assertEquals(1, files.size());

        URI studyUri = catalogManager.getStudyUri(study.getId());
        CatalogManagerTest.createDebugFile(studyUri.resolve("data/test/folder/").resolve("file2.txt").getPath());
        File root = catalogManager.searchFile(study.getId(), new Query("name", "."), sessionIdUser).first();
        files = fileScanner.scan(root, studyUri, FileScanner.FileScannerPolicy.REPLACE, true, true, sessionIdUser);

        assertEquals(1, files.size());
        files.forEach((f) -> assertTrue(f.getDiskUsage() > 0));
        files.forEach((f) -> assertEquals(f.getStatus().getName(), File.FileStatus.READY));
        files.forEach((f) -> assertTrue(f.getAttributes().containsKey("checksum")));
    }

    @Test
    public void testResyncStudy() throws IOException, CatalogException {
        CatalogManagerTest.createDebugFile(directory.resolve("file1.txt").toString());

        //ReSync study folder. Will detect any difference.
        FileScanner fileScanner = new FileScanner(catalogManager);
        List<File> files;
        files = fileScanner.reSync(study, true, sessionIdUser);
        assertEquals(0, files.size());

        //Add one extra file. ReSync study folder.
        URI studyUri = catalogManager.getStudyUri(study.getId());
        Path filePath = CatalogManagerTest.createDebugFile(studyUri.resolve("data/test/folder/").resolve("file_scanner_test_file.txt").getPath()).toPath();
        files = fileScanner.reSync(study, true, sessionIdUser);

        assertEquals(1, files.size());
        File file = files.get(0);
        assertTrue(file.getDiskUsage() > 0);
        assertEquals(File.FileStatus.READY, file.getStatus().getName());
        assertTrue(file.getAttributes().containsKey("checksum"));


        //Delete file. CheckStudyFiles. Will detect one File.Status.MISSING file
        Files.delete(filePath);
        files = fileScanner.checkStudyFiles(study, true, sessionIdUser);

        assertEquals(1, files.size());
        assertEquals(File.FileStatus.MISSING, files.get(0).getStatus().getName());
        String originalChecksum = files.get(0).getAttributes().get("checksum").toString();

        //Restore file. CheckStudyFiles. Will detect one re-tracked file. Checksum must be different.
        CatalogManagerTest.createDebugFile(filePath.toString());
        files = fileScanner.checkStudyFiles(study, true, sessionIdUser);

        assertEquals(1, files.size());
        assertEquals(File.FileStatus.READY, files.get(0).getStatus().getName());
        String newChecksum = files.get(0).getAttributes().get("checksum").toString();
        assertFalse(originalChecksum.equals(newChecksum));

        //Delete file. ReSync. Will detect one File.Status.MISSING file (like checkFile)
        Files.delete(filePath);
        files = fileScanner.reSync(study, true, sessionIdUser);

        assertEquals(1, files.size());
        assertEquals(File.FileStatus.MISSING, files.get(0).getStatus().getName());
        originalChecksum = files.get(0).getAttributes().get("checksum").toString();

        //Restore file. CheckStudyFiles. Will detect one found file. Checksum must be different.
        CatalogManagerTest.createDebugFile(filePath.toString());
        files = fileScanner.reSync(study, true, sessionIdUser);

        assertEquals(1, files.size());
        assertEquals(File.FileStatus.READY, files.get(0).getStatus().getName());
        newChecksum = files.get(0).getAttributes().get("checksum").toString();
        assertFalse(originalChecksum.equals(newChecksum));

    }

    @Test
    public void testComplexAdd() throws IOException, CatalogException {

        CatalogManagerTest.createDebugFile(directory.resolve("file1.vcf.gz").toString());
        CatalogManagerTest.createDebugFile(directory.resolve("file1.vcf.variants.json").toString());
        CatalogManagerTest.createDebugFile(directory.resolve("file1.vcf.variants.json.gz").toString());
        CatalogManagerTest.createDebugFile(directory.resolve("file1.vcf.variants.json.snappy").toString());
        CatalogManagerTest.createDebugFile(directory.resolve("file2.bam").toString());
        CatalogManagerTest.createDebugFile(directory.resolve("file2.sam.gz").toString());

        FileScanner fileScanner = new FileScanner(catalogManager);
        List<File> files = fileScanner.scan(folder, directory.toUri(), FileScanner.FileScannerPolicy.REPLACE, true, true, sessionIdUser);

        Map<String, File> map = files.stream().collect(Collectors.toMap(File::getName, (f) -> f));

        assertEquals(6, files.size());
        files.forEach((file) -> assertEquals(File.FileStatus.READY, file.getStatus().getName()));
        assertEquals(File.Bioformat.VARIANT, map.get("file1.vcf.gz").getBioformat());
        assertEquals(File.Bioformat.VARIANT, map.get("file1.vcf.variants.json").getBioformat());
        assertEquals(File.Bioformat.VARIANT, map.get("file1.vcf.variants.json.gz").getBioformat());
        assertEquals(File.Bioformat.VARIANT, map.get("file1.vcf.variants.json.snappy").getBioformat());
        assertEquals(File.Bioformat.ALIGNMENT, map.get("file2.bam").getBioformat());
        assertEquals(File.Bioformat.ALIGNMENT, map.get("file2.sam.gz").getBioformat());

        assertEquals(File.Format.VCF, map.get("file1.vcf.gz").getFormat());
        assertEquals(File.Format.JSON, map.get("file1.vcf.variants.json").getFormat());
        assertEquals(File.Format.JSON, map.get("file1.vcf.variants.json.gz").getFormat());
        assertEquals(File.Format.JSON, map.get("file1.vcf.variants.json.snappy").getFormat());
        assertEquals(File.Format.BAM, map.get("file2.bam").getFormat());
        assertEquals(File.Format.SAM, map.get("file2.sam.gz").getFormat());

    }


}