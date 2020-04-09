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

package org.opencb.opencga.catalog.utils;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.catalog.managers.CatalogManagerTest;
import org.opencb.opencga.catalog.managers.FileUtils;
import org.opencb.opencga.core.common.IOUtils;
import org.opencb.opencga.core.models.*;

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

        catalogManager.getUserManager().create("user", "User Name", "mail@ebi.ac.uk", PASSWORD, "", null, Account.Type.FULL, null);
        sessionIdUser = catalogManager.getUserManager().login("user", PASSWORD).getToken();
        project = catalogManager.getProjectManager().create("Project about some genomes", "1000G", "", "ACME", "Homo sapiens",
                null, null, "GRCh38", new QueryOptions(), sessionIdUser).first();
        study = catalogManager.getStudyManager().create(String.valueOf(project.getId()), "Phase 1", "phase1", Study.Type.TRIO, null,
                "Done", null, null, null, null, null, null, null, null, sessionIdUser).first();
        folder = catalogManager.getFileManager().createFolder(Long.toString(study.getId()), Paths.get("data/test/folder/").toString(),
                null, true, null, QueryOptions.empty(), sessionIdUser).first();

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

        QueryResult<File> queryResult = catalogManager.getFileManager().create(Long.toString(study.getId()), File.Type.FILE, File.Format.PLAIN, File.Bioformat.NONE, folder.getPath() + "file1.txt", null, "", new File.FileStatus(File.FileStatus.STAGE), 0, -1, null, -1, null, null, false, null, null, sessionIdUser);

        new FileUtils(catalogManager).upload(CatalogManagerTest.createDebugFile().toURI(), queryResult.first(), null, sessionIdUser, false, false, true, true, Long.MAX_VALUE);
        File file = catalogManager.getFileManager().get(queryResult.first().getId(), null, sessionIdUser).first();

        CatalogManagerTest.createDebugFile(directory.resolve("file1.txt").toString());
        List<File> files = new FileScanner(catalogManager).scan(folder, directory.toUri(), FileScanner.FileScannerPolicy.DELETE, false,
                true, sessionIdUser);

        files.forEach((File f) -> assertFalse(f.getAttributes().containsKey("checksum")));
        assertEquals(File.FileStatus.DELETED, getFile(file.getId()).getStatus().getName());
    }

    public File getFile(long id) throws CatalogException {
        return catalogManager.getFileManager().get(study.getId(), new Query(FileDBAdaptor.QueryParams.ID.key(), id).append(FileDBAdaptor
                .QueryParams.STATUS_NAME.key(), Status.DELETED + "," + Status.TRASHED + "," + Status.READY), null, sessionIdUser)
                .first();
    }

    @Test
    public void testDeleteTrashed() throws IOException, CatalogException {
        QueryResult<File> queryResult = catalogManager.getFileManager().create(Long.toString(study.getId()), File.Type.FILE, File.Format.PLAIN, File.Bioformat.NONE, folder.getPath() + "file1.txt", null, "", new File.FileStatus(File.FileStatus.STAGE), 0, -1, null, -1, null, null, false, null, null, sessionIdUser);
        new FileUtils(catalogManager).upload(CatalogManagerTest.createDebugFile().toURI(), queryResult.first(), null, sessionIdUser, false, false, true, true, Long.MAX_VALUE);
        File file = catalogManager.getFileManager().get(queryResult.first().getId(), null, sessionIdUser).first();

        catalogManager.getFileManager().delete(null, Long.toString(file.getId()), new QueryOptions(), sessionIdUser);

        QueryResult<File> fileQueryResult = catalogManager.getFileManager().get(String.valueOf(study.getId()),
                new Query()
                        .append(FileDBAdaptor.QueryParams.ID.key(), file.getId())
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

        QueryResult<File> queryResult1 = catalogManager.getFileManager().create(Long.toString(study.getId()), File.Type.FILE, File.Format.PLAIN, File.Bioformat.NONE, folder.getPath() + "file1.txt", null, "", new File.FileStatus(File.FileStatus.STAGE), 0, -1, null, -1, null, null, false, null, null, sessionIdUser);
        new FileUtils(catalogManager).upload(CatalogManagerTest.createDebugFile().toURI(), queryResult1.first(), null, sessionIdUser, false, false, true, true, Long.MAX_VALUE);

        File file = catalogManager.getFileManager().get(queryResult1.first().getId(), null, sessionIdUser).first();

        QueryResult<File> queryResult = catalogManager.getFileManager().create(Long.toString(study.getId()), File.Type.FILE, File.Format.PLAIN, File.Bioformat.NONE, folder.getPath() + "s/file2.txt", null, "", new File.FileStatus(File.FileStatus.STAGE), 0, -1, null, -1, null, null, true, null, null, sessionIdUser);
        new FileUtils(catalogManager).upload(CatalogManagerTest.createDebugFile().toURI(), queryResult.first(), null, sessionIdUser, false, false, true, true, Long.MAX_VALUE);
        catalogManager.getFileManager().get(queryResult.first().getId(), null, sessionIdUser).first();

        FileScanner fileScanner = new FileScanner(catalogManager);
        fileScanner.scan(folder, directory.toUri(), FileScanner.FileScannerPolicy.REPLACE, true, true, sessionIdUser);

        File replacedFile = catalogManager.getFileManager().get(file.getId(), null, sessionIdUser).first();
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

        URI studyUri = study.getUri();
        CatalogManagerTest.createDebugFile(studyUri.resolve("data/test/folder/").resolve("file2.txt").getPath());
        File root = catalogManager.getFileManager().get(study.getId(), new Query("name", "."), null, sessionIdUser).first();
        files = fileScanner.scan(root, studyUri, FileScanner.FileScannerPolicy.REPLACE, true, true, sessionIdUser);

        assertEquals(1, files.size());
        files.forEach((f) -> assertTrue(f.getSize() > 0));
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
        URI studyUri = study.getUri();
        Path filePath = CatalogManagerTest.createDebugFile(studyUri.resolve("data/test/folder/").resolve("file_scanner_test_file.txt").getPath()).toPath();
        files = fileScanner.reSync(study, true, sessionIdUser);

        assertEquals(1, files.size());
        File file = files.get(0);
        assertTrue(file.getSize() > 0);
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