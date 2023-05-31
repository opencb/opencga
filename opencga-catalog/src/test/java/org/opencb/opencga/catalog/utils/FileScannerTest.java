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

package org.opencb.opencga.catalog.utils;

import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.IOManager;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.catalog.managers.CatalogManagerTest;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.IOUtils;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileStatus;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

@Category(MediumTests.class)
public class FileScannerTest {
    @Rule
    public CatalogManagerExternalResource catalogManagerExternalResource = new CatalogManagerExternalResource();

    private CatalogManager catalogManager;
    private String sessionIdUser;
    private File folder;
    private Study study;
    private Project project;
    private Path directory;

    protected static final QueryOptions INCLUDE_RESULT = new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true);

    @Before
    public void setUp() throws IOException, CatalogException {
        catalogManager = catalogManagerExternalResource.getCatalogManager();

        String opencgaToken = catalogManager.getUserManager().loginAsAdmin(TestParamConstants.ADMIN_PASSWORD).getToken();

        catalogManager.getUserManager().create("user", "User Name", "mail@ebi.ac.uk", TestParamConstants.PASSWORD, "", null, Account.AccountType.FULL, opencgaToken);
        sessionIdUser = catalogManager.getUserManager().login("user", TestParamConstants.PASSWORD).getToken();
        project = catalogManager.getProjectManager().create("1000G", "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", INCLUDE_RESULT, sessionIdUser).first();
        study = catalogManager.getStudyManager().create(project.getId(), "phase1", null, "Phase 1", "Done", null, null, null, null, INCLUDE_RESULT, sessionIdUser).first();
        folder = catalogManager.getFileManager().createFolder(study.getId(), Paths.get("data/test/folder/").toString(),
                true, null, QueryOptions.empty(), sessionIdUser).first();

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
        files.forEach((File file) -> assertTrue(StringUtils.isNotEmpty(file.getChecksum())));

    }

    @Test
    public void testDeleteExisting() throws IOException, CatalogException {
        DataResult<File> queryResult;
        try (InputStream inputStream = new BufferedInputStream(new FileInputStream(CatalogManagerTest.createDebugFile()))) {
            queryResult = catalogManager.getFileManager().upload(study.getFqn(), inputStream,
                    new File().setPath(folder.getPath() + "file1.txt"), false, false, false, sessionIdUser);
        }
        File file = queryResult.first();

        CatalogManagerTest.createDebugFile(directory.resolve("file1.txt").toString());
        List<File> files = new FileScanner(catalogManager).scan(folder, directory.toUri(), FileScanner.FileScannerPolicy.DELETE, false,
                true, sessionIdUser);

        files.forEach((File f) -> assertFalse(f.getAttributes().containsKey("checksum")));
        assertEquals(FileStatus.DELETED, getDeletedFile(file.getUid()).getInternal().getStatus().getId());
    }

    public File getDeletedFile(long id) throws CatalogException {
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.UID.key(), id)
                .append(FileDBAdaptor.QueryParams.DELETED.key(), true);
        return catalogManager.getFileManager().search(study.getFqn(), query, null, sessionIdUser).first();
    }

    @Test
    public void testReplaceExisting() throws IOException, CatalogException {
        // Create and register file1.txt and s/file2.txt
        File file;
        try (InputStream inputStream = new BufferedInputStream(new FileInputStream(CatalogManagerTest.createDebugFile()))) {
            file = catalogManager.getFileManager().upload(study.getFqn(), inputStream,
                    new File().setPath(folder.getPath() + "file1.txt"), false, true, true, sessionIdUser).first();
        }
        try (InputStream inputStream = new BufferedInputStream(new FileInputStream(CatalogManagerTest.createDebugFile()))) {
            catalogManager.getFileManager().upload(study.getFqn(), inputStream,
                    new File().setPath(folder.getPath() + "s/file2.txt"), false, true, true, sessionIdUser).first();
        }

        // Create same file structure, and replace
        CatalogManagerTest.createDebugFile(directory.resolve("file1.txt").toString());
        Files.createDirectory(directory.resolve("s/"));
        CatalogManagerTest.createDebugFile(directory.resolve("s/file2.txt").toString());

        FileScanner fileScanner = new FileScanner(catalogManager);
        fileScanner.scan(folder, directory.toUri(), FileScanner.FileScannerPolicy.REPLACE, true, true, sessionIdUser);

        File replacedFile = catalogManager.getFileManager().get(study.getFqn(), file.getPath(), null, sessionIdUser).first();
        assertEquals(FileStatus.READY, replacedFile.getInternal().getStatus().getId());
        assertEquals(file.getUid(), replacedFile.getUid());
        assertNotEquals(replacedFile.getChecksum(), file.getChecksum());
        assertEquals(replacedFile.getChecksum(), catalogManager.getIoManagerFactory().getDefault().calculateChecksum(replacedFile.getUri()));
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
        List<File> files = fileScanner.scan(this.folder, directory.toUri(), FileScanner.FileScannerPolicy.DELETE, true, false, uriPredicate, sessionIdUser);

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

        Path studyUriPath = Paths.get(study.getUri());
        CatalogManagerTest.createDebugFile(studyUriPath.resolve("data/test/folder/").resolve("file2.txt").toString());
        File root = catalogManager.getFileManager().search(study.getFqn(), new Query("name", "."), null, sessionIdUser).first();
        files = fileScanner.scan(root, studyUriPath.toUri(), FileScanner.FileScannerPolicy.REPLACE, true, true, sessionIdUser);

        assertEquals(1, files.size());
        files.forEach((f) -> assertTrue(f.getSize() > 0));
        files.forEach((f) -> assertEquals(f.getInternal().getStatus().getId(), FileStatus.READY));
        files.forEach((f) -> assertTrue(StringUtils.isNotEmpty(f.getChecksum())));
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
        Path studyUriPath = Paths.get(study.getUri());
        // Create the directories
        catalogManager.getIoManagerFactory().getDefault().createDirectory(studyUriPath.resolve("data/test/folder/").toUri(), true);
        Path filePath = CatalogManagerTest
                .createDebugFile(studyUriPath.resolve("data/test/folder/").resolve("file_scanner_test_file.txt").toString()).toPath();
        files = fileScanner.reSync(study, true, sessionIdUser);

        assertEquals(1, files.size());
        File file = files.get(0);
        assertTrue(file.getSize() > 0);
        assertEquals(FileStatus.READY, file.getInternal().getStatus().getId());
        assertTrue(StringUtils.isNotEmpty(file.getChecksum()));

        //Delete file. CheckStudyFiles. Will detect one File.Status.MISSING file
        Files.delete(filePath);
        files = fileScanner.checkStudyFiles(study, true, sessionIdUser);

        assertEquals(1, files.size());
        assertEquals(FileStatus.MISSING, files.get(0).getInternal().getStatus().getId());
        String originalChecksum = files.get(0).getChecksum();

        //Restore file. CheckStudyFiles. Will detect one re-tracked file. Checksum must be different.
        CatalogManagerTest.createDebugFile(filePath.toString());
        files = fileScanner.checkStudyFiles(study, true, sessionIdUser);

        assertEquals(1, files.size());
        assertEquals(FileStatus.READY, files.get(0).getInternal().getStatus().getId());
        String newChecksum = files.get(0).getChecksum();
        assertNotEquals(originalChecksum, newChecksum);

        //Delete file. ReSync. Will detect one File.Status.MISSING file (like checkFile)
        Files.delete(filePath);
        files = fileScanner.reSync(study, true, sessionIdUser);

        assertEquals(1, files.size());
        assertEquals(FileStatus.MISSING, files.get(0).getInternal().getStatus().getId());
        originalChecksum = files.get(0).getChecksum();

        //Restore file. CheckStudyFiles. Will detect one found file. Checksum must be different.
        CatalogManagerTest.createDebugFile(filePath.toString());
        files = fileScanner.reSync(study, true, sessionIdUser);

        assertEquals(1, files.size());
        assertEquals(FileStatus.READY, files.get(0).getInternal().getStatus().getId());
        newChecksum = files.get(0).getChecksum();
        assertNotEquals(originalChecksum, newChecksum);

    }

    @Test
    public void testComplexAdd() throws IOException, CatalogException, URISyntaxException {

        IOManager ioManager = catalogManager.getIoManagerFactory().getDefault();
        URI fileUri = getClass().getResource("/biofiles/variant-test-file.vcf.gz").toURI();
        ioManager.copy(fileUri, directory.resolve("file1.vcf.gz").toUri());

        CatalogManagerTest.createDebugFile(directory.resolve("file1.vcf.variants.json").toString());
        CatalogManagerTest.createDebugFile(directory.resolve("file1.vcf.variants.json.gz").toString());
        CatalogManagerTest.createDebugFile(directory.resolve("file1.vcf.variants.json.snappy").toString());
        CatalogManagerTest.createDebugFile(directory.resolve("file2.bam").toString());
        CatalogManagerTest.createDebugFile(directory.resolve("file2.sam.gz").toString());

        FileScanner fileScanner = new FileScanner(catalogManager);
        List<File> files = fileScanner.scan(folder, directory.toUri(), FileScanner.FileScannerPolicy.REPLACE, true, true, sessionIdUser);

        Map<String, File> map = files.stream().collect(Collectors.toMap(File::getName, (f) -> f));

        assertEquals(6, files.size());
        files.forEach((file) -> assertEquals(FileStatus.READY, file.getInternal().getStatus().getId()));
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