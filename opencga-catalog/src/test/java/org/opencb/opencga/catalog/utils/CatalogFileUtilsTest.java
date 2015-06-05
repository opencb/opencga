/*
 * Copyright 2015 OpenCB
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
import org.junit.rules.ExpectedException;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.core.config.DataStoreServerAddress;
import org.opencb.datastore.mongodb.MongoDBConfiguration;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.CatalogManagerTest;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.catalog.utils.CatalogFileUtils;
import org.opencb.opencga.core.common.IOUtils;
import org.opencb.opencga.core.common.StringUtils;
import org.opencb.opencga.core.common.TimeUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by jacobo on 28/01/15.
 */
public class CatalogFileUtilsTest {

    CatalogFileUtils catalogFileUtils;
    private int studyId;
    private String userSessionId;
    private String adminSessionId;
    private CatalogManager catalogManager;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void before() throws CatalogException, IOException {

        InputStream is = this.getClass().getClassLoader().getResourceAsStream("catalog.properties");
        Properties properties = new Properties();
        properties.load(is);


        MongoDBConfiguration mongoDBConfiguration = MongoDBConfiguration.builder()
                .add("username", properties.getProperty(CatalogManager.CATALOG_DB_USER, ""))
                .add("password", properties.getProperty(CatalogManager.CATALOG_DB_PASSWORD, ""))
                .add("authenticationDatabase", properties.getProperty(CatalogManager.CATALOG_DB_AUTHENTICATION_DB, ""))
                .build();

        String[] split = properties.getProperty(CatalogManager.CATALOG_DB_HOSTS).split(",")[0].split(":");
        DataStoreServerAddress dataStoreServerAddress = new DataStoreServerAddress(
                split[0], 27017);


        CatalogManagerTest.clearCatalog(properties);
        catalogManager = new CatalogManager(properties);

        //Create USER
        catalogManager.createUser("user", "name", "mi@mail.com", "asdf", "", null);
        userSessionId = catalogManager.login("user", "asdf", "--").getResult().get(0).getString("sessionId");
        adminSessionId = catalogManager.login("admin", "admin", "--").getResult().get(0).getString("sessionId");
        int projectId = catalogManager.createProject("user", "proj", "proj", "", "", null, userSessionId).getResult().get(0).getId();
        studyId = catalogManager.createStudy(projectId, "std", "std", Study.Type.CONTROL_SET, "", userSessionId).getResult().get(0).getId();

        catalogFileUtils = new CatalogFileUtils(catalogManager);
    }


    @Test
    public void updateTest() throws IOException, CatalogException {
        QueryResult<File> fileQueryResult;
        URI sourceUri;

        sourceUri = CatalogManagerTest.createDebugFile().toURI();
        fileQueryResult = catalogManager.createFile(
                studyId, File.Format.PLAIN, File.Bioformat.NONE, "item." + TimeUtils.getTimeMillis() + ".txt", "file at root", true, -1, userSessionId);
        catalogFileUtils.upload(sourceUri, fileQueryResult.getResult().get(0), null, adminSessionId, false, false, true, false, 1000);

        sourceUri = CatalogManagerTest.createDebugFile().toURI();
        fileQueryResult = catalogManager.createFile(
                studyId, File.Format.PLAIN, File.Bioformat.NONE, "item." + TimeUtils.getTimeMillis() + ".txt", "file at root", true, -1, userSessionId);
        catalogFileUtils.upload(sourceUri, fileQueryResult.getResult().get(0), null, adminSessionId, false, false, true, false, 100000000);

        sourceUri = CatalogManagerTest.createDebugFile().toURI();
        fileQueryResult = catalogManager.createFile(
                studyId, File.Format.PLAIN, File.Bioformat.NONE, "item." + TimeUtils.getTimeMillis() + ".txt", "file at root", true, -1, userSessionId);
        catalogFileUtils.upload(sourceUri, fileQueryResult.getResult().get(0), null, adminSessionId, false, false, true, true);
    }

    @Test
    public void linkFileTest() throws IOException, CatalogException {

        java.io.File createdFile;
        URI sourceUri;
        createdFile = CatalogManagerTest.createDebugFile();
        sourceUri = createdFile.toURI();
        File file;
        URI fileUri;

        file = catalogManager.createFile(studyId, File.Format.PLAIN, File.Bioformat.NONE,
                "item." + TimeUtils.getTimeMillis() + ".txt", "file at root", true, -1, userSessionId).first();
        catalogFileUtils.link(file, true, sourceUri, false, userSessionId);

        file = catalogManager.getFile(file.getId(), userSessionId).first();
        fileUri = catalogManager.getFileUri(file);
        assertEquals(sourceUri, fileUri);
        assertTrue(createdFile.exists());

        catalogManager.renameFile(file.getId(), "newName", userSessionId);
        file = catalogManager.getFile(file.getId(), userSessionId).first();
        fileUri = catalogManager.getFileUri(file);
        assertEquals(sourceUri, fileUri);
        assertTrue(createdFile.exists());

        createdFile = CatalogManagerTest.createDebugFile();
        sourceUri = createdFile.toURI();
        catalogFileUtils.link(file, true, sourceUri, true, userSessionId);

        createdFile = CatalogManagerTest.createDebugFile();
        sourceUri = createdFile.toURI();
        thrown.expect(CatalogException.class);
        catalogFileUtils.link(file, true, sourceUri, false, userSessionId);
    }

    @Test
    public void linkFolderTest() throws Exception {
        Path directory = Paths.get("/tmp/linkFolderTest");
        IOUtils.deleteDirectory(directory);
        Files.createDirectory(directory);
        List<java.io.File> createdFiles = new LinkedList<>();
        createdFiles.add(CatalogManagerTest.createDebugFile(directory.resolve("file1.txt").toString()));
        createdFiles.add(CatalogManagerTest.createDebugFile(directory.resolve("file2.txt").toString()));
        Files.createDirectory(directory.resolve("dir"));
        createdFiles.add(CatalogManagerTest.createDebugFile(directory.resolve("dir").resolve("file2.txt").toString()));
        Files.createDirectory(directory.resolve("dir").resolve("subdir"));
        createdFiles.add(CatalogManagerTest.createDebugFile(directory.resolve("dir").resolve("subdir").resolve("file3.txt").toString()));
        URI sourceUri = directory.toUri();

        URI fileUri;

        //Create folder & link
        File folder = catalogManager.createFile(studyId, File.Type.FOLDER, null, null,
                "test", null, null, null, File.Status.STAGE, 0, -1, null, -1, null, null, true, null, userSessionId).first();
        folder = catalogFileUtils.link(folder, true, sourceUri, false, userSessionId);

        fileUri = catalogManager.getFileUri(folder);
        assertEquals(sourceUri, fileUri);
        for (java.io.File createdFile : createdFiles) {
            assertTrue(createdFile.exists());
        }
        for (File f : catalogManager.getAllFiles(studyId, new QueryOptions("path", "~" + folder.getPath()), userSessionId).getResult()) {
            assertEquals(File.Status.READY, f.getStatus());
            if (f.getType() != File.Type.FOLDER) {
                assertTrue(f.getAttributes().containsKey("checksum"));
                assertTrue(f.getUri() != null);
            }
        }
        assertTrue(!Paths.get(catalogManager.getStudyUri(studyId).resolve(folder.getName())).toFile().exists());

        //Delete folder. Should trash everything.
        catalogManager.deleteFolder(folder.getId(), userSessionId);
        for (java.io.File createdFile : createdFiles) {
            assertTrue(createdFile.exists());
        }
        for (File f : catalogManager.getAllFiles(studyId, new QueryOptions("path", "~" + folder.getPath()), userSessionId).getResult()) {
            assertEquals(File.Status.TRASHED, f.getStatus());
            if (f.getType() != File.Type.FOLDER) {
                assertTrue(f.getAttributes().containsKey("checksum"));
                assertTrue(f.getUri() != null);
            }
        }

        //Final delete folder. Should not remove original files.
        catalogFileUtils.delete(catalogManager.getFile(folder.getId(), userSessionId).first(), userSessionId);
        for (java.io.File createdFile : createdFiles) {
            assertTrue(createdFile.exists());
        }
        for (File f : catalogManager.getAllFiles(studyId, new QueryOptions("path", "~" + folder.getPath()), userSessionId).getResult()) {
            assertEquals(File.Status.DELETED, f.getStatus());
            if (f.getType() != File.Type.FOLDER) {
                assertTrue(f.getAttributes().containsKey("checksum"));
                assertTrue(f.getUri() != null);
            }
        }

//        catalogManager.renameFile(file.getId(), "newName", userSessionId);
//        file = catalogManager.getFile(file.getId(), userSessionId).first();
//        fileUri = catalogManager.getFileUri(file);
//        assertEquals(sourceUri, fileUri);
//        assertTrue(createdFile.exists());
//
//        createdFile = CatalogManagerTest.createDebugFile();
//        sourceUri = createdFile.toURI();
//        catalogFileUtils.link(file, true, sourceUri, true, userSessionId);
//
//        createdFile = CatalogManagerTest.createDebugFile();
//        sourceUri = createdFile.toURI();
//        thrown.expect(CatalogException.class);
//        catalogFileUtils.link(file, true, sourceUri, false, userSessionId);
    }

}
