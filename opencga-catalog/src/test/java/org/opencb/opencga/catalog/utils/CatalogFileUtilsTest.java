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

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBConfiguration;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.catalog.managers.FileUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileCreateParams;
import org.opencb.opencga.core.models.file.FileStatus;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.Account;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;

import static org.junit.Assert.*;

/**
 * Created by jacobo on 28/01/15.
 */
public class CatalogFileUtilsTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();
    FileUtils catalogFileUtils;
    private long studyUid;
    private String studyFqn;
    private String userSessionId;
    //    private String adminSessionId;
    private CatalogManager catalogManager;

    private static final QueryOptions INCLUDE_RESULT = new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true);

    @Before
    public void before() throws CatalogException, IOException, URISyntaxException {
        Configuration configuration = Configuration.load(getClass().getResource("/configuration-test.yml")
                .openStream());
        MongoDBConfiguration mongoDBConfiguration = MongoDBConfiguration.builder()
                .add("username", configuration.getCatalog().getDatabase().getUser())
                .add("password", configuration.getCatalog().getDatabase().getPassword())
                .add("authenticationDatabase", configuration.getCatalog().getDatabase().getOptions().get("authenticationDatabase"))
                .build();

        CatalogManagerExternalResource.clearCatalog(configuration);
        catalogManager = new CatalogManager(configuration);
        catalogManager.installCatalogDB(configuration.getAdmin().getSecretKey(), TestParamConstants.ADMIN_PASSWORD, "opencga@admin.com", "", true, false);

        //Create USER
        catalogManager.getUserManager().create("user", "name", "mi@mail.com", TestParamConstants.PASSWORD, "", null, Account.AccountType.FULL, null);
        userSessionId = catalogManager.getUserManager().login("user", TestParamConstants.PASSWORD).getToken();
//        adminSessionId = catalogManager.login("admin", "admin", "--").getResults().get(0).getString("sessionId");
        String projectId = catalogManager.getProjectManager().create("proj", "proj", "", "Homo sapiens",
                null, "GRCh38", INCLUDE_RESULT, userSessionId).getResults().get(0).getId();
        Study study = catalogManager.getStudyManager().create(projectId, "std", "std", "std", "", null, null, null, null, INCLUDE_RESULT,
                userSessionId).getResults().get(0);
        studyUid = study.getUid();
        studyFqn = study.getFqn();

        catalogFileUtils = new FileUtils(catalogManager);
    }

    @Test
    public void checkFileTest() throws CatalogException, IOException {
        File file;
        File returnedFile;

        file = catalogManager.getFileManager().create(studyFqn,
                new FileCreateParams()
                        .setType(File.Type.FILE)
                        .setFormat(File.Format.PLAIN)
                        .setBioformat(File.Bioformat.NONE)
                        .setPath("item." + TimeUtils.getTimeMillis() + ".txt")
                        .setDescription("file at root")
                        .setContent(RandomStringUtils.randomAlphanumeric(100)),
                true, userSessionId).first();
        returnedFile = catalogFileUtils.checkFile(studyFqn, file, true, userSessionId);

        assertSame("Should not modify the status, so should return the same file.", file, returnedFile);
        assertEquals(InternalStatus.READY, file.getInternal().getStatus().getId());

//        /** Check READY and existing file **/
//        catalogFileUtils.upload(sourceUri, file, null, userSessionId, false, false, false, true);
//        fileUri = catalogManager.getFileManager().getUri(file);
//        file = catalogManager.getFileManager().get(studyFqn, file.getPath(), null, userSessionId).first();
//        returnedFile = catalogFileUtils.checkFile(studyFqn, file, true, userSessionId);
//
//        assertSame("Should not modify the READY and existing file, so should return the same file.", file, returnedFile);


        /** Check READY and missing file **/
        assertTrue(new java.io.File(file.getUri()).delete());
        returnedFile = catalogFileUtils.checkFile(studyFqn, file, true, userSessionId);

        assertNotSame(file, returnedFile);
        assertEquals(FileStatus.MISSING, returnedFile.getInternal().getStatus().getId());

        /** Check MISSING file still missing **/
        file = catalogManager.getFileManager().get(studyFqn, file.getPath(), null, userSessionId).first();
        returnedFile = catalogFileUtils.checkFile(studyFqn, file, true, userSessionId);

        assertEquals("Should not modify the still MISSING file, so should return the same file.", file.getInternal().getStatus().getId(),
                returnedFile.getInternal().getStatus().getId());
        //assertSame("Should not modify the still MISSING file, so should return the same file.", file, returnedFile);

        /** Check MISSING file with found file **/
        FileOutputStream os = new FileOutputStream(file.getUri().getPath());
        os.write(RandomStringUtils.randomAlphanumeric(1000).getBytes());
        os.write('\n');
        os.close();
        returnedFile = catalogFileUtils.checkFile(studyFqn, file, true, userSessionId);

        assertNotSame(file, returnedFile);
        assertEquals(FileStatus.READY, returnedFile.getInternal().getStatus().getId());

//        /** Check TRASHED file with found file **/
//        FileUpdateParams updateParams = new FileUpdateParams()
//                .setStatus(new File.FileStatus(File.FileStatus.PENDING_DELETE));
//        catalogManager.getFileManager().update(studyFqn, new Query(FileDBAdaptor.QueryParams.UID.key(), file.getUid()), updateParams,
//                QueryOptions.empty(), userSessionId);
//        catalogManager.getFileManager().delete(studyFqn, new Query(FileDBAdaptor.QueryParams.UID.key(), file.getUid()), null,
//                userSessionId);
//
//        Query query = new Query()
//                .append(FileDBAdaptor.QueryParams.UID.key(), file.getUid());
//        DataResult<File> fileDataResult = catalogManager.getFileManager().search(studyFqn, query, QueryOptions.empty(), userSessionId);
//
//        file = fileDataResult.first();
//        returnedFile = catalogFileUtils.checkFile(studyFqn, file, true, userSessionId);
//
//        assertSame(file, returnedFile);
//        assertEquals(File.FileStatus.TRASHED, returnedFile.getStatus().getName());
//
//
//        /** Check TRASHED file with missing file **/
////        catalogManager.getFileManager().delete(Long.toString(file.getId()), null, userSessionId);
//        assertTrue(Paths.get(file.getUri()).toFile().delete());
//
//        returnedFile = catalogFileUtils.checkFile(studyFqn, file, true, userSessionId);
//
////        assertNotSame(file, returnedFile);
//        assertEquals(File.FileStatus.TRASHED, returnedFile.getStatus().getName());
    }

}
