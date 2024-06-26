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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManagerTest;
import org.opencb.opencga.catalog.managers.FileUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileCreateParams;
import org.opencb.opencga.core.models.file.FileStatus;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import java.io.FileOutputStream;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Created by jacobo on 28/01/15.
 */
@Category(MediumTests.class)
public class CatalogFileUtilsTest extends AbstractManagerTest {
//
//    @Rule
//    public ExpectedException thrown = ExpectedException.none();
    FileUtils catalogFileUtils;
//    private long studyUid;
//    private String studyFqn;
//    private String ownerToken;
//    //    private String adminSessionId;
//    private CatalogManager catalogManager;
//    private String organizationId = "test";
//
//    private static final QueryOptions INCLUDE_RESULT = new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true);

    @Before
    public void before() throws Exception {
        setUp();
//        Configuration configuration = Configuration.load(getClass().getResource("/configuration-test.yml")
//                .openStream());
//        MongoDBConfiguration mongoDBConfiguration = MongoDBConfiguration.builder()
//                .add("username", configuration.getCatalog().getDatabase().getUser())
//                .add("password", configuration.getCatalog().getDatabase().getPassword())
//                .add("authenticationDatabase", configuration.getCatalog().getDatabase().getOptions().get("authenticationDatabase"))
//                .build();
//
//        CatalogManagerExternalResource.clearCatalog(configuration);
//        catalogManager = new CatalogManager(configuration);
//        catalogManager.installCatalogDB("HS256", configuration.getAdmin().getSecretKey(), TestParamConstants.ADMIN_PASSWORD, "opencga@admin.com", true);
//
//        String opencgaToken = catalogManager.getUserManager().loginAsAdmin(TestParamConstants.ADMIN_PASSWORD).getToken();
//
//        //Create USER
//        catalogManager.getUserManager().create(organizationId, "user", "name", "mi@mail.com", TestParamConstants.PASSWORD, "", null, Account.AccountType.FULL, opencgaToken);
//        ownerToken = catalogManager.getUserManager().login(organizationId, "user", TestParamConstants.PASSWORD).getToken();
////        adminSessionId = catalogManager.login("admin", "admin", "--").getResults().get(0).getString("sessionId");
//        String projectId = catalogManager.getProjectManager().create(organizationId, "proj", "proj", "", "Homo sapiens",
//                null, "GRCh38", INCLUDE_RESULT, ownerToken).getResults().get(0).getId();
//        Study study = catalogManager.getStudyManager().create(projectId, "std", "std", "std", "", null, null, null, null, INCLUDE_RESULT,
//                ownerToken).getResults().get(0);
//        studyUid = study.getUid();
//        studyFqn = study.getFqn();

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
                true, ownerToken).first();
        returnedFile = catalogFileUtils.checkFile(studyFqn, file, true, ownerToken);

        assertSame("Should not modify the status, so should return the same file.", file, returnedFile);
        assertEquals(InternalStatus.READY, file.getInternal().getStatus().getId());

//        /** Check READY and existing file **/
//        catalogFileUtils.upload(sourceUri, file, null, ownerToken, false, false, false, true);
//        fileUri = catalogManager.getFileManager().getUri(file);
//        file = catalogManager.getFileManager().get(studyFqn, file.getPath(), null, ownerToken).first();
//        returnedFile = catalogFileUtils.checkFile(studyFqn, file, true, ownerToken);
//
//        assertSame("Should not modify the READY and existing file, so should return the same file.", file, returnedFile);


        /** Check READY and missing file **/
        assertTrue(new java.io.File(file.getUri()).delete());
        returnedFile = catalogFileUtils.checkFile(studyFqn, file, true, ownerToken);

        assertNotSame(file, returnedFile);
        assertEquals(FileStatus.MISSING, returnedFile.getInternal().getStatus().getId());

        /** Check MISSING file still missing **/
        file = catalogManager.getFileManager().get(studyFqn, file.getPath(), null, ownerToken).first();
        returnedFile = catalogFileUtils.checkFile(studyFqn, file, true, ownerToken);

        assertEquals("Should not modify the still MISSING file, so should return the same file.", file.getInternal().getStatus().getId(),
                returnedFile.getInternal().getStatus().getId());
        //assertSame("Should not modify the still MISSING file, so should return the same file.", file, returnedFile);

        /** Check MISSING file with found file **/
        FileOutputStream os = new FileOutputStream(file.getUri().getPath());
        os.write(RandomStringUtils.randomAlphanumeric(1000).getBytes());
        os.write('\n');
        os.close();
        returnedFile = catalogFileUtils.checkFile(studyFqn, file, true, ownerToken);

        assertNotSame(file, returnedFile);
        assertEquals(FileStatus.READY, returnedFile.getInternal().getStatus().getId());

//        /** Check TRASHED file with found file **/
//        FileUpdateParams updateParams = new FileUpdateParams()
//                .setStatus(new File.FileStatus(File.FileStatus.PENDING_DELETE));
//        catalogManager.getFileManager().update(studyFqn, new Query(FileDBAdaptor.QueryParams.UID.key(), file.getUid()), updateParams,
//                QueryOptions.empty(), ownerToken);
//        catalogManager.getFileManager().delete(studyFqn, new Query(FileDBAdaptor.QueryParams.UID.key(), file.getUid()), null,
//                ownerToken);
//
//        Query query = new Query()
//                .append(FileDBAdaptor.QueryParams.UID.key(), file.getUid());
//        DataResult<File> fileDataResult = catalogManager.getFileManager().search(studyFqn, query, QueryOptions.empty(), ownerToken);
//
//        file = fileDataResult.first();
//        returnedFile = catalogFileUtils.checkFile(studyFqn, file, true, ownerToken);
//
//        assertSame(file, returnedFile);
//        assertEquals(File.FileStatus.TRASHED, returnedFile.getStatus().getName());
//
//
//        /** Check TRASHED file with missing file **/
////        catalogManager.getFileManager().delete(Long.toString(file.getId()), null, ownerToken);
//        assertTrue(Paths.get(file.getUri()).toFile().delete());
//
//        returnedFile = catalogFileUtils.checkFile(studyFqn, file, true, ownerToken);
//
////        assertNotSame(file, returnedFile);
//        assertEquals(File.FileStatus.TRASHED, returnedFile.getStatus().getName());
    }

}
