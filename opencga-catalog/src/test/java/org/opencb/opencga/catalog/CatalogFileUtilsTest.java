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

package org.opencb.opencga.catalog;

import org.junit.Before;
import org.junit.Test;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.core.config.DataStoreServerAddress;
import org.opencb.datastore.mongodb.MongoDBConfiguration;
import org.opencb.opencga.catalog.beans.File;
import org.opencb.opencga.catalog.beans.Study;
import org.opencb.opencga.catalog.beans.User;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBAdaptor;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Properties;

/**
 * Created by jacobo on 28/01/15.
 */
public class CatalogFileUtilsTest {

    CatalogFileUtils catalogFileUtils;
    private int studyId;
    private String userSessionId;
    private String adminSessionId;
    private CatalogManager catalogManager;

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

        DataStoreServerAddress dataStoreServerAddress = new DataStoreServerAddress(
                properties.getProperty(CatalogManager.CATALOG_DB_HOSTS).split(",")[0], 27017);

        CatalogMongoDBAdaptor catalogDBAdaptor = new CatalogMongoDBAdaptor(Arrays.asList(dataStoreServerAddress),
                mongoDBConfiguration, properties.getProperty(CatalogManager.CATALOG_DB_DATABASE, ""));


        catalogManager = new CatalogManager(catalogDBAdaptor, properties);

        //Create ADMIN
        catalogManager.createUser("admin", "name", "mi@mail.com", "asdf", "", null);
        catalogDBAdaptor.modifyUser("admin", new ObjectMap("role", User.Role.ADMIN));

        //Create USER
        catalogManager.createUser("user", "name", "mi@mail.com", "asdf", "", null);
        userSessionId = catalogManager.login("user", "asdf", "--").getResult().get(0).getString("sessionId");
        adminSessionId = catalogManager.login("admin", "asdf", "--").getResult().get(0).getString("sessionId");
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

}
