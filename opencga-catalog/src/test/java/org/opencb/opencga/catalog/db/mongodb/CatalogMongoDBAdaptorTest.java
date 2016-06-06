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

package org.opencb.opencga.catalog.db.mongodb;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.DataStoreServerAddress;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBConfiguration;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CatalogMongoDBAdaptorTest extends GenericTest {

    static CatalogMongoDBAdaptorFactory catalogDBAdaptor;

    static User user1;
    static User user2;
    static User user3;
    static User user4;

    @Rule
    public ExpectedException thrown = ExpectedException.none();
    CatalogUserDBAdaptor catalogUserDBAdaptor;
    CatalogProjectDBAdaptor catalogProjectDBAdaptor;
    CatalogFileDBAdaptor catalogFileDBAdaptor;
    CatalogJobDBAdaptor catalogJobDBAdaptor;
    CatalogStudyDBAdaptor catalogStudyDBAdaptor;
    CatalogIndividualDBAdaptor catalogIndividualDBAdaptor;
    CatalogPanelDBAdaptor catalogPanelDBAdaptor;

    private CatalogConfiguration catalogConfiguration;

    @AfterClass
    public static void afterClass() {
        catalogDBAdaptor.close();
    }

    @Before
    public void before() throws IOException, CatalogException {
        catalogConfiguration = CatalogConfiguration.load(getClass().getResource("/catalog-configuration-test.yml")
                .openStream());

        DataStoreServerAddress dataStoreServerAddress = new DataStoreServerAddress(
                catalogConfiguration.getDatabase().getHosts().get(0).split(":")[0], 27017);

        MongoDBConfiguration mongoDBConfiguration = MongoDBConfiguration.builder()
                .add("username", catalogConfiguration.getDatabase().getUser())
                .add("password", catalogConfiguration.getDatabase().getPassword())
                .add("authenticationDatabase", catalogConfiguration.getDatabase().getOptions().get("authenticationDatabase"))
                .build();

        String database = catalogConfiguration.getDatabase().getDatabase();
        /**
         * Database is cleared before each execution
         */
//        clearDB(dataStoreServerAddress, mongoCredentials);
        MongoDataStoreManager mongoManager = new MongoDataStoreManager(dataStoreServerAddress.getHost(), dataStoreServerAddress.getPort());
        MongoDataStore db = mongoManager.get(database);
        db.getDb().drop();

        catalogDBAdaptor = new CatalogMongoDBAdaptorFactory(Collections.singletonList(dataStoreServerAddress), mongoDBConfiguration,
                database);
        catalogUserDBAdaptor = catalogDBAdaptor.getCatalogUserDBAdaptor();
        catalogStudyDBAdaptor = catalogDBAdaptor.getCatalogStudyDBAdaptor();
        catalogProjectDBAdaptor = catalogDBAdaptor.getCatalogProjectDbAdaptor();
        catalogFileDBAdaptor = catalogDBAdaptor.getCatalogFileDBAdaptor();
        catalogJobDBAdaptor = catalogDBAdaptor.getCatalogJobDBAdaptor();
        catalogIndividualDBAdaptor = catalogDBAdaptor.getCatalogIndividualDBAdaptor();
        catalogPanelDBAdaptor = catalogDBAdaptor.getCatalogPanelDBAdaptor();
        initDefaultCatalogDB();
    }

    public void initDefaultCatalogDB() throws CatalogException {

        assertTrue(!catalogDBAdaptor.isCatalogDBReady());
        catalogDBAdaptor.installCatalogDB(catalogConfiguration);
//        catalogDBAdaptor.initializeCatalogDB(new Admin());

        /**
         * Let's init the database with some basic data to perform each of the tests
         */
        user1 = new User("jcoll", "Jacobo Coll", "jcoll@ebi", "1234", "", User.Role.USER, new User.UserStatus(), "", 100, 1000,
                Arrays.<Project>asList(new Project("project", "P1", "", new Status(), ""), new Project("project", "P2", "", new Status(),
                        ""), new Project("project", "P3", "", new Status(), "")),
                Collections.<Tool>emptyList(), Collections.<Session>emptyList(), Collections.<String, Object>emptyMap(), Collections
                .<String, Object>emptyMap());
        QueryResult createUser = catalogUserDBAdaptor.insertUser(user1, null);
        assertNotNull(createUser.getResult());

        user2 = new User("jmmut", "Jose Miguel", "jmmut@ebi", "1111", "ACME", User.Role.USER, new User.UserStatus());
        createUser = catalogUserDBAdaptor.insertUser(user2, null);
        assertNotNull(createUser.getResult());

        user3 = new User("imedina", "Nacho", "nacho@gmail", "2222", "SPAIN", User.Role.USER, new User.UserStatus(), "", 1222, 122222,
                Arrays.asList(new Project(-1, "90 GigaGenomes", "90G", "today", "very long description", "Spain", new Status(), "", 0,
                        Arrays.asList(new Study(-1, "Study name", "ph1", Study.Type.CONTROL_SET, "", "", new Status(), "", 0, "", null,
                                        null, Collections.<Experiment>emptyList(),
                                        Arrays.asList(
                                                new File("data/", File.Type.FOLDER, File.Format.PLAIN, File.Bioformat.NONE, "data/",
                                                        null, null, "", new File.FileStatus(File.FileStatus.READY), 1000),
                                                new File("file.vcf", File.Type.FILE, File.Format.PLAIN, File.Bioformat.NONE, "data/file" +
                                                        ".vcf", null, null, "", new File.FileStatus(File.FileStatus.READY), 1000)
                                        ), Collections.<Job>emptyList(), new LinkedList<Sample>(), new LinkedList<Dataset>(), new
                                        LinkedList<Cohort>(), Collections.emptyList(), new LinkedList<VariableSet>(), null, null, Collections.<String,
                                        Object>emptyMap(),
                                        Collections.<String, Object>emptyMap()
                                )
                        ), Collections.<String, Object>emptyMap())
                ),
                Collections.<Tool>emptyList(), Collections.<Session>emptyList(),
                Collections.<String, Object>emptyMap(), Collections.<String, Object>emptyMap());
        createUser = catalogUserDBAdaptor.insertUser(user3, null);
        assertNotNull(createUser.getResult());

        user4 = new User("pfurio", "Pedro", "pfurio@blabla", "pfuriopass", "Organization", User.Role.USER, new User.UserStatus(), "", 0, 50000,
                Arrays.asList(new Project(-1, "lncRNAs", "lncRNAs", "today", "My description", "My org", new Status(), "", 0, Arrays.asList(
                                new Study(-1, "spongeScan", "sponges", Study.Type.COLLECTION, "", "", new Status(), "", 0, "", null, null,
                                        null, Arrays.asList(
                                                new File("data/", File.Type.FOLDER, File.Format.UNKNOWN, File.Bioformat.NONE, "data/", null,
                                                        null, "Description", new File.FileStatus(File.FileStatus.READY), 10),
                                                new File("file1.txt", File.Type.FILE, File.Format.COMMA_SEPARATED_VALUES,
                                                        File.Bioformat.NONE, "data/file1.txt", null, null, "Description",
                                                        new File.FileStatus(File.FileStatus.READY), 100),
                                                new File("file2.txt", File.Type.FILE, File.Format.COMMA_SEPARATED_VALUES,
                                                        File.Bioformat.NONE, "data/file2.txt", null, null, "Description2",
                                                        new File.FileStatus(File.FileStatus.READY), 100),
                                                new File("alignment.bam", File.Type.FILE, File.Format.BAM, File.Bioformat.ALIGNMENT,
                                                        "data/alignment.bam", null, null, "Tophat alignment file",
                                                        new File.FileStatus(File.FileStatus.READY), 5000)
                                                ), Collections.<Job>emptyList(), new LinkedList<>(), new LinkedList<>(), new
                                        LinkedList<>(), Collections.emptyList(), new LinkedList<>(), null, null, Collections.emptyMap(),
                                        Collections.emptyMap()),
                                new Study(-1, "MINECO", "mineco", Study.Type.COLLECTION, "", "", new Status(), "", 0, "", null, null, null,
                                        Arrays.asList(
                                                new File("data/", File.Type.FOLDER, File.Format.UNKNOWN, File.Bioformat.NONE, "data/", null,
                                                        null, "Description", new File.FileStatus(File.FileStatus.READY), 10),
                                                new File("m_file1.txt", File.Type.FILE, File.Format.COMMA_SEPARATED_VALUES,
                                                        File.Bioformat.NONE, "data/file1.txt", null, null, "Description",
                                                        new File.FileStatus(File.FileStatus.READY), 100),
                                                new File("m_alignment.bam", File.Type.FILE, File.Format.BAM, File.Bioformat.ALIGNMENT,
                                                        "data/alignment.bam", null, null, "Tophat alignment file",
                                                        new File.FileStatus(File.FileStatus.READY), 5000)
                                        ), Collections.<Job>emptyList(), new LinkedList<>(), new LinkedList<>(), new LinkedList<>(), Collections.emptyList(), new LinkedList<>(), null, null, Collections.emptyMap(), Collections.emptyMap())
                        ), Collections.emptyMap())
                ),
                Collections.<Tool>emptyList(), Collections.<Session>emptyList(),
                Collections.<String, Object>emptyMap(), Collections.<String, Object>emptyMap());

        createUser = catalogUserDBAdaptor.insertUser(user4, null);
        assertNotNull(createUser.getResult());

        QueryOptions options = new QueryOptions("includeStudies", true);
        options.put("includeFiles", true);
        options.put("includeJobs", true);
        options.put("includeSamples", true);
        user1 = catalogUserDBAdaptor.getUser(CatalogMongoDBAdaptorTest.user1.getId(), options, null).first();
        user2 = catalogUserDBAdaptor.getUser(CatalogMongoDBAdaptorTest.user2.getId(), options, null).first();
        user3 = catalogUserDBAdaptor.getUser(CatalogMongoDBAdaptorTest.user3.getId(), options, null).first();
        user4 = catalogUserDBAdaptor.getUser(CatalogMongoDBAdaptorTest.user4.getId(), options, null).first();

    }

    /*
    @Test
    public void initializeInitializedDB() throws CatalogDBException {
        assertTrue(catalogDBAdaptor.isCatalogDBReady());
        thrown.expect(CatalogDBException.class);
        catalogDBAdaptor.initializeCatalogDB();
    }
*/
}
