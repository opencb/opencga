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

package org.opencb.opencga.catalog.db.mongodb;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.common.Status;
import org.opencb.commons.datastore.core.DataStoreServerAddress;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.catalog.auth.authentication.JwtManager;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.common.PasswordUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Admin;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileInternal;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.project.ProjectInternal;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.Group;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyInternal;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.models.user.UserInternal;
import org.opencb.opencga.core.models.user.UserQuota;
import org.opencb.opencga.core.models.user.UserStatus;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;

import static org.junit.Assert.assertTrue;

@Category(MediumTests.class)
public class MongoDBAdaptorTest extends GenericTest {

    public MongoDBAdaptorFactory catalogDBAdaptor;

    static User user1;
    static User user2;
    static User user3;
    static User user4;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    UserMongoDBAdaptor catalogUserDBAdaptor;
    ProjectMongoDBAdaptor catalogProjectDBAdaptor;
    FileMongoDBAdaptor catalogFileDBAdaptor;
    JobMongoDBAdaptor catalogJobDBAdaptor;
    StudyMongoDBAdaptor catalogStudyDBAdaptor;
    IndividualMongoDBAdaptor catalogIndividualDBAdaptor;
    PanelMongoDBAdaptor catalogPanelDBAdaptor;

    private Configuration configuration;

    @After
    public void after() {
        catalogDBAdaptor.close();
    }

    @Before
    public void before() throws IOException, CatalogException {
        configuration = Configuration.load(getClass().getResource("/configuration-test.yml")
                .openStream());

        DataStoreServerAddress dataStoreServerAddress = new DataStoreServerAddress(
                configuration.getCatalog().getDatabase().getHosts().get(0).split(":")[0], 27017);

        String database;
        if (StringUtils.isNotEmpty(configuration.getDatabasePrefix())) {
            if (!configuration.getDatabasePrefix().endsWith("_")) {
                database = configuration.getDatabasePrefix() + "_catalog";
            } else {
                database = configuration.getDatabasePrefix() + "catalog";
            }
        } else {
            database = "opencga_test_catalog";
        }

        /**
         * Database is cleared before each execution
         */
//        clearDB(dataStoreServerAddress, mongoCredentials);
        MongoDataStoreManager mongoManager = new MongoDataStoreManager(dataStoreServerAddress.getHost(), dataStoreServerAddress.getPort());
        MongoDataStore db = mongoManager.get(database);
        db.getDb().drop();
        mongoManager.close();

        configuration.setAdmin(new Admin()
                .setAlgorithm("HS256")
                .setSecretKey(PasswordUtils.getStrongRandomPassword(JwtManager.SECRET_KEY_MIN_LENGTH))
        );
        catalogDBAdaptor = new MongoDBAdaptorFactory(configuration);
        catalogUserDBAdaptor = catalogDBAdaptor.getCatalogUserDBAdaptor();
        catalogStudyDBAdaptor = catalogDBAdaptor.getCatalogStudyDBAdaptor();
        catalogProjectDBAdaptor = catalogDBAdaptor.getCatalogProjectDbAdaptor();
        catalogFileDBAdaptor = catalogDBAdaptor.getCatalogFileDBAdaptor();
        catalogJobDBAdaptor = catalogDBAdaptor.getCatalogJobDBAdaptor();
        catalogIndividualDBAdaptor = catalogDBAdaptor.getCatalogIndividualDBAdaptor();
        catalogPanelDBAdaptor = catalogDBAdaptor.getCatalogPanelDBAdaptor();
        initDefaultCatalogDB();
    }

    Sample getSample(long studyUid, String sampleId) throws CatalogDBException {
        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(SampleDBAdaptor.QueryParams.ID.key(), sampleId);
        return catalogDBAdaptor.getCatalogSampleDBAdaptor().get(query, QueryOptions.empty()).first();
    }

    Individual getIndividual(long studyUid, String individualId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query()
                .append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(IndividualDBAdaptor.QueryParams.ID.key(), individualId);
        return catalogIndividualDBAdaptor.get(query, QueryOptions.empty()).first();
    }

    Job getJob(long studyUid, String jobId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query()
                .append(JobDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(JobDBAdaptor.QueryParams.ID.key(), jobId);
        return catalogJobDBAdaptor.get(query, QueryOptions.empty()).first();
    }

    Project getProject(String userId, String projectId) throws CatalogDBException {
        Query query = new Query()
                .append(ProjectDBAdaptor.QueryParams.USER_ID.key(), userId)
                .append(ProjectDBAdaptor.QueryParams.ID.key(), projectId);
        return catalogProjectDBAdaptor.get(query, QueryOptions.empty()).first();
    }

    public void initDefaultCatalogDB() throws CatalogException {

        assertTrue(!catalogDBAdaptor.isCatalogDBReady());
        catalogDBAdaptor.createAllCollections(configuration);
        catalogDBAdaptor.initialiseMetaCollection(configuration.getAdmin());
        catalogDBAdaptor.getCatalogMetaDBAdaptor().createIndexes();
//        catalogDBAdaptor.initializeCatalogDB(new Admin());

        /**
         * Let's init the database with some basic data to perform each of the tests
         */
        user1 = new User("jcoll", "Jacobo Coll", "jcoll@ebi", "", null, new UserInternal(new UserStatus()), new UserQuota(-1, -1, -1, -1),
                Collections.emptyList(), Collections.emptyList(), new HashMap<>(), new LinkedList<>(), new HashMap<>());
        catalogUserDBAdaptor.insert(user1, "1234", null);
        catalogProjectDBAdaptor.insert(new Project("P1", "project", "", null, 1, ProjectInternal.init()), "jcoll", null);
        catalogProjectDBAdaptor.insert(new Project("P2", "project", "", null, 1, ProjectInternal.init()), "jcoll", null);
        catalogProjectDBAdaptor.insert(new Project("P3", "project", "", null, 1, ProjectInternal.init()), "jcoll", null);

        user2 = new User("jmmut", "Jose Miguel", "jmmut@ebi", "ACME", new UserInternal(new UserStatus()));
        catalogUserDBAdaptor.insert(user2, "1111", null);

        user3 = new User("imedina", "Nacho", "nacho@gmail", "SPAIN", null, new UserInternal(new UserStatus()), new UserQuota(-1, -1, -1, -1),
                Collections.emptyList(), Collections.emptyList(), new HashMap<>(), new LinkedList<>(), new HashMap<>());
        catalogUserDBAdaptor.insert(user3, "2222", null);
        catalogProjectDBAdaptor.insert(new Project("pr1", "90 GigaGenomes", TimeUtils.getTime(), TimeUtils.getTime(), "very long description", null, null,
                Collections.emptyList(), 1, ProjectInternal.init(), Collections.emptyMap()
        ), "imedina", null);
        catalogStudyDBAdaptor.insert(catalogProjectDBAdaptor.get(new Query(ProjectDBAdaptor.QueryParams.ID.key(), "pr1"), null).first(),
                new Study("name", "Study name", "ph1", TimeUtils.getTime(), TimeUtils.getTime(), "", null, null, null, 0,
                        Arrays.asList(new Group("@members", Collections.emptyList())), Arrays.asList(
                        new File("data/", File.Type.DIRECTORY, File.Format.PLAIN, File.Bioformat.NONE, "data/", null, "",
                                FileInternal.init(), 1000, 1),
                        new File("file.vcf", File.Type.FILE, File.Format.PLAIN, File.Bioformat.NONE, "data/file.vcf", null, "",
                                FileInternal.init(), 1000, 1)
                ), Collections.emptyList(), new LinkedList<>(), new LinkedList<>(), new LinkedList<>(),
                        new LinkedList<>(), Collections.emptyList(), new LinkedList<>(), new LinkedList<>(),
                        null, null, 1, new Status(), StudyInternal.init(), new LinkedList<>(), Collections.emptyMap()), null);

        user4 = new User("pfurio", "Pedro", "pfurio@blabla", "Organization", null, new UserInternal(new UserStatus()),
                new UserQuota(-1, -1, -1, -1), Collections.emptyList(), Collections.emptyList(), new HashMap<>(), new LinkedList<>(),
                new HashMap<>());

        catalogUserDBAdaptor.insert(user4, "pfuriopass", null);
        catalogProjectDBAdaptor.insert(new Project("pr", "lncRNAs", TimeUtils.getTime(), TimeUtils.getTime(), "My description", null, null,
                Collections.emptyList(), 1, ProjectInternal.init(), Collections.emptyMap()), "pfurio", null);
        catalogStudyDBAdaptor.insert(catalogProjectDBAdaptor.get(new Query(ProjectDBAdaptor.QueryParams.ID.key(), "pr"), null).first(),
                new Study("spongeScan", "spongeScan", "sponges", TimeUtils.getTime(), TimeUtils.getTime(), "", null, null, null,
                        0, Arrays.asList(new Group("@members", Collections.emptyList())), Arrays.asList(
                        new File("data/", File.Type.DIRECTORY, File.Format.UNKNOWN, File.Bioformat.NONE, "data/",
                                null, "Description", FileInternal.init(), 10, 1),
                        new File("file1.txt", File.Type.FILE, File.Format.COMMA_SEPARATED_VALUES,
                                File.Bioformat.NONE, "data/file1.txt", null, "Description", FileInternal.init(), 100, 1),
                        new File("file2.txt", File.Type.FILE, File.Format.COMMA_SEPARATED_VALUES,
                                File.Bioformat.NONE, "data/file2.txt", null, "Description2", FileInternal.init(), 100, 1),
                        new File("alignment.bam", File.Type.FILE, File.Format.BAM, File.Bioformat.ALIGNMENT,
                                "data/alignment.bam", null, "Tophat alignment file", FileInternal.init(), 5000, 1)
                ), Collections.emptyList(), new LinkedList<>(), new LinkedList<>(), new LinkedList<>(), new LinkedList<>(),
                        Collections.emptyList(), new LinkedList<>(), new LinkedList<>(), null, null, 1, new Status(),
                        StudyInternal.init(), new LinkedList<>(), Collections.emptyMap()), null);
        catalogStudyDBAdaptor.insert(catalogProjectDBAdaptor.get(new Query(ProjectDBAdaptor.QueryParams.ID.key(), "pr"), null).first(),
                new Study("mineco", "MINECO", "mineco", TimeUtils.getTime(), TimeUtils.getTime(), "", null, null, null, 0,
                        Arrays.asList(new Group("@members", Collections.emptyList())), Arrays.asList(
                        new File("data/", File.Type.DIRECTORY, File.Format.UNKNOWN, File.Bioformat.NONE, "data/",
                                null, "Description", FileInternal.init(), 10, 1),
                        new File("m_file1.txt", File.Type.FILE, File.Format.COMMA_SEPARATED_VALUES,
                                File.Bioformat.NONE, "data/file1.txt", null, "Description", FileInternal.init(), 100, 1),
                        new File("m_alignment.bam", File.Type.FILE, File.Format.BAM, File.Bioformat.ALIGNMENT,
                                "data/alignment.bam", null, "Tophat alignment file", FileInternal.init(), 5000, 1)
                ), Collections.emptyList(), new LinkedList<>(), new LinkedList<>(), new LinkedList<>(), new LinkedList<>(),
                        Collections.emptyList(), new LinkedList<>(), new LinkedList<>(), null, null, 1, new Status(),
                        StudyInternal.init(), new LinkedList<>(), Collections.emptyMap()), null);

        QueryOptions options = new QueryOptions("includeStudies", true);
        options.put("includeFiles", true);
        options.put("includeJobs", true);
        options.put("includeSamples", true);
        user1 = catalogUserDBAdaptor.get(MongoDBAdaptorTest.user1.getId(), options).first();
        user2 = catalogUserDBAdaptor.get(MongoDBAdaptorTest.user2.getId(), options).first();
        user3 = catalogUserDBAdaptor.get(MongoDBAdaptorTest.user3.getId(), options).first();
        user4 = catalogUserDBAdaptor.get(MongoDBAdaptorTest.user4.getId(), options).first();

    }

    public Configuration getConfiguration() {
        return configuration;
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
