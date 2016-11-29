/*
 * Copyright 2015-2016 OpenCB
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

package org.opencb.opencga.storage.core.local.variant;

import org.junit.Before;
import org.junit.Rule;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.catalog.config.Policies;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogFileUtils;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.utils.FileMetadataReader;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.config.DatabaseCredentials;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.config.StorageEngineConfiguration;
import org.opencb.opencga.storage.core.config.StorageEtlConfiguration;
import org.opencb.opencga.storage.core.local.OpenCGATestExternalResource;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.dummy.DummyStudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Collections;

import static org.opencb.biodata.models.variant.StudyEntry.DEFAULT_COHORT;
import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.DB_NAME;
import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.getResourceUri;

/**
 * Created on 05/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class AbstractVariantStorageOperationTest {
    protected CatalogManager catalogManager;

    protected String sessionId;

    protected final String userId = "user";

    protected long projectId;
    protected long studyId;
    protected long studyId2;
    protected long outputId;
    protected long outputId2;

    protected FileMetadataReader fileMetadataReader;
    protected CatalogFileUtils catalogFileUtils;
    protected org.opencb.opencga.storage.core.local.variant.VariantStorageManager variantManager;

    protected final String dbName = DB_NAME;
    protected static final String STORAGE_ENGINE_MOCKUP = DummyVariantStorageManager.STORAGE_ENGINE_ID;
    protected static final String STORAGE_ENGINE_MONGODB = "mongodb";
    protected static final String STORAGE_ENGINE_HADOOP = "hadoop";
    private Logger logger = LoggerFactory.getLogger(AbstractVariantStorageOperationTest.class);



    @Rule
    public OpenCGATestExternalResource opencga = new OpenCGATestExternalResource(getStorageEngine().equals(STORAGE_ENGINE_HADOOP));

    @Before
    public final void setUpAbstract() throws Exception {
        catalogManager = opencga.getCatalogManager();
        StorageConfiguration storageConfiguration = opencga.getStorageConfiguration();
        storageConfiguration.setDefaultStorageEngineId(STORAGE_ENGINE_MOCKUP);
        storageConfiguration.getStorageEngines().add(new StorageEngineConfiguration(
                STORAGE_ENGINE_MOCKUP,
                new StorageEtlConfiguration(),
                new StorageEtlConfiguration(DummyVariantStorageManager.class.getName(), new ObjectMap(), new DatabaseCredentials()),
                new ObjectMap()
        ));
        StorageManagerFactory.configure(storageConfiguration);
        DummyStudyConfigurationManager.clear();

        variantManager = new org.opencb.opencga.storage.core.local.variant.VariantStorageManager(catalogManager, storageConfiguration);
        clearDB(dbName);

        fileMetadataReader = FileMetadataReader.get(catalogManager);
        catalogFileUtils = new CatalogFileUtils(catalogManager);
        Policies policies = new Policies();
        policies.setUserCreation(Policies.UserCreation.ALWAYS);

        User user = catalogManager.createUser(userId, "User", "user@email.org", "user", "ACME", null, null).first();
        sessionId = catalogManager.login(userId, "user", "localhost").first().getString("sessionId");
        projectId = catalogManager.createProject("p1", "p1", "Project 1", "ACME", null, sessionId).first().getId();
        studyId = catalogManager.createStudy(projectId, "s1", "s1", Study.Type.CASE_CONTROL, null, "Study 1", null,
                null, null, null, Collections.singletonMap(File.Bioformat.VARIANT, new DataStore(getStorageEngine(), dbName)), null,
                Collections.singletonMap(VariantStorageManager.Options.AGGREGATED_TYPE.key(), getAggregation()),
                null, sessionId).first().getId();
        outputId = catalogManager.createFolder(studyId, Paths.get("data", "index"), true, null, sessionId).first().getId();

        studyId2 = catalogManager.createStudy(projectId, "s2", "s2", Study.Type.CASE_CONTROL, null, "Study 2", null,
                null, null, null, Collections.singletonMap(File.Bioformat.VARIANT, new DataStore(getStorageEngine(), dbName)), null,
                Collections.singletonMap(VariantStorageManager.Options.AGGREGATED_TYPE.key(), getAggregation()),
                null, sessionId).first().getId();
        outputId2 = catalogManager.createFolder(studyId2, Paths.get("data", "index"), true, null, sessionId).first().getId();

    }

    protected String getStorageEngine() {
        return STORAGE_ENGINE_MOCKUP;
    }

    protected abstract VariantSource.Aggregation getAggregation();

    private void clearDB(String dbName) {
        logger.info("Cleaning MongoDB {}" , dbName);
        MongoDataStoreManager mongoManager = new MongoDataStoreManager("localhost", 27017);
        MongoDataStore mongoDataStore = mongoManager.get(dbName);
        mongoManager.drop(dbName);
    }

    protected File create(String resourceName) throws IOException, CatalogException {
        return create(studyId, getResourceUri(resourceName));
    }

    protected File create(long studyId, URI uri) throws IOException, CatalogException {
        File file;
        file = fileMetadataReader.create(studyId, uri, "data/vcfs/", "", true, null, sessionId).first();
//        File.Format format = FormatDetector.detect(uri);
//        File.Bioformat bioformat = BioformatDetector.detect(uri);
//        file = catalogManager.createFile(studyId, format, bioformat, "data/vcfs/", "", true, -1, sessionId).first();
        catalogFileUtils.upload(uri, file, null, sessionId, false, false, true, false, Long.MAX_VALUE);
        return catalogManager.getFile(file.getId(), sessionId).first();
    }

    protected Cohort getDefaultCohort(long studyId) throws CatalogException {
        return catalogManager.getAllCohorts(studyId, new Query(CohortDBAdaptor.QueryParams.NAME.key(), DEFAULT_COHORT),
                new QueryOptions(), sessionId).first();
    }
}
