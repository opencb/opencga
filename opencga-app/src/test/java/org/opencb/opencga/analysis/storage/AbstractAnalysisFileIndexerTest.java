package org.opencb.opencga.analysis.storage;

import org.junit.Before;
import org.junit.Rule;
import org.opencb.biodata.models.variant.VariantSource;
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
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

import static org.opencb.biodata.models.variant.StudyEntry.DEFAULT_COHORT;
import static org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils.DB_NAME;
import static org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils.getResourceUri;

/**
 * Created on 05/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class AbstractAnalysisFileIndexerTest {
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

    protected final String dbName = DB_NAME;
    protected static final String STORAGE_ENGINE_MONGODB = "mongodb";
    protected static final String STORAGE_ENGINE_HADOOP = "hadoop";
    protected static final String STORAGE_ENGINE = STORAGE_ENGINE_MONGODB;
    private Logger logger = LoggerFactory.getLogger(AbstractAnalysisFileIndexerTest.class);

    @Rule
    public OpenCGATestExternalResource opencga = new OpenCGATestExternalResource(getStorageEngine().equals(STORAGE_ENGINE_HADOOP));

    @Before
    public final void setUpAbstract() throws CatalogException, IOException {
        Path openCGA = opencga.getOpencgaHome();

        catalogManager = opencga.getCatalogManager();
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
        return STORAGE_ENGINE_MONGODB;
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
        catalogFileUtils.upload(uri, file, null, sessionId, false, false, true, false, Long.MAX_VALUE);
        return catalogManager.getFile(file.getId(), sessionId).first();
    }

    protected Cohort getDefaultCohort(long studyId) throws CatalogException {
        return catalogManager.getAllCohorts(studyId, new Query(CohortDBAdaptor.QueryParams.NAME.key(), DEFAULT_COHORT),
                new QueryOptions(), sessionId).first();
    }
}
