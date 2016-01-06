package org.opencb.opencga.catalog.db.mongodb;

import org.junit.Before;
import org.junit.Test;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.config.DataStoreServerAddress;
import org.opencb.datastore.mongodb.MongoDBConfiguration;
import org.opencb.datastore.mongodb.MongoDataStore;
import org.opencb.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.db.api.CatalogAuditDBAdaptor;

import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;

/**
 * Created on 18/08/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogMongoAuditDBAdaptorTest {

    private CatalogAuditDBAdaptor auditDbAdaptor;

    @Before
    public void beforeClass() throws Exception {
        InputStream is = CatalogMongoDBAdaptorTest.class.getClassLoader().getResourceAsStream("catalog.properties");
        Properties properties = new Properties();
        properties.load(is);

        DataStoreServerAddress dataStoreServerAddress = new DataStoreServerAddress(
                properties.getProperty(CatalogManager.CATALOG_DB_HOSTS).split(",")[0], 27017);

        MongoDBConfiguration mongoDBConfiguration = MongoDBConfiguration.builder()
                .add("username", properties.getProperty(CatalogManager.CATALOG_DB_USER, ""))
                .add("password", properties.getProperty(CatalogManager.CATALOG_DB_PASSWORD, ""))
                .add("authenticationDatabase", properties.getProperty(CatalogManager.CATALOG_DB_AUTHENTICATION_DB, ""))
                .build();

        String database = properties.getProperty(CatalogManager.CATALOG_DB_DATABASE);
        /**
         * Database is cleared before each execution
         */
//        clearDB(dataStoreServerAddress, mongoCredentials);
        MongoDataStoreManager mongoManager = new MongoDataStoreManager(dataStoreServerAddress.getHost(), dataStoreServerAddress.getPort());
        MongoDataStore db = mongoManager.get(database);
        db.getDb().dropDatabase();


        auditDbAdaptor = new CatalogMongoDBAdaptor(Collections.singletonList(dataStoreServerAddress), mongoDBConfiguration, database)
                .getCatalogAuditDbAdaptor();
    }

    @Test
    public void testInsertAuditRecord() throws Exception {
        auditDbAdaptor.insertAuditRecord(new AuditRecord(23, AuditRecord.Resource.sample, "update", new ObjectMap("name", "HG0001"), new
                ObjectMap("name", "HG0002"), System.currentTimeMillis(), "admin", "", new ObjectMap()));
        auditDbAdaptor.insertAuditRecord(new AuditRecord(23, AuditRecord.Resource.sample, "update", new ObjectMap("name", "HG0002"), new
                ObjectMap("name", "HG0003"), System.currentTimeMillis(), "admin", "", new ObjectMap()));
        auditDbAdaptor.insertAuditRecord(new AuditRecord(23, AuditRecord.Resource.sample, "update", new ObjectMap("description", ""), new
                ObjectMap("description", "New sample"), System.currentTimeMillis(), "admin", "", new ObjectMap()));
    }

//    @Test
//    public void testGet() throws Exception {
//
//    }
}