package org.opencb.opencga.catalog.db.mongodb;

import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.datastore.core.DataStoreServerAddress;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.mongodb.MongoDBConfiguration;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.db.api.AuditDBAdaptor;

import java.util.Collections;

/**
 * Created on 18/08/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogMongoAuditDBAdaptorTest {

    private AuditDBAdaptor auditDbAdaptor;

    @Before
    public void beforeClass() throws Exception {
        CatalogConfiguration catalogConfiguration = CatalogConfiguration.load(getClass().getResource("/catalog-configuration-test.yml")
                .openStream());

        DataStoreServerAddress dataStoreServerAddress = new DataStoreServerAddress(
                catalogConfiguration.getDatabase().getHosts().get(0).split(":")[0], 27017);

        MongoDBConfiguration mongoDBConfiguration = MongoDBConfiguration.builder()
                .add("username", catalogConfiguration.getDatabase().getUser())
                .add("password", catalogConfiguration.getDatabase().getPassword())
                .add("authenticationDatabase", catalogConfiguration.getDatabase().getOptions().get("authenticationDatabase"))
                .build();

//        String database = catalogConfiguration.getDatabase().getDatabase();
        String database;
        if(StringUtils.isNotEmpty(catalogConfiguration.getDatabasePrefix())) {
            if (!catalogConfiguration.getDatabasePrefix().endsWith("_")) {
                database = catalogConfiguration.getDatabasePrefix() + "_catalog";
            } else {
                database = catalogConfiguration.getDatabasePrefix() + "catalog";
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


        auditDbAdaptor = new MongoDBAdaptorFactory(Collections.singletonList(dataStoreServerAddress), mongoDBConfiguration, database)
                .getCatalogAuditDbAdaptor();
    }

    @Test
    public void testInsertAuditRecord() throws Exception {
        auditDbAdaptor.insertAuditRecord(new AuditRecord(23, AuditRecord.Resource.sample, AuditRecord.Action.update,
                AuditRecord.Magnitude.medium, new ObjectMap("name", "HG0001"), new ObjectMap("name", "HG0002"), System.currentTimeMillis(),
                "admin", "", new ObjectMap()));
        auditDbAdaptor.insertAuditRecord(new AuditRecord(23, AuditRecord.Resource.sample, AuditRecord.Action.update,
                AuditRecord.Magnitude.medium, new ObjectMap("name", "HG0002"), new ObjectMap("name", "HG0003"), System.currentTimeMillis(),
                "admin", "", new ObjectMap()));
        auditDbAdaptor.insertAuditRecord(new AuditRecord(23, AuditRecord.Resource.sample, AuditRecord.Action.update,
                AuditRecord.Magnitude.medium, new ObjectMap("description", ""), new ObjectMap("description", "New sample"),
                System.currentTimeMillis(), "admin", "", new ObjectMap()));
    }

//    @Test
//    public void testGet() throws Exception {
//
//    }
}