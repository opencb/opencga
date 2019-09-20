/*
 * Copyright 2015-2017 OpenCB
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
import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.datastore.core.DataStoreServerAddress;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.mongodb.MongoDBConfiguration;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.db.api.AuditDBAdaptor;
import org.opencb.opencga.catalog.utils.UUIDUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;

import java.util.Collections;

/**
 * Created on 18/08/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class AuditMongoDBAdaptorTest {

    private AuditDBAdaptor auditDbAdaptor;

    @Before
    public void beforeClass() throws Exception {
        Configuration configuration = Configuration.load(getClass().getResource("/configuration-test.yml")
                .openStream());

        DataStoreServerAddress dataStoreServerAddress = new DataStoreServerAddress(
                configuration.getCatalog().getDatabase().getHosts().get(0).split(":")[0], 27017);

        MongoDBConfiguration mongoDBConfiguration = MongoDBConfiguration.builder()
                .add("username", configuration.getCatalog().getDatabase().getUser())
                .add("password", configuration.getCatalog().getDatabase().getPassword())
                .add("authenticationDatabase", configuration.getCatalog().getDatabase().getOptions().get("authenticationDatabase"))
                .build();

//        String database = catalogConfiguration.getDatabase().getDatabase();
        String database;
        if(StringUtils.isNotEmpty(configuration.getDatabasePrefix())) {
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

        auditDbAdaptor = new MongoDBAdaptorFactory(Collections.singletonList(dataStoreServerAddress), mongoDBConfiguration, database)
                .getCatalogAuditDbAdaptor();
    }

    @Test
    public void testInsertAuditRecord() throws Exception {
        auditDbAdaptor.insertAuditRecord(new AuditRecord("user", "api", UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.AUDIT),
                "sampleId", "sampleUuid", "studyId", "studyUuid", new ObjectMap(), AuditRecord.Entity.SAMPLE,
                AuditRecord.Action.CREATE, AuditRecord.SUCCESS, TimeUtils.getDate(), new ObjectMap(), ));
        auditDbAdaptor.insertAuditRecord(new AuditRecord("user", "api", UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.AUDIT),
                "sampleId2", "sampleUuid2", "studyId", "studyUuid", new ObjectMap(), AuditRecord.Entity.SAMPLE,
                AuditRecord.Action.CREATE, AuditRecord.SUCCESS, TimeUtils.getDate(), new ObjectMap(), ));
        auditDbAdaptor.insertAuditRecord(new AuditRecord("user", "api", UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.AUDIT),
                "sampleId3", "sampleUuid3", "studyId", "studyUuid", new ObjectMap(), AuditRecord.Entity.SAMPLE,
                AuditRecord.Action.CREATE, AuditRecord.SUCCESS, TimeUtils.getDate(), new ObjectMap(), ));
    }

//    @Test
//    public void testGet() throws Exception {
//
//    }
}