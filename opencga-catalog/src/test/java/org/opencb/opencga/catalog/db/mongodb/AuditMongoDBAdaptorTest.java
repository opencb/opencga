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

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

/**
 * Created on 18/08/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Category(MediumTests.class)
public class AuditMongoDBAdaptorTest extends AbstractMongoDBAdaptorTest {

    @Test
    public void testInsertAuditRecord() throws Exception {
        dbAdaptorFactory.getCatalogAuditDbAdaptor(organizationId)
                .insertAuditRecord(new AuditRecord(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT),
                        UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT), "user", "api", Enums.Action.CREATE,
                        Enums.Resource.SAMPLE, "sampleId", "sampleUuid", "studyId", "studyUuid", new ObjectMap(),
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), TimeUtils.getDate(), new ObjectMap()));
        dbAdaptorFactory.getCatalogAuditDbAdaptor(organizationId)
                .insertAuditRecord(new AuditRecord(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT),
                        UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT), "user", "api", Enums.Action.CREATE,
                        Enums.Resource.SAMPLE, "sampleId2", "sampleUuid2", "studyId", "studyUuid", new ObjectMap(),
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), TimeUtils.getDate(), new ObjectMap()));
        dbAdaptorFactory.getCatalogAuditDbAdaptor(organizationId)
                .insertAuditRecord(new AuditRecord(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT),
                        UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT), "user", "api", Enums.Action.CREATE,
                        Enums.Resource.SAMPLE, "sampleId3", "sampleUuid3", "studyId", "studyUuid", new ObjectMap(),
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), TimeUtils.getDate(), new ObjectMap()));
    }

//    @Test
//    public void testGet() throws Exception {
//
//    }
}