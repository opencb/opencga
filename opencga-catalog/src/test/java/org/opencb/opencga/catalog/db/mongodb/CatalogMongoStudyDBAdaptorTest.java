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
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Created by pfurio on 19/01/16.
 */
public class CatalogMongoStudyDBAdaptorTest {

    static CatalogMongoDBAdaptorFactory dbAdaptorFactory;

    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private CatalogMongoStudyDBAdaptor catalogStudyDBAdaptor;

    @AfterClass
    public static void afterClass() {
        CatalogMongoDBAdaptorTest.afterClass();
    }

    @Before
    public void before() throws IOException, CatalogDBException {
        CatalogMongoDBAdaptorTest dbAdaptorTest = new CatalogMongoDBAdaptorTest();
        dbAdaptorTest.before();

        dbAdaptorFactory = CatalogMongoDBAdaptorTest.catalogDBAdaptor;
        catalogStudyDBAdaptor = dbAdaptorFactory.getCatalogStudyDBAdaptor();
    }

    @Test
    public void updateDiskUsage() throws Exception {
        catalogStudyDBAdaptor.updateDiskUsage(5,100);
        assertEquals(2100, catalogStudyDBAdaptor.getStudy(5,null).getResult().get(0).getDiskUsage());
        catalogStudyDBAdaptor.updateDiskUsage(5,-200);
        assertEquals(1900, catalogStudyDBAdaptor.getStudy(5,null).getResult().get(0).getDiskUsage());
    }


}
