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
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;

import java.io.IOException;

/**
 * Created by pfurio on 19/01/16.
 */
public class CatalogMongoUserDBAdaptorTest {

    static CatalogMongoDBAdaptorFactory dbAdaptorFactory;

    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private CatalogMongoUserDBAdaptor catalogUserDBAdaptor;

    @AfterClass
    public static void afterClass() {
        CatalogMongoDBAdaptorTest.afterClass();
    }

    @Before
    public void before() throws IOException, CatalogDBException {
        CatalogMongoDBAdaptorTest dbAdaptorTest = new CatalogMongoDBAdaptorTest();
        dbAdaptorTest.before();

        dbAdaptorFactory = CatalogMongoDBAdaptorTest.catalogDBAdaptor;
        catalogUserDBAdaptor = dbAdaptorFactory.getCatalogUserDBAdaptor();
    }

    @Test
    public void nativeGet() throws Exception {
        Query query = new Query("id", "imedina");
        QueryResult queryResult = catalogUserDBAdaptor.nativeGet(query, null);
        System.out.println("HOLA");
    }


}
