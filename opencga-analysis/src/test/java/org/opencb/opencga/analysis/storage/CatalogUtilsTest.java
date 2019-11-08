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

package org.opencb.opencga.analysis.storage;

import org.junit.Test;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by pfurio on 02/12/16.
 */
public class CatalogUtilsTest {

    @Test
    public void parseSampleAnnotationQuery() throws Exception {
        Query query = CatalogUtils.parseSampleAnnotationQuery("age>20;" + SampleDBAdaptor.QueryParams.PHENOTYPES.key() + "=hpo:123,hpo:456;" + SampleDBAdaptor.QueryParams.ID.key() + "=smith", SampleDBAdaptor.QueryParams::getParam);

        assertEquals(3, query.size());

        assertTrue(query.containsKey(SampleDBAdaptor.QueryParams.ID.key()));
        assertEquals("=smith", query.getString(SampleDBAdaptor.QueryParams.ID.key()));

        assertTrue(query.containsKey(SampleDBAdaptor.QueryParams.ANNOTATION.key()));
        assertEquals("annotation.age>20", query.getString(SampleDBAdaptor.QueryParams.ANNOTATION.key()));

        assertTrue(query.containsKey(SampleDBAdaptor.QueryParams.PHENOTYPES.key()));
        assertEquals("=hpo:123,hpo:456", query.getString(SampleDBAdaptor.QueryParams.PHENOTYPES.key()));
    }

}