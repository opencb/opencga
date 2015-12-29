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

package org.opencb.opencga.storage.mongodb.alignment;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.junit.Test;
import org.opencb.biodata.models.alignment.stats.MeanCoverage;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.test.GenericTest;

import static org.junit.Assert.assertEquals;

public class DBObjectToMeanCoverageConverterTest extends GenericTest {

    @Test
    public void testConvertToDataModelType() throws Exception {
        DBObjectToMeanCoverageConverter converter = new DBObjectToMeanCoverageConverter();

        testConvert(converter, new MeanCoverage(1000, "1k", new Region("3", 14001, 15000), (float) 0.123454));
        testConvert(converter, new MeanCoverage(1000000, "1m", new Region("3", 14000001, 15000000), (float) 9.8765));
        testConvert(converter, new MeanCoverage(10, "10", new Region("3", 14001, 14010), (float) 0.12345));
        testConvert(converter, new MeanCoverage(2000, "2k", new Region("3", 14001, 16000), (float) 9.8765));
    }

    private void testConvert(DBObjectToMeanCoverageConverter converter, MeanCoverage meancoverage) {
        DBObject object = converter.convertToStorageType(meancoverage);
        DBObject id = converter.getIdObject(meancoverage);
        BasicDBList filesList = new BasicDBList();
        filesList.add(object);

        DBObject objectInFile = new BasicDBObject(CoverageMongoDBWriter.FILES_FIELD, filesList);
        objectInFile.putAll(id);
        DBObject objectWithId = new BasicDBObject();
        objectWithId.putAll(id);
        objectWithId.putAll(object);

        MeanCoverage mc1 = converter.convertToDataModelType(objectInFile);
        MeanCoverage mc2 = converter.convertToDataModelType(objectWithId);

        assertEquals(meancoverage, mc1);
        assertEquals(meancoverage, mc2);
    }
}