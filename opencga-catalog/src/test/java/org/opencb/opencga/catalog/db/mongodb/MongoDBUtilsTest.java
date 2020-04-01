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

import org.bson.Document;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Created by pfurio on 3/3/16.
 */
public class MongoDBUtilsTest extends MongoDBAdaptorTest {
    /////////// Other tests
    @Test
    public void replaceDots() {

        Document original = new Document("o.o", 4).append("4.4", Arrays.asList(1, 3, 4, new Document("933.44", "df.sdf")))
                .append("key", new Document("key....k", "value...2.2.2"));
        Document o = new Document("o.o", 4).append("4.4", Arrays.asList(1, 3, 4, new Document("933.44", "df.sdf"))).append
                ("key", new Document("key....k", "value...2.2.2"));
        System.out.println(o);

        MongoDBUtils.replaceDotsInKeys(o);
        System.out.println(o);

        MongoDBUtils.restoreDotsInKeys(o);
        System.out.println(o);

        Assert.assertEquals(original, o);

    }
}
