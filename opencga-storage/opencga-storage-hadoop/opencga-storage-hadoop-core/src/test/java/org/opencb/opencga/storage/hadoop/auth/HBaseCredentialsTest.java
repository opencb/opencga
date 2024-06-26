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

package org.opencb.opencga.storage.hadoop.auth;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

/**
 * Created on 05/10/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Category(ShortTests.class)
public class HBaseCredentialsTest {


    @Test
    public void testToString() {

        HBaseCredentials credentials = new HBaseCredentials("node1.hdp,node2.hdp:2181", null, null, null, null, "/hbase-unsecure");

        String s = credentials.toString();

        HBaseCredentials credentials2 = new HBaseCredentials(s);

//        System.out.println("credentials = " + credentials);
//        System.out.println("credentials2 = " + credentials2);
        Assert.assertEquals(credentials, credentials2);
        Assert.assertEquals(credentials.toString(), credentials2.toString());

    }

}
