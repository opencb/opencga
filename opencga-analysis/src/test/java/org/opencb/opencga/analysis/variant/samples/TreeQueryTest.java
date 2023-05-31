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

package org.opencb.opencga.analysis.variant.samples;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import static org.junit.Assert.assertEquals;

@Category(ShortTests.class)
public class TreeQueryTest {

    @Test(expected = IllegalArgumentException.class)
    public void testFail() throws Exception {
        // Query param 'id' found multiple times in statement 'id=A AND id=B'.
        new TreeQuery("id=A AND id=B");
    }

    @Test
    public void testParse() throws JsonProcessingException {
        assertEquals("key=value AND key3>50", new TreeQuery("key = value AND key3>50").toString());
        assertEquals("(key=value) AND (key3>50)", new TreeQuery("(key=value) AND (key3>50)").toString());
        assertEquals("(key=value) AND (key3>50)", new TreeQuery("  ( key=value   )   AND   ( key3>50  )").toString());
        assertEquals("((key=value) AND (key3>50)) OR (key5>=2323)", new TreeQuery("((key=value) AND (key3>50)) OR (key5>=2323)").toString());
        assertEquals("(NOT ((key=value) AND (key3>50))) OR (NOT (key5>=2323))", new TreeQuery("NOT ((key=value) AND (key3>50)) OR NOT (key5>=2323)").toString());
        assertEquals("(id=A) AND (id=B)", new TreeQuery("(id=A) AND (id=B)").toString());
        assertEquals("(id=A) OR (id=B)", new TreeQuery("id=A OR id=B").toString());
        assertEquals("(id=A AND K=4) OR (id=B)", new TreeQuery("id=A AND K=4 OR id=B").toString());
        assertEquals("(id=A) OR (K=4 AND id=B)", new TreeQuery("id=A OR K=4 AND id=B").toString());
        assertEquals("((A=1) OR (B=2)) AND (C=3 AND D=4)", new TreeQuery("((A=1 OR B=2) AND C=3 AND D=4)").toString());
        assertEquals("((A=1) OR (B=2)) AND (C=3 AND D=4)", new TreeQuery("(A=1 OR B=2) AND C=3 AND D=4").toString());
//        assertEquals("((A=1) OR (B=2)) AND (C=3 and D=4)", new TreeQuery("(A=1 or B=2) and C=3 and D=4").toString());

        System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(new TreeQuery("((key=value) AND NOT (key3>50)) OR (key5>=2323)")));
    }
}