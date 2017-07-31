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

package org.opencb.opencga.storage.core.variant.io;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat.*;

/**
 * Created on 07/12/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantWriterFactoryTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void checkOutputTest() throws Exception {
        assertEquals("path/myFile.json.gz", VariantWriterFactory.checkOutput("path/myFile", JSON_GZ));
        assertEquals("path/myFile.json.gz", VariantWriterFactory.checkOutput("path/myFile.json", JSON_GZ));
        assertEquals("path/myFile.json.gz", VariantWriterFactory.checkOutput("path/myFile.json.gz", JSON_GZ));
        assertEquals("path/myFile.json.gz", VariantWriterFactory.checkOutput("path/myFile.json.gz.", JSON_GZ));
        assertEquals("path/myFile.stats.tsv.gz", VariantWriterFactory.checkOutput("path/myFile.", STATS_GZ));
        assertEquals("path/myFile.stats.tsv.gz", VariantWriterFactory.checkOutput("path/myFile", STATS_GZ));
        assertEquals("path/myFile.stats.tsv.gz", VariantWriterFactory.checkOutput("path/myFile.stats", STATS_GZ));
        assertEquals("path/myFile.stats.tsv.gz", VariantWriterFactory.checkOutput("path/myFile.stats.", STATS_GZ));
        assertEquals("path/myFile.stats.tsv.gz", VariantWriterFactory.checkOutput("path/myFile.stats.tsv", STATS_GZ));
        assertEquals("path/myFile.stats.tsv.gz", VariantWriterFactory.checkOutput("path/myFile.stats.tsv.gz", STATS_GZ));
    }

    @Test
    public void checkBadOutputTest() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        VariantWriterFactory.checkOutput("path/", JSON_GZ);
    }
}