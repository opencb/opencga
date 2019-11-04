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
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.metadata.VariantFileHeader;
import org.opencb.biodata.models.variant.metadata.VariantFileHeaderComplexLine;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantDBAdaptor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat.JSON_GZ;
import static org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat.STATS_GZ;

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
        assertEquals("myFile.json.gz", VariantWriterFactory.checkOutput("myFile", JSON_GZ));
        assertEquals("myFile.json.gz", VariantWriterFactory.checkOutput("myFile.json", JSON_GZ));
        assertEquals("myFile.json.gz", VariantWriterFactory.checkOutput("myFile.json.gz", JSON_GZ));
        assertEquals("myFile.json.gz", VariantWriterFactory.checkOutput("myFile.json.gz.", JSON_GZ));
        assertEquals("myFile.stats.tsv.gz", VariantWriterFactory.checkOutput("myFile.", STATS_GZ));
        assertEquals("myFile.stats.tsv.gz", VariantWriterFactory.checkOutput("myFile", STATS_GZ));
        assertEquals("myFile.stats.tsv.gz", VariantWriterFactory.checkOutput("myFile.stats", STATS_GZ));
        assertEquals("myFile.stats.tsv.gz", VariantWriterFactory.checkOutput("myFile.stats.", STATS_GZ));
        assertEquals("myFile.stats.tsv.gz", VariantWriterFactory.checkOutput("myFile.stats.tsv", STATS_GZ));
        assertEquals("myFile.stats.tsv.gz", VariantWriterFactory.checkOutput("myFile.stats.tsv.gz", STATS_GZ));
    }

    @Test
    public void checkBadOutputTest() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        VariantWriterFactory.checkOutput("path/", JSON_GZ);
    }

    @Test
    public void testContigLengthNull() throws IOException, StorageEngineException {
        DummyVariantDBAdaptor dbAdaptor = new DummyVariantDBAdaptor("opencga");
        VariantFileHeader header = new VariantFileHeader();
        header.setComplexLines(Arrays.asList(
                new VariantFileHeaderComplexLine("contig", "chr1", null, null, null, Collections.singletonMap("length", null)),
                new VariantFileHeaderComplexLine("contig", "chr2", null, null, null, Collections.singletonMap("length", "")),
                new VariantFileHeaderComplexLine("contig", "chr3", null, null, null, Collections.singletonMap("length", ".")),
                new VariantFileHeaderComplexLine("contig", "chr4", null, null, null, Collections.singletonMap("length", "1234"))
        ));
        StudyMetadata study = dbAdaptor.getMetadataManager().createStudy("study");
        dbAdaptor.getMetadataManager().unsecureUpdateStudyMetadata(study.setVariantHeader(header));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(10000);
        DataWriter<Variant> writer = new VariantWriterFactory(dbAdaptor).newDataWriter(
                VariantWriterFactory.VariantOutputFormat.VCF,
                outputStream, new Query(), new QueryOptions());


        writer.open();
        writer.pre();
        // Write only header
        writer.post();
        writer.close();

        String s = outputStream.toString();
        assertThat(s, containsString("##contig=<ID=chr1>"));
        assertThat(s, containsString("##contig=<ID=chr2>"));
        assertThat(s, containsString("##contig=<ID=chr3>"));
        assertThat(s, containsString("##contig=<ID=chr4,length=1234>"));
    }


}