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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.metadata.VariantFileHeader;
import org.opencb.biodata.models.variant.metadata.VariantFileHeaderComplexLine;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageEngine;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat.*;

/**
 * Created on 07/12/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Category(ShortTests.class)
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

    private static final String STUDY1 = "study1";
    private static final String STUDY2 = "study2";

    @Before
    public void setUp() {
        DummyVariantStorageEngine.clear();
    }

    private VariantWriterFactory buildFactory(String... studies) throws Exception {
        DummyVariantDBAdaptor dbAdaptor = new DummyVariantDBAdaptor("opencga");
        VariantStorageMetadataManager mm = dbAdaptor.getMetadataManager();
        mm.getAndUpdateProjectMetadata(new ObjectMap());
        for (String study : studies) {
            mm.createStudy(study);
        }
        return new VariantWriterFactory(mm);
    }

    // --- validateQuery: VCF format ---

    @Test
    public void validateQueryVcfSetsDefaultUnknownGenotype() throws Exception {
        Query query = new VariantQuery().study(STUDY1);
        buildFactory(STUDY1).validateQuery(VCF, query);
        assertEquals("./.", query.getString(UNKNOWN_GENOTYPE.key()));
    }

    @Test
    public void validateQueryVcfPreservesExistingUnknownGenotype() throws Exception {
        Query query = new VariantQuery().study(STUDY1);
        query.put(UNKNOWN_GENOTYPE.key(), "0/0");
        buildFactory(STUDY1).validateQuery(VCF, query);
        assertEquals("0/0", query.getString(UNKNOWN_GENOTYPE.key()));
    }

    @Test
    public void validateQueryVcfMultipleIncludeStudiesThrows() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(containsString(INCLUDE_STUDY.key()));
        // Two studies in DB, no INCLUDE_STUDY filter → both are included
        buildFactory(STUDY1, STUDY2).validateQuery(VCF, new Query());
    }

    @Test
    public void validateQueryVcfNoStudyFilterWithTwoStudiesThrows() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(containsString(STUDY1));
        // Include only study1, but no STUDY filter — variants from study2 would appear with no study1 data
        buildFactory(STUDY1, STUDY2).validateQuery(VCF, new VariantQuery().includeStudy(STUDY1));
    }

    @Test
    public void validateQueryVcfMismatchedStudyFilterThrows() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(containsString(STUDY1));
        thrown.expectMessage(containsString(STUDY2));
        // Include study1 in output but filter by study2 — study1 data missing for study2-only variants
        buildFactory(STUDY1, STUDY2).validateQuery(VCF, new VariantQuery().includeStudy(STUDY1).study(STUDY2));
    }

    @Test
    public void validateQueryVcfMatchingStudyFilterOk() throws Exception {
        // Include study1 and filter by study1 — consistent, no exception
        Query query = new VariantQuery().includeStudy(STUDY1).study(STUDY1);
        buildFactory(STUDY1, STUDY2).validateQuery(VCF, query);
        assertEquals("./.", query.getString(UNKNOWN_GENOTYPE.key()));
    }

    // --- validateQuery: JSON_SPARSE format ---

    @Test
    public void validateQuerySparseSetsSparseFlags() throws Exception {
        Query query = new VariantQuery();
        buildFactory(STUDY1).validateQuery(JSON_SPARSE, query);
        assertTrue(query.getBoolean(VariantQueryUtils.SPARSE_SAMPLES.key()));
        assertTrue(query.getBoolean(INCLUDE_SAMPLE_ID.key()));
    }

    @Test
    public void validateQuerySparseWithIncludeGenotypeFalseThrows() throws Exception {
        thrown.expect(VariantQueryException.class);
        thrown.expectMessage(containsString(VariantQueryParam.INCLUDE_GENOTYPE.key()));
        Query query = new VariantQuery();
        query.put(VariantQueryParam.INCLUDE_GENOTYPE.key(), false);
        buildFactory(STUDY1).validateQuery(JSON_SPARSE, query);
    }

    // --- validateQuery: multi-study formats skip the study count check ---

    @Test
    public void validateQueryJsonMultiStudyOk() throws Exception {
        // JSON is multi-study, so no study restriction is applied even with two studies
        buildFactory(STUDY1, STUDY2).validateQuery(JSON, new Query());
    }

    @Test
    public void testContigLengthNull() throws IOException, StorageEngineException {
        DummyVariantStorageEngine.clear();
        DummyVariantDBAdaptor dbAdaptor = new DummyVariantDBAdaptor("opencga");
        VariantFileHeader header = new VariantFileHeader();
        header.setComplexLines(Arrays.asList(
                new VariantFileHeaderComplexLine("contig", "chr1", null, null, null, Collections.singletonMap("length", null)),
                new VariantFileHeaderComplexLine("contig", "chr2", null, null, null, Collections.singletonMap("length", "")),
                new VariantFileHeaderComplexLine("contig", "chr3", null, null, null, Collections.singletonMap("length", ".")),
                new VariantFileHeaderComplexLine("contig", "chr4", null, null, null, Collections.singletonMap("length", "1234"))
        ));
        VariantStorageMetadataManager metadataManager = dbAdaptor.getMetadataManager();
        metadataManager.getAndUpdateProjectMetadata(new ObjectMap());
        StudyMetadata study = metadataManager.createStudy("study");
        metadataManager.unsecureUpdateStudyMetadata(study.setVariantHeader(header));
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