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

package org.opencb.opencga.storage.mongodb.variant.load;

import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;

import java.net.URI;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * Created on 08/12/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoVariantImporterTest extends VariantStorageBaseTest implements MongoDBVariantStorageTest {


    private StudyMetadata studyMetadata;

    @Before
    public void setUp() throws Exception {
        studyMetadata = newStudyMetadata();
        runDefaultETL(smallInputUri, variantStorageEngine, studyMetadata,
                new ObjectMap(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), "GL,DS"));
    }

    @Test
    public void testSimpleImport() throws Exception {
        URI outputFile = newOutputUri().resolve("export.avro");

        System.out.println("outputFile = " + outputFile);
        variantStorageEngine.exportData(outputFile, VariantOutputFormat.AVRO, null, new Query(), new QueryOptions());

        clearDB(DB_NAME);

        variantStorageEngine.importData(outputFile, new ObjectMap());

        for (Variant variant : variantStorageEngine.getDBAdaptor()) {
            assertEquals(4, variant.getStudies().get(0).getSamples().size());
        }
    }


    @Test
    public void testImportSomeSamples() throws Exception {
        URI outputFile = newOutputUri().resolve("export.avro");

        System.out.println("outputFile = " + outputFile);
        List<String> samples = new LinkedList<>(metadataManager.getIndexedSamplesMap(studyMetadata.getId()).keySet()).subList(1, 3);
        Set<String> samplesSet = new HashSet<>(samples);
        Query query = new Query(VariantQueryParam.INCLUDE_SAMPLE.key(), samples);
        variantStorageEngine.exportData(outputFile, VariantOutputFormat.AVRO, null, query, new QueryOptions());

        clearDB(DB_NAME);

        variantStorageEngine.importData(outputFile, new ObjectMap());

        for (Variant variant : variantStorageEngine.getDBAdaptor()) {
            assertEquals(2, variant.getStudies().get(0).getSamples().size());
            assertEquals(samplesSet, variant.getStudies().get(0).getSamplesName());
        }
    }

    @Test
    public void testImportExcludeSamples() throws Exception {
        URI outputFile = newOutputUri().resolve("export.avro");

        System.out.println("outputFile = " + outputFile);
        Query query = new Query();
        QueryOptions queryOptions = new QueryOptions(QueryOptions.EXCLUDE, VariantField.STUDIES_SAMPLES.toString());
        variantStorageEngine.exportData(outputFile, VariantOutputFormat.AVRO, null, query, queryOptions);

        clearDB(DB_NAME);

        variantStorageEngine.importData(outputFile, new ObjectMap());

        for (Variant variant : variantStorageEngine.getDBAdaptor()) {
            assertEquals(0, variant.getStudies().get(0).getSamples().size());
        }
    }

    @Test
    public void testImportEmptySamples() throws Exception {
        URI outputFile = newOutputUri().resolve("export.avro");

        System.out.println("outputFile = " + outputFile);
        Query query = new Query(VariantQueryParam.INCLUDE_SAMPLE.key(), ".");
        QueryOptions queryOptions = new QueryOptions();
        variantStorageEngine.exportData(outputFile, VariantOutputFormat.AVRO, null, query, queryOptions);

        clearDB(DB_NAME);

        variantStorageEngine.importData(outputFile, new ObjectMap());

        for (Variant variant : variantStorageEngine.getDBAdaptor()) {
            assertEquals(0, variant.getStudies().get(0).getSamples().size());
        }
    }

}
