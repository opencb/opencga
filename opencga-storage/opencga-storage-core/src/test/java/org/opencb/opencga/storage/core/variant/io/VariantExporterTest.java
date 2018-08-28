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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.dummy.DummyProjectMetadataAdaptor;
import org.opencb.opencga.storage.core.variant.dummy.DummyStudyConfigurationAdaptor;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageTest;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Paths;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertTrue;

/**
 * Created on 06/12/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantExporterTest extends VariantStorageBaseTest implements DummyVariantStorageTest {

    @Before
    public void setUp() throws Exception {
        runDefaultETL(inputUri, variantStorageEngine, newStudyConfiguration(),
                new QueryOptions()
//                        .append(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), "GL,DS")
                        .append(VariantStorageEngine.Options.ANNOTATE.key(), false));
    }

    @After
    public void tearDown() throws Exception {
        DummyProjectMetadataAdaptor.writeAndClear(getTmpRootDir());
        DummyStudyConfigurationAdaptor.writeAndClear(getTmpRootDir());
    }

    @Test
    public void exportStudyTest() throws Exception {
        variantStorageEngine.exportData(null, VariantOutputFormat.VCF, new Query(), new QueryOptions());
        // It may happen that the VcfExporter closes the StandardOutput.
        // Check System.out is not closed
        System.out.println(getClass().getSimpleName() + ": System out not closed!");
    }

    @Test
    public void exportStudyJsonTest() throws Exception {
        URI output = newOutputUri().resolve("variant.json.gz");
        variantStorageEngine.exportData(output, VariantOutputFormat.JSON_GZ, new Query(), new QueryOptions());

        System.out.println("output = " + output);
        assertTrue(Paths.get(output).toFile().exists());
        assertTrue(Paths.get(output.getPath() + VariantExporter.METADATA_FILE_EXTENSION).toFile().exists());

        // Check gzip format
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(output.getPath()))))) {
            int i = 0;
            while (true) {
                String line = br.readLine();
                if (line == null) {
                    break;
                }
                System.out.println("[" + i++ + "]: " + line);
            }
        }
    }

}
