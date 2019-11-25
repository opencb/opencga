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
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantStorageTest;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat;
import org.opencb.opencga.storage.core.variant.io.json.VariantJsonReader;

import java.io.*;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.*;

/**
 * Created on 06/12/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantExporterTest extends VariantStorageBaseTest implements DummyVariantStorageTest {

    @Before
    public void setUp() throws Exception {
        runDefaultETL(smallInputUri, variantStorageEngine, newStudyMetadata(),
                new QueryOptions()
//                        .append(VariantStorageEngine.Options.EXTRA_FORMAT_FIELDS.key(), "GL,DS")
                        .append(VariantStorageOptions.ANNOTATE.key(), false));
    }

    @After
    public void tearDown() throws Exception {
        DummyVariantStorageMetadataDBAdaptorFactory.writeAndClear(getTmpRootDir());
    }

    @Test
    public void exportStudyTest() throws Exception {
        variantStorageEngine.exportData(null, VariantOutputFormat.VCF, null, new Query(), new QueryOptions());
        // It may happen that the VcfExporter closes the StandardOutput.
        // Check System.out is not closed
        System.out.println(getClass().getSimpleName() + ": System out not closed!");
    }

    @Test
    public void exportStudyJsonTest() throws Exception {
        URI output = newOutputUri().resolve("variant.json.gz");
        variantStorageEngine.exportData(output, VariantOutputFormat.JSON_GZ, null, new Query(), new QueryOptions());

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

    @Test
    public void exportJsonGivenVariantsFileTest() throws Exception {
        URI outputDir = newOutputUri();
        URI outputFile = outputDir.resolve("subset.variants.json.gz");
        URI variantsFile = outputDir.resolve("variants.tsv");
        List<String> expectedVariants = new ArrayList<>();
        try (PrintStream out = new PrintStream(new FileOutputStream(variantsFile.getPath()))) {
            int i = 0;
            out.println("#CHROM\tPOS\tID\tREF\tALT");
            for (Variant v : variantStorageEngine) {
                if (i++ % 5 == 0) {
                    expectedVariants.add(v.toString());
                    out.println(v.getChromosome() + "\t"+v.getStart()+"\t.\t"+v.getReference()+"\t"+v.getAlternate()+"");
                }
            }
        }

        variantStorageEngine.exportData(outputFile, VariantOutputFormat.JSON_GZ, variantsFile, new Query(), new QueryOptions(), null);

        assertTrue(Paths.get(outputFile).toFile().exists());
        assertTrue(Paths.get(outputFile.getPath() + VariantExporter.METADATA_FILE_EXTENSION).toFile().exists());

        // Check gzip format
        int numVariants = 0;
        for (Variant variant : new VariantJsonReader(Collections.emptyMap(), outputFile.getPath())) {
            assertThat(expectedVariants, hasItem(variant.toString()));
            numVariants++;
        }
        assertEquals(expectedVariants.size(), numVariants);
    }

    @Test
    public void exportTpedTest() throws Exception {
        URI output = newOutputUri().resolve("variant" + VariantExporter.TPED_FILE_EXTENSION);
        variantStorageEngine.exportData(output, VariantOutputFormat.TPED, null, new Query(), new QueryOptions());

        System.out.println("output = " + output);
        assertTrue(Paths.get(output).toFile().exists());

        // Check gzip format
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(output.getPath())))) {
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
