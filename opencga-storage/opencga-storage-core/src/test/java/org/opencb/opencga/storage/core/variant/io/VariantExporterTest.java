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
import org.junit.Ignore;
import org.junit.Test;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.SampleEntry;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.StorageEngineTest;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
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
@Ignore
@StorageEngineTest
public abstract class VariantExporterTest extends VariantStorageBaseTest {

    @Before
    public void setUp() throws Exception {
        runDefaultETL(smallInputUri, variantStorageEngine, newStudyMetadata(),
                new QueryOptions()
//                        .append(VariantStorageEngine.Options.EXTRA_FORMAT_FIELDS.key(), "GL,DS")
                        .append(VariantStorageOptions.ANNOTATE.key(), false));
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
    public void exportJsonSparseTest() throws Exception {
        URI output = newOutputUri().resolve("variant.sparse.json");
        variantStorageEngine.exportData(output, VariantOutputFormat.JSON_SPARSE, null,
                new VariantQuery().includeSampleAll(), new QueryOptions());

        System.out.println("output = " + output);
        assertTrue(Paths.get(output).toFile().exists());

        List<Variant> sparseVariants = new ArrayList<>();
        for (Variant variant : new VariantJsonReader(null, output.getPath())) {
            sparseVariants.add(variant);
            assertNotNull(variant.getStudies());
            assertFalse(variant.getStudies().isEmpty());
            // Sparse output clears samplesPosition
            assertNull(variant.getStudies().get(0).getSamplesPosition());
            for (SampleEntry sample : variant.getStudies().get(0).getSamples()) {
                assertNotNull(sample.getSampleId());
                assertNotNull(sample.getFileIndex());
                assertNotNull(variant.getStudies().get(0).getFile(sample.getFileIndex()));
                assertNotNull(sample.getData());
                assertFalse(sample.getData().isEmpty());
                String gt = sample.getData().get(0);
                assertNotNull(gt);
                assertFalse("HOM_REF genotype in sparse output: " + gt,
                        GenotypeClass.HOM_REF.test(gt));
                assertFalse("MISS genotype in sparse output: " + gt,
                        GenotypeClass.MISS.test(gt));
            }
        }
        assertTrue("No variants in sparse output", sparseVariants.size() > 0);

        // Export regular JSON (with sampleId) and apply VariantSparseFilterTask manually.
        // Compare: native sparse output must match task-based output.
        URI jsonOutput = newOutputUri().resolve("variant.full.json");
        variantStorageEngine.exportData(jsonOutput, VariantOutputFormat.JSON, null,
                new VariantQuery().includeSampleAll().includeSampleId(true), new QueryOptions());

        List<Variant> fullVariants = new ArrayList<>();
        for (Variant variant : new VariantJsonReader(null, jsonOutput.getPath())) {
            fullVariants.add(variant);
        }
        new VariantSparseFilterTask().apply(fullVariants);

        assertEquals("Number of variants should match", fullVariants.size(), sparseVariants.size());
        for (int i = 0; i < sparseVariants.size(); i++) {
            Variant sparse = sparseVariants.get(i);
            Variant fromTask = fullVariants.get(i);
            assertEquals("Variant mismatch at " + i, sparse.toString(), fromTask.toString());
            for (int s = 0; s < sparse.getStudies().size(); s++) {
                StudyEntry sparseStudy = sparse.getStudies().get(s);
                StudyEntry taskStudy = fromTask.getStudies().get(s);
                assertEquals("Sample count mismatch at variant " + sparse,
                        sparseStudy.getSamples().size(), taskStudy.getSamples().size());
                for (int j = 0; j < sparseStudy.getSamples().size(); j++) {
                    SampleEntry sparseSample = sparseStudy.getSamples().get(j);
                    SampleEntry taskSample = taskStudy.getSamples().get(j);
                    assertEquals("SampleId mismatch at variant " + sparse + " sample " + j,
                            sparseSample.getSampleId(), taskSample.getSampleId());
                    assertEquals("GT mismatch at variant " + sparse + " sample " + j,
                            sparseSample.getData().get(0), taskSample.getData().get(0));
                }
            }
        }
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
