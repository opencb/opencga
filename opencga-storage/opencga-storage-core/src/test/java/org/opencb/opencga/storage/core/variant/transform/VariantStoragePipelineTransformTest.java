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

package org.opencb.opencga.storage.core.variant.transform;

import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.metadata.VariantFileHeaderComplexLine;
import org.opencb.biodata.tools.variant.normalizer.extensions.VafVariantNormalizerExtension;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.io.avro.AvroDataReader;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.core.common.YesNoAuto;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.exceptions.StoragePipelineException;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;

import java.io.*;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.io.VariantReaderUtils.MALFORMED_FILE;

/**
 * Created on 01/04/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class VariantStoragePipelineTransformTest extends VariantStorageBaseTest {


    private static InputStream in;
    private static PrintStream out;

    @BeforeClass
    public static void beforeClass() throws Exception {
        in = System.in;
        out = System.out;
    }

    @Before
    public void setUp() throws Exception {
        // Reset input/output stream
        System.setIn(in);
        System.setOut(out);
    }

    @Test
    public void transformIsolated() throws Exception {

        ObjectMap params = new ObjectMap();
        params.put(VariantStorageOptions.STUDY.key(), STUDY_NAME);
        URI outputUri = newOutputUri();

        VariantStorageEngine variantStorageManager = getVariantStorageEngine();
        variantStorageManager.getConfiguration().getVariantEngine(variantStorageManager.getStorageEngineId()).getDatabase()
                .setHosts(Collections.singletonList("1.1.1.1"));
        StoragePipelineResult etlResult = runETL(variantStorageManager, smallInputUri, outputUri, params, true, true, false);
        System.out.println("etlResult = " + etlResult);


        String[] malformedFiles = Paths.get(outputUri).toFile().list((dir, name) -> name.contains(MALFORMED_FILE));
        assertEquals(0, malformedFiles.length);

    }

    /**
     * Corrupted file index. This test must fail
     */
    @Test
    public void corruptedTransformNoFailTest() throws Exception {

        ObjectMap params = new ObjectMap(VariantStorageOptions.TRANSFORM_FAIL_ON_MALFORMED_VARIANT.key(), YesNoAuto.YES);
        params.append(VariantStorageOptions.STUDY.key(), STUDY_NAME);

        URI outputUri = newOutputUri();

        thrown.expect(StoragePipelineException.class);
        try {
            runETL(getVariantStorageEngine(), corruptedInputUri, outputUri, params, true, true, false);
        } catch (StoragePipelineException e) {
            assertEquals(1, e.getResults().size());

            System.out.println(e.getResults().get(0));
            assertTrue(e.getResults().get(0).isTransformExecuted());
            assertNotNull(e.getResults().get(0).getTransformError());
            assertTrue(e.getResults().get(0).getTransformTimeMillis() > 0);
            assertFalse(e.getResults().get(0).isLoadExecuted());

            String[] malformedFiles = Paths.get(outputUri).toFile().list((dir, name) -> name.contains(MALFORMED_FILE));
            assertEquals(1, malformedFiles.length);
            throw e;
        }
    }

    @Test
    public void corruptedTransformTest() throws Exception {

        ObjectMap params = new ObjectMap()
                .append(VariantStorageOptions.TRANSFORM_FAIL_ON_MALFORMED_VARIANT.key(), YesNoAuto.NO)
                .append(VariantStorageOptions.TRANSFORM_BATCH_SIZE.key(), 2)
                .append(VariantStorageOptions.STUDY.key(), STUDY_NAME);
        URI outputUri = newOutputUri();
        StoragePipelineResult result = runETL(getVariantStorageEngine(), corruptedInputUri, outputUri, params, true, true, false);

        String[] malformedFiles = Paths.get(outputUri).toFile().list((dir, name) -> name.contains(MALFORMED_FILE));
        assertEquals(1, malformedFiles.length);
        assertEquals(2, result.getTransformStats().getInt("malformed lines"));
    }

    @Test
    public void transformFromSTDIN() throws Exception {

        URI outputUri = newOutputUri();

        VariantStorageEngine variantStorageManager = getVariantStorageEngine();

        InputStream inputStream = FileUtils.newInputStream(Paths.get(smallInputUri));
        System.setIn(inputStream);


        ObjectMap options = variantStorageManager.getOptions();

        options.append(VariantStorageOptions.STUDY.key(), STUDY_NAME);
        options.append(VariantStorageOptions.STDIN.key(), true);

        URI inputFile = Paths.get(smallInputUri).getFileName().toUri();
        System.out.println("inputFile = " + inputFile);
        assertFalse("File should not exist in that specific location", Paths.get(inputFile).toFile().exists());
        StoragePipelineResult storagePipelineResult =
                variantStorageManager.index(Collections.singletonList(inputFile), outputUri, true, true, false).get(0);

        assertEquals(999, countLinesFromAvro(Paths.get(storagePipelineResult.getTransformResult()).toFile()));
    }

    @Test
    public void transformToJsonSTDOUT() throws Exception {
        URI outputUri = newOutputUri();

        VariantStorageEngine variantStorageManager = getVariantStorageEngine();

        File outputFile = Paths.get(outputUri.resolve("outputFile.json")).toFile();
        try (PrintStream os = new PrintStream(new FileOutputStream(outputFile))) {
            System.setOut(os);

            ObjectMap options = variantStorageManager.getOptions();

            options.append(VariantStorageOptions.STDOUT.key(), true);
            options.append(VariantStorageOptions.TRANSFORM_FORMAT.key(), "json");
            options.append(VariantStorageOptions.STUDY.key(), STUDY_NAME);

            variantStorageManager.index(Collections.singletonList(smallInputUri), outputUri, true, true, false).get(0);
        } finally {
            System.setOut(out);
        }

        assertEquals(999, countLines(outputFile));
    }

    @Test
    public void transformToAvroSTDOUT() throws Exception {
        URI outputUri = newOutputUri();

        VariantStorageEngine variantStorageManager = getVariantStorageEngine();

        File outputFile = Paths.get(outputUri.resolve("outputFile.avro")).toFile();
        try (PrintStream os = new PrintStream(new FileOutputStream(outputFile))) {
            System.setOut(os);

            ObjectMap options = variantStorageManager.getOptions();

            options.append(VariantStorageOptions.STUDY.key(), STUDY_NAME);
            options.append(VariantStorageOptions.STDOUT.key(), true);
            options.append(VariantStorageOptions.TRANSFORM_FORMAT.key(), "avro");

            variantStorageManager.index(Collections.singletonList(smallInputUri), outputUri, true, true, false).get(0);
        } finally {
            System.setOut(out);
        }

        assertEquals(999, countLinesFromAvro(outputFile));
    }

    @Test
    public void transformGvcf() throws Exception {
        URI outputUri = newOutputUri();

        VariantStorageEngine variantStorageManager = getVariantStorageEngine();
        variantStorageManager.getOptions()
                .append(VariantStorageOptions.STUDY.key(), STUDY_NAME)
                .append(VariantStorageOptions.TRANSFORM_FORMAT.key(), "avro");

        URI platinumFile = getPlatinumFile(0);

        StoragePipelineResult result = variantStorageManager.index(Collections.singletonList(platinumFile), outputUri, true, true, false).get(0);
        List<Variant> list = variantStorageManager.getVariantReaderUtils().getVariantReader(result.getTransformResult(), null)
                .stream()
                .filter(v -> v.getReference().equals("N"))
                .collect(Collectors.toList());
        assertEquals(2, list.size());
        assertEquals("1:1-10000:N:.", list.get(0).toString());
        assertEquals("1:31001-54321:N:.", list.get(1).toString());
    }

    @Test
    public void transformNormalizeExtensions() throws Exception {
        URI outputUri = newOutputUri();

        VariantStorageEngine variantStorageManager = getVariantStorageEngine();
        variantStorageManager.getOptions().put(VariantStorageOptions.NORMALIZATION_EXTENSIONS.key(), "VAF");
        variantStorageManager.getOptions().put(VariantStorageOptions.STUDY.key(), "study1");

        URI platinumFile = getPlatinumFile(0);

        StoragePipelineResult result = variantStorageManager.index(Collections.singletonList(platinumFile), outputUri, true, true, true).get(0);
        List<Variant> list = variantStorageManager.getVariantReaderUtils().getVariantReader(result.getTransformResult(), null)
                .stream()
                .filter(v -> !v.getType().equals(VariantType.NO_VARIATION))
                .collect(Collectors.toList());
        for (Variant variant : list) {
            StudyEntry studyEntry = variant.getStudies().get(0);
//            System.out.println(variant + " = " + variant.toJson());
            assertThat(studyEntry.getSampleDataKeys(), CoreMatchers.hasItem(VafVariantNormalizerExtension.EXT_VAF));
            int idx = studyEntry.getSampleDataKeys().indexOf(VafVariantNormalizerExtension.EXT_VAF);
            assertNotNull(studyEntry.getSample(0).getData().get(idx));
            assertNotEquals("", studyEntry.getSample(0).getData().get(idx));
        }

        VariantFileMetadata variantFileMetadata = variantStorageManager.getVariantReaderUtils().readVariantFileMetadata(result.getTransformResult());
        List<VariantFileHeaderComplexLine> header = variantFileMetadata.getHeader().getComplexLines().stream()
                .filter(f -> f.getKey().equals("FORMAT") && f.getId().equals(VafVariantNormalizerExtension.EXT_VAF))
                .collect(Collectors.toList());
        assertEquals(1, header.size());
    }

    public int countLines(File outputFile) throws IOException {
        int numLines = 0;
        try (DataInputStream is = new DataInputStream(new FileInputStream(outputFile))) {
            String s;

            s = is.readLine();
            while (s != null) {
                numLines++;
                s = is.readLine();
            }
        }
        return numLines;
    }

    public int countLinesFromAvro(File file) {
        AvroDataReader<VariantAvro> reader = new AvroDataReader<>(file, VariantAvro.class);
        reader.open();
        reader.pre();
        List<VariantAvro> read = reader.read(2000);
        reader.post();
        reader.close();

        return read.size();
    }

}
