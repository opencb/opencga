/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.storage.core.variant;

import org.junit.*;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.json.VariantJsonReader;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

/**
 * @author Jacobo Coll <jacobo167@gmail.com>
 */
@Ignore
public abstract class VariantStorageManagerTest extends VariantStorageManagerTestUtils {

    @Test
    public void basicIndex() throws Exception {
        clearDB(DB_NAME);
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        ETLResult etlResult = runDefaultETL(variantStorageManager, studyConfiguration);
        Assert.assertTrue("Incorrect transform file extension " + etlResult.transformResult + ". Expected 'variants.json.snappy'" ,
                Paths.get(etlResult.transformResult).toFile().getName().endsWith("variants.json.snappy"));

        checkTransformedVariants(etlResult.transformResult, studyConfiguration);
        checkLoadedVariants(variantStorageManager.getDBAdaptor(null), studyConfiguration, true, false);
    }

    /**
     * Single Thread indexation. "Old Style" indexation
     *  With samples and "src"
     *  Gzip compression
     **/
    @Test
    public void singleThreadIndex() throws Exception {
        clearDB(DB_NAME);
        ObjectMap params = new ObjectMap();
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        params.put(VariantStorageManager.STUDY_CONFIGURATION, studyConfiguration);
        params.put(VariantStorageManager.FILE_ID, 6);
        params.put(VariantStorageManager.COMPRESS_METHOD, "gZiP");
        params.put(VariantStorageManager.TRANSFORM_THREADS, 1);
        params.put(VariantStorageManager.LOAD_THREADS, 1);
        params.put(VariantStorageManager.INCLUDE_GENOTYPES, true);
        params.put(VariantStorageManager.INCLUDE_SRC, true);
        params.put(VariantStorageManager.DB_NAME, DB_NAME);
        ETLResult etlResult = runETL(variantStorageManager, params, true, true, true);

        Assert.assertTrue("Incorrect transform file extension " + etlResult.transformResult + ". Expected 'variants.json.gz'" ,
                Paths.get(etlResult.transformResult).toFile().getName().endsWith("variants.json.gz"));

        checkTransformedVariants(etlResult.transformResult, studyConfiguration);
        checkLoadedVariants(variantStorageManager.getDBAdaptor(null), studyConfiguration, true, false);

    }

    /**
     * Fast indexation.
     *  Without "src" and samples information.
     *  MultiThreads
     *  CompressMethod snappy
     *
     **/
    @Test
    public void fastIndex() throws Exception {
        clearDB(DB_NAME);
        ObjectMap params = new ObjectMap();
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        params.put(VariantStorageManager.STUDY_CONFIGURATION, studyConfiguration);
        params.put(VariantStorageManager.FILE_ID, 6);
        params.put(VariantStorageManager.COMPRESS_METHOD, "snappy");
        params.put(VariantStorageManager.TRANSFORM_THREADS, 8);
        params.put(VariantStorageManager.LOAD_THREADS, 8);
        params.put(VariantStorageManager.INCLUDE_GENOTYPES, false);
        params.put(VariantStorageManager.INCLUDE_SRC, false);
        params.put(VariantStorageManager.DB_NAME, DB_NAME);
        ETLResult etlResult = runETL(variantStorageManager, params, true, true, true);

        Assert.assertTrue("Incorrect transform file extension " + etlResult.transformResult + ". Expected 'variants.json.snappy'" ,
                Paths.get(etlResult.transformResult).toFile().getName().endsWith("variants.json.snappy"));

        checkTransformedVariants(etlResult.transformResult, studyConfiguration);
        checkLoadedVariants(variantStorageManager.getDBAdaptor(null), studyConfiguration, false, false);

    }

    /**
     * Corrupted file index. This test must fail
     */
    @Test
    public void corruptedIndexTest() throws Exception {

        thrown.expect(StorageManagerException.class);
        runDefaultETL(corruptedInputUri, getVariantStorageManager(), newStudyConfiguration());

    }
    /* ---------------------------------------------------- */
    /* Check methods for loaded and transformed Variants    */
    /* ---------------------------------------------------- */


    private void checkTransformedVariants(URI variantsJson, StudyConfiguration studyConfiguration) {
        long start = System.currentTimeMillis();
        VariantJsonReader variantJsonReader = new VariantJsonReader(new VariantSource(VCF_TEST_FILE_NAME, "6", "", ""),
                variantsJson.getPath(),
                variantsJson.getPath().replace("variants", "file"));

        variantJsonReader.open();
        variantJsonReader.pre();

        List<Variant> read;
        int numVariants = 0;
        while ((read = variantJsonReader.read(100)) != null && !read.isEmpty()) {
            numVariants += read.size();
        }

        variantJsonReader.post();
        variantJsonReader.close();

        Assert.assertEquals(NUM_VARIANTS, numVariants); //9792
        logger.info("checkTransformedVariants time : " + (System.currentTimeMillis() - start) / 1000.0 + "s");
    }

    private void checkLoadedVariants(VariantDBAdaptor dbAdaptor, StudyConfiguration studyConfiguration, boolean includeSamples, boolean includeSrc) {
        long start = System.currentTimeMillis();
        int numVariants = 0;
        String expectedStudyId = Integer.toString(studyConfiguration.getStudyId());
        QueryResult allVariants = dbAdaptor.getAllVariants(new QueryOptions("limit", 1));
        Assert.assertEquals(1, allVariants.getNumResults());
        Assert.assertEquals(NUM_VARIANTS, allVariants.getNumTotalResults());
        for (Variant variant : dbAdaptor) {
            for (Map.Entry<String, VariantSourceEntry> entry : variant.getSourceEntries().entrySet()) {
                Assert.assertEquals(expectedStudyId, entry.getValue().getStudyId());
                if (includeSamples) {
                    Assert.assertNotNull(entry.getValue().getSamplesData());
                    Assert.assertNotEquals(0, entry.getValue().getSamplesData().size());

                    Assert.assertEquals(studyConfiguration.getSampleIds().size(), entry.getValue().getSamplesData().size());
                    Assert.assertEquals(studyConfiguration.getSampleIds().keySet(), entry.getValue().getSamplesData().keySet());
                }
                if (includeSrc) {
                    Assert.assertNotNull(entry.getValue().getAttribute("src"));
                }
            }
            numVariants++;
        }
        Assert.assertEquals(NUM_VARIANTS, numVariants); //9792
        logger.info("checkLoadedVariants time : " + (System.currentTimeMillis() - start)/1000.0 + "s");
    }


}
