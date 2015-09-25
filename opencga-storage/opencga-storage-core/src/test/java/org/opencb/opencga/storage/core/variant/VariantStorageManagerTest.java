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
import org.opencb.biodata.models.variant.VariantStudy;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.io.json.VariantJsonReader;

import java.net.URI;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Ignore
public abstract class VariantStorageManagerTest extends VariantStorageManagerTestUtils {

    @Test
    public void basicIndex() throws Exception {
        clearDB(DB_NAME);
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        ETLResult etlResult = runDefaultETL(variantStorageManager, studyConfiguration);
        assertTrue("Incorrect transform file extension " + etlResult.transformResult + ". Expected 'variants.json.snappy'",
                Paths.get(etlResult.transformResult).toFile().getName().endsWith("variants.json.gz"));

        assertTrue(studyConfiguration.getIndexedFiles().contains(6));
        checkTransformedVariants(etlResult.transformResult, studyConfiguration);
        checkLoadedVariants(variantStorageManager.getDBAdaptor(DB_NAME), studyConfiguration, true, false);
    }

    @Test
    public void multiIndex() throws Exception {
        clearDB(DB_NAME);
        int expectedNumVariants = NUM_VARIANTS - 37; //37 variants have been removed from this dataset because had the genotype 0|0 for each sample
        StudyConfiguration studyConfigurationMultiFile = new StudyConfiguration(1, "multi");

        ETLResult etlResult;
        ObjectMap options = new ObjectMap()
                .append(VariantStorageManager.Options.STUDY_TYPE.key(), VariantStudy.StudyType.CONTROL)
                .append(VariantStorageManager.Options.CALCULATE_STATS.key(), true)
                .append(VariantStorageManager.Options.ANNOTATE.key(), false);
        runDefaultETL(getResourceUri("1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"), variantStorageManager, studyConfigurationMultiFile, options.append(VariantStorageManager.Options.FILE_ID.key(), 5));
        Integer defaultCohortId = studyConfigurationMultiFile.getCohortIds().get(VariantSourceEntry.DEFAULT_COHORT);
        assertTrue(studyConfigurationMultiFile.getCohorts().containsKey(defaultCohortId));
        assertEquals(500, studyConfigurationMultiFile.getCohorts().get(defaultCohortId).size());
        assertTrue(studyConfigurationMultiFile.getIndexedFiles().contains(5));
        runDefaultETL(getResourceUri("501-1000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"), variantStorageManager, studyConfigurationMultiFile, options.append(VariantStorageManager.Options.FILE_ID.key(), 6));
        assertEquals(1000, studyConfigurationMultiFile.getCohorts().get(defaultCohortId).size());
        assertTrue(studyConfigurationMultiFile.getIndexedFiles().contains(6));
        runDefaultETL(getResourceUri("1001-1500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"), variantStorageManager, studyConfigurationMultiFile, options.append(VariantStorageManager.Options.FILE_ID.key(), 7));
        assertEquals(1500, studyConfigurationMultiFile.getCohorts().get(defaultCohortId).size());
        assertTrue(studyConfigurationMultiFile.getIndexedFiles().contains(7));
        runDefaultETL(getResourceUri("1501-2000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"), variantStorageManager, studyConfigurationMultiFile, options.append(VariantStorageManager.Options.FILE_ID.key(), 8));
        assertEquals(2000, studyConfigurationMultiFile.getCohorts().get(defaultCohortId).size());
        assertTrue(studyConfigurationMultiFile.getIndexedFiles().contains(8));
        runDefaultETL(getResourceUri("2001-2504.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"), variantStorageManager, studyConfigurationMultiFile, options.append(VariantStorageManager.Options.FILE_ID.key(), 9));
        assertEquals(2504, studyConfigurationMultiFile.getCohorts().get(defaultCohortId).size());
        assertTrue(studyConfigurationMultiFile.getIndexedFiles().contains(5));
        assertTrue(studyConfigurationMultiFile.getIndexedFiles().contains(6));
        assertTrue(studyConfigurationMultiFile.getIndexedFiles().contains(7));
        assertTrue(studyConfigurationMultiFile.getIndexedFiles().contains(8));
        assertTrue(studyConfigurationMultiFile.getIndexedFiles().contains(9));

        VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(DB_NAME);
        checkLoadedVariants(dbAdaptor, studyConfigurationMultiFile, true, false, expectedNumVariants);


        //Load, in a new study, the same dataset in one single file
        StudyConfiguration studyConfigurationSingleFile = new StudyConfiguration(2, "single");
        etlResult = runDefaultETL(getResourceUri("filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"), variantStorageManager, studyConfigurationSingleFile, options.append(VariantStorageManager.Options.FILE_ID.key(), 10));
        assertTrue(studyConfigurationSingleFile.getIndexedFiles().contains(10));

        checkTransformedVariants(etlResult.transformResult, studyConfigurationSingleFile, expectedNumVariants);


        //Check that both studies contains the same information
        VariantDBIterator iterator = dbAdaptor.iterator(new Query(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), studyConfigurationMultiFile.getStudyId() + "," + studyConfigurationSingleFile.getStudyId()), new QueryOptions());
        int numVariants = 0;
        for (; iterator.hasNext();) {
            Variant variant = iterator.next();
            numVariants++;
//            Map<String, VariantSourceEntry> map = variant.getSourceEntries().values().stream().collect(Collectors.toMap(VariantSourceEntry::getStudyId, Function.<VariantSourceEntry>identity()));
            Map<String, VariantSourceEntry> map = variant.getSourceEntries();

            assertTrue(map.containsKey(studyConfigurationMultiFile.getStudyName()));
            assertTrue(map.containsKey(studyConfigurationSingleFile.getStudyName()));
            assertEquals(map.get(studyConfigurationSingleFile.getStudyName()).getSamplesData(), map.get(studyConfigurationMultiFile.getStudyName()).getSamplesData());
        }
        assertEquals(expectedNumVariants, numVariants);

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
        params.put(VariantStorageManager.Options.STUDY_CONFIGURATION.key(), studyConfiguration);
        params.put(VariantStorageManager.Options.FILE_ID.key(), 6);
        params.put(VariantStorageManager.Options.COMPRESS_METHOD.key(), "gZiP");
        params.put(VariantStorageManager.Options.TRANSFORM_THREADS.key(), 1);
        params.put(VariantStorageManager.Options.LOAD_THREADS.key(), 1);
        params.put(VariantStorageManager.Options.INCLUDE_GENOTYPES.key(), true);
        params.put(VariantStorageManager.Options.INCLUDE_SRC.key(), true);
        params.put(VariantStorageManager.Options.DB_NAME.key(), DB_NAME);
        ETLResult etlResult = runETL(variantStorageManager, params, true, true, true);

        assertTrue("Incorrect transform file extension " + etlResult.transformResult + ". Expected 'variants.json.gz'",
                Paths.get(etlResult.transformResult).toFile().getName().endsWith("variants.json.gz"));

        assertTrue(studyConfiguration.getIndexedFiles().contains(6));
        checkTransformedVariants(etlResult.transformResult, studyConfiguration);
        checkLoadedVariants(variantStorageManager.getDBAdaptor(DB_NAME), studyConfiguration, true, false);

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
        params.put(VariantStorageManager.Options.STUDY_CONFIGURATION.key(), studyConfiguration);
        params.put(VariantStorageManager.Options.FILE_ID.key(), 6);
        params.put(VariantStorageManager.Options.COMPRESS_METHOD.key(), "snappy");
        params.put(VariantStorageManager.Options.TRANSFORM_THREADS.key(), 8);
        params.put(VariantStorageManager.Options.LOAD_THREADS.key(), 8);
        params.put(VariantStorageManager.Options.INCLUDE_GENOTYPES.key(), false);
        params.put(VariantStorageManager.Options.INCLUDE_SRC.key(), false);
        params.put(VariantStorageManager.Options.DB_NAME.key(), DB_NAME);
        ETLResult etlResult = runETL(variantStorageManager, params, true, true, true);

        assertTrue("Incorrect transform file extension " + etlResult.transformResult + ". Expected 'variants.json.snappy'",
                Paths.get(etlResult.transformResult).toFile().getName().endsWith("variants.json.snappy"));

        assertTrue(studyConfiguration.getIndexedFiles().contains(6));
        checkTransformedVariants(etlResult.transformResult, studyConfiguration);
        checkLoadedVariants(variantStorageManager.getDBAdaptor(DB_NAME), studyConfiguration, false, false);

    }

    /**
     * Corrupted file index. This test must fail
     */
    @Test
    public void corruptedIndexTest() throws Exception {

        thrown.expect(StorageManagerException.class);
        runDefaultETL(corruptedInputUri, getVariantStorageManager(), newStudyConfiguration());

    }

    @Test
    public void checkAndUpdateStudyConfigurationWithoutSampleIdsTest() throws StorageManagerException {
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        studyConfiguration.getSampleIds().put("s0", 1);
        studyConfiguration.getSampleIds().put("s10", 4);
        Integer fileId = 5;
        VariantSource source = createVariantSource(studyConfiguration, fileId);
        ObjectMap options = new ObjectMap();
        VariantStorageManager.checkAndUpdateStudyConfiguration(studyConfiguration, fileId, source, options);
        assertTrue(studyConfiguration.getSampleIds().keySet().containsAll(Arrays.asList("s0", "s1", "s2", "s3", "s4", "s5")));
        assertTrue(studyConfiguration.getSamplesInFiles().get(fileId).stream()
                        .map(s -> studyConfiguration.getSampleIds().inverse().get(s))
                        .collect(Collectors.toList())
                        .equals(Arrays.asList("s0", "s1", "s2", "s3", "s4", "s5"))
        );
        assertEquals(Integer.valueOf(1), studyConfiguration.getSampleIds().get("s0"));
        studyConfiguration.getSamplesInFiles().get(fileId).forEach((i) -> System.out.println(studyConfiguration.getSampleIds().inverse().get(i) + " = " + i));
    }

    @Test
    public void checkAndUpdateStudyConfigurationWithSampleIdsTest() throws StorageManagerException {
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        Integer fileId = 5;
        VariantSource source = createVariantSource(studyConfiguration, fileId);
        studyConfiguration.getSampleIds().put("s10", 4);
        ObjectMap options = new ObjectMap(VariantStorageManager.Options.SAMPLE_IDS.key(), "s0:20,s1:21,s2:22,s3:23,s4:24,s5:25");
        VariantStorageManager.checkAndUpdateStudyConfiguration(studyConfiguration, fileId, source, options);
        assertTrue(studyConfiguration.getSampleIds().keySet().containsAll(Arrays.asList("s0", "s1", "s2", "s3", "s4", "s5")));
        assertEquals(Arrays.asList("s0", "s1", "s2", "s3", "s4", "s5"),
                studyConfiguration.getSamplesInFiles().get(fileId).stream()
                        .map(s -> studyConfiguration.getSampleIds().inverse().get(s))
                        .collect(Collectors.toList())
        );
        assertEquals(Arrays.asList(20, 21, 22, 23, 24, 25), new ArrayList<>(studyConfiguration.getSamplesInFiles().get(fileId)));
        assertEquals(Integer.valueOf(20), studyConfiguration.getSampleIds().get("s0"));
        assertEquals(Integer.valueOf(21), studyConfiguration.getSampleIds().get("s1"));
        assertEquals(Integer.valueOf(22), studyConfiguration.getSampleIds().get("s2"));
        assertEquals(Integer.valueOf(23), studyConfiguration.getSampleIds().get("s3"));
        assertEquals(Integer.valueOf(24), studyConfiguration.getSampleIds().get("s4"));
        assertEquals(Integer.valueOf(25), studyConfiguration.getSampleIds().get("s5"));
        studyConfiguration.getSamplesInFiles().get(fileId).forEach((i) -> System.out.println(studyConfiguration.getSampleIds().inverse().get(i) + " = " + i));
    }

    @Test
    public void checkAndUpdateStudyConfigurationWithSamplesInFilesTest() throws StorageManagerException {
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        Integer fileId = 5;
        VariantSource source = createVariantSource(studyConfiguration, fileId);
        ObjectMap options = new ObjectMap(VariantStorageManager.Options.SAMPLE_IDS.key(), "s0:20,s1:21,s2:22,s3:23,s4:24,s5:25");
        studyConfiguration.getSamplesInFiles().put(fileId, new LinkedHashSet<>(Arrays.asList(20, 21, 22, 23, 24, 25)));
        VariantStorageManager.checkAndUpdateStudyConfiguration(studyConfiguration, fileId, source, options);
    }

    @Test
    public void checkAndUpdateStudyConfigurationWithRepeatedSampleIdsTest() throws StorageManagerException {
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        Integer fileId = 5;
        VariantSource source = createVariantSource(studyConfiguration, fileId);
        ObjectMap options = new ObjectMap(VariantStorageManager.Options.SAMPLE_IDS.key(), "s0:20,s1:21,s2:22,s3:23,s4:24,s5:25");
        studyConfiguration.getSampleIds().put("s0", 0);

        thrown.expect(StorageManagerException.class);
        thrown.expectMessage("s0:20");   //Already present
        VariantStorageManager.checkAndUpdateStudyConfiguration(studyConfiguration, fileId, source, options);
    }

    @Test
    public void checkAndUpdateStudyConfigurationWithExtraSampleIdsTest() throws StorageManagerException {
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        Integer fileId = 5;
        VariantSource source = createVariantSource(studyConfiguration, fileId);
        ObjectMap options = new ObjectMap(VariantStorageManager.Options.SAMPLE_IDS.key(), "s0:20,s1:21,s2:22,s3:23,s4:24,s5:25,UNEXISTING_SAMPLE:30");

        thrown.expect(StorageManagerException.class);
        thrown.expectMessage("UNEXISTING_SAMPLE");   //Not in file
        VariantStorageManager.checkAndUpdateStudyConfiguration(studyConfiguration, fileId, source, options);
    }

    @Test
    public void checkAndUpdateStudyConfigurationWithAlphanumericSampleIdsTest() throws StorageManagerException {
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        Integer fileId = 5;
        VariantSource source = createVariantSource(studyConfiguration, fileId);
        ObjectMap options = new ObjectMap(VariantStorageManager.Options.SAMPLE_IDS.key(), "s0:20,s1:21,s2:22,s3:23,s4:NaN,s5:25");

        thrown.expect(StorageManagerException.class);
        thrown.expectMessage("NaN");   //Not a number
        VariantStorageManager.checkAndUpdateStudyConfiguration(studyConfiguration, fileId, source, options);
    }

    @Test
    public void checkAndUpdateStudyConfigurationWithMalformedSampleIds1Test() throws StorageManagerException {
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        Integer fileId = 5;
        VariantSource source = createVariantSource(studyConfiguration, fileId);
        ObjectMap options = new ObjectMap(VariantStorageManager.Options.SAMPLE_IDS.key(), "s0:20,s1:21,s2:22,s3:23,s4:24,s5:");

        thrown.expect(StorageManagerException.class);
        thrown.expectMessage("s5:");   //Malformed
        VariantStorageManager.checkAndUpdateStudyConfiguration(studyConfiguration, fileId, source, options);
    }
    @Test
    public void checkAndUpdateStudyConfigurationWithMalformedSampleIds2Test() throws StorageManagerException {
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        Integer fileId = 5;
        VariantSource source = createVariantSource(studyConfiguration, fileId);
        ObjectMap options = new ObjectMap(VariantStorageManager.Options.SAMPLE_IDS.key(), "s0:20,s1:21,s2:22,s3,s4:24,s5:25");

        thrown.expect(StorageManagerException.class);
        thrown.expectMessage("s3");   //Malformed
        VariantStorageManager.checkAndUpdateStudyConfiguration(studyConfiguration, fileId, source, options);
    }

    @Test
    public void checkAndUpdateStudyConfigurationWithMissingSampleIdsTest() throws StorageManagerException {
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        Integer fileId = 5;
        VariantSource source = createVariantSource(studyConfiguration, fileId);
        ObjectMap options = new ObjectMap(VariantStorageManager.Options.SAMPLE_IDS.key(), "s0:20");

        thrown.expect(StorageManagerException.class);
        thrown.expectMessage("[s1, s2, s3, s4, s5]");   //Missing samples
        VariantStorageManager.checkAndUpdateStudyConfiguration(studyConfiguration, fileId, source, options);
    }

    @Test
    public void checkAndUpdateStudyConfigurationWithMissingSamplesInFilesTest() throws StorageManagerException {
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        Integer fileId = 5;
        VariantSource source = createVariantSource(studyConfiguration, fileId);
        ObjectMap options = new ObjectMap(VariantStorageManager.Options.SAMPLE_IDS.key(), "s0:20,s1:21,s2:22,s3:23,s4:24,s5:25");
        studyConfiguration.getSamplesInFiles().put(fileId, new LinkedHashSet<>(Arrays.asList(20, 21, 22, 23, 24)));
        thrown.expect(StorageManagerException.class);
        thrown.expectMessage("s5");
        VariantStorageManager.checkAndUpdateStudyConfiguration(studyConfiguration, fileId, source, options);
    }

    @Test
    public void checkAndUpdateStudyConfigurationWithExtraSamplesInFilesTest() throws StorageManagerException {
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        Integer fileId = 5;
        VariantSource source = createVariantSource(studyConfiguration, fileId);
        ObjectMap options = new ObjectMap(VariantStorageManager.Options.SAMPLE_IDS.key(), "s0:20,s1:21,s2:22,s3:23,s4:24,s5:25");
        studyConfiguration.getSampleIds().put("GhostSample", 0);
        studyConfiguration.getSamplesInFiles().put(fileId, new LinkedHashSet<>(Arrays.asList(20, 21, 22, 23, 24, 25, 0)));
        thrown.expect(StorageManagerException.class);
        VariantStorageManager.checkAndUpdateStudyConfiguration(studyConfiguration, fileId, source, options);
    }

    protected VariantSource createVariantSource(StudyConfiguration studyConfiguration, Integer fileId) {
        studyConfiguration.getFileIds().put("fileName", fileId);
        VariantSource source = new VariantSource("fileName", fileId.toString(), studyConfiguration.getStudyId() + "" , studyConfiguration.getStudyName());
        source.getSamplesPosition().put("s0", 0);
        source.getSamplesPosition().put("s1", 1);
        source.getSamplesPosition().put("s2", 2);
        source.getSamplesPosition().put("s3", 3);
        source.getSamplesPosition().put("s4", 4);
        source.getSamplesPosition().put("s5", 5);
        return source;
    }

    /* ---------------------------------------------------- */
    /* Check methods for loaded and transformed Variants    */
    /* ---------------------------------------------------- */


    private void checkTransformedVariants(URI variantsJson, StudyConfiguration studyConfiguration) {
        checkTransformedVariants(variantsJson, studyConfiguration, NUM_VARIANTS);
    }

    private void checkTransformedVariants(URI variantsJson, StudyConfiguration studyConfiguration, int expectedNumVariants) {
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

        assertEquals(expectedNumVariants, numVariants); //9792
        logger.info("checkTransformedVariants time : " + (System.currentTimeMillis() - start) / 1000.0 + "s");
    }

    private void checkLoadedVariants(VariantDBAdaptor dbAdaptor, StudyConfiguration studyConfiguration, boolean includeSamples, boolean includeSrc) {
        checkLoadedVariants(dbAdaptor, studyConfiguration, includeSamples, includeSrc, NUM_VARIANTS/*9792*/);
    }

    private void checkLoadedVariants(VariantDBAdaptor dbAdaptor, StudyConfiguration studyConfiguration, boolean includeSamples, boolean includeSrc, int expectedNumVariants) {
        long start = System.currentTimeMillis();
        int numVariants = 0;
        String expectedStudyId = studyConfiguration.getStudyName();
        QueryResult allVariants = dbAdaptor.get(new Query(), new QueryOptions("limit", 1));
        assertEquals(1, allVariants.getNumResults());
        assertEquals(expectedNumVariants, allVariants.getNumTotalResults());
        for (Integer fileId : studyConfiguration.getIndexedFiles()) {
            assertTrue(studyConfiguration.getHeaders().containsKey(fileId));
        }
        for (Variant variant : dbAdaptor) {
            for (Map.Entry<String, VariantSourceEntry> entry : variant.getSourceEntries().entrySet()) {
                assertEquals(expectedStudyId, entry.getValue().getStudyId());
                if (includeSamples) {
                    Assert.assertNotNull(entry.getValue().getSamplesData());
                    Assert.assertEquals(2504, entry.getValue().getSamplesData().size());

                    assertEquals(studyConfiguration.getSampleIds().size(), entry.getValue().getSamplesData().size());
                    assertEquals(studyConfiguration.getSampleIds().keySet(), entry.getValue().getSamplesData().keySet());
                }
                if (includeSrc) {
                    Assert.assertNotNull(entry.getValue().getAttribute("src"));
                }
                for (Integer cohortId : studyConfiguration.getCalculatedStats()) {
                    String cohortName = StudyConfiguration.inverseMap(studyConfiguration.getCohortIds()).get(cohortId);
                    assertTrue(entry.getValue().getCohortStats().containsKey(cohortName));
                    assertEquals(variant + " has incorrect stats for cohort \"" + cohortName + "\":" + cohortId,
                            studyConfiguration.getCohorts().get(cohortId).size(),
                            entry.getValue().getCohortStats().get(cohortName).getGenotypesCount().values().stream().reduce((a, b) -> a + b).orElse(0).intValue());
                }
            }
            numVariants++;
        }
        assertEquals(expectedNumVariants, numVariants);
        logger.info("checkLoadedVariants time : " + (System.currentTimeMillis() - start)/1000.0 + "s");
    }


}
