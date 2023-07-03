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

package org.opencb.opencga.storage.hadoop.variant;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExternalResource;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.operations.variant.VariantAggregateFamilyParams;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBAdaptor;

import java.util.*;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.*;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
@Category(MediumTests.class)
public class VariantTableRemoveTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();

    @Before
    public void setUp() throws Exception {
        clearDB(DB_NAME);
    }

    private VariantFileMetadata loadFile(String resource, StudyMetadata studyMetadata, Map<? extends String, ?> map) throws Exception {
        return VariantHbaseTestUtils.loadFile(getVariantStorageEngine(), DB_NAME, outputUri, resource, studyMetadata, map);
    }

    private void removeFile(String file, StudyMetadata studyMetadata, Map<? extends String, ?> map) throws Exception {
        Integer fileId = metadataManager.getFileId(studyMetadata.getId(), file);
        System.out.printf("Remove File ID %s for %s", fileId, file);
        VariantHbaseTestUtils.removeFile(getVariantStorageEngine(), file, studyMetadata, map, outputUri);
    }

    @Test
    @Ignore
    public void removeFileTestMergeAdvanced() throws Exception {
        StudyMetadata studyMetadata = VariantStorageBaseTest.newStudyMetadata();
        System.out.println("studyMetadata = " + studyMetadata);
        String studyName = studyMetadata.getName();

        ObjectMap options = new ObjectMap(HadoopVariantStorageOptions.VARIANT_TABLE_INDEXES_SKIP.key(), true)
                .append(VariantStorageOptions.MERGE_MODE.key(), VariantStorageEngine.MergeMode.ADVANCED);
        loadFile("s1.genome.vcf", studyMetadata, options);
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri());
        Map<String, Variant> variants = buildVariantsIdx();
        assertFalse(variants.containsKey("1:10014:A:G"));

        assertThat(variants.keySet(), hasItem("1:10013:T:C"));
        assertEquals("0/1", variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s1", "GT"));
        assertEquals(null, variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s2", "GT"));

        loadFile("s2.genome.vcf", studyMetadata, options);
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri());
        variants = buildVariantsIdx();
        assertThat(variants.keySet(), hasItem("1:10014:A:G"));
        assertEquals("0/2", variants.get("1:10014:A:G").getStudy(studyName).getSampleData("s1", "GT"));
        assertEquals("0/1", variants.get("1:10014:A:G").getStudy(studyName).getSampleData("s2", "GT"));

        assertThat(variants.keySet(), hasItem("1:10013:T:C"));
        assertEquals("0/1", variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s1", "GT"));
        assertEquals("0/0", variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s2", "GT"));

        // delete
        removeFile("s2.genome.vcf", studyMetadata, Collections.emptyMap());
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri());
        variants = buildVariantsIdx();
        if (variants.containsKey("1:10014:A:G")) {
            System.out.println(variants.get("1:10014:A:G").getImpl());
        }
        assertThat(variants.keySet(), not(hasItem("1:10014:A:G")));

        assertThat(variants.keySet(), hasItem("1:10013:T:C"));
        assertEquals("0/1", variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s1", "GT"));
        assertEquals(null, variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s2", "GT"));

        checkSampleIndexTable(studyMetadata, getVariantStorageEngine().getDBAdaptor(), "s2.genome.vcf");
    }

    @Test
    public void removeFileTestMergeBasic() throws Exception {
        StudyMetadata studyMetadata = VariantStorageBaseTest.newStudyMetadata();
        String studyName = studyMetadata.getName();

        ObjectMap options = new ObjectMap(HadoopVariantStorageOptions.VARIANT_TABLE_INDEXES_SKIP.key(), true)
                .append(VariantStorageOptions.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC);
        loadFile("s1.genome.vcf", studyMetadata, options);
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri());
        Map<String, Variant> variants = buildVariantsIdx();
        assertFalse(variants.containsKey("1:10014:A:G"));

        assertThat(variants.keySet(), hasItem("1:10013:T:C"));
        assertEquals("0/1", variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s1", "GT"));
        assertEquals(null, variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s2", "GT"));

        loadFile("s2.genome.vcf", studyMetadata, options);
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri());
        variants = buildVariantsIdx();
        assertThat(variants.keySet(), hasItem("1:10014:A:G"));
//        assertEquals("0/2", variants.get("1:10014:A:G").getStudy(studyName).getSampleData("s1", "GT"));
        assertEquals("0/1", variants.get("1:10014:A:G").getStudy(studyName).getSampleData("s2", "GT"));

        assertThat(variants.keySet(), hasItem("1:10013:T:C"));
        assertEquals("0/1", variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s1", "GT"));
//        assertEquals("0/0", variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s2", "GT"));

        // delete
        removeFile("s2.genome.vcf", studyMetadata, Collections.emptyMap());
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri());
        variants = buildVariantsIdx();
        if (variants.containsKey("1:10014:A:G")) {
            System.out.println(variants.get("1:10014:A:G").getImpl());
        }
        checkSampleIndexTable(studyMetadata, getVariantStorageEngine().getDBAdaptor(), "s2.genome.vcf");

        assertThat(variants.keySet(), hasItem("1:10013:T:C"));
        assertEquals("0/1", variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s1", "GT"));
        assertEquals(null, variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s2", "GT"));

        assertThat(variants.keySet(), hasItem("1:10014:A:G"));
        variantStorageEngine.calculateStats(studyMetadata.getName(), Collections.singletonList(StudyEntry.DEFAULT_COHORT), new QueryOptions());
        getVariantStorageEngine().variantsPrune(false, false, newOutputUri());
        variants = buildVariantsIdx();
        assertThat(variants.keySet(), not(hasItem("1:10014:A:G")));
    }

    @Test
    public void removeFileTestMultiFile() throws Exception {
        StudyMetadata studyMetadata = VariantStorageBaseTest.newStudyMetadata();

        loadFile("s1.genome.vcf", studyMetadata, new ObjectMap());
        loadFile("s2.genome.vcf", studyMetadata, new ObjectMap());
        loadFile("s1_s2.genome.vcf", studyMetadata, new ObjectMap(VariantStorageOptions.LOAD_MULTI_FILE_DATA.key(), true));
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri());
        removeFile("s2.genome.vcf", studyMetadata, Collections.emptyMap());
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri());
        variantStorageEngine.calculateStats(studyMetadata.getName(), Collections.singletonList(StudyEntry.DEFAULT_COHORT), new QueryOptions());

        List<Variant> results = variantStorageEngine.getDBAdaptor().get(new VariantQuery().sample("s2"), new QueryOptions(VariantHadoopDBAdaptor.NATIVE, false)).getResults();
        assertNotEquals(0, results.size());

        results = variantStorageEngine.getDBAdaptor().get(new VariantQuery().sample("s2").includeFile(ParamConstants.NONE), new QueryOptions(VariantHadoopDBAdaptor.NATIVE, false)).getResults();
        assertNotEquals(0, results.size());
    }

    @Test
    public void removeFileTestMergeBasicFillGaps() throws Exception {
        StudyMetadata studyMetadata = VariantStorageBaseTest.newStudyMetadata();
        System.out.println("studyMetadata = " + studyMetadata);
        String studyName = studyMetadata.getName();

        ObjectMap options = new ObjectMap(HadoopVariantStorageOptions.VARIANT_TABLE_INDEXES_SKIP.key(), true)
                .append(VariantStorageOptions.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC);
        loadFile("s1.genome.vcf", studyMetadata, options);
        VariantHadoopDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        VariantHbaseTestUtils.printVariants(dbAdaptor, newOutputUri());
        Map<String, Variant> variants = buildVariantsIdx();
        assertFalse(variants.containsKey("1:10014:A:G"));

        assertThat(variants.keySet(), hasItem("1:10013:T:C"));
        assertEquals("0/1", variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s1", "GT"));
        assertEquals(null, variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s2", "GT"));

        loadFile("s2.genome.vcf", studyMetadata, options);

        ((VariantStorageEngine) getVariantStorageEngine()).aggregateFamily(studyName, new VariantAggregateFamilyParams(Arrays.asList("s1", "s2"), false), options);

        VariantHbaseTestUtils.printVariants(dbAdaptor, newOutputUri());
        variants = buildVariantsIdx();
        assertThat(variants.keySet(), hasItem("1:10014:A:G"));
        assertEquals("0/2", variants.get("1:10014:A:G").getStudy(studyName).getSampleData("s1", "GT"));
        assertEquals("0/1", variants.get("1:10014:A:G").getStudy(studyName).getSampleData("s2", "GT"));

        assertThat(variants.keySet(), hasItem("1:10013:T:C"));
        assertEquals("0/1", variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s1", "GT"));
        assertEquals("0/0", variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s2", "GT"));

        // delete
        removeFile("s2.genome.vcf", studyMetadata, Collections.emptyMap());
        VariantHbaseTestUtils.printVariants(dbAdaptor, newOutputUri());
        variants = buildVariantsIdx();
        if (variants.containsKey("1:10014:A:G")) {
            System.out.println(variants.get("1:10014:A:G").getImpl());
        }

        checkSampleIndexTable(studyMetadata, dbAdaptor, "s2.genome.vcf");

        // FIXME: This variant should be removed!
        // assertThat(variants.keySet(), not(hasItem("1:10014:A:G")));

        assertThat(variants.keySet(), hasItem("1:10013:T:C"));
        assertEquals("0/1", variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s1", "GT"));
        assertEquals(null, variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s2", "GT"));
    }

    @Test
    public void removeSingleFileTest() throws Exception {
        StudyMetadata studyMetadata = VariantStorageBaseTest.newStudyMetadata();
        System.out.println("studyMetadata = " + studyMetadata);
        String studyName = studyMetadata.getName();

        Map<String, Object> options = Collections.singletonMap(HadoopVariantStorageOptions.VARIANT_TABLE_INDEXES_SKIP.key(), true);
        loadFile("s1.genome.vcf", studyMetadata, options);
        Map<String, Variant> variants = buildVariantsIdx();

        assertFalse(variants.containsKey("1:10014:A:G"));
        assertTrue(variants.containsKey("1:10013:T:C"));
        assertEquals("0/1", variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s1", "GT"));

        VariantHadoopDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri());
        // delete
        removeFile("s1.genome.vcf", studyMetadata, options);

        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri());

        checkSampleIndexTable(studyMetadata, dbAdaptor, "s1.genome.vcf");

        variants = buildVariantsIdx();
        assertEquals("Expected none variants", 0, variants.size());
        assertEquals("Expected none indexed files", 0, metadataManager.getIndexedFiles(studyMetadata.getId()).size());
    }

    private Map<String, Variant> buildVariantsIdx() throws Exception {
        VariantHadoopDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        Map<String, Variant> variants = new HashMap<>();
        System.out.println("Build Variant map");
        for (Variant variant : dbAdaptor.iterable(new VariantQuery().includeSampleAll().study(STUDY_NAME), new QueryOptions())) {
            if (variant.getStudies().isEmpty()) {
                continue;
            }
            String v = variant.toString();
            assertFalse(variants.containsKey(v));
            variants.put(v, variant);
            VariantAnnotation a = variant.getAnnotation();
            variant.setAnnotation(null);
            System.out.println(variant.toJson());
            variant.setAnnotation(a);
        }
        System.out.println("End. size : " + variants.size());
        return variants;
    }

    protected void checkSampleIndexTable(StudyMetadata studyMetadata, VariantHadoopDBAdaptor dbAdaptor, String removedFile) throws Exception {
        FileMetadata fileMetadata = metadataManager.getFileMetadata(studyMetadata.getId(), removedFile);
        LinkedHashSet<Integer> sampleIds = fileMetadata.getSamples();
        SampleIndexDBAdaptor sampleIndexDBAdaptor = new SampleIndexDBAdaptor(dbAdaptor.getHBaseManager(),
                dbAdaptor.getTableNameGenerator(), dbAdaptor.getMetadataManager());
        for (Integer sampleId : sampleIds) {
            assertFalse(sampleIndexDBAdaptor.iteratorByGt(studyMetadata.getId(), sampleId).hasNext());
        }
    }

}
