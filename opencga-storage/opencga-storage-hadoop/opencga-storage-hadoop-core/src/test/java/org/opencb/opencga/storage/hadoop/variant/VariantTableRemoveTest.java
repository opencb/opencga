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
import org.junit.rules.ExternalResource;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
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
public class VariantTableRemoveTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();

    @Before
    public void setUp() throws Exception {
        clearDB(DB_NAME);
        clearDB(getVariantStorageEngine().getArchiveTableName(STUDY_ID));
    }

    private VariantFileMetadata loadFile(String resource, StudyConfiguration studyConfiguration, Map<? extends String, ?> map) throws Exception {
        return VariantHbaseTestUtils.loadFile(getVariantStorageEngine(), DB_NAME, outputUri, resource, studyConfiguration, map);
    }

    private void removeFile(String file, StudyConfiguration studyConfiguration, Map<? extends String, ?> map) throws Exception {
        Integer fileId = studyConfiguration.getFileIds().get(file);
        System.out.printf("Remove File ID %s for %s", fileId, file);
        VariantHbaseTestUtils.removeFile(getVariantStorageEngine(), DB_NAME, fileId, studyConfiguration, map);
    }

    @Test
    @Ignore
    public void removeFileTestMergeAdvanced() throws Exception {
        StudyConfiguration studyConfiguration = VariantStorageBaseTest.newStudyConfiguration();
        System.out.println("studyConfiguration = " + studyConfiguration);
        String studyName = studyConfiguration.getStudyName();

        ObjectMap options = new ObjectMap(HadoopVariantStorageEngine.VARIANT_TABLE_INDEXES_SKIP, true)
                .append(VariantStorageEngine.Options.MERGE_MODE.key(), VariantStorageEngine.MergeMode.ADVANCED);
        loadFile("s1.genome.vcf", studyConfiguration, options);
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri());
        Map<String, Variant> variants = buildVariantsIdx();
        assertFalse(variants.containsKey("1:10014:A:G"));

        assertThat(variants.keySet(), hasItem("1:10013:T:C"));
        assertEquals("0/1", variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s1", "GT"));
        assertEquals(null, variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s2", "GT"));

        loadFile("s2.genome.vcf", studyConfiguration, options);
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri());
        variants = buildVariantsIdx();
        assertThat(variants.keySet(), hasItem("1:10014:A:G"));
        assertEquals("0/2", variants.get("1:10014:A:G").getStudy(studyName).getSampleData("s1", "GT"));
        assertEquals("0/1", variants.get("1:10014:A:G").getStudy(studyName).getSampleData("s2", "GT"));

        assertThat(variants.keySet(), hasItem("1:10013:T:C"));
        assertEquals("0/1", variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s1", "GT"));
        assertEquals("0/0", variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s2", "GT"));

        // delete
        removeFile("s2.genome.vcf", studyConfiguration, Collections.emptyMap());
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri());
        variants = buildVariantsIdx();
        if (variants.containsKey("1:10014:A:G")) {
            System.out.println(variants.get("1:10014:A:G").getImpl());
        }
        assertThat(variants.keySet(), not(hasItem("1:10014:A:G")));

        assertThat(variants.keySet(), hasItem("1:10013:T:C"));
        assertEquals("0/1", variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s1", "GT"));
        assertEquals(null, variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s2", "GT"));

        checkSampleIndexTable(studyConfiguration, getVariantStorageEngine().getDBAdaptor(), "s2.genome.vcf");
    }

    @Test
    public void removeFileTestMergeBasic() throws Exception {
        StudyConfiguration studyConfiguration = VariantStorageBaseTest.newStudyConfiguration();
        System.out.println("studyConfiguration = " + studyConfiguration);
        String studyName = studyConfiguration.getStudyName();

        ObjectMap options = new ObjectMap(HadoopVariantStorageEngine.VARIANT_TABLE_INDEXES_SKIP, true)
                .append(VariantStorageEngine.Options.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC);
        loadFile("s1.genome.vcf", studyConfiguration, options);
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri());
        Map<String, Variant> variants = buildVariantsIdx();
        assertFalse(variants.containsKey("1:10014:A:G"));

        assertThat(variants.keySet(), hasItem("1:10013:T:C"));
        assertEquals("0/1", variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s1", "GT"));
        assertEquals(null, variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s2", "GT"));

        loadFile("s2.genome.vcf", studyConfiguration, options);
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri());
        variants = buildVariantsIdx();
        assertThat(variants.keySet(), hasItem("1:10014:A:G"));
//        assertEquals("0/2", variants.get("1:10014:A:G").getStudy(studyName).getSampleData("s1", "GT"));
        assertEquals("0/1", variants.get("1:10014:A:G").getStudy(studyName).getSampleData("s2", "GT"));

        assertThat(variants.keySet(), hasItem("1:10013:T:C"));
        assertEquals("0/1", variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s1", "GT"));
//        assertEquals("0/0", variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s2", "GT"));

        // delete
        removeFile("s2.genome.vcf", studyConfiguration, Collections.emptyMap());
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri());
        variants = buildVariantsIdx();
        if (variants.containsKey("1:10014:A:G")) {
            System.out.println(variants.get("1:10014:A:G").getImpl());
        }
        checkSampleIndexTable(studyConfiguration, getVariantStorageEngine().getDBAdaptor(), "s2.genome.vcf");

        // FIXME: This variant should be removed!
        // assertThat(variants.keySet(), not(hasItem("1:10014:A:G")));

        assertThat(variants.keySet(), hasItem("1:10013:T:C"));
        assertEquals("0/1", variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s1", "GT"));
        assertEquals(null, variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s2", "GT"));
    }

    @Test
    public void removeFileTestMergeBasicFillGaps() throws Exception {
        StudyConfiguration studyConfiguration = VariantStorageBaseTest.newStudyConfiguration();
        System.out.println("studyConfiguration = " + studyConfiguration);
        String studyName = studyConfiguration.getStudyName();

        ObjectMap options = new ObjectMap(HadoopVariantStorageEngine.VARIANT_TABLE_INDEXES_SKIP, true)
                .append(VariantStorageEngine.Options.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC);
        loadFile("s1.genome.vcf", studyConfiguration, options);
        VariantHadoopDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        VariantHbaseTestUtils.printVariants(dbAdaptor, newOutputUri());
        Map<String, Variant> variants = buildVariantsIdx();
        assertFalse(variants.containsKey("1:10014:A:G"));

        assertThat(variants.keySet(), hasItem("1:10013:T:C"));
        assertEquals("0/1", variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s1", "GT"));
        assertEquals(null, variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s2", "GT"));

        loadFile("s2.genome.vcf", studyConfiguration, options);

        getVariantStorageEngine().fillGaps(studyName, Arrays.asList("s1", "s2"), options);

        VariantHbaseTestUtils.printVariants(dbAdaptor, newOutputUri());
        variants = buildVariantsIdx();
        assertThat(variants.keySet(), hasItem("1:10014:A:G"));
        assertEquals("0/2", variants.get("1:10014:A:G").getStudy(studyName).getSampleData("s1", "GT"));
        assertEquals("0/1", variants.get("1:10014:A:G").getStudy(studyName).getSampleData("s2", "GT"));

        assertThat(variants.keySet(), hasItem("1:10013:T:C"));
        assertEquals("0/1", variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s1", "GT"));
        assertEquals("0/0", variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s2", "GT"));

        // delete
        removeFile("s2.genome.vcf", studyConfiguration, Collections.emptyMap());
        VariantHbaseTestUtils.printVariants(dbAdaptor, newOutputUri());
        variants = buildVariantsIdx();
        if (variants.containsKey("1:10014:A:G")) {
            System.out.println(variants.get("1:10014:A:G").getImpl());
        }

        checkSampleIndexTable(studyConfiguration, dbAdaptor, "s2.genome.vcf");

        // FIXME: This variant should be removed!
        // assertThat(variants.keySet(), not(hasItem("1:10014:A:G")));

        assertThat(variants.keySet(), hasItem("1:10013:T:C"));
        assertEquals("0/1", variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s1", "GT"));
        assertEquals(null, variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s2", "GT"));
    }

    @Test
    public void removeSingleFileTest() throws Exception {
        StudyConfiguration studyConfiguration = VariantStorageBaseTest.newStudyConfiguration();
        System.out.println("studyConfiguration = " + studyConfiguration);
        String studyName = studyConfiguration.getStudyName();

        Map<String, Object> options = Collections.singletonMap(HadoopVariantStorageEngine.VARIANT_TABLE_INDEXES_SKIP, true);
        loadFile("s1.genome.vcf", studyConfiguration, options);
        Map<String, Variant> variants = buildVariantsIdx();

        assertFalse(variants.containsKey("1:10014:A:G"));
        assertTrue(variants.containsKey("1:10013:T:C"));
        assertEquals("0/1", variants.get("1:10013:T:C").getStudy(studyName).getSampleData("s1", "GT"));

        VariantHadoopDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri());
        // delete
        removeFile("s1.genome.vcf", studyConfiguration, options);

        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri());

        checkSampleIndexTable(studyConfiguration, dbAdaptor, "s1.genome.vcf");

        variants = buildVariantsIdx();
        assertEquals("Expected none variants", 0, variants.size());
        assertEquals("Expected none indexed files", 0, studyConfiguration.getIndexedFiles().size());
    }

    private Map<String, Variant> buildVariantsIdx() throws Exception {
        VariantHadoopDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        Map<String, Variant> variants = new HashMap<>();
        System.out.println("Build Variant map");
        for (Variant variant : dbAdaptor) {
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

    protected void checkSampleIndexTable(StudyConfiguration studyConfiguration, VariantHadoopDBAdaptor dbAdaptor, String removedFile) throws Exception {
        LinkedHashSet<Integer> sampleIds = studyConfiguration.getSamplesInFiles().get(studyConfiguration.getFileIds().get(removedFile));
        SampleIndexDBAdaptor sampleIndexDBAdaptor = new SampleIndexDBAdaptor(getVariantStorageEngine().getDBAdaptor().getGenomeHelper(), dbAdaptor.getHBaseManager(),
                dbAdaptor.getTableNameGenerator(), dbAdaptor.getStudyConfigurationManager());
        for (Integer sampleId : sampleIds) {
            assertFalse(sampleIndexDBAdaptor.rawIterator(studyConfiguration.getStudyId(), sampleId).hasNext());
        }
    }

}
