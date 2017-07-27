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

package org.opencb.opencga.storage.core.metadata;

import com.google.common.collect.BiMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options.SAMPLE_IDS;

/**
 * Created on 04/04/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class StudyConfigurationManagerTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    protected StudyConfiguration newStudyConfiguration() {
        return new StudyConfiguration(1, "Study");
    }

    @Test
    public void checkAndUpdateStudyConfigurationWithoutSampleIdsTest() throws StorageEngineException {
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        studyConfiguration.getSampleIds().put("s0", 1);
        studyConfiguration.getSampleIds().put("s10", 4);
        Integer fileId = 5;
        VariantSource source = createVariantSource(studyConfiguration, fileId);
        ObjectMap options = new ObjectMap();
        StudyConfigurationManager.checkAndUpdateStudyConfiguration(studyConfiguration, fileId, source, options);
        assertTrue(studyConfiguration.getSampleIds().keySet().containsAll(Arrays.asList("s0", "s1", "s2", "s3", "s4", "s5")));
        assertTrue(studyConfiguration.getSamplesInFiles().get(fileId).stream()
                .map(s -> studyConfiguration.getSampleIds().inverse().get(s))
                .collect(Collectors.toList())
                .equals(Arrays.asList("s0", "s1", "s2", "s3", "s4", "s5"))
        );
        assertEquals(Integer.valueOf(1), studyConfiguration.getSampleIds().get("s0"));
        studyConfiguration.getSamplesInFiles().get(fileId).forEach((i) -> System.out.println(studyConfiguration.getSampleIds().inverse()
                .get(i) + " = " + i));
    }

    @Test
    public void checkAndUpdateStudyConfigurationWithSampleIdsTest() throws StorageEngineException {
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        Integer fileId = 5;
        VariantSource source = createVariantSource(studyConfiguration, fileId);
        studyConfiguration.getSampleIds().put("s10", 4);
        ObjectMap options = new ObjectMap(SAMPLE_IDS.key(), "s0:20,s1:21,s2:22,s3:23,s4:24,s5:25");
        StudyConfigurationManager.checkAndUpdateStudyConfiguration(studyConfiguration, fileId, source, options);
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
        studyConfiguration.getSamplesInFiles().get(fileId).forEach((i) -> System.out.println(studyConfiguration.getSampleIds().inverse()
                .get(i) + " = " + i));
    }

    @Test
    public void checkAndUpdateStudyConfigurationWithSamplesInFilesTest() throws StorageEngineException {
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        Integer fileId = 5;
        VariantSource source = createVariantSource(studyConfiguration, fileId);
        ObjectMap options = new ObjectMap(SAMPLE_IDS.key(), "s0:20,s1:21,s2:22,s3:23,s4:24,s5:25");
        studyConfiguration.getSamplesInFiles().put(fileId, new LinkedHashSet<>(Arrays.asList(20, 21, 22, 23, 24, 25)));
        StudyConfigurationManager.checkAndUpdateStudyConfiguration(studyConfiguration, fileId, source, options);
    }

    @Test
    public void checkAndUpdateStudyConfigurationWithRepeatedSampleIdsTest() throws StorageEngineException {
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        Integer fileId = 5;
        VariantSource source = createVariantSource(studyConfiguration, fileId);
        ObjectMap options = new ObjectMap(SAMPLE_IDS.key(), "s0:20,s1:21,s2:22,s3:23,s4:24,s5:25");
        studyConfiguration.getSampleIds().put("s0", 0);

        thrown.expect(StorageEngineException.class);
        thrown.expectMessage("s0:20");   //Already present
        StudyConfigurationManager.checkAndUpdateStudyConfiguration(studyConfiguration, fileId, source, options);
    }

    @Test
    public void checkAndUpdateStudyConfigurationWithExtraSampleIdsTest() throws StorageEngineException {
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        Integer fileId = 5;
        VariantSource source = createVariantSource(studyConfiguration, fileId);
        ObjectMap options = new ObjectMap(SAMPLE_IDS.key(), "s0:20,s1:21,s2:22,s3:23,s4:24,s5:25," +
                "UNEXISTING_SAMPLE:30");

        thrown.expect(StorageEngineException.class);
        thrown.expectMessage("UNEXISTING_SAMPLE");   //Not in file
        StudyConfigurationManager.checkAndUpdateStudyConfiguration(studyConfiguration, fileId, source, options);
    }

    @Test
    public void checkAndUpdateStudyConfigurationWithAlphanumericSampleIdsTest() throws StorageEngineException {
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        Integer fileId = 5;
        VariantSource source = createVariantSource(studyConfiguration, fileId);
        ObjectMap options = new ObjectMap(SAMPLE_IDS.key(), "s0:20,s1:21,s2:22,s3:23,s4:NaN,s5:25");

        thrown.expect(StorageEngineException.class);
        thrown.expectMessage("NaN");   //Not a number
        StudyConfigurationManager.checkAndUpdateStudyConfiguration(studyConfiguration, fileId, source, options);
    }

    @Test
    public void checkAndUpdateStudyConfigurationWithMalformedSampleIds1Test() throws StorageEngineException {
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        Integer fileId = 5;
        VariantSource source = createVariantSource(studyConfiguration, fileId);
        ObjectMap options = new ObjectMap(SAMPLE_IDS.key(), "s0:20,s1:21,s2:22,s3:23,s4:24,s5:");

        thrown.expect(StorageEngineException.class);
        thrown.expectMessage("s5:");   //Malformed
        StudyConfigurationManager.checkAndUpdateStudyConfiguration(studyConfiguration, fileId, source, options);
    }

    @Test
    public void checkAndUpdateStudyConfigurationWithMalformedSampleIds2Test() throws StorageEngineException {
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        Integer fileId = 5;
        VariantSource source = createVariantSource(studyConfiguration, fileId);
        ObjectMap options = new ObjectMap(SAMPLE_IDS.key(), "s0:20,s1:21,s2:22,s3,s4:24,s5:25");

        thrown.expect(StorageEngineException.class);
        thrown.expectMessage("s3");   //Malformed
        StudyConfigurationManager.checkAndUpdateStudyConfiguration(studyConfiguration, fileId, source, options);
    }

    @Test
    public void checkAndUpdateStudyConfigurationWithMissingSampleIdsTest() throws StorageEngineException {
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        Integer fileId = 5;
        VariantSource source = createVariantSource(studyConfiguration, fileId);
        ObjectMap options = new ObjectMap(SAMPLE_IDS.key(), "s0:20");

        thrown.expect(StorageEngineException.class);
        thrown.expectMessage("[s1, s2, s3, s4, s5]");   //Missing samples
        StudyConfigurationManager.checkAndUpdateStudyConfiguration(studyConfiguration, fileId, source, options);
    }

    @Test
    public void checkAndUpdateStudyConfigurationWithMissingSamplesInFilesTest() throws StorageEngineException {
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        Integer fileId = 5;
        VariantSource source = createVariantSource(studyConfiguration, fileId);
        ObjectMap options = new ObjectMap(SAMPLE_IDS.key(), "s0:20,s1:21,s2:22,s3:23,s4:24,s5:25");
        studyConfiguration.getSamplesInFiles().put(fileId, new LinkedHashSet<>(Arrays.asList(20, 21, 22, 23, 24)));
        thrown.expect(StorageEngineException.class);
        thrown.expectMessage("s5");
        StudyConfigurationManager.checkAndUpdateStudyConfiguration(studyConfiguration, fileId, source, options);
    }

    @Test
    public void checkAndUpdateStudyConfigurationWithExtraSamplesInFilesTest() throws StorageEngineException {
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        Integer fileId = 5;
        VariantSource source = createVariantSource(studyConfiguration, fileId);
        ObjectMap options = new ObjectMap(SAMPLE_IDS.key(), "s0:20,s1:21,s2:22,s3:23,s4:24,s5:25");
        studyConfiguration.getSampleIds().put("GhostSample", 0);
        studyConfiguration.getSamplesInFiles().put(fileId, new LinkedHashSet<>(Arrays.asList(20, 21, 22, 23, 24, 25, 0)));
        thrown.expect(StorageEngineException.class);
        StudyConfigurationManager.checkAndUpdateStudyConfiguration(studyConfiguration, fileId, source, options);
    }

    @Test
    public void getIndexedSamplesPositionTest() {
        StudyConfiguration studyConfiguration = new StudyConfiguration(1, "study");
        studyConfiguration.getIndexedFiles().add(1);
        studyConfiguration.getSamplesInFiles().put(1, new LinkedHashSet<>(Arrays.asList(0, 1, 2, 3)));
        studyConfiguration.getIndexedFiles().add(2);
        studyConfiguration.getSamplesInFiles().put(2, new LinkedHashSet<>(Arrays.asList(2, 3, 4)));
        studyConfiguration.getSampleIds().put("s0", 0);
        studyConfiguration.getSampleIds().put("s1", 1);
        studyConfiguration.getSampleIds().put("s2", 2);
        studyConfiguration.getSampleIds().put("s3", 3);
        studyConfiguration.getSampleIds().put("s4", 4);
        studyConfiguration.getSampleIds().put("s5", 5);
        BiMap<String, Integer> indexedSamplesPosition = StudyConfiguration.getIndexedSamplesPosition(studyConfiguration);
        assertEquals(5, indexedSamplesPosition.size());
        assertEquals(0, indexedSamplesPosition.get("s0").intValue());
        assertEquals(1, indexedSamplesPosition.get("s1").intValue());
        assertEquals(2, indexedSamplesPosition.get("s2").intValue());
        assertEquals(3, indexedSamplesPosition.get("s3").intValue());
        assertEquals(4, indexedSamplesPosition.get("s4").intValue());
        assertEquals(null, indexedSamplesPosition.get("s5"));

    }

    protected VariantSource createVariantSource(StudyConfiguration studyConfiguration, Integer fileId) {
        studyConfiguration.getFileIds().put("fileName", fileId);
        VariantSource source = new VariantSource("fileName", fileId.toString(), studyConfiguration.getStudyId() + "", studyConfiguration
                .getStudyName());
        Map<String, Integer> samplesPosition = new HashMap<>();
        samplesPosition.put("s0", 0);
        samplesPosition.put("s1", 1);
        samplesPosition.put("s2", 2);
        samplesPosition.put("s3", 3);
        samplesPosition.put("s4", 4);
        samplesPosition.put("s5", 5);
        source.setSamplesPosition(samplesPosition);
        return source;
    }

}