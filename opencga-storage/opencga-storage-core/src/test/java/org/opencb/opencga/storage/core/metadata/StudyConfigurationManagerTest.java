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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.dummy.DummyProjectMetadataAdaptor;
import org.opencb.opencga.storage.core.variant.dummy.DummyStudyConfigurationAdaptor;
import org.opencb.opencga.storage.core.variant.dummy.DummyVariantFileMetadataDBAdaptor;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created on 04/04/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class StudyConfigurationManagerTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private StudyConfigurationManager scm;

    @Before
    public void setUp() throws Exception {
        DummyProjectMetadataAdaptor.clear();
        DummyStudyConfigurationAdaptor.clear();
        scm = new StudyConfigurationManager(new DummyProjectMetadataAdaptor(), new DummyStudyConfigurationAdaptor(), new DummyVariantFileMetadataDBAdaptor());
    }

    protected StudyConfiguration newStudyConfiguration() {
        return new StudyConfiguration(1, "Study");
    }

    @Test
    public void checkAndUpdateStudyConfigurationWithoutSampleIdsTest() throws StorageEngineException {
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        studyConfiguration.getSampleIds().put("s0", scm.newSampleId(studyConfiguration));
        studyConfiguration.getSampleIds().put("s10", scm.newSampleId(studyConfiguration));
        Integer fileId = 5;
        VariantFileMetadata source = createVariantFileMetadata(studyConfiguration, fileId);
        ObjectMap options = new ObjectMap();
        scm.registerFileSamples(studyConfiguration, fileId, source, options);
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
        VariantFileMetadata source = createVariantFileMetadata(studyConfiguration, fileId);
        studyConfiguration.getSampleIds().put("s10", scm.newSampleId(studyConfiguration));
        ObjectMap options = new ObjectMap();
        scm.registerFileSamples(studyConfiguration, fileId, source, options);
        assertTrue(studyConfiguration.getSampleIds().keySet().containsAll(Arrays.asList("s0", "s1", "s2", "s3", "s4", "s5")));
        assertEquals(Arrays.asList("s0", "s1", "s2", "s3", "s4", "s5"),
                studyConfiguration.getSamplesInFiles().get(fileId).stream()
                        .map(s -> studyConfiguration.getSampleIds().inverse().get(s))
                        .collect(Collectors.toList())
        );
        assertEquals(Arrays.asList(2, 3, 4, 5, 6, 7), new ArrayList<>(studyConfiguration.getSamplesInFiles().get(fileId)));
        assertEquals(2, studyConfiguration.getSampleIds().get("s0").intValue());
        assertEquals(3, studyConfiguration.getSampleIds().get("s1").intValue());
        assertEquals(4, studyConfiguration.getSampleIds().get("s2").intValue());
        assertEquals(5, studyConfiguration.getSampleIds().get("s3").intValue());
        assertEquals(6, studyConfiguration.getSampleIds().get("s4").intValue());
        assertEquals(7, studyConfiguration.getSampleIds().get("s5").intValue());
        studyConfiguration.getSamplesInFiles().get(fileId).forEach((i) -> System.out.println(studyConfiguration.getSampleIds().inverse()
                .get(i) + " = " + i));
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

    protected VariantFileMetadata createVariantFileMetadata(StudyConfiguration studyConfiguration, Integer fileId) {
        studyConfiguration.getFileIds().put("fileName", fileId);
        VariantFileMetadata source = new VariantFileMetadata("fileName", fileId.toString());
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