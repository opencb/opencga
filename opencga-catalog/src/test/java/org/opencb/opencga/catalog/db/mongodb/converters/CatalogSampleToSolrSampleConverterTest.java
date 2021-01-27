/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.catalog.db.mongodb.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.map.HashedMap;
import org.junit.Test;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.opencga.catalog.stats.solr.SampleSolrModel;
import org.opencb.opencga.catalog.stats.solr.converters.CatalogSampleToSolrSampleConverter;
import org.opencb.opencga.catalog.stats.solr.converters.SolrConverterUtil;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualPopulation;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleAclEntry;
import org.opencb.opencga.core.models.sample.SampleInternal;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyAclEntry;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by wasim on 13/08/18.
 */
public class CatalogSampleToSolrSampleConverterTest {

    @Test
    public void SampleToSolrTest() throws JsonProcessingException {
        ObjectMapper objectMapper = JacksonUtils.getDefaultObjectMapper();

        Study study = new Study().setFqn("user@project:study").setAttributes(new HashMap<>())
                .setVariableSets(Collections.singletonList(AnnotationHelper.createVariableSet()));
        List<Map<String, Object>> studyAclEntry = Arrays.asList(
                objectMapper.readValue(objectMapper.writeValueAsString(new StudyAclEntry("user1", EnumSet.noneOf(StudyAclEntry.StudyPermissions.class))), Map.class),
                objectMapper.readValue(objectMapper.writeValueAsString(new StudyAclEntry("user2", EnumSet.allOf(StudyAclEntry.StudyPermissions.class))), Map.class),
                objectMapper.readValue(objectMapper.writeValueAsString(new StudyAclEntry("user3", EnumSet.allOf(StudyAclEntry.StudyPermissions.class))), Map.class),
                objectMapper.readValue(objectMapper.writeValueAsString(new StudyAclEntry("user4", EnumSet.noneOf(StudyAclEntry.StudyPermissions.class))), Map.class)
        );
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("OPENCGA_ACL", SolrConverterUtil.parseInternalOpenCGAAcls(studyAclEntry));
        study.setAttributes(attributes);

        Individual individual = new Individual();
        individual.setUuid("uuid").setEthnicity("spanish").setKaryotypicSex(IndividualProperty.KaryotypicSex.XX).
                setPopulation(new IndividualPopulation("valencian", "", ""));

        Sample sample = new Sample();
        new Status("READY");
        sample.setUid(500).setRelease(3).setVersion(2).setInternal(SampleInternal.init())
                .setSomatic(true).setCreationDate(TimeUtils.getTime())
                .setAnnotationSets(AnnotationHelper.createAnnotation());

        List<Map> sampleAclEntry = Arrays.asList(
                objectMapper.readValue(objectMapper.writeValueAsString(new SampleAclEntry("user1", EnumSet.of(SampleAclEntry.SamplePermissions.VIEW, SampleAclEntry.SamplePermissions.UPDATE))), Map.class),
                objectMapper.readValue(objectMapper.writeValueAsString(new SampleAclEntry("user2", EnumSet.noneOf(SampleAclEntry.SamplePermissions.class))), Map.class)
        );
        attributes = new HashMap<>();
        attributes.put("OPENCGA_ACL", sampleAclEntry);
        attributes.put("individual", individual);
        sample.setAttributes(attributes);

        SampleSolrModel sampleSolrModel = new CatalogSampleToSolrSampleConverter(study).convertToStorageType(sample);

        assertEquals(sampleSolrModel.getUid(), sample.getUid());
        assertEquals(sampleSolrModel.getRelease(), sample.getRelease());
        assertEquals(sampleSolrModel.getVersion(), sample.getVersion());
        assertEquals(sampleSolrModel.getStatus(), sample.getInternal().getStatus().getName());
        assertEquals(sampleSolrModel.isSomatic(), sample.isSomatic());
        assertEquals(sampleSolrModel.getPhenotypes().size(), 0);

        assertEquals(Arrays.asList(1, 2, 3, 4, 11, 12, 13, 14, 21), sampleSolrModel.getAnnotations().get("annotations__im__vsId.a.ab2.ab2c1.ab2c1d1"));
        assertEquals(Arrays.asList(true, false, false), sampleSolrModel.getAnnotations().get("annotations__bm__vsId.a.ab1.ab1c1"));
        assertEquals("hello world", sampleSolrModel.getAnnotations().get("annotations__s__vsId.a.ab1.ab1c2"));
        assertEquals(Arrays.asList("hello ab2c1d2 1", "hello ab2c1d2 2"), sampleSolrModel.getAnnotations().get("annotations__sm__vsId.a.ab2.ab2c1.ab2c1d2"));
        assertEquals(Arrays.asList(Arrays.asList("hello"), Arrays.asList("hello2", "bye2"), Arrays.asList("byeee2", "hellooo2")), sampleSolrModel.getAnnotations().get("annotations__sm__vsId.a.ab3.ab3c1.ab3c1d1"));
        assertEquals(Arrays.asList(2.0, 4.0, 24.0), sampleSolrModel.getAnnotations().get("annotations__dm__vsId.a.ab3.ab3c1.ab3c1d2"));
        assertNull(sampleSolrModel.getAnnotations().get("nothing"));
        assertEquals(sampleSolrModel.getAnnotations().keySet().size(), 6);

    }
}
