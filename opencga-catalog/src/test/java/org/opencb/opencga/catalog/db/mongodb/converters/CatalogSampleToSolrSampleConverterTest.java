package org.opencb.opencga.catalog.db.mongodb.converters;

import org.apache.commons.collections.map.HashedMap;
import org.junit.Test;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.opencga.catalog.stats.solr.SampleSolrModel;
import org.opencb.opencga.catalog.stats.solr.converters.CatalogSampleToSolrSampleConverter;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualPopulation;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.sample.SampleInternal;
import org.opencb.opencga.core.models.study.Study;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by wasim on 13/08/18.
 */
public class CatalogSampleToSolrSampleConverterTest {

    @Test
    public void SampleToSolrTest() {

        Study study = new Study().setFqn("user@project:study").setAttributes(new HashMap<>())
                .setVariableSets(Collections.singletonList(AnnotationHelper.createVariableSet()));
        Individual individual = new Individual();
        individual.setUuid("uuid").setEthnicity("spanish").setKaryotypicSex(IndividualProperty.KaryotypicSex.XX).
                setPopulation(new IndividualPopulation("valencian", "", ""));

        Sample sample = new Sample();
        new Status("READY");
        sample.setUid(500).setRelease(3).setVersion(2).setInternal(new SampleInternal(new Status()))
                .setSomatic(true).setCreationDate(TimeUtils.getTime())
                .setAnnotationSets(AnnotationHelper.createAnnotation());

        Map<String, Object> attributes = new HashedMap();
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
