package org.opencb.opencga.catalog.db.mongodb.converters;

import org.apache.commons.collections.map.HashedMap;
import org.junit.Test;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.opencga.catalog.stats.solr.SampleSolrModel;
import org.opencb.opencga.catalog.stats.solr.converters.CatalogSampleToSolrSampleConverter;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.study.Study;

import java.util.Arrays;
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

        Study study = new Study().setFqn("user@project:study").setAttributes(new HashMap<>());
        Individual individual = new Individual();
        individual.setUuid("uuid").setEthnicity("spanish").setKaryotypicSex(IndividualProperty.KaryotypicSex.XX).
                setPopulation(new Individual.Population("valencian", "", ""));

        Sample sample = new Sample();
        sample.setUid(500).setSource("Lab").setRelease(3).setVersion(2).setStatus(new Status("READY"))
                .setType("Sample").setSomatic(true).setCreationDate(TimeUtils.getTime())
                .setAnnotationSets(AnnotationHelper.createAnnotation());

        Map<String, Object> attributes = new HashedMap();
        attributes.put("individual", individual);
        sample.setAttributes(attributes);

        SampleSolrModel sampleSolrModel = new CatalogSampleToSolrSampleConverter(study).convertToStorageType(sample);

        assertEquals(sampleSolrModel.getUid(), sample.getUid());
        assertEquals(sampleSolrModel.getSource(), sample.getSource());
        assertEquals(sampleSolrModel.getRelease(), sample.getRelease());
        assertEquals(sampleSolrModel.getVersion(), sample.getVersion());
        assertEquals(sampleSolrModel.getStatus(), sample.getStatus().getName());
        assertEquals(sampleSolrModel.getType(), sample.getType());
        assertEquals(sampleSolrModel.isSomatic(), sample.isSomatic());
        assertEquals(sampleSolrModel.getPhenotypes().size(), 0);

        assertEquals(sampleSolrModel.getAnnotations().get("annotations__o__annotName.vsId.a.ab2.ab2c1.ab2c1d1"),
                Arrays.asList(1, 2, 3, 4, 11, 12, 13, 14, 21));
        assertEquals(sampleSolrModel.getAnnotations().get("annotations__o__annotName.vsId.a.ab1.ab1c1"),
                Arrays.asList(true, false, false));
        assertEquals(sampleSolrModel.getAnnotations().get("annotations__s__annotName.vsId.a.ab1.ab1c2"), "hello world");
        assertEquals(sampleSolrModel.getAnnotations().get("annotations__o__annotName.vsId.a.ab2.ab2c1.ab2c1d2"),
                Arrays.asList("hello ab2c1d2 1", "hello ab2c1d2 2"));
        assertEquals(sampleSolrModel.getAnnotations().get("annotations__o__annotName.vsId.a.ab3.ab3c1.ab3c1d1"),
                Arrays.asList(Arrays.asList("hello"), Arrays.asList("hello2", "bye2"), Arrays.asList("byeee2", "hellooo2")));
        assertEquals(sampleSolrModel.getAnnotations().get("annotations__o__annotName.vsId.a.ab3.ab3c1.ab3c1d2"),
                Arrays.asList(2.0, 4.0, 24.0));
        assertNull(sampleSolrModel.getAnnotations().get("nothing"));
        assertEquals(sampleSolrModel.getAnnotations().keySet().size(), 6);

    }
}
