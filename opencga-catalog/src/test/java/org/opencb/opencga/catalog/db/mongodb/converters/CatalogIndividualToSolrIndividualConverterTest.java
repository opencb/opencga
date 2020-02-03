package org.opencb.opencga.catalog.db.mongodb.converters;

import org.junit.Test;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.biodata.models.pedigree.Multiples;
import org.opencb.opencga.catalog.stats.solr.IndividualSolrModel;
import org.opencb.opencga.catalog.stats.solr.converters.CatalogIndividualToSolrIndividualConverter;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.study.Study;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by wasim on 13/08/18.
 */
public class CatalogIndividualToSolrIndividualConverterTest {

    @Test
    public void IndividualToSolrTest() {
        Study study = new Study().setFqn("user@project:study").setAttributes(new HashMap<>())
                .setVariableSets(Collections.singletonList(AnnotationHelper.createVariableSet()));
        Individual individual = new Individual("Id", "individual", IndividualProperty.Sex.MALE, "Spanish",
                new Individual.Population("valencian", "", ""), 2, AnnotationHelper.createAnnotation(), null);

        individual.setUid(300).setMultiples(new Multiples("twin", Arrays.asList("Pedro")))
                .setKaryotypicSex(IndividualProperty.KaryotypicSex.XX).setVersion(4).setStatus(new Status("READY")).
                setLifeStatus(IndividualProperty.LifeStatus.ABORTED).setAffectationStatus(IndividualProperty.AffectationStatus.AFFECTED).
                setSamples(Arrays.asList(new Sample().setId("1"), new Sample().setId("2"))).setParentalConsanguinity(true);

        IndividualSolrModel individualSolrModel = new CatalogIndividualToSolrIndividualConverter(study).convertToStorageType(individual);

        assertEquals(individualSolrModel.getUid(), individual.getUid());
        assertEquals(individualSolrModel.getMultiplesType(), individual.getMultiples().getType());
        assertEquals(individualSolrModel.getSex(), individual.getSex().name());
        assertEquals(individualSolrModel.getKaryotypicSex(), individual.getKaryotypicSex().name());
        assertEquals(individualSolrModel.getEthnicity(), individual.getEthnicity());
        assertEquals(individualSolrModel.getPopulation(), individual.getPopulation().getName());
        assertEquals(individualSolrModel.getRelease(), individual.getRelease());

        Date date = TimeUtils.toDate(individual.getCreationDate());
        LocalDate localDate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();

        assertEquals(localDate.getYear(), individualSolrModel.getCreationYear());
        assertEquals(localDate.getMonth().toString(), individualSolrModel.getCreationMonth());
        assertEquals(localDate.getDayOfMonth(), individualSolrModel.getCreationDay());
        assertEquals(localDate.getDayOfMonth(), individualSolrModel.getCreationDay());
        assertEquals(localDate.getDayOfWeek().toString(), individualSolrModel.getCreationDayOfWeek());

        assertEquals(individualSolrModel.getVersion(), individual.getVersion());
        assertEquals(individualSolrModel.getStatus(), individual.getStatus().getName());
        assertEquals(individualSolrModel.getLifeStatus(), individual.getLifeStatus().name());
        assertEquals(individualSolrModel.getAffectationStatus(), individual.getAffectationStatus().name());
        assertEquals(individualSolrModel.getPhenotypes().size(), 0);
        assertEquals(individualSolrModel.isParentalConsanguinity(), individual.isParentalConsanguinity());

        assertEquals(Arrays.asList(1, 2, 3, 4, 11, 12, 13, 14, 21), individualSolrModel.getAnnotations().get("annotations__im__vsId.a.ab2.ab2c1.ab2c1d1"));
        assertEquals(Arrays.asList(true, false, false), individualSolrModel.getAnnotations().get("annotations__bm__vsId.a.ab1.ab1c1"));
        assertEquals("hello world", individualSolrModel.getAnnotations().get("annotations__s__vsId.a.ab1.ab1c2"));
        assertEquals(Arrays.asList("hello ab2c1d2 1", "hello ab2c1d2 2"), individualSolrModel.getAnnotations().get("annotations__sm__vsId.a.ab2.ab2c1.ab2c1d2"));
        assertEquals(Arrays.asList(Arrays.asList("hello"), Arrays.asList("hello2", "bye2"), Arrays.asList("byeee2", "hellooo2")), individualSolrModel.getAnnotations().get("annotations__sm__vsId.a.ab3.ab3c1.ab3c1d1"));
        assertEquals(Arrays.asList(2.0, 4.0, 24.0), individualSolrModel.getAnnotations().get("annotations__dm__vsId.a.ab3.ab3c1.ab3c1d2"));
        assertNull(individualSolrModel.getAnnotations().get("nothing"));
        assertEquals(individualSolrModel.getAnnotations().keySet().size(), 6);

    }
}
