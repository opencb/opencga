package org.opencb.opencga.catalog.db.mongodb.converters;

import org.junit.Test;
import org.opencb.opencga.catalog.stats.solr.CohortSolrModel;
import org.opencb.opencga.catalog.stats.solr.converters.CatalogCohortToSolrCohortConverter;
import org.opencb.opencga.core.models.Cohort;
import org.opencb.opencga.core.models.Sample;
import org.opencb.opencga.core.models.Study;

import java.util.Arrays;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by wasim on 13/08/18.
 */
public class CatalogCohortToSolrCohortConverterTest {


    @Test
    public void CohortToSolrTest() {
        Cohort cohort = new Cohort("id", Study.Type.CASE_SET, new Date().toString(), "test", Arrays.asList(new Sample().setId("1"), new Sample().setId("2")), 2, null);
        cohort.setUid(200).setStatus(new Cohort.CohortStatus("CALCULATING")).setAnnotationSets(AnnotationHelper.createAnnotation());
        CohortSolrModel cohortSolrModel = new CatalogCohortToSolrCohortConverter().convertToStorageType(cohort);

        assertEquals(cohortSolrModel.getUid(), cohort.getUid());
        assertEquals(cohortSolrModel.getStatus(), cohort.getStatus().getName());
        assertEquals(cohortSolrModel.getCreationDate(), cohort.getCreationDate());
        assertEquals(cohortSolrModel.getType(), cohort.getType().name());
        assertEquals(cohortSolrModel.getRelease(), cohort.getRelease());
        assertEquals(cohortSolrModel.getSamples(), cohort.getSamples().size());

        assertEquals(cohortSolrModel.getAnnotations().get("annotations__o__annotName.vsId.a.ab2.ab2c1.ab2c1d1"), Arrays.asList(1, 2, 3, 4, 11, 12, 13, 14, 21));
        assertEquals(cohortSolrModel.getAnnotations().get("annotations__o__annotName.vsId.a.ab1.ab1c1"), Arrays.asList(true, false, false));
        assertEquals(cohortSolrModel.getAnnotations().get("annotations__s__annotName.vsId.a.ab1.ab1c2"), "hello world");
        assertEquals(cohortSolrModel.getAnnotations().get("annotations__o__annotName.vsId.a.ab2.ab2c1.ab2c1d2"), Arrays.asList("hello ab2c1d2 1", "hello ab2c1d2 2"));
        assertEquals(cohortSolrModel.getAnnotations().get("annotations__o__annotName.vsId.a.ab3.ab3c1.ab3c1d1"), Arrays.asList(Arrays.asList("hello"), Arrays.asList("hello2", "bye2"), Arrays.asList("byeee2", "hellooo2")));
        assertEquals(cohortSolrModel.getAnnotations().get("annotations__o__annotName.vsId.a.ab3.ab3c1.ab3c1d2"), Arrays.asList(2.0, 4.0, 24.0));
        assertNull(cohortSolrModel.getAnnotations().get("nothing"));
        assertEquals(cohortSolrModel.getAnnotations().keySet().size(), 6);
    }
}
