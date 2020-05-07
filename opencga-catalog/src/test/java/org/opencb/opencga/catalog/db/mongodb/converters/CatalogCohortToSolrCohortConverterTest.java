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

import org.junit.Test;
import org.opencb.opencga.catalog.stats.solr.CohortSolrModel;
import org.opencb.opencga.catalog.stats.solr.converters.CatalogCohortToSolrCohortConverter;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.cohort.CohortInternal;
import org.opencb.opencga.core.models.cohort.CohortStatus;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.sample.Sample;
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
public class CatalogCohortToSolrCohortConverterTest {

    @Test
    public void CohortToSolrTest() {
        Study study = new Study().setFqn("user@project:study").setAttributes(new HashMap<>())
                .setVariableSets(Collections.singletonList(AnnotationHelper.createVariableSet()));
        Cohort cohort = new Cohort("id", Enums.CohortType.CASE_SET, TimeUtils.getTime(), "test",
                Arrays.asList(new Sample().setId("1"), new Sample().setId("2")), 2, null)
                .setAttributes(new HashMap<>());
        cohort.setUid(200).setInternal(new CohortInternal(new CohortStatus("CALCULATING"))).setAnnotationSets(AnnotationHelper.createAnnotation());
        CohortSolrModel cohortSolrModel = new CatalogCohortToSolrCohortConverter(study).convertToStorageType(cohort);

        assertEquals(cohortSolrModel.getUid(), cohort.getUid());
        assertEquals(cohortSolrModel.getStatus(), cohort.getInternal().getStatus().getName());

        Date date = TimeUtils.toDate(cohort.getCreationDate());
        LocalDate localDate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();

        assertEquals(localDate.getYear(), cohortSolrModel.getCreationYear());
        assertEquals(localDate.getMonth().toString(), cohortSolrModel.getCreationMonth());
        assertEquals(localDate.getDayOfMonth(), cohortSolrModel.getCreationDay());
        assertEquals(localDate.getDayOfMonth(), cohortSolrModel.getCreationDay());
        assertEquals(localDate.getDayOfWeek().toString(), cohortSolrModel.getCreationDayOfWeek());
        cohortSolrModel.setStatus(cohort.getInternal().getStatus().getName());

        assertEquals(cohortSolrModel.getType(), cohort.getType().name());
        assertEquals(cohortSolrModel.getRelease(), cohort.getRelease());
        assertEquals(cohortSolrModel.getNumSamples(), cohort.getSamples().size());

        assertEquals(Arrays.asList(1, 2, 3, 4, 11, 12, 13, 14, 21), cohortSolrModel.getAnnotations().get("annotations__im__vsId.a.ab2.ab2c1.ab2c1d1"));
        assertEquals(Arrays.asList(true, false, false), cohortSolrModel.getAnnotations().get("annotations__bm__vsId.a.ab1.ab1c1"));
        assertEquals("hello world", cohortSolrModel.getAnnotations().get("annotations__s__vsId.a.ab1.ab1c2"));
        assertEquals(Arrays.asList("hello ab2c1d2 1", "hello ab2c1d2 2"), cohortSolrModel.getAnnotations().get("annotations__sm__vsId.a.ab2.ab2c1.ab2c1d2"));
        assertEquals(Arrays.asList(Arrays.asList("hello"), Arrays.asList("hello2", "bye2"), Arrays.asList("byeee2", "hellooo2")), cohortSolrModel.getAnnotations().get("annotations__sm__vsId.a.ab3.ab3c1.ab3c1d1"));
        assertEquals(Arrays.asList(2.0, 4.0, 24.0), cohortSolrModel.getAnnotations().get("annotations__dm__vsId.a.ab3.ab3c1.ab3c1d2"));
        assertNull(cohortSolrModel.getAnnotations().get("nothing"));
        assertEquals(cohortSolrModel.getAnnotations().keySet().size(), 6);
    }
}
