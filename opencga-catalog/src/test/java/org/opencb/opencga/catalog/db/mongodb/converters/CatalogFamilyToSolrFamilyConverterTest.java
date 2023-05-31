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
import org.junit.experimental.categories.Category;
import org.opencb.opencga.catalog.stats.solr.FamilySolrModel;
import org.opencb.opencga.catalog.stats.solr.converters.CatalogFamilyToSolrFamilyConverter;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.family.FamilyInternal;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by wasim on 13/08/18.
 */
@Category(ShortTests.class)
public class CatalogFamilyToSolrFamilyConverterTest {

    @Test
    public void FamilyToSolrTest() {
        Study study = new Study().setFqn("user@project:study").setAttributes(new HashMap<>())
                .setVariableSets(Collections.singletonList(AnnotationHelper.createVariableSet()));
        Family family = new Family("id", "family", null, null,
                Arrays.asList(new Individual().setId("I1"), new Individual().setId("I2")), "test", 1000, AnnotationHelper.createAnnotation(), null);
        family.setUid(100).setInternal(FamilyInternal.init()).setRelease(1).setVersion(2);
        FamilySolrModel familySolrModel = new CatalogFamilyToSolrFamilyConverter(study).convertToStorageType(family);

        assertEquals(familySolrModel.getUid(), family.getUid());
        assertEquals(familySolrModel.getStatus(), family.getInternal().getStatus().getId());
        assertEquals(familySolrModel.getNumMembers(), family.getMembers().size());
        assertEquals(familySolrModel.getRelease(), family.getRelease());
        assertEquals(familySolrModel.getVersion(), family.getVersion());
        assertEquals(familySolrModel.getPhenotypes().size(), 0);

        assertEquals(Arrays.asList(1, 2, 3, 4, 11, 12, 13, 14, 21), familySolrModel.getAnnotations().get("annotations__im__vsId.a.ab2.ab2c1.ab2c1d1"));
        assertEquals(Arrays.asList(true, false, false), familySolrModel.getAnnotations().get("annotations__bm__vsId.a.ab1.ab1c1"));
        assertEquals("hello world", familySolrModel.getAnnotations().get("annotations__s__vsId.a.ab1.ab1c2"));
        assertEquals(Arrays.asList("hello ab2c1d2 1", "hello ab2c1d2 2"), familySolrModel.getAnnotations().get("annotations__sm__vsId.a.ab2.ab2c1.ab2c1d2"));
        assertEquals(Arrays.asList(Arrays.asList("hello"), Arrays.asList("hello2", "bye2"), Arrays.asList("byeee2", "hellooo2")), familySolrModel.getAnnotations().get("annotations__sm__vsId.a.ab3.ab3c1.ab3c1d1"));
        assertEquals(Arrays.asList(2.0, 4.0, 24.0), familySolrModel.getAnnotations().get("annotations__dm__vsId.a.ab3.ab3c1.ab3c1d2"));
        assertNull(familySolrModel.getAnnotations().get("nothing"));
        assertEquals(familySolrModel.getAnnotations().keySet().size(), 6);

    }
}
