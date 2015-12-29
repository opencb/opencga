/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.storage.mongodb.variant;

import org.junit.Test;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorTest;

import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author Alejandro Aleman Ramos <aaleman@cipf.es>
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class VariantMongoDBAdaptorTest extends VariantDBAdaptorTest implements MongoVariantStorageManagerTestUtils {

    @Test
    public void deleteStudyTest() throws Exception {
        fileIndexed = false;
        dbAdaptor.deleteStudy(studyConfiguration.getStudyName(), new QueryOptions("purge", false));
        for (Variant variant : dbAdaptor) {
            for (Map.Entry<String, StudyEntry> entry : variant.getStudiesMap().entrySet()) {
                assertFalse(entry.getValue().getStudyId().equals(studyConfiguration.getStudyId() + ""));
            }
        }
        QueryResult<Long> allVariants = dbAdaptor.count(new Query());
        assertEquals(NUM_VARIANTS, allVariants.first().intValue());
    }

    @Test
    public void deleteAndPurgeStudyTest() throws Exception {
        fileIndexed = false;
        dbAdaptor.deleteStudy(studyConfiguration.getStudyName(), new QueryOptions("purge", true));
        for (Variant variant : dbAdaptor) {
            for (Map.Entry<String, StudyEntry> entry : variant.getStudiesMap().entrySet()) {
                assertFalse(entry.getValue().getStudyId().equals(studyConfiguration.getStudyId() + ""));
            }
        }
        QueryResult<Variant> allVariants = dbAdaptor.get(new Query(), new QueryOptions());
        assertEquals(0, allVariants.getNumTotalResults());
    }

    @Test
    public void deleteStatsTest() throws Exception {
        fileIndexed = false;
        String deletedCohort = "cohort2";
        dbAdaptor.deleteStats(studyConfiguration.getStudyName(), deletedCohort, new QueryOptions());

        for (Variant variant : dbAdaptor) {
            for (Map.Entry<String, StudyEntry> entry : variant.getStudiesMap().entrySet()) {
                assertFalse("The cohort '" + deletedCohort + "' is not completely deleted in variant: '" + variant + "'", entry.getValue
                        ().getStats().keySet().contains(deletedCohort));
            }
        }
        QueryResult<Long> allVariants = dbAdaptor.count(new Query());
        assertEquals(NUM_VARIANTS, allVariants.first().intValue());
    }

    @Test
    public void deleteAnnotationTest() throws Exception {
        fileIndexed = false;
        Query query = new Query(VariantDBAdaptor.VariantQueryParams.ANNOTATION_EXISTS.key(), true);
        query.put(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), studyConfiguration.getStudyId());
        long numAnnotatedVariants = dbAdaptor.count(query).first();

        assertEquals("All variants should be annotated", NUM_VARIANTS, numAnnotatedVariants);

        query = new Query(VariantDBAdaptor.VariantQueryParams.CHROMOSOME.key(), "1");
        query.put(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), studyConfiguration.getStudyId());
        long numVariantsChr1 = dbAdaptor.count(query).first();
        dbAdaptor.deleteAnnotation("", new Query(VariantDBAdaptor.VariantQueryParams.CHROMOSOME.key(), "1"), new QueryOptions());

        query = new Query(VariantDBAdaptor.VariantQueryParams.ANNOTATION_EXISTS.key(), false);
        query.put(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), studyConfiguration.getStudyId());
        long numVariantsNoAnnotation = dbAdaptor.count(query).first();

        assertNotEquals(numVariantsChr1, NUM_VARIANTS);
        assertEquals(numVariantsChr1, numVariantsNoAnnotation);
    }

}