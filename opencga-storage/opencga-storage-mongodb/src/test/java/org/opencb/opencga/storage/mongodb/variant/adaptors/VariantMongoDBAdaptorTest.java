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

package org.opencb.opencga.storage.mongodb.variant.adaptors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorTest;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author Alejandro Aleman Ramos <aaleman@cipf.es>
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class VariantMongoDBAdaptorTest extends VariantDBAdaptorTest implements MongoDBVariantStorageTest {


    @Before
    public void setUpLoggers() throws Exception {
        logLevel("debug");
    }

    @After
    public void resetLoggers() throws Exception {
        logLevel("info");
    }

    @Override
    public void after() throws IOException {
        super.after();
        closeConnections();
    }

    @Test
    public void removeStudyTest() throws Exception {
        fileIndexed = false;
        ((VariantMongoDBAdaptor) dbAdaptor).removeStudy(studyConfiguration.getStudyName(), new QueryOptions("purge", false));
        for (Variant variant : dbAdaptor) {
            for (Map.Entry<String, StudyEntry> entry : variant.getStudiesMap().entrySet()) {
                assertFalse(entry.getValue().getStudyId().equals(studyConfiguration.getStudyId() + ""));
            }
        }
        QueryResult<Long> allVariants = dbAdaptor.count(new Query());
        assertEquals(NUM_VARIANTS, allVariants.first().intValue());
    }

    @Test
    public void removeAndPurgeStudyTest() throws Exception {
        fileIndexed = false;
        ((VariantMongoDBAdaptor) dbAdaptor).removeStudy(studyConfiguration.getStudyName(), new QueryOptions("purge", true));
        for (Variant variant : dbAdaptor) {
            for (Map.Entry<String, StudyEntry> entry : variant.getStudiesMap().entrySet()) {
                assertFalse(entry.getValue().getStudyId().equals(studyConfiguration.getStudyId() + ""));
            }
        }
        QueryResult<Variant> allVariants = dbAdaptor.get(new Query(), new QueryOptions());
        assertEquals(0, allVariants.getNumTotalResults());
    }

    @Test
    public void removeStatsTest() throws Exception {
        fileIndexed = false;
        String deletedCohort = "cohort2";
        ((VariantMongoDBAdaptor) dbAdaptor).removeStats(studyConfiguration.getStudyName(), deletedCohort, new QueryOptions());

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
    public void removeAnnotationTest() throws Exception {
        fileIndexed = false;
        Query query = new Query(VariantQueryParam.ANNOTATION_EXISTS.key(), true);
        query.put(VariantQueryParam.STUDIES.key(), studyConfiguration.getStudyId());
        long numAnnotatedVariants = dbAdaptor.count(query).first();

        assertEquals("All variants should be annotated", NUM_VARIANTS, numAnnotatedVariants);

        query = new Query(VariantQueryParam.CHROMOSOME.key(), "1");
        query.put(VariantQueryParam.STUDIES.key(), studyConfiguration.getStudyId());
        long numVariantsChr1 = dbAdaptor.count(query).first();
        ((VariantMongoDBAdaptor) dbAdaptor).removeAnnotation("", new Query(VariantQueryParam.CHROMOSOME.key(), "1"), new QueryOptions());

        query = new Query(VariantQueryParam.ANNOTATION_EXISTS.key(), false);
        query.put(VariantQueryParam.STUDIES.key(), studyConfiguration.getStudyId());
        long numVariantsNoAnnotation = dbAdaptor.count(query).first();

        assertNotEquals(numVariantsChr1, NUM_VARIANTS);
        assertEquals(numVariantsChr1, numVariantsNoAnnotation);
    }

}