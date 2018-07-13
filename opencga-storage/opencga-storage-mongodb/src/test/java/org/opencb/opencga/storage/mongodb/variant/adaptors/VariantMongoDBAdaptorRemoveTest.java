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
import org.opencb.biodata.models.metadata.SampleSetType;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.annotators.CellBaseRestVariantAnnotator;
import org.opencb.opencga.storage.core.variant.stats.DefaultVariantStatisticsManager;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.Assert.*;

/**
 * @author Alejandro Aleman Ramos <aaleman@cipf.es>
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class VariantMongoDBAdaptorRemoveTest extends VariantStorageBaseTest implements MongoDBVariantStorageTest {


    private VariantDBAdaptor dbAdaptor;
    private StudyConfiguration studyConfiguration;
    private int numVariants;

    @Before
    public void setUpLoggers() throws Exception {
        logLevel("debug");
    }

    @After
    public void resetLoggers() throws Exception {
        logLevel("info");
    }

    @Override
    @Before
    public void before() throws Exception {

        dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        studyConfiguration = newStudyConfiguration();
//            variantSource = new VariantSource(smallInputUri.getPath(), "testAlias", "testStudy", "Study for testing purposes");
        clearDB(DB_NAME);
        ObjectMap params = new ObjectMap(VariantStorageEngine.Options.STUDY_TYPE.key(), SampleSetType.FAMILY)
                .append(VariantStorageEngine.Options.ANNOTATE.key(), true)
                .append(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), "DS,GL")
                .append(VariantAnnotationManager.VARIANT_ANNOTATOR_CLASSNAME, CellBaseRestVariantAnnotator.class.getName())
                .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), true);

//        HashSet FORMAT = new HashSet<>();
//        if (!params.getBoolean(VariantStorageEngine.Options.EXCLUDE_GENOTYPES.key(),
//                VariantStorageEngine.Options.EXCLUDE_GENOTYPES.defaultValue())) {
//            FORMAT.add("GT");
//        }
//        FORMAT.addAll(params.getAsStringList(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key()));

        StoragePipelineResult etlResult = runDefaultETL(smallInputUri, getVariantStorageEngine(), studyConfiguration, params);
        VariantFileMetadata fileMetadata = variantStorageEngine.getVariantReaderUtils().readVariantFileMetadata(Paths.get(etlResult.getTransformResult().getPath()).toUri());
        numVariants = getExpectedNumLoadedVariants(fileMetadata);

        Integer indexedFileId = studyConfiguration.getIndexedFiles().iterator().next();


        //Calculate stats
        QueryOptions options = new QueryOptions(VariantStorageEngine.Options.STUDY.key(), STUDY_NAME)
                .append(VariantStorageEngine.Options.LOAD_BATCH_SIZE.key(), 100)
                .append(DefaultVariantStatisticsManager.OUTPUT, outputUri)
                .append(DefaultVariantStatisticsManager.OUTPUT_FILE_NAME, "cohort1.cohort2.stats");
        Iterator<Integer> iterator = studyConfiguration.getSamplesInFiles().get(indexedFileId).iterator();

        /** Create cohorts **/
        HashSet<Integer> cohort1 = new HashSet<>();
        cohort1.add(iterator.next());
        cohort1.add(iterator.next());

        HashSet<Integer> cohort2 = new HashSet<>();
        cohort2.add(iterator.next());
        cohort2.add(iterator.next());

        Map<String, Integer> cohortIds = new HashMap<>();
        cohortIds.put("cohort1", 10);
        cohortIds.put("cohort2", 11);

        studyConfiguration.getCohortIds().putAll(cohortIds);
        studyConfiguration.getCohorts().put(10, cohort1);
        studyConfiguration.getCohorts().put(11, cohort2);

        dbAdaptor.getStudyConfigurationManager().updateStudyConfiguration(studyConfiguration, QueryOptions.empty());

        variantStorageEngine.calculateStats(studyConfiguration.getStudyName(),
                new ArrayList<>(cohortIds.keySet()), options);

    }

    @After
    public void after() throws IOException {
        closeConnections();
    }

    @Test
    public void removeStudyTest() throws Exception {
        ((VariantMongoDBAdaptor) dbAdaptor).removeStudy(studyConfiguration.getStudyName(), new QueryOptions("purge", false));
        for (Variant variant : dbAdaptor) {
            for (Map.Entry<String, StudyEntry> entry : variant.getStudiesMap().entrySet()) {
                assertFalse(entry.getValue().getStudyId().equals(studyConfiguration.getStudyId() + ""));
            }
        }
        QueryResult<Long> allVariants = dbAdaptor.count(new Query());
        assertEquals(numVariants, allVariants.first().intValue());
    }

    @Test
    public void removeAndPurgeStudyTest() throws Exception {
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
        String deletedCohort = "cohort2";
        ((VariantMongoDBAdaptor) dbAdaptor).removeStats(studyConfiguration.getStudyName(), deletedCohort, new QueryOptions());

        for (Variant variant : dbAdaptor) {
            for (Map.Entry<String, StudyEntry> entry : variant.getStudiesMap().entrySet()) {
                assertFalse("The cohort '" + deletedCohort + "' is not completely deleted in variant: '" + variant + "'", entry.getValue
                        ().getStats().keySet().contains(deletedCohort));
            }
        }
        QueryResult<Long> allVariants = dbAdaptor.count(new Query());
        assertEquals(numVariants, allVariants.first().intValue());
    }

    @Test
    public void removeAnnotationTest() throws Exception {
        Query query = new Query(VariantQueryParam.ANNOTATION_EXISTS.key(), true);
        query.put(VariantQueryParam.STUDY.key(), studyConfiguration.getStudyId());
        long numAnnotatedVariants = dbAdaptor.count(query).first();

        assertEquals("All variants should be annotated", numVariants, numAnnotatedVariants);

        query = new Query(VariantQueryParam.REGION.key(), "1");
        query.put(VariantQueryParam.STUDY.key(), studyConfiguration.getStudyId());
        long numVariantsChr1 = dbAdaptor.count(query).first();
        ((VariantMongoDBAdaptor) dbAdaptor).removeAnnotation("", new Query(VariantQueryParam.REGION.key(), "1"), new QueryOptions());

        query = new Query(VariantQueryParam.ANNOTATION_EXISTS.key(), false);
        query.put(VariantQueryParam.STUDY.key(), studyConfiguration.getStudyId());
        long numVariantsNoAnnotation = dbAdaptor.count(query).first();

        assertNotEquals(numVariantsChr1, numVariants);
        assertEquals(numVariantsChr1, numVariantsNoAnnotation);
    }

}