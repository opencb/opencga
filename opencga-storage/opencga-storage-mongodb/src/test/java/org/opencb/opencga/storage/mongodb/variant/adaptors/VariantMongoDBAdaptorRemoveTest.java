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
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
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
    private StudyMetadata studyMetadata;
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
        studyMetadata = newStudyMetadata();
//            variantSource = new VariantSource(smallInputUri.getPath(), "testAlias", "testStudy", "Study for testing purposes");
        clearDB(DB_NAME);
        ObjectMap params = new ObjectMap()
                .append(VariantStorageOptions.ANNOTATE.key(), true)
                .append(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), "DS,GL")
                .append(VariantStorageOptions.ANNOTATOR_CLASS.key(), CellBaseRestVariantAnnotator.class.getName())
                .append(VariantStorageOptions.STATS_CALCULATE.key(), true);

//        HashSet FORMAT = new HashSet<>();
//        if (!params.getBoolean(VariantStorageEngine.Options.EXCLUDE_GENOTYPES.key(),
//                VariantStorageEngine.Options.EXCLUDE_GENOTYPES.defaultValue())) {
//            FORMAT.add("GT");
//        }
//        FORMAT.addAll(params.getAsStringList(VariantStorageEngine.Options.EXTRA_FORMAT_FIELDS.key()));

        StoragePipelineResult etlResult = runDefaultETL(smallInputUri, getVariantStorageEngine(), studyMetadata, params);
        VariantFileMetadata fileMetadata = variantStorageEngine.getVariantReaderUtils().readVariantFileMetadata(Paths.get(etlResult.getTransformResult().getPath()).toUri());
        numVariants = getExpectedNumLoadedVariants(fileMetadata);

        Integer indexedFileId = metadataManager.getIndexedFiles(studyMetadata.getId()).iterator().next();


        //Calculate stats
        QueryOptions options = new QueryOptions(VariantStorageOptions.STUDY.key(), STUDY_NAME)
                .append(VariantStorageOptions.LOAD_BATCH_SIZE.key(), 100)
                .append(DefaultVariantStatisticsManager.OUTPUT, outputUri)
                .append(DefaultVariantStatisticsManager.OUTPUT_FILE_NAME, "cohort1.cohort2.stats");
        Iterator<Integer> iterator = metadataManager.getFileMetadata(studyMetadata.getId(), indexedFileId).getSamples().iterator();

        /** Create cohorts **/
        HashSet<String> cohort1 = new HashSet<>();
        cohort1.add(metadataManager.getSampleName(studyMetadata.getId(), iterator.next()));
        cohort1.add(metadataManager.getSampleName(studyMetadata.getId(), iterator.next()));

        HashSet<String> cohort2 = new HashSet<>();
        cohort2.add(metadataManager.getSampleName(studyMetadata.getId(), iterator.next()));
        cohort2.add(metadataManager.getSampleName(studyMetadata.getId(), iterator.next()));

        Map<String, Set<String>> cohorts = new HashMap<>();
        cohorts.put("cohort1", cohort1);
        cohorts.put("cohort2", cohort2);
        metadataManager.registerCohorts(studyMetadata.getName(), cohorts);

        variantStorageEngine.calculateStats(studyMetadata.getName(),
                new ArrayList<>(cohorts.keySet()), options);

    }

    @After
    public void after() throws IOException {
        closeConnections();
    }

    @Test
    public void removeStudyTest() throws Exception {
        ((VariantMongoDBAdaptor) dbAdaptor).removeStudy(studyMetadata.getName(), System.currentTimeMillis(), new QueryOptions("purge", false));
        for (Variant variant : dbAdaptor) {
            for (Map.Entry<String, StudyEntry> entry : variant.getStudiesMap().entrySet()) {
                assertFalse(entry.getValue().getStudyId().equals(studyMetadata.getId() + ""));
            }
        }
        DataResult<Long> allVariants = dbAdaptor.count(new Query());
        assertEquals(numVariants, allVariants.first().intValue());
    }

    @Test
    public void removeAndPurgeStudyTest() throws Exception {
        ((VariantMongoDBAdaptor) dbAdaptor).removeStudy(studyMetadata.getName(), System.currentTimeMillis(), new QueryOptions("purge", true));
        for (Variant variant : dbAdaptor) {
            for (Map.Entry<String, StudyEntry> entry : variant.getStudiesMap().entrySet()) {
                assertFalse(entry.getValue().getStudyId().equals(studyMetadata.getId() + ""));
            }
        }
        DataResult<Variant> allVariants = dbAdaptor.get(new Query(), new QueryOptions());
        assertEquals(0, allVariants.getNumTotalResults());
    }

    @Test
    public void removeStatsTest() throws Exception {
        String deletedCohort = "cohort2";
        ((VariantMongoDBAdaptor) dbAdaptor).removeStats(studyMetadata.getName(), deletedCohort, new QueryOptions());

        for (Variant variant : dbAdaptor) {
            for (Map.Entry<String, StudyEntry> entry : variant.getStudiesMap().entrySet()) {
                assertFalse("The cohort '" + deletedCohort + "' is not completely deleted in variant: '" + variant + "'", entry.getValue
                        ().getStats().keySet().contains(deletedCohort));
            }
        }
        DataResult<Long> allVariants = dbAdaptor.count(new Query());
        assertEquals(numVariants, allVariants.first().intValue());
    }

    @Test
    public void removeAnnotationTest() throws Exception {
        Query query = new Query(VariantQueryParam.ANNOTATION_EXISTS.key(), true);
        query.put(VariantQueryParam.STUDY.key(), studyMetadata.getId());
        long numAnnotatedVariants = dbAdaptor.count(query).first();

        assertEquals("All variants should be annotated", numVariants, numAnnotatedVariants);

        query = new Query(VariantQueryParam.REGION.key(), "1");
        query.put(VariantQueryParam.STUDY.key(), studyMetadata.getId());
        long numVariantsChr1 = dbAdaptor.count(query).first();
        ((VariantMongoDBAdaptor) dbAdaptor).removeAnnotation("", new Query(VariantQueryParam.REGION.key(), "1"), new QueryOptions());

        query = new Query(VariantQueryParam.ANNOTATION_EXISTS.key(), false);
        query.put(VariantQueryParam.STUDY.key(), studyMetadata.getId());
        long numVariantsNoAnnotation = dbAdaptor.count(query).first();

        assertNotEquals(numVariantsChr1, numVariants);
        assertEquals(numVariantsChr1, numVariantsNoAnnotation);
    }

}