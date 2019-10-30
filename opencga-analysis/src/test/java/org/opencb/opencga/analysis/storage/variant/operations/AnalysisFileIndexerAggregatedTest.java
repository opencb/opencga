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

package org.opencb.opencga.analysis.storage.variant.operations;

import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.models.Cohort;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.opencb.biodata.models.variant.StudyEntry.DEFAULT_COHORT;

/**
 * Created on 05/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class AnalysisFileIndexerAggregatedTest extends AbstractVariantStorageOperationTest {

    private List<File> files = new ArrayList<>();
    private Logger logger = LoggerFactory.getLogger(AnalysisFileIndexerAggregatedTest.class);

    @Before
    public void beforeAggregatedIndex() throws Exception {
        files.add(create("variant-test-aggregated-file.vcf.gz"));
    }

    @Test
    public void testIndexWithAggregatedStats() throws Exception {
        QueryOptions queryOptions = new QueryOptions(VariantStorageEngine.Options.ANNOTATE.key(), false)
                .append(VariantStorageEngine.Options.AGGREGATED_TYPE.key(), Aggregation.BASIC);

        queryOptions.put(VariantStorageEngine.Options.CALCULATE_STATS.key(), true);
        queryOptions.putIfNotNull(StorageOperation.CATALOG_PATH, outputId);
        variantManager.index(studyFqn, files.get(0).getId(), opencga.createTmpOutdir(studyId, "index", sessionId), queryOptions, sessionId);
        assertEquals(0, getDefaultCohort(studyId).getSamples().size());
        assertEquals(Cohort.CohortStatus.READY, getDefaultCohort(studyId).getStatus().getName());
        StatsVariantStorageTest.checkCalculatedAggregatedStats(Collections.singleton(DEFAULT_COHORT), dbName);
    }

    @Override
    protected Aggregation getAggregation() {
        return Aggregation.BASIC;
    }
}
