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

package org.opencb.opencga.analysis.variant.manager.operations;

import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.models.cohort.CohortStatus;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
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
public class AnalysisFileIndexerAggregatedTest extends AbstractVariantOperationManagerTest {

    private List<File> files = new ArrayList<>();
    private Logger logger = LoggerFactory.getLogger(AnalysisFileIndexerAggregatedTest.class);

    @Before
    public void beforeAggregatedIndex() throws Exception {
        files.add(create("variant-test-aggregated-file.vcf.gz"));
    }

    @Test
    public void testIndexWithAggregatedStats() throws Exception {
        QueryOptions queryOptions = new QueryOptions(VariantStorageOptions.ANNOTATE.key(), false)
                .append(VariantStorageOptions.STATS_AGGREGATION.key(), Aggregation.BASIC);

        queryOptions.put(VariantStorageOptions.STATS_CALCULATE.key(), true);
        variantManager.index(studyFqn, files.get(0).getId(), opencga.createTmpOutdir(studyId, "index", sessionId), queryOptions, sessionId);
        assertEquals(0, getDefaultCohort(studyId).getSamples().size());
        assertEquals(CohortStatus.READY, getDefaultCohort(studyId).getInternal().getStatus().getName());
        StatsVariantStorageTest.checkCalculatedAggregatedStats(Collections.singleton(DEFAULT_COHORT), dbName);
    }

    @Override
    protected Aggregation getAggregation() {
        return Aggregation.BASIC;
    }
}
