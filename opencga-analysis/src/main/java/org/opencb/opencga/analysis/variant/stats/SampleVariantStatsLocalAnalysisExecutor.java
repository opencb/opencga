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

package org.opencb.opencga.analysis.variant.stats;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.biodata.tools.variant.stats.SampleVariantStatsCalculator;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.opencga.analysis.StorageToolExecutor;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.variant.SampleVariantStatsAnalysisExecutor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.io.db.VariantDBReader;

import java.util.List;

@ToolExecutor(id="opencga-local", tool = SampleVariantStatsAnalysis.ID,
        framework = ToolExecutor.Framework.LOCAL,
        source = ToolExecutor.Source.STORAGE)
public class SampleVariantStatsLocalAnalysisExecutor extends SampleVariantStatsAnalysisExecutor implements StorageToolExecutor {

    @Override
    public int getDefaultBatchSize() {
        return 5000;
    }
    @Override
    public int getMaxBatchSize() {
        return 10000;
    }

    @Override
    public void run() throws ToolException {

        VariantStorageManager variantStorageManager = getVariantStorageManager();

        List<String> sampleNames = getSampleNames();

        Query query = new Query(getVariantQuery())
                .append(VariantQueryParam.STUDY.key(), getStudy())
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), sampleNames);

        List<SampleVariantStats> stats;
        try {
            DataResult<VariantMetadata> metadata = variantStorageManager.getMetadata(query, new QueryOptions(), getToken());
            SampleVariantStatsCalculator calculator = new SampleVariantStatsCalculator(metadata.first().getStudies().get(0));

            ProgressLogger progressLogger = new ProgressLogger("Variants processed:");
            VariantDBIterator iterator = variantStorageManager.iterator(query, new QueryOptions(), getToken());

            ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder().setNumTasks(1).setBatchSize(10).build();

            ParallelTaskRunner<Variant, Variant> ptr = new ParallelTaskRunner<>(
                    new VariantDBReader(iterator),
                    calculator.then((List<Variant> b) -> {
                        progressLogger.increment(b.size());
                        return b;
                    }),
                    null,
                    config);

            ptr.run();

            stats = calculator.getSampleVariantStats();
        } catch (Exception e) {
            throw new ToolExecutorException(e);
        }

        writeStatsToFile(stats);
    }
}
