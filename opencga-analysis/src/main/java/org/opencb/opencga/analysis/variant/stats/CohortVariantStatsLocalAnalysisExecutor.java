package org.opencb.opencga.analysis.variant.stats;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.biodata.models.variant.stats.VariantSetStats;
import org.opencb.biodata.tools.variant.stats.VariantSetStatsCalculator;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.opencga.core.analysis.variant.CohortVariantStatsAnalysisExecutor;
import org.opencb.opencga.core.exception.AnalysisException;
import org.opencb.opencga.core.exception.AnalysisExecutorException;
import org.opencb.opencga.analysis.variant.VariantStorageAnalysisExecutor;
import org.opencb.opencga.core.annotations.AnalysisExecutor;
import org.opencb.opencga.analysis.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.io.db.VariantDBReader;

import java.util.List;

@AnalysisExecutor(id="opencga-local", analysis= CohortVariantStatsAnalysis.ID,
        framework = AnalysisExecutor.Framework.LOCAL,
        source = AnalysisExecutor.Source.STORAGE)
public class CohortVariantStatsLocalAnalysisExecutor extends CohortVariantStatsAnalysisExecutor implements VariantStorageAnalysisExecutor {

    @Override
    public void run() throws AnalysisException {


        VariantStorageManager variantStorageManager = getVariantStorageManager();

        List<String> sampleNames = getSampleNames();

        Query query = new Query()
                .append(VariantQueryParam.STUDY.key(), getStudy())
                .append(VariantQueryParam.SAMPLE.key(), sampleNames);

        VariantSetStats stats;
        try {
            DataResult<VariantMetadata> metadata = variantStorageManager.getMetadata(query, new QueryOptions(), getToken());
            VariantSetStatsCalculator calculator = new VariantSetStatsCalculator(metadata.first().getStudies().get(0));

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

            stats = calculator.getStats();
        } catch (Exception e) {
            throw new AnalysisExecutorException(e);
        }

        writeStatsToFile(stats.getImpl());
    }

}
