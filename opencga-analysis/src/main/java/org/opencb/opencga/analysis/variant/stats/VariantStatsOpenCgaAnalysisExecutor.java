package org.opencb.opencga.analysis.variant.stats;

import org.apache.commons.io.FileUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.biodata.tools.variant.stats.VariantStatsCalculator;
import org.opencb.biodata.tools.variant.stats.writer.VariantStatsTsvExporter;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.run.Task;
import org.opencb.opencga.analysis.OpenCgaAnalysisExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.manager.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.io.db.VariantDBReader;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.analysis.exceptions.AnalysisExecutorException;
import org.opencb.oskar.analysis.variant.stats.VariantStatsAnalysis;
import org.opencb.oskar.analysis.variant.stats.VariantStatsAnalysisExecutor;
import org.opencb.oskar.core.annotations.AnalysisExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

@AnalysisExecutor(id="opencga-local", analysis= VariantStatsAnalysis.ID,
        framework = AnalysisExecutor.Framework.ITERATOR,
        source = AnalysisExecutor.Source.OPENCGA)
public class VariantStatsOpenCgaAnalysisExecutor extends VariantStatsAnalysisExecutor implements OpenCgaAnalysisExecutor {

    private final Logger logger = LoggerFactory.getLogger(VariantStatsOpenCgaAnalysisExecutor.class);

    @Override
    public void exec() throws AnalysisException {

        VariantStorageManager manager = getVariantStorageManager();
        Query query = new Query(getVariantsQuery())
                .append(VariantQueryParam.STUDY.key(), getStudy())
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), getSamples());

        QueryOptions queryOptions = new QueryOptions(QueryOptions.EXCLUDE, Arrays.asList(VariantField.STUDIES_STATS));

        try (OutputStream os = new BufferedOutputStream(FileUtils.openOutputStream(getOutputFile().toFile()))) {
            VariantDBReader reader = new VariantDBReader(manager.iterator(query, queryOptions, getSessionId()));
            String cohort = getCohort();

            ProgressLogger progressLogger = new ProgressLogger("Variants processed:");
            Task<Variant, Variant> task = Task.forEach(variant -> {
                StudyEntry study = variant.getStudies().get(0);
                VariantStats stats = VariantStatsCalculator.calculate(variant, study);
                study.setStats(Collections.singletonMap(cohort, stats));
                progressLogger.increment(1);
                return variant;
            });

            VariantStatsTsvExporter writer = new VariantStatsTsvExporter(os, getStudy(), Collections.singletonList(cohort));

            ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder().build();
            ParallelTaskRunner<Variant, Variant> ptr = new ParallelTaskRunner<>(reader, task, writer, config);

            ptr.run();

            arm.updateResult(analysisResult ->
                    analysisResult.getAttributes().put("numVariantStats", writer.getWrittenVariants()));
        } catch (ExecutionException | IOException | CatalogException | StorageEngineException e) {
            throw new AnalysisExecutorException(e);
        }
    }
}
