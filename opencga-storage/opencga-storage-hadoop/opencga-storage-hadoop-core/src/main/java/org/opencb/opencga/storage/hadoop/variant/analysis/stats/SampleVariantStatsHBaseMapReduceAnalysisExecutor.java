package org.opencb.opencga.storage.hadoop.variant.analysis.stats;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.variant.SampleVariantStatsAnalysisExecutor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.analysis.HadoopVariantStorageToolExecutor;
import org.opencb.opencga.storage.hadoop.variant.stats.SampleVariantStatsDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@ToolExecutor(id="hbase-mapreduce", tool = "sample-variant-stats",
        framework = ToolExecutor.Framework.MAP_REDUCE,
        source = ToolExecutor.Source.HBASE)
public class SampleVariantStatsHBaseMapReduceAnalysisExecutor
        extends SampleVariantStatsAnalysisExecutor implements HadoopVariantStorageToolExecutor {

    private static final int DEFAULT_SAMPLES_BATCH_SIZE = 1000;
    private static final int MAX_SAMPLES_BATCH_SIZE = 4000;
    private static Logger logger = LoggerFactory.getLogger(SampleVariantStatsHBaseMapReduceAnalysisExecutor.class);

    @Override
    public void run() throws ToolException {
        String study = getStudy();
        List<String> sampleNames = getSampleNames();

        HadoopVariantStorageEngine engine = getHadoopVariantStorageEngine();

        try {
            VariantHadoopDBAdaptor dbAdaptor = engine.getDBAdaptor();
            int studyId = engine.getMetadataManager().getStudyId(study);

            for (String sampleName : sampleNames) {
                Integer sampleId = engine.getMetadataManager().getSampleId(studyId, sampleName, true);
                if (sampleId == null) {
                    throw VariantQueryException.sampleNotFound(sampleName, study);
                }
            }

            VariantQuery query = engine.parseQuery(getVariantQuery(), new QueryOptions()).getQuery();
            // SampleData and FileData filters should not include the sample or file names.
            // The parser would add them. Restore the original query values (if any)
            query.putIfNotNull(VariantQueryParam.SAMPLE_DATA.key(), getVariantQuery().get(VariantQueryParam.SAMPLE_DATA.key()));
            query.putIfNotNull(VariantQueryParam.FILE_DATA.key(), getVariantQuery().get(VariantQueryParam.FILE_DATA.key()));
            ObjectMap params = new ObjectMap(engine.getOptions())
                    .appendAll(query)
                    .append(SampleVariantStatsDriver.SAMPLES, sampleNames)
                    .append(SampleVariantStatsDriver.OUTPUT, getOutputFile().toAbsolutePath().toUri());
            engine.getMRExecutor().run(SampleVariantStatsDriver.class, SampleVariantStatsDriver.buildArgs(
                    dbAdaptor.getArchiveTableName(studyId),
                    dbAdaptor.getVariantTable(),
                    studyId,
                    null,
                    params
            ), "Calculate sample variant stats");

        } catch (Exception e) {
            throw new ToolExecutorException(e);
        }
    }

    @Override
    public int getDefaultBatchSize() {
        return DEFAULT_SAMPLES_BATCH_SIZE;
    }

    @Override
    public int getMaxBatchSize() {
        return MAX_SAMPLES_BATCH_SIZE;
    }
}
