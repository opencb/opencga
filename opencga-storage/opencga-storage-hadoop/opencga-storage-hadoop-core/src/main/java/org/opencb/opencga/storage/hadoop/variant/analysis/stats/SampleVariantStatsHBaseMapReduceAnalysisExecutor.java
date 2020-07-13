package org.opencb.opencga.storage.hadoop.variant.analysis.stats;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.tools.variant.SampleVariantStatsAnalysisExecutor;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.analysis.HadoopVariantStorageToolExecutor;
import org.opencb.opencga.storage.hadoop.variant.stats.SampleVariantStatsDriver;

import java.util.List;

@ToolExecutor(id="hbase-mapreduce", tool = "sample-variant-stats",
        framework = ToolExecutor.Framework.MAP_REDUCE,
        source = ToolExecutor.Source.HBASE)
public class SampleVariantStatsHBaseMapReduceAnalysisExecutor
        extends SampleVariantStatsAnalysisExecutor implements HadoopVariantStorageToolExecutor {

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

            ObjectMap params = new ObjectMap(engine.getOptions())
                    .append(SampleVariantStatsDriver.SAMPLES, sampleNames)
                    .append(SampleVariantStatsDriver.OUTPUT, getOutputFile().toAbsolutePath().toUri());
            engine.getMRExecutor().run(SampleVariantStatsDriver.class, SampleVariantStatsDriver.buildArgs(
                    dbAdaptor.getArchiveTableName(studyId),
                    dbAdaptor.getVariantTable(),
                    studyId,
                    null,
                    params
            ), engine.getOptions(), "Calculate sample variant stats");
        } catch (VariantQueryException | StorageEngineException e) {
            throw new ToolExecutorException(e);
        }
    }

}
