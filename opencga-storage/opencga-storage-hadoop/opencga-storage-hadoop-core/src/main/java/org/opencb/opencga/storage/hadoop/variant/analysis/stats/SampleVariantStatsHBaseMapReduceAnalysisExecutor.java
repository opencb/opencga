package org.opencb.opencga.storage.hadoop.variant.analysis.stats;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.analysis.HadoopVariantAnalysisExecutor;
import org.opencb.opencga.storage.hadoop.variant.stats.SampleVariantStatsDriver;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.analysis.exceptions.AnalysisExecutorException;
import org.opencb.oskar.analysis.variant.stats.SampleVariantStatsAnalysis;
import org.opencb.oskar.analysis.variant.stats.SampleVariantStatsAnalysisExecutor;
import org.opencb.oskar.core.annotations.AnalysisExecutor;

import java.util.List;

@AnalysisExecutor(id="hbase-mapreduce", analysis= SampleVariantStatsAnalysis.ID,
        framework = AnalysisExecutor.Framework.MAP_REDUCE,
        source = AnalysisExecutor.Source.HBASE)
public class SampleVariantStatsHBaseMapReduceAnalysisExecutor
        extends SampleVariantStatsAnalysisExecutor implements HadoopVariantAnalysisExecutor {

    @Override
    public void exec() throws AnalysisException {
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

            ObjectMap params = new ObjectMap()
                    .append(SampleVariantStatsDriver.SAMPLES, sampleNames)
                    .append(SampleVariantStatsDriver.OUTPUT, getOutputFile().toAbsolutePath().toUri());
            engine.getMRExecutor().run(SampleVariantStatsDriver.class, SampleVariantStatsDriver.buildArgs(
                    dbAdaptor.getArchiveTableName(studyId),
                    dbAdaptor.getVariantTable(),
                    studyId,
                    null,
                    params
            ), params, "Calculate sample variant stats");
        } catch (VariantQueryException | StorageEngineException e) {
            throw new AnalysisExecutorException(e);
        }
    }

}
