package org.opencb.opencga.storage.hadoop.variant.analysis.stats;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.analysis.variant.VariantStatsAnalysisExecutor;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.exception.AnalysisException;
import org.opencb.opencga.core.exception.AnalysisExecutorException;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.analysis.HadoopVariantAnalysisExecutor;
import org.opencb.opencga.storage.hadoop.variant.stats.VariantStatsDriver;
import org.opencb.opencga.core.annotations.AnalysisExecutor;

@AnalysisExecutor(id = "hbase-mapreduce", analysis = "variant-stats",
        framework = AnalysisExecutor.Framework.MAP_REDUCE,
        source = AnalysisExecutor.Source.HBASE)
public class VariantStatsHBaseMapReduceAnalysisExecutor extends VariantStatsAnalysisExecutor implements HadoopVariantAnalysisExecutor {

    @Override
    public void run() throws AnalysisException {
        HadoopVariantStorageEngine engine = getHadoopVariantStorageEngine();

        VariantHadoopDBAdaptor dbAdaptor;
        int studyId;
        Integer cohortId;
        String realCohortName = getCohort();
        String temporaryCohortName = "TEMP_" + realCohortName + "_" + TimeUtils.getTimeMillis();
        try {
            dbAdaptor = engine.getDBAdaptor();

            studyId = dbAdaptor.getMetadataManager().getStudyId(getStudy());

            cohortId = dbAdaptor.getMetadataManager().registerCohort(getStudy(), temporaryCohortName, getSamples());
            dbAdaptor.getMetadataManager().updateCohortMetadata(studyId, cohortId, cohortMetadata -> {
                cohortMetadata.getAttributes().put("alias", realCohortName);
                cohortMetadata.setStatus("TEMPORARY", TaskMetadata.Status.RUNNING);
                return cohortMetadata;
            });
        } catch (StorageEngineException e) {
            throw new AnalysisExecutorException(e);
        }

        try {
            ObjectMap params = new ObjectMap(getVariantsQuery())
                    .append(VariantStatsDriver.COHORTS, temporaryCohortName)
                    .append(VariantStatsDriver.OUTPUT, getOutputFile().toAbsolutePath().toUri());
            engine.getMRExecutor().run(VariantStatsDriver.class, VariantStatsDriver.buildArgs(
                    dbAdaptor.getArchiveTableName(studyId),
                    dbAdaptor.getVariantTable(),
                    studyId,
                    null,
                    params
            ), params, "Calculate sample variant stats");

        } catch (VariantQueryException | StorageEngineException e) {
            throw new AnalysisExecutorException(e);
        } finally {
            dbAdaptor.getMetadataManager().removeCohort(studyId, cohortId);
        }
    }
}
