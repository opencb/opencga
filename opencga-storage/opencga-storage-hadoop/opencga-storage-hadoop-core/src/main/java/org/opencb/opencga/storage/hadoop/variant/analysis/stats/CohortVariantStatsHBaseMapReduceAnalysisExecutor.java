package org.opencb.opencga.storage.hadoop.variant.analysis.stats;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.utils.StringUtils;
import org.opencb.opencga.core.analysis.variant.CohortVariantStatsAnalysisExecutor;
import org.opencb.opencga.core.annotations.AnalysisExecutor;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.exception.AnalysisException;
import org.opencb.opencga.core.exception.AnalysisExecutorException;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.analysis.HadoopVariantAnalysisExecutor;
import org.opencb.opencga.storage.hadoop.variant.stats.CohortVariantStatsDriver;

@AnalysisExecutor(id = "hbase-mapreduce", analysis = "cohort-variant-stats",
        framework = AnalysisExecutor.Framework.MAP_REDUCE,
        source = AnalysisExecutor.Source.HBASE)
public class CohortVariantStatsHBaseMapReduceAnalysisExecutor
        extends CohortVariantStatsAnalysisExecutor implements HadoopVariantAnalysisExecutor {

    @Override
    public void run() throws AnalysisException {

        HadoopVariantStorageEngine engine = getHadoopVariantStorageEngine();

        VariantHadoopDBAdaptor dbAdaptor;
        int studyId;
        Integer cohortId;

        String temporaryCohortName = "TEMP_" + StringUtils.randomString(5) + "_" + TimeUtils.getTimeMillis();
        try {
            dbAdaptor = engine.getDBAdaptor();

            studyId = dbAdaptor.getMetadataManager().getStudyId(getStudy());

            cohortId = dbAdaptor.getMetadataManager().registerCohort(getStudy(), temporaryCohortName, getSampleNames());
            dbAdaptor.getMetadataManager().updateCohortMetadata(studyId, cohortId, cohortMetadata -> {
                cohortMetadata.setStatus("TEMPORARY", TaskMetadata.Status.RUNNING);
                return cohortMetadata;
            });
        } catch (StorageEngineException e) {
            throw new AnalysisExecutorException(e);
        }

        try {
//            Query variantsQuery = getVariantsQuery();
            Query variantsQuery = new Query();

            ObjectMap params = new ObjectMap(variantsQuery)
                    .append(CohortVariantStatsDriver.COHORT, temporaryCohortName)
                    .append(CohortVariantStatsDriver.OUTPUT, getOutputFile().toAbsolutePath().toUri());
            engine.getMRExecutor().run(CohortVariantStatsDriver.class, CohortVariantStatsDriver.buildArgs(
                    dbAdaptor.getArchiveTableName(studyId),
                    dbAdaptor.getVariantTable(),
                    studyId,
                    null,
                    params
            ), params, "Calculate cohort variant stats");

        } catch (VariantQueryException | StorageEngineException e) {
            throw new AnalysisExecutorException(e);
        } finally {
            dbAdaptor.getMetadataManager().removeCohort(studyId, cohortId);
        }
    }

}
