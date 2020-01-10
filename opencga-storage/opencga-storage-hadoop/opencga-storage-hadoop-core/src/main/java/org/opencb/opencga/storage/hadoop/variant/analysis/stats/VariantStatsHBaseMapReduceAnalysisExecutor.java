package org.opencb.opencga.storage.hadoop.variant.analysis.stats;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.tools.variant.VariantStatsAnalysisExecutor;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.exception.ToolException;
import org.opencb.opencga.core.exception.ToolExecutorException;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.analysis.HadoopVariantStorageToolExecutor;
import org.opencb.opencga.storage.hadoop.variant.stats.VariantStatsDriver;
import org.opencb.opencga.core.annotations.ToolExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@ToolExecutor(id = "hbase-mapreduce", tool = "variant-stats",
        framework = ToolExecutor.Framework.MAP_REDUCE,
        source = ToolExecutor.Source.HBASE)
public class VariantStatsHBaseMapReduceAnalysisExecutor extends VariantStatsAnalysisExecutor implements HadoopVariantStorageToolExecutor {

    @Override
    public void run() throws ToolException {
        HadoopVariantStorageEngine engine = getHadoopVariantStorageEngine();

        VariantHadoopDBAdaptor dbAdaptor;
        int studyId;
        List<Integer> cohortIds = new ArrayList<>();
        List<String> temporaryCohortNames = new ArrayList<>();
        try {
            dbAdaptor = engine.getDBAdaptor();

            studyId = dbAdaptor.getMetadataManager().getStudyId(getStudy());

            for (Map.Entry<String, List<String>> entry : getCohorts().entrySet()) {

                String realCohortName = entry.getKey();
                String temporaryCohortName = "TEMP_" + realCohortName + "_" + TimeUtils.getTimeMillis();
                temporaryCohortNames.add(temporaryCohortName);

                int cohortId = dbAdaptor.getMetadataManager().registerCohort(getStudy(), temporaryCohortName, entry.getValue());

                dbAdaptor.getMetadataManager().updateCohortMetadata(studyId, cohortId, cohortMetadata -> {
                    cohortMetadata.getAttributes().put("alias", realCohortName);
                    cohortMetadata.setStatus("TEMPORARY", TaskMetadata.Status.RUNNING);
                    return cohortMetadata;
                });
                cohortIds.add(cohortId);
            }


        } catch (StorageEngineException e) {
            throw new ToolExecutorException(e);
        }

        try {
            Query variantsQuery = getVariantsQuery();
            variantsQuery = engine.preProcessQuery(variantsQuery, new QueryOptions());
            ObjectMap params = new ObjectMap(variantsQuery)
                    .append(VariantStatsDriver.COHORTS, temporaryCohortNames)
                    .append(VariantStatsDriver.OUTPUT, getOutputFile().toAbsolutePath().toUri());
            engine.getMRExecutor().run(VariantStatsDriver.class, VariantStatsDriver.buildArgs(
                    dbAdaptor.getArchiveTableName(studyId),
                    dbAdaptor.getVariantTable(),
                    studyId,
                    null,
                    params
            ), engine.getOptions(), "Calculate sample variant stats");

        } catch (VariantQueryException | StorageEngineException e) {
            throw new ToolExecutorException(e);
        } finally {
            for (Integer cohortId : cohortIds) {
                dbAdaptor.getMetadataManager().removeCohort(studyId, cohortId);
            }
        }
    }

}
