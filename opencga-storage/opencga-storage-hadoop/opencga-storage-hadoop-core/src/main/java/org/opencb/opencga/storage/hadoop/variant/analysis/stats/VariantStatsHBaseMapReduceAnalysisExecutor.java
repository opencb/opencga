package org.opencb.opencga.storage.hadoop.variant.analysis.stats;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.analysis.variant.VariantStatsAnalysisExecutor;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.exception.AnalysisException;
import org.opencb.opencga.core.exception.AnalysisExecutorException;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.analysis.HadoopVariantAnalysisExecutor;
import org.opencb.opencga.storage.hadoop.variant.stats.VariantStatsDriver;
import org.opencb.opencga.core.annotations.AnalysisExecutor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@AnalysisExecutor(id = "hbase-mapreduce", analysis = "variant-stats",
        framework = AnalysisExecutor.Framework.MAP_REDUCE,
        source = AnalysisExecutor.Source.HBASE)
public class VariantStatsHBaseMapReduceAnalysisExecutor extends VariantStatsAnalysisExecutor implements HadoopVariantAnalysisExecutor {

    @Override
    public void run() throws AnalysisException {
        if (isIndex()) {
            calculateAndIndex();
        } else {
            calculate();
        }
    }

    private void calculateAndIndex() throws AnalysisExecutorException {
        QueryOptions calculateStatsOptions = new QueryOptions(executorParams);

        VariantStorageEngine variantStorageEngine = getHadoopVariantStorageEngine();
        try {
            calculateStatsOptions.putAll(getVariantsQuery());

            variantStorageEngine.calculateStats(getStudy(), getCohorts(), calculateStatsOptions);
            variantStorageEngine.close();
        } catch (IOException | StorageEngineException e) {
            throw new AnalysisExecutorException(e);
        }
    }

    public void calculate() throws AnalysisException {
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
            throw new AnalysisExecutorException(e);
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
            throw new AnalysisExecutorException(e);
        } finally {
            for (Integer cohortId : cohortIds) {
                dbAdaptor.getMetadataManager().removeCohort(studyId, cohortId);
            }
        }
    }
}
