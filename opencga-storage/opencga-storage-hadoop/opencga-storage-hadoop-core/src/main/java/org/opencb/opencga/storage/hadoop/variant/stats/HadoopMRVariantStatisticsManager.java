package org.opencb.opencga.storage.hadoop.variant.stats;

import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Created on 14/11/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopMRVariantStatisticsManager extends VariantStatisticsManager {

    private final VariantHadoopDBAdaptor dbAdaptor;
    private final Logger logger = LoggerFactory.getLogger(HadoopMRVariantStatisticsManager.class);
    private final MRExecutor mrExecutor;
    private final ObjectMap baseOptions;

    public HadoopMRVariantStatisticsManager(VariantHadoopDBAdaptor dbAdaptor, MRExecutor mrExecutor, ObjectMap options) {
        this.dbAdaptor = dbAdaptor;
        this.mrExecutor = mrExecutor;
        baseOptions = options;
    }

    @Override
    public void calculateStatistics(String study, List<String> cohorts, QueryOptions inputOptions)
            throws IOException, StorageEngineException {
        QueryOptions options = new QueryOptions(baseOptions);
        if (inputOptions != null) {
            options.putAll(inputOptions);
        }
        VariantStorageMetadataManager metadataManager = dbAdaptor.getMetadataManager();
        StudyMetadata sm = metadataManager.getStudyMetadata(study);

        if (isAggregated(sm, options)) {
            Aggregation aggregation = getAggregation(sm, options);
            VariantStatsMapper.setAggregation(options, aggregation);
            VariantStatsMapper.setAggregationMappingProperties(options, VariantStatisticsManager.getAggregationMappingProperties(options));
//            throw new StorageEngineException("Unsupported calculate aggregated statistics with map-reduce. Please, use "
//                    + HadoopVariantStorageEngine.STATS_LOCAL + '=' + true);
        }
        boolean overwriteStats = options.getBoolean(VariantStorageOptions.STATS_OVERWRITE.key(), false);
//
//        DefaultVariantStatisticsManager.checkAndUpdateStudyConfigurationCohorts(sc, cohorts.stream()
//                    .collect(Collectors.toMap(c -> c, c -> Collections.emptySet())), null, updateStats, overwriteStats);
//        dbAdaptor.getStudyConfigurationManager().updateStudyConfiguration(sc, options);

        preCalculateStats(metadataManager, sm, cohorts, overwriteStats, options);

        options.put(VariantStatsDriver.COHORTS, cohorts);
        options.remove(VariantStatsDriver.OUTPUT);

        boolean error = false;
        try {
            String[] args = VariantStatsDriver.buildArgs(
                    dbAdaptor.getTableNameGenerator().getArchiveTableName(sm.getId()),
                    dbAdaptor.getTableNameGenerator().getVariantTableName(),
                    sm.getId(), Collections.emptyList(), options);
            mrExecutor.run(VariantStatsDriver.class, args, options, "Calculate stats of cohorts " + cohorts);
        } catch (Exception e) {
            error = true;
            throw e;
        } finally {
            postCalculateStats(metadataManager, sm, cohorts, error);
        }

        dbAdaptor.updateStatsColumns(sm);

    }
}
