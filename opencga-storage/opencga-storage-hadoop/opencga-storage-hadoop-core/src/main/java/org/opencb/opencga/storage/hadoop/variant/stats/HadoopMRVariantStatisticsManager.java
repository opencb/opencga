package org.opencb.opencga.storage.hadoop.variant.stats;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

/**
 * Created on 14/11/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopMRVariantStatisticsManager implements VariantStatisticsManager {

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
        VariantStorageMetadataManager metadataManager = dbAdaptor.getVariantStorageMetadataManager();
        StudyConfiguration sc = metadataManager.getStudyConfiguration(study, options).first();

        if (sc.isAggregated()) {
            throw new StorageEngineException("Unsupported calculate aggregated statistics with map-reduce. Please, use "
                    + HadoopVariantStorageEngine.STATS_LOCAL + '=' + true);
        }
        boolean updateStats = options.getBoolean(VariantStorageEngine.Options.UPDATE_STATS.key(), false);
//        boolean overwriteStats = options.getBoolean(VariantStorageEngine.Options.OVERWRITE_STATS.key(), false);
//
//        DefaultVariantStatisticsManager.checkAndUpdateStudyConfigurationCohorts(sc, cohorts.stream()
//                    .collect(Collectors.toMap(c -> c, c -> Collections.emptySet())), null, updateStats, overwriteStats);
//        dbAdaptor.getStudyConfigurationManager().updateStudyConfiguration(sc, options);

        VariantStatisticsManager.checkAndUpdateCalculatedCohorts(sc, cohorts, updateStats);

        List<Integer> cohortIds = metadataManager.getCohortIds(sc.getId(), cohorts);

        options.put(VariantStatsMapper.COHORTS, cohortIds);
        VariantStatsMapper.setAggregationMappingProperties(options, VariantStatisticsManager.getAggregationMappingProperties(options));

        String[] args = VariantStatsDriver.buildArgs(
                dbAdaptor.getTableNameGenerator().getArchiveTableName(sc.getStudyId()),
                dbAdaptor.getTableNameGenerator().getVariantTableName(),
                sc.getStudyId(), Collections.emptyList(), options);
        mrExecutor.run(VariantStatsDriver.class, args, options, "Calculate stats of cohorts " + cohorts);

        metadataManager.updateStudyConfiguration(sc, options);
        try {
            dbAdaptor.updateStatsColumns(sc);
        } catch (SQLException e) {
            throw new IOException(e);
        }

    }
}
