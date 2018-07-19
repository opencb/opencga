package org.opencb.opencga.storage.hadoop.variant.stats;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
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
        StudyConfiguration sc = dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(study, options).first();

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

        List<Integer> cohortIds = StudyConfigurationManager.getCohortIdsFromStudy(cohorts, sc);

        String hadoopRoute = options.getString(HadoopVariantStorageEngine.HADOOP_BIN, "hadoop");
        String jar = HadoopVariantStorageEngine.getJarWithDependencies(options);

        options.put(VariantStatsMapper.COHORTS, cohortIds);
        VariantStatsMapper.setAggregationMappingProperties(options, VariantStatisticsManager.getAggregationMappingProperties(options));

        Class execClass = VariantStatsDriver.class;
        String executable = hadoopRoute + " jar " + jar + ' ' + execClass.getName();
        String[] args = VariantStatsDriver.buildArgs(
                dbAdaptor.getTableNameGenerator().getArchiveTableName(sc.getStudyId()),
                dbAdaptor.getTableNameGenerator().getVariantTableName(),
                sc.getStudyId(), Collections.emptyList(), options);

        long startTime = System.currentTimeMillis();
        logger.info("------------------------------------------------------");
        logger.info("Calculate stats of cohorts {} into variants table '{}'", cohorts, dbAdaptor.getVariantTable());
        logger.debug(executable + ' ' + Arrays.toString(args));
        logger.info("------------------------------------------------------");
        int exitValue = mrExecutor.run(executable, args);
        logger.info("------------------------------------------------------");
        logger.info("Exit value: {}", exitValue);
        logger.info("Total time: {}s", (System.currentTimeMillis() - startTime) / 1000.0);
        if (exitValue != 0) {
            throw new StorageEngineException("Error calculating stats for cohorts " + cohorts);
        }

        dbAdaptor.getStudyConfigurationManager().updateStudyConfiguration(sc, options);
        try {
            dbAdaptor.updateStatsColumns(sc);
        } catch (SQLException e) {
            throw new IOException(e);
        }

        logger.info("Finishing stats calculation, time: {}s", (System.currentTimeMillis() - startTime) / 1000.0);

    }
}
