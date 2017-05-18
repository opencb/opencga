package org.opencb.opencga.storage.hadoop.variant.stats;

import com.google.common.collect.BiMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.AbstractAnalysisTableDriver;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

/**
 * Created by mh719 on 21/11/2016.
 */
public class VariantTableStatsDriver extends AbstractAnalysisTableDriver {

    private final Logger logger = LoggerFactory.getLogger(VariantTableStatsDriver.class);

    public VariantTableStatsDriver() {
        super();
    }

    public VariantTableStatsDriver(Configuration conf) {
        super(conf);
    }

    @Override
    protected void parseAndValidateParameters() {
        // nothing to do
    }

    @Override
    protected Class<? extends TableMapper> getMapperClass() {
        return AnalysisStatsMapper.class;
    }

    @Override
    protected Job setupJob(Job job, String archiveTable, String variantTable, List<Integer> files) throws IOException {
        // QUERY design
        Scan scan = createVariantsTableScan();

        // Read and write into the same table
        initMapReduceJob(job, getMapperClass(), variantTable, variantTable, scan);

        return job;
    }

    @Override
    protected String getJobOperationName() {
        return "Calculate stats";
    }

    @Override
    protected void preExecution(String variantTable) throws IOException, StorageEngineException {
        String defaultCohortName = StudyEntry.DEFAULT_COHORT;
        // TODO needs to be removed after integration

        int studyId = getHelper().getStudyId();
        long lock = getStudyConfigurationManager().lockStudy(studyId);
        StudyConfiguration sc = null;
        try {
            sc = readStudyConfiguration();
            BiMap<String, Integer> indexedSamples = StudyConfiguration.getIndexedSamples(sc);

            final Integer defaultCohortId;
            if (sc.getCohortIds().containsKey(defaultCohortName)) { //Check if "defaultCohort" exists
                defaultCohortId = sc.getCohortIds().get(defaultCohortName);
                if (sc.getCalculatedStats().contains(defaultCohortId)) { //Check if "defaultCohort"
                    // is calculated
                    //Check if the samples number are different
                    if (!indexedSamples.values().equals(sc.getCohorts().get(defaultCohortId))) {
                        sc.getCalculatedStats().remove(defaultCohortId);
                        sc.getInvalidStats().add(defaultCohortId);
                    }
                }
            } else {
                throw new IllegalStateException("No default cohort found!!!");
            }
            sc.getCohorts().put(defaultCohortId, indexedSamples.values());
            getStudyConfigurationManager().updateStudyConfiguration(sc, new QueryOptions());
        } finally {
            getStudyConfigurationManager().unLockStudy(studyId, lock);
        }
        // update PHOENIX definition with statistic columns
        VariantPhoenixHelper variantPhoenixHelper = new VariantPhoenixHelper(getHelper());
        try (Connection connection = variantPhoenixHelper.newJdbcConnection()) {
            variantPhoenixHelper.updateStatsColumns(connection, variantTable, sc);
        } catch (SQLException | ClassNotFoundException e) {
            logger.error("Problems updating PHOENIX table!!!", e);
            throw new IllegalStateException("Problems updating PHOENIX table", e);
        }
    }

    @Override
    protected void postExecution(boolean succeed) throws IOException, StorageEngineException {
        super.postExecution(succeed);
        if (succeed) {
            int studyId = getHelper().getStudyId();
            long lock = getStudyConfigurationManager().lockStudy(studyId);
            try {
                StudyConfiguration sc = readStudyConfiguration();
                sc.setCalculatedStats(sc.getCohortIds().values()); // update
                sc.setInvalidStats(Collections.emptySet());
                getStudyConfigurationManager().updateStudyConfiguration(sc, new QueryOptions());
            } finally {
                getStudyConfigurationManager().unLockStudy(studyId, lock);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(new VariantTableStatsDriver().privateMain(args));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
