package org.opencb.opencga.storage.hadoop.variant.analysis.julie;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapreduce.Job;
import org.opencb.biodata.models.core.Region;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.*;

public class JulieToolDriver extends AbstractVariantsTableDriver {

    private Map<Integer, List<Integer>> cohorts = new HashMap<>();
    private final Logger logger = LoggerFactory.getLogger(JulieToolDriver.class);

    public static final String COHORTS = "cohorts";
    public static final String OVERWRITE = "overwrite";
    private String region;
    private boolean overwrite;

    @Override
    protected Map<String, String> getParams() {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("--" + COHORTS, "<studyId:cohortId>");
        params.put("--" + OVERWRITE, "<true|false>");
        params.put("--" + VariantQueryParam.REGION.key(), "<region>");
        return params;
    }

    @Override
    protected void parseAndValidateParameters() throws IOException {
        super.parseAndValidateParameters();
        VariantStorageMetadataManager metadataManager = getMetadataManager();

        region = getParam(VariantQueryParam.REGION.key());
        String overwrite = getParam(OVERWRITE, "false");
        this.overwrite = Boolean.parseBoolean(overwrite);

        String param = getParam(COHORTS);
        if (StringUtils.isNotEmpty(param)) {
            for (String value : param.split(",")) {
                int idx = value.lastIndexOf(":");
                if (idx < 0) {
                    throw new IllegalArgumentException("Error parsing cohort parameter. Expected \"{studyId}:{cohortId}\"");
                }
                String study = value.substring(0, idx);
                String cohort = value.substring(idx + 1);

                int studyId = metadataManager.getStudyId(study);
                Integer cohortId = metadataManager.getCohortId(studyId, cohort);
                if (cohortId == null) {
                    throw new IllegalArgumentException("Unknown cohort " + cohortId);
                }
                cohorts.computeIfAbsent(studyId, key -> new LinkedList<>()).add(cohortId);
            }
        } else {
            for (Integer studyId : metadataManager.getStudies().values()) {
                List<Integer> studyCohorts = new LinkedList<>();
                cohorts.put(studyId, studyCohorts);
                for (CohortMetadata c : metadataManager.getCalculatedCohorts(studyId)) {
                    studyCohorts.add(c.getId());
                }
            }
        }
    }

    @Override
    protected Job setupJob(Job job, String archiveTable, String variantTable) throws IOException {
        Scan scan = new Scan();
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, VariantPhoenixSchema.VariantColumn.FULL_ANNOTATION.bytes());
        for (Map.Entry<Integer, List<Integer>> entry : cohorts.entrySet()) {
            Integer studyId = entry.getKey();
            for (Integer cohortId : entry.getValue()) {
                scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, VariantPhoenixSchema.getStatsColumn(studyId, cohortId).bytes());
            }
        }
        if (StringUtils.isNotEmpty(region)) {
            logger.info("Execute julie tool for region " + region);
            VariantHBaseQueryParser.addRegionFilter(scan, new Region(region));
        }
        if (overwrite) {
            logger.info("PopulationFrequency overwrite: " + overwrite
                    + " --> Remove existing population frequency values before inserting new ones");
        } else {
            logger.info("PopulationFrequency overwrite: " + overwrite
                    + " --> Keep existing population frequency. Add new values, replacing the matching ones.");
        }
        job.getConfiguration().setBoolean(OVERWRITE, overwrite);

        VariantMapReduceUtil.configureMapReduceScan(scan, getConf());
        logger.info("Scan: " + scan);

        VariantMapReduceUtil.initVariantRowMapperJobFromHBase(job, variantTable, scan, JulieToolMapper.class, false);
        VariantMapReduceUtil.setOutputHBaseTable(job, variantTable);
        VariantMapReduceUtil.setNoneReduce(job);
        VariantMapReduceUtil.setNoneTimestamp(job);
        return job;
    }

    @Override
    protected String getJobOperationName() {
        return "julie-tool";
    }


    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        main(args, (Class<? extends AbstractVariantsTableDriver>) MethodHandles.lookup().lookupClass());
    }
}
