package org.opencb.opencga.storage.hadoop.variant.gaps;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.opencb.opencga.storage.hadoop.variant.stats.VariantStatsDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.opencb.opencga.storage.hadoop.variant.gaps.PrepareFillMissingMapper.setIndexedFiles;

/**
 * Created on 14/02/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class PrepareFillMissingDriver extends AbstractVariantsTableDriver {

    private static final Logger LOG = LoggerFactory.getLogger(PrepareFillMissingDriver.class);

    public PrepareFillMissingDriver() {
    }

    public PrepareFillMissingDriver(Configuration conf) {
        super(conf);
    }

    @Override
    protected void parseAndValidateParameters() throws IOException {
        super.parseAndValidateParameters();
    }

    @Override
    protected Class<PrepareFillMissingMapper> getMapperClass() {
        return PrepareFillMissingMapper.class;
    }

    @Override
    protected Job setupJob(Job job, String archiveTableName, String variantTableName) throws IOException {
        ObjectMap options = new ObjectMap();
        getConf().iterator().forEachRemaining(entry -> options.put(entry.getKey(), entry.getValue()));

        StudyConfiguration sc = readStudyConfiguration();
        setIndexedFiles(job.getConfiguration(), sc.getIndexedFiles());

        Scan scan = new Scan();
        String region = getConf().get(VariantQueryParam.REGION.key());
        if (StringUtils.isNotEmpty(region)) {
            VariantHBaseQueryParser.addRegionFilter(scan, new Region(region));
        }
        scan.setCacheBlocks(false);
        scan.addColumn(getHelper().getColumnFamily(), VariantPhoenixHelper.getStudyColumn(getStudyId()).bytes());
        scan.addColumn(getHelper().getColumnFamily(), VariantPhoenixHelper.getFillMissingColumn(getStudyId()).bytes());

        LOG.info(scan.toString());

        // input
        VariantMapReduceUtil.initTableMapperJob(job, variantTableName, archiveTableName, scan, PrepareFillMissingMapper.class);

        return job;
    }

    @Override
    protected String getJobOperationName() {
        String regionStr = getConf().get(VariantQueryParam.REGION.key());
        return "prepare_fill_missing" + (StringUtils.isNotEmpty(regionStr) ? "_" + regionStr : "");
    }

    public int privateMain(String[] args) throws Exception {
        return privateMain(args, getConf());
    }

    public int privateMain(String[] args, Configuration conf) throws Exception {
        // info https://code.google.com/p/temapred/wiki/HbaseWithJava
        if (conf != null) {
            setConf(conf);
        }
        return ToolRunner.run(this, args);
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(new PrepareFillMissingDriver().privateMain(args));
        } catch (Exception e) {
            LOG.error("Error executing " + VariantStatsDriver.class, e);
            System.exit(1);
        }
    }
}
