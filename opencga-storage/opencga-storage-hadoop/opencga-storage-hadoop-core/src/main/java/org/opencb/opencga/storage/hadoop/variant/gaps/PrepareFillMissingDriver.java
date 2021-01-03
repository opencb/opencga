package org.opencb.opencga.storage.hadoop.variant.gaps;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapreduce.Job;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashSet;

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

        LinkedHashSet<Integer> indexedFiles = getMetadataManager().getIndexedFiles(getStudyId());
        setIndexedFiles(job.getConfiguration(), indexedFiles);

        Scan scan = new Scan();
        String region = getConf().get(VariantQueryParam.REGION.key());
        if (StringUtils.isNotEmpty(region)) {
            VariantHBaseQueryParser.addRegionFilter(scan, new Region(region));
        }
        scan.setCacheBlocks(false);
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, VariantPhoenixSchema.getStudyColumn(getStudyId()).bytes());
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, VariantPhoenixSchema.getFillMissingColumn(getStudyId()).bytes());

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

    public static void main(String[] args) {
        try {
            System.exit(new PrepareFillMissingDriver().privateMain(args));
        } catch (Exception e) {
            LOG.error("Error executing " + PrepareFillMissingDriver.class, e);
            System.exit(1);
        }
    }
}
