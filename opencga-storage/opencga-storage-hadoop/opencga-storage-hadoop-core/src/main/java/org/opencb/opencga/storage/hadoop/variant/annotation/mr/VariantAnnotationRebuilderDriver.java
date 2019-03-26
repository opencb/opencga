package org.opencb.opencga.storage.hadoop.variant.annotation.mr;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.HBaseToVariantAnnotationConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.VariantAnnotationToHBaseConverter;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jacobo on 26/03/19.
 */
public class VariantAnnotationRebuilderDriver extends AbstractVariantsTableDriver {

    private String region;
    private final Logger logger = LoggerFactory.getLogger(VariantAnnotationRebuilderDriver.class);

    @Override
    protected Map<String, String> getParams() {
        Map<String, String> params = new HashMap<>();
        params.put("--" + VariantQueryParam.REGION.key(), "<region>");
        return params;
    }

    @Override
    protected void parseAndValidateParameters() throws IOException {
        super.parseAndValidateParameters();

        region = getParam(VariantQueryParam.REGION.key(), "");
    }

    @Override
    protected Class<VariantAnnotationRebuilderMapper> getMapperClass() {
        return VariantAnnotationRebuilderMapper.class;
    }

    @Override
    protected Job setupJob(Job job, String archiveTable, String variantTable) throws IOException {

        Scan scan = new Scan();
        if (StringUtils.isNotEmpty(region)) {
            logger.info("Regenerate annotations for region " + region);
            VariantHBaseQueryParser.addRegionFilter(scan, new Region(region));
        }
        VariantMapReduceUtil.configureMapReduceScan(scan, getConf());
        VariantMapReduceUtil.initTableMapperJob(job, variantTable, variantTable, scan, getMapperClass());
        VariantMapReduceUtil.setNoneReduce(job);

        return job;
    }

    @Override
    protected String getJobOperationName() {
        return "regenerate variant annotation columns";
    }

    public static class VariantAnnotationRebuilderMapper extends TableMapper<ImmutableBytesWritable, Put> {

        private GenomeHelper helper;
        private VariantAnnotationToHBaseConverter converter;
        private HBaseToVariantAnnotationConverter toAnnotationConverter;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            helper = new GenomeHelper(context.getConfiguration());
            converter = new VariantAnnotationToHBaseConverter(helper, null, -1);
            toAnnotationConverter = new HBaseToVariantAnnotationConverter(helper, 0);
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context)
                throws IOException, InterruptedException {
            VariantAnnotation annotation = toAnnotationConverter.convert(value);
            Put put = converter.convert(annotation);

            context.write(key, put);
        }
    }


    public static void main(String[] args) {
        int exitCode;
        try {
            exitCode = ToolRunner.run(new VariantAnnotationRebuilderDriver(), args);
        } catch (Exception e) {
            e.printStackTrace();
            exitCode = 1;
        }
        System.exit(exitCode);
    }
}
