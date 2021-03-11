package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.HBaseToVariantAnnotationConverter;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created on 08/01/19.
 * <code>
 * export HADOOP_CLASSPATH=$(hbase classpath | tr ":" "\n" | grep "/conf" | tr "\n" ":")
 * hadoop jar opencga-storage-hadoop-core-X.Y.Z-dev-jar-with-dependencies.jar
 *      org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexDriver
 *      $VARIANTS_TABLE_NAME [--region $REGION]
 *      ....
 * </code>
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class AnnotationIndexDriver extends AbstractVariantsTableDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(AnnotationIndexDriver.class);
    private Region region;
    private String annotationIndexTable;

    @Override
    protected void parseAndValidateParameters() throws IOException {
        super.parseAndValidateParameters();

        Configuration conf = getConf();
        String regionStr = conf.get(VariantQueryParam.REGION.key(), conf.get("--" + VariantQueryParam.REGION.key()));
        if (StringUtils.isNotEmpty(regionStr)) {
            region = new Region(regionStr);
        } else {
            region = null;
        }

        annotationIndexTable = getTableNameGenerator().getAnnotationIndexTableName();
    }

    @Override
    protected Job setupJob(Job job, String archiveTable, String variantTable) throws IOException {
        int caching = job.getConfiguration().getInt(HadoopVariantStorageOptions.MR_HBASE_SCAN_CACHING.key(),
                HadoopVariantStorageOptions.MR_HBASE_SCAN_CACHING.defaultValue());
        LOGGER.info("Scan set Caching to " + caching);

        Scan scan = new Scan();
        if (region != null) {
            VariantHBaseQueryParser.addRegionFilter(scan, region);
        }

        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, VariantPhoenixSchema.VariantColumn.FULL_ANNOTATION.bytes());

        VariantMapReduceUtil.initTableMapperJob(job, variantTable, scan, getMapperClass());
        VariantMapReduceUtil.setOutputHBaseTable(job, annotationIndexTable);

        return job;
    }

    @Override
    protected void preExecution() throws IOException {
        try (HBaseManager hBaseManager = new HBaseManager(getConf())) {
            AnnotationIndexDBAdaptor.createTableIfNeeded(hBaseManager, annotationIndexTable);
        }
    }

    @Override
    protected Class<AnnotationIndexMapper> getMapperClass() {
        return AnnotationIndexMapper.class;
    }

    @Override
    protected String getJobOperationName() {
        return "create_annotation_index";
    }

    public static class AnnotationIndexMapper extends TableMapper<ImmutableBytesWritable, Put> {
        private HBaseToVariantAnnotationConverter annotationConverter;
        private AnnotationIndexConverter annotationIndexConverter;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            annotationConverter = new HBaseToVariantAnnotationConverter();
            annotationIndexConverter = new AnnotationIndexConverter(
                    new SampleIndexSchema(VariantMapReduceUtil.getSampleIndexConfiguration(context.getConfiguration())));
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            VariantAnnotation annotation = annotationConverter.convert(value);
            Put put = annotationIndexConverter.convertToPut(annotation);
            context.write(key, put);
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(new AnnotationIndexDriver().privateMain(args, null));
        } catch (Exception e) {
            LOGGER.error("Error executing " + AnnotationIndexDriver.class, e);
            System.exit(1);
        }
    }

}
