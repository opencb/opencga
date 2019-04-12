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
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHBaseQueryParser;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.HBaseToVariantAnnotationConverter;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
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

        String dbName = HBaseVariantTableNameGenerator.getDBNameFromVariantsTableName(getVariantsTable());
        annotationIndexTable = new HBaseVariantTableNameGenerator(dbName, getConf()).getAnnotationIndexTableName();
    }

    @Override
    protected Job setupJob(Job job, String archiveTable, String variantTable) throws IOException {
        int caching = job.getConfiguration().getInt(HadoopVariantStorageEngine.MAPREDUCE_HBASE_SCAN_CACHING, 100);
        LOGGER.info("Scan set Caching to " + caching);

        Scan scan = new Scan();
        if (region != null) {
            VariantHBaseQueryParser.addRegionFilter(scan, region);
        }

        scan.addColumn(getHelper().getColumnFamily(), VariantPhoenixHelper.VariantColumn.FULL_ANNOTATION.bytes());

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
            annotationConverter = new HBaseToVariantAnnotationConverter(new GenomeHelper(context.getConfiguration()), -1);
            annotationIndexConverter = new AnnotationIndexConverter();
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            VariantAnnotation annotation = annotationConverter.convert(value);
            Put put = annotationIndexConverter.convertToPut(annotation);
            context.write(key, put);
        }
    }

}
