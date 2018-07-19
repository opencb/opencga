package org.opencb.opencga.storage.hadoop.variant.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantStorageMetadataDBAdaptorFactory;

import java.io.IOException;

import static org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil.getQueryFromConfig;
import static org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil.getQueryOptionsFromConfig;

/**
 * Created on 27/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseVariantTableInputFormat extends AbstractVariantsTableInputFormat<ImmutableBytesWritable, Result> {

    public static final String USE_SAMPLE_INDEX_TABLE_INPUT_FORMAT = "hbase_variant_table_input_format.use_sample_index_table_input_format";
    public static final String MULTI_SCANS = "hbase_variant_table_input_format.multi_scans";
    private HBaseToVariantConverter<Result> converter;

    @Override
    protected void init(Configuration configuration) throws IOException {
        if (configuration.getBoolean(MULTI_SCANS, false)) {
            MultiTableInputFormat tableInputFormat;
            if (configuration.getBoolean(USE_SAMPLE_INDEX_TABLE_INPUT_FORMAT, false)) {
                tableInputFormat = new MultiRegionSampleIndexTableInputFormat();
            } else {
                tableInputFormat = new MultiTableInputFormat();
            }
            tableInputFormat.setConf(configuration);
            inputFormat = tableInputFormat;
        } else {
            TableInputFormat tableInputFormat;
            if (configuration.getBoolean(USE_SAMPLE_INDEX_TABLE_INPUT_FORMAT, false)) {
                tableInputFormat = new SampleIndexTableInputFormat();
            } else {
                tableInputFormat = new TableInputFormat();
            }
            tableInputFormat.setConf(configuration);
            inputFormat = tableInputFormat;
        }
//            configuration.forEach(entry -> System.out.println(entry.getKey() + " = " + entry.getValue()));
        VariantTableHelper helper = new VariantTableHelper(configuration);
        VariantQueryUtils.SelectVariantElements selectVariantElements;
        try (StudyConfigurationManager scm = new StudyConfigurationManager(new HBaseVariantStorageMetadataDBAdaptorFactory(helper))) {
            Query query = getQueryFromConfig(configuration);
            QueryOptions queryOptions = getQueryOptionsFromConfig(configuration);
            selectVariantElements = VariantQueryUtils.parseSelectElements(query, queryOptions, scm);
        }

        converter = HBaseToVariantConverter.fromResult(helper).configure(configuration).setSelectVariantElements(selectVariantElements);
    }

    @Override
    public RecordReader<ImmutableBytesWritable, Variant> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        if (inputFormat == null) {
            init(context.getConfiguration());
        }
        RecordReader<ImmutableBytesWritable, Result> recordReader = inputFormat.createRecordReader(split, context);
        return new RecordReaderTransform<>(recordReader, converter::convert);
    }
}
