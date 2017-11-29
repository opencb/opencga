package org.opencb.opencga.storage.hadoop.variant.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;

import java.io.IOException;

/**
 * Created on 27/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseVariantTableInputFormat extends AbstractVariantsTableInputFormat<ImmutableBytesWritable, Result> {

    private HBaseToVariantConverter<Result> converter;

    @Override
    protected void init(Configuration configuration) throws IOException {
        TableInputFormat tableInputFormat = new TableInputFormat();
//            configuration.forEach(entry -> System.out.println(entry.getKey() + " = " + entry.getValue()));
        tableInputFormat.setConf(configuration);
        inputFormat = tableInputFormat;
        converter = HBaseToVariantConverter.fromResult(new VariantTableHelper(configuration)).configure(configuration);
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
