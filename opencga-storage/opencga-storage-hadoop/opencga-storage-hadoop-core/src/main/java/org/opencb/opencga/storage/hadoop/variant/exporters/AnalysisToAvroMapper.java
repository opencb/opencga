package org.opencb.opencga.storage.hadoop.variant.exporters;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.opencga.storage.hadoop.variant.AbstractHBaseMapReduce;
import org.opencb.opencga.storage.hadoop.variant.index.AbstractVariantTableMapReduce;

import java.io.IOException;

/**
 * Created by mh719 on 06/12/2016.
 */
public class AnalysisToAvroMapper extends AbstractHBaseMapReduce<AvroKey<VariantAvro>, NullWritable> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException,
            InterruptedException {
        Variant variant = this.getHbaseToVariantConverter().convert(value);
        context.write(new AvroKey<>(variant.getImpl()), NullWritable.get());
        context.getCounter(AbstractVariantTableMapReduce.COUNTER_GROUP_NAME, "avro").increment(1);
    }
}
