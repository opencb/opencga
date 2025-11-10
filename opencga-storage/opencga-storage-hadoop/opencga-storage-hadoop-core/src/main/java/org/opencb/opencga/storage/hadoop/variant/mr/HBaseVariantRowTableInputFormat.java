package org.opencb.opencga.storage.hadoop.variant.mr;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.mapreduce.JobContext;
import org.opencb.opencga.storage.hadoop.variant.converters.VariantRow;

import java.io.IOException;
import java.util.function.Function;

public class HBaseVariantRowTableInputFormat extends AbstractHBaseVariantTableInputFormat<VariantRow> {

    protected Function<Result, VariantRow> initConverter(JobContext context) throws IOException {
        return VariantRow::new;
    }

}
