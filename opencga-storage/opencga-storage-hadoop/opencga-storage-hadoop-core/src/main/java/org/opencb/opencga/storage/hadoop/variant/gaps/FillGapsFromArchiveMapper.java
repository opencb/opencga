package org.opencb.opencga.storage.hadoop.variant.gaps;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.opencb.opencga.storage.hadoop.variant.index.AbstractArchiveTableMapper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Created on 15/01/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FillGapsFromArchiveMapper extends AbstractArchiveTableMapper {

    private FillGapsFromArchiveTask task;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        task = new FillGapsFromArchiveTask(
                getHBaseManager(), getHelper().getArchiveTableAsString(), getStudyConfiguration(), getHelper(), false);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

    @Override
    protected void map(VariantMapReduceContext ctx) throws IOException, InterruptedException {

        List<Put> puts = task.apply(Collections.singletonList(ctx.getValue()));

        for (Put put : puts) {
            ctx.getContext().write(new ImmutableBytesWritable(put.getRow()), put);
        }

    }


}
