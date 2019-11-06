package org.opencb.opencga.storage.hadoop.variant;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.opencb.opencga.storage.hadoop.utils.DeleteHBaseColumnDriver;
import org.opencb.opencga.storage.hadoop.variant.search.HadoopVariantSearchIndexUtils;

import java.io.IOException;

/**
 * Created on 23/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantsTableDeleteColumnMapper extends DeleteHBaseColumnDriver.DeleteHBaseColumnMapper {

    private byte[] columnFamily;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        new GenomeHelper(context.getConfiguration());
        columnFamily = GenomeHelper.COLUMN_FAMILY_BYTES;
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
        super.map(key, result, context);
        context.write(key, HadoopVariantSearchIndexUtils.addNotSyncStatus(new Put(result.getRow()), columnFamily));
    }
}
