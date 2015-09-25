/**
 * 
 */
package org.opencb.opencga.storage.hadoop.mr;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
public class GenomeVariantCombiner extends Reducer<ImmutableBytesWritable, Put, ImmutableBytesWritable, Put> {

    private final AtomicReference<GenomeVariantHelper> helper = new AtomicReference<GenomeVariantHelper>();

    @Override
    protected void setup (Reducer<ImmutableBytesWritable, Put, ImmutableBytesWritable, Put>.Context context) throws IOException,
            InterruptedException {
        this.helper.set(new GenomeVariantHelper(context.getConfiguration()));
    }

    @Override
    protected void reduce (ImmutableBytesWritable key, Iterable<Put> input,
            Reducer<ImmutableBytesWritable, Put, ImmutableBytesWritable, Put>.Context cxt) throws IOException, InterruptedException {
        VcfSlice joinedSlice = getHelper().join(key.copyBytes(), input);
        Put joinedPut = getHelper().wrap(joinedSlice);
        cxt.write(key, joinedPut);
    }

    public GenomeVariantHelper getHelper () {
        return helper.get();
    }
}
