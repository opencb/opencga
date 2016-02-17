package org.opencb.opencga.storage.hadoop.variant.archive.mr;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos.VcfSlice;
import org.opencb.biodata.tools.variant.converter.VariantToVcfSliceConverter;
import org.opencb.biodata.tools.variant.converter.VcfSliceToVariantListConverter;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created on 15/02/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VcfSlicerReducer extends TableReducer<ImmutableBytesWritable, VcfSliceWritable, ImmutableBytesWritable> {

    private ArchiveHelper helper;

    private VcfSliceToVariantListConverter converterFromSlice;
    private VariantToVcfSliceConverter converterToSlice;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        helper = new ArchiveHelper(context.getConfiguration());
        converterFromSlice = new VcfSliceToVariantListConverter(helper.getMeta());
        converterToSlice = new VariantToVcfSliceConverter();
    }

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<VcfSliceWritable> values, Context context)
            throws IOException, InterruptedException {


        List<Variant> variants = new ArrayList<>();
        int slicePosition = 0;
        int numSlices = 0;
        for (VcfSliceWritable vcfSlice : values) {
            variants.addAll(converterFromSlice.convert(vcfSlice.get()));
            slicePosition = vcfSlice.get().getPosition(); //All the positions should be the same
            numSlices++;
        }


        context.getCounter("OPENCGA.HBASE", "VCF_REDUCE_COUNT").increment(1);
        context.getCounter("OPENCGA.HBASE", "VCF_REDUCE_COUNT_" + numSlices).increment(1);
        VcfSlice joinedSlice = converterToSlice.convert(variants, slicePosition);

        Put put = helper.wrap(joinedSlice);

        context.write(key, put);


    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
