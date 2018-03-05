package org.opencb.opencga.storage.hadoop.variant.gaps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveRowKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.mr.AnalysisTableMapReduceHelper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created on 14/02/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class PrepareFillMissingMapper extends TableMapper<ImmutableBytesWritable, Put> {

    public static final byte[] EMPTY_VALUE = new byte[0];
    public static final ImmutableBytesWritable EMPTY_IMMUTABLE_BYTES = new ImmutableBytesWritable(EMPTY_VALUE);
    private ArchiveRowKeyFactory rowKeyFactory;
    private List<Integer> fileBatches;
    private GenomeHelper helper;
    private byte[] family;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        helper = new GenomeHelper(context.getConfiguration());
        family = helper.getColumnFamily();
        rowKeyFactory = new ArchiveRowKeyFactory(context.getConfiguration());
        fileBatches = getFileBatches(context.getConfiguration());
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        Variant variant = VariantPhoenixKeyFactory.extractVariantFromVariantRowKey(value.getRow());
        byte[] column = FillMissingFromArchiveTask.getArchiveVariantColumn(variant);
        long sliceId = rowKeyFactory.getSliceId(variant.getStart());
        String chromosome = variant.getChromosome();
        for (Integer fileBatch : fileBatches) {
            Put put = new Put(Bytes.toBytes(rowKeyFactory.generateBlockIdFromSliceAndBatch(fileBatch, chromosome, sliceId)));
            put.addColumn(family, column, EMPTY_VALUE);
            context.write(EMPTY_IMMUTABLE_BYTES, put);
        }
        context.getCounter(AnalysisTableMapReduceHelper.COUNTER_GROUP_NAME, "PUT").increment(fileBatches.size());
        context.getCounter(AnalysisTableMapReduceHelper.COUNTER_GROUP_NAME, "VARIANTS").increment(1);
    }

    public static List<Integer> getFileBatches(Configuration configuration) {
        return Arrays.stream(configuration.getStrings("file_batches")).map(Integer::parseInt).collect(Collectors.toList());
    }

    public static void setFileBatches(Configuration configuration, Collection<?> fileBatches) {
        configuration.set("file_batches", fileBatches.stream().map(Object::toString).collect(Collectors.joining(",")));
    }
}
