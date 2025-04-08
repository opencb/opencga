package org.opencb.opencga.storage.hadoop.variant.gaps.write;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantTableHelper;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantsTableMapReduceHelper;

import java.io.IOException;

/**
 * Created on 09/03/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FillMissingHBaseWriterMapper extends Mapper<BytesWritable, BytesWritable, ImmutableBytesWritable, Put> {

    private ImmutableBytesWritable variantsTable;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        VariantTableHelper helper = new VariantTableHelper(context.getConfiguration());
        variantsTable = new ImmutableBytesWritable(helper.getVariantsTable());
    }

    @Override
    protected void map(BytesWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {

        ClientProtos.MutationProto proto;
//        proto = ClientProtos.MutationProto.PARSER.parseFrom(new ByteArrayInputStream(value.getBytes(), 0, value.getLength()));
        proto = ClientProtos.MutationProto.PARSER.parseFrom(value.getBytes(), 0, value.getLength());

//        System.out.println(VariantPhoenixKeyFactory.extractVariantFromVariantRowKey(key.copyBytes()));

        Put put = ProtobufUtil.toPut(proto);
        context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "puts").increment(1);
        context.write(new ImmutableBytesWritable(variantsTable), put);

        // Indicate that the process is still alive
        context.progress();
    }
}
