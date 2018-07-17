package org.opencb.opencga.storage.hadoop.variant.gaps;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.io.BytesWritable;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantsTableMapReduceHelper;

import java.io.IOException;
import java.util.Map;

/**
 * Created on 09/03/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FillMissingFromArchiveMapper extends TableMapper<BytesWritable, BytesWritable> {
    private AbstractFillFromArchiveTask task;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        VariantTableHelper helper = new VariantTableHelper(context.getConfiguration());

        StudyConfiguration studyConfiguration = helper.readStudyConfiguration();
        boolean overwrite = FillGapsFromArchiveMapper.isOverwrite(context.getConfiguration());
        task = new FillMissingFromArchiveTask(studyConfiguration, helper, overwrite);
        task.setQuiet(true);
        task.pre();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        try {
            task.post();
        } finally {
            updateStats(context);
        }
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        AbstractFillFromArchiveTask.FillResult fillResult = task.apply(value);

        for (Put put : fillResult.getVariantPuts()) {
            ClientProtos.MutationProto proto = ProtobufUtil.toMutation(ClientProtos.MutationProto.MutationType.PUT, put);
            context.write(new BytesWritable(put.getRow()), new BytesWritable(proto.toByteArray()));
        }
        for (Put put : fillResult.getSamplesIndexPuts()) {
            setSampleIndexTablePut(put);
            ClientProtos.MutationProto proto = ProtobufUtil.toMutation(ClientProtos.MutationProto.MutationType.PUT, put);
            context.write(new BytesWritable(put.getRow()), new BytesWritable(proto.toByteArray()));
        }
        updateStats(context);
    }

    public static void setSampleIndexTablePut(Put put) {
        put.setAttribute("s", new byte[]{1});
    }

    public static boolean isSampleIndexTablePut(Put put) {
        byte[] s = put.getAttribute("s");
        return s != null && s[0] == 1;
    }

    private void updateStats(Context context) {
        for (Map.Entry<String, Long> entry : task.takeStats().entrySet()) {
            context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, entry.getKey()).increment(entry.getValue());
        }
    }

}
