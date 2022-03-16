package org.opencb.opencga.storage.hadoop.variant.gaps;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.io.BytesWritable;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.hadoop.variant.mr.AbstractHBaseVariantMapper;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantsTableMapReduceHelper;

import java.io.IOException;
import java.util.Map;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions.FILL_MISSING_GAP_GENOTYPE;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions.FILL_MISSING_SIMPLIFIED_MULTIALLELIC_VARIANTS;

/**
 * Created on 09/03/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FillMissingFromArchiveMapper extends AbstractHBaseVariantMapper<BytesWritable, BytesWritable> {
    private AbstractFillFromArchiveTask task;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        VariantStorageMetadataManager metadataManager = getMetadataManager();
        boolean overwrite = FillGapsFromArchiveMapper.isOverwrite(context.getConfiguration());
        boolean simplifiedNewMultiAllelicVariants = context.getConfiguration().getBoolean(
                FILL_MISSING_SIMPLIFIED_MULTIALLELIC_VARIANTS.key(),
                FILL_MISSING_SIMPLIFIED_MULTIALLELIC_VARIANTS.defaultValue());
        String gapsGenotype = context.getConfiguration().get(
                FILL_MISSING_GAP_GENOTYPE.key(),
                FILL_MISSING_GAP_GENOTYPE.defaultValue());
        task = new FillMissingFromArchiveTask(getStudyMetadata(), metadataManager,
                context.getConfiguration(), overwrite, simplifiedNewMultiAllelicVariants, gapsGenotype);
        task.setQuiet(true);
        task.pre();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        try {
            task.post();
        } catch (StorageEngineException e) {
            throw new IOException(e);
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
        updateStats(context);
    }

    private void updateStats(Context context) {
        for (Map.Entry<String, Long> entry : task.takeStats().entrySet()) {
            context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, entry.getKey()).increment(entry.getValue());
        }
    }

}
