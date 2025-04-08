package org.opencb.opencga.storage.hadoop.variant.gaps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapper;

import java.io.IOException;
import java.util.Collection;

/**
 * Created on 26/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FillGapsMapper extends VariantMapper<ImmutableBytesWritable, Mutation> {

    private FillGapsFromVariantTask fillGapsTask;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration configuration = context.getConfiguration();
        HBaseManager hBaseManager = new HBaseManager(configuration);

        String archiveTableName = context.getConfiguration().get(ArchiveTableHelper.CONFIG_ARCHIVE_TABLE_NAME);
        Collection<Integer> samples = FillGapsFromArchiveMapper.getSamples(configuration);

        fillGapsTask = new FillGapsFromVariantTask(hBaseManager, archiveTableName,
                getStudyMetadata(), getMetadataManager(), configuration, samples);
        fillGapsTask.pre();

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        try {
            fillGapsTask.post();
        } catch (StorageEngineException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void map(Object key, Variant variant, Context context) throws IOException, InterruptedException {
        Put put = fillGapsTask.fillGaps(variant);
        if (put != null && !put.isEmpty()) {
            context.write(new ImmutableBytesWritable(put.getRow()), put);
        }
    }
}
