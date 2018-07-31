package org.opencb.opencga.storage.hadoop.variant.gaps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveDriver;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;
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
        VariantTableHelper helper = new VariantTableHelper(configuration);

        String archiveTableName = context.getConfiguration().get(ArchiveDriver.CONFIG_ARCHIVE_TABLE_NAME);
        Collection<Integer> samples = FillGapsFromArchiveMapper.getSamples(configuration);

        StudyConfiguration studyConfiguration = helper.readStudyConfiguration();

        fillGapsTask = new FillGapsFromVariantTask(hBaseManager, archiveTableName, studyConfiguration, helper, samples);
        fillGapsTask.pre();

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        fillGapsTask.post();
    }

    @Override
    protected void map(Object key, Variant variant, Context context) throws IOException, InterruptedException {
        Put put = fillGapsTask.fillGaps(variant);
        if (put != null && !put.isEmpty()) {
            context.write(new ImmutableBytesWritable(put.getRow()), put);
        }
    }
}
