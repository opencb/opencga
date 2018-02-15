package org.opencb.opencga.storage.hadoop.variant.gaps;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.opencb.opencga.storage.hadoop.variant.index.AbstractArchiveTableMapper;
import org.opencb.opencga.storage.hadoop.variant.mr.AnalysisTableMapReduceHelper;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created on 15/01/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FillGapsFromArchiveMapper extends AbstractArchiveTableMapper {

    public static final String SAMPLES = "samples";
    public static final String FILL_GAPS = "fillGaps";
    private AbstractFillFromArchiveTask task;

    public static void setSamples(Job job, Collection<Integer> sampleIds) {
        job.getConfiguration().set(SAMPLES, sampleIds.stream().map(Object::toString).collect(Collectors.joining(",")));
    }

    public static Collection<Integer> getSamples(Configuration configuration) {
        Collection<Integer> samples = new ArrayList<>();
        for (String sample : configuration.get(SAMPLES, "").split(",")) {
            if (!sample.isEmpty()) {
                samples.add(Integer.valueOf(sample));
            }
        }
        return samples;
    }

    public static boolean isFillGaps(Configuration configuration) {
        if (StringUtils.isEmpty(configuration.get(FILL_GAPS))) {
            throw new IllegalArgumentException("Missing param " + FILL_GAPS);
        }
        return configuration.getBoolean(FILL_GAPS, false);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        if (isFillGaps(context.getConfiguration())) {
            Collection<Integer> samples = getSamples(context.getConfiguration());
            task = new FillGapsFromArchiveTask(getHBaseManager(),
                    getHelper().getArchiveTableAsString(),
                    getStudyConfiguration(), getHelper(),
                    samples);
        } else {
            task = new FillMissingFromArchiveTask(getHBaseManager(), getStudyConfiguration(), getHelper());
        }
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
    protected void map(VariantMapReduceContext ctx) throws IOException, InterruptedException {

        List<Put> puts = task.apply(Collections.singletonList(ctx.getValue()));

        for (Put put : puts) {
            ctx.getContext().write(new ImmutableBytesWritable(put.getRow()), put);
        }
        updateStats(ctx.getContext());
    }

    private void updateStats(Context context) {
        for (Map.Entry<String, Long> entry : task.takeStats().entrySet()) {
            context.getCounter(AnalysisTableMapReduceHelper.COUNTER_GROUP_NAME, entry.getKey()).increment(entry.getValue());
        }
    }
}
