package org.opencb.opencga.storage.hadoop.variant;

import com.google.common.collect.BiMap;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Mapper;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by mh719 on 06/12/2016.
 */
public abstract class AbstractHBaseVariantMapper<KEYOUT, VALUEOUT> extends TableMapper<KEYOUT, VALUEOUT> {
    private final AtomicReference<AnalysisTableMapReduceHelper> mrHelper = new AtomicReference<>();

    @Override
    protected void setup(Mapper<ImmutableBytesWritable, Result, KEYOUT, VALUEOUT>.Context context) throws IOException,
            InterruptedException {
        super.setup(context);
        mrHelper.set(new AnalysisTableMapReduceHelper(context));
    }

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        getMrHelper().close();
    }

    public AnalysisTableMapReduceHelper getMrHelper() {
        return mrHelper.get();
    }

    public VariantTableHelper getHelper() {
        return getMrHelper().getHelper();
    }

    public HBaseManager getHBaseManager() {
        return getMrHelper().getHBaseManager();
    }

    public long getTimestamp() {
        return getMrHelper().getTimestamp();
    }

    public BiMap<String, Integer> getIndexedSamples() {
        return getMrHelper().getIndexedSamples();
    }

    public StudyConfiguration getStudyConfiguration() {
        return getMrHelper().getStudyConfiguration();
    }

    public HBaseToVariantConverter getHbaseToVariantConverter() {
        return getMrHelper().getHbaseToVariantConverter();
    }

    public void startStep() {
        getMrHelper().startStep();
    }

    public void endStep(String name) {
        getMrHelper().endStep(name);
    }

    public void addStepDuration(String name, long nanotime) {
        getMrHelper().addStepDuration(name, nanotime);
    }

}
