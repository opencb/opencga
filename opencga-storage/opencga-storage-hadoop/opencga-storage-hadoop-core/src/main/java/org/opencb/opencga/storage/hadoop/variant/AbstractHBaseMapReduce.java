package org.opencb.opencga.storage.hadoop.variant;

import com.google.common.collect.BiMap;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Mapper;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by mh719 on 06/12/2016.
 */
public abstract class AbstractHBaseMapReduce<KEYOUT, VALUEOUT> extends TableMapper<KEYOUT, VALUEOUT> {
    private final AtomicReference<OpencgaMapReduceHelper> mrHelper = new AtomicReference<>();

    @Override
    protected void setup(Mapper<ImmutableBytesWritable, Result, KEYOUT, VALUEOUT>.Context context) throws IOException,
            InterruptedException {
        super.setup(context);
        mrHelper.set(new OpencgaMapReduceHelper(context));
        mrHelper.get().init();
    }

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        if (null != this.getHelper()) {
            this.getHelper().close();
        }
        getMrHelper().cleanup();
    }

    public OpencgaMapReduceHelper getMrHelper() {
        return mrHelper.get();
    }

    public void setMrHelper(OpencgaMapReduceHelper mrHelper) {
        this.mrHelper.set(mrHelper);
    }

    public VariantTableHelper getHelper() {
        return getMrHelper().getHelper();
    }

    public long getTimestamp() {
        return getMrHelper().getTimestamp();
    }

    public void setTimestamp(long timestamp) {
        getMrHelper().setTimestamp(timestamp);
    }

    public BiMap<String, Integer> getIndexedSamples() {
        return getMrHelper().getIndexedSamples();
    }

    public void setIndexedSamples(BiMap<String, Integer> indexedSamples) {
        getMrHelper().setIndexedSamples(indexedSamples);
    }

    public StudyConfiguration getStudyConfiguration() {
        return getMrHelper().getStudyConfiguration();
    }

    public void setStudyConfiguration(StudyConfiguration studyConfiguration) {
        getMrHelper().setStudyConfiguration(studyConfiguration);
    }

    public HBaseToVariantConverter getHbaseToVariantConverter() {
        return getMrHelper().getHbaseToVariantConverter();
    }

    public void setHbaseToVariantConverter(HBaseToVariantConverter hbaseToVariantConverter) {
        getMrHelper().setHbaseToVariantConverter(hbaseToVariantConverter);
    }

    public void startTime() {
        getMrHelper().startTime();
    }

    public void endTime(String name) {
        getMrHelper().endTime(name);
    }

    public void registerRuntime(String name, long runtime) {
        getMrHelper().registerRuntime(name, runtime);
    }

    public Map<String, Long> getTimes() {
        return getMrHelper().getTimes();
    }

    public void setHelper(VariantTableHelper helper) {
        getMrHelper().setHelper(helper);
    }
}
