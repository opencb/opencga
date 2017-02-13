package org.opencb.opencga.storage.hadoop.variant;

import com.google.common.collect.BiMap;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.mapreduce.Mapper;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.index.AbstractVariantTableDriver;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseStudyConfigurationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by mh719 on 22/12/2016.
 */
public class OpencgaMapReduceHelper {
    private Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final Mapper.Context context;
    private VariantTableHelper helper;
    private StudyConfiguration studyConfiguration;
    private HBaseToVariantConverter hbaseToVariantConverter;
    private long timestamp = HConstants.LATEST_TIMESTAMP;
    private volatile BiMap<String, Integer> indexedSamples;
    private final Map<String, Long> timeSum = new ConcurrentHashMap<>();
    private final AtomicLong lastTime = new AtomicLong(0);


    public OpencgaMapReduceHelper(Mapper.Context context) {
        this.context = context;
    }

    public void init() throws IOException {
        Thread.currentThread().setName(context.getTaskAttemptID().toString());
        LOG.debug("Setup configuration");

        // Setup configurationHBaseToVariantConverter// Setup configuration
        helper = new VariantTableHelper(context.getConfiguration());
        this.studyConfiguration = getHelper().loadMeta(); // Variant meta

        HBaseStudyConfigurationManager scm = new HBaseStudyConfigurationManager(getHelper(), getHelper().getOutputTableAsString(),
                getHelper().getConf(), new ObjectMap());
        hbaseToVariantConverter = new HBaseToVariantConverter(getHelper(), scm)
                .setFailOnEmptyVariants(true)
                .setSimpleGenotypes(false);
        this.indexedSamples = StudyConfiguration.getIndexedSamples(this.studyConfiguration);
        timestamp = context.getConfiguration().getLong(AbstractVariantTableDriver.TIMESTAMP, -1);
        if (timestamp == -1) {
            throw new IllegalArgumentException("Missing TimeStamp");
        }
    }

    public VariantTableHelper getHelper() {
        return helper;
    }

    public void setHelper(VariantTableHelper helper) {
        this.helper = helper;
    }

    public void cleanup() throws IOException {
        if (null != this.getHelper()) {
            this.getHelper().close();
        }
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public BiMap<String, Integer> getIndexedSamples() {
        return indexedSamples;
    }

    public void setIndexedSamples(BiMap<String, Integer> indexedSamples) {
        this.indexedSamples = indexedSamples;
    }

    public StudyConfiguration getStudyConfiguration() {
        return studyConfiguration;
    }

    public void setStudyConfiguration(StudyConfiguration studyConfiguration) {
        this.studyConfiguration = studyConfiguration;
    }

    public HBaseToVariantConverter getHbaseToVariantConverter() {
        return hbaseToVariantConverter;
    }

    public void setHbaseToVariantConverter(HBaseToVariantConverter hbaseToVariantConverter) {
        this.hbaseToVariantConverter = hbaseToVariantConverter;
    }

    /**
     * Sets the lastTime value to the {@link System#currentTimeMillis}.
     */
    public void startTime() {
        lastTime.set(System.nanoTime());
    }

    /**
     * Calculates the delay between the last saved time and the current {@link System#currentTimeMillis}
     * Resets the last time.
     *
     * @param name Name of the last code block
     */
    public void endTime(String name) {
        long time = System.nanoTime();
        registerRuntime(name, time - lastTime.get());
        lastTime.set(time);
    }

    public void registerRuntime(String name, long runtime) {
        timeSum.compute(name, (k, v) -> Objects.isNull(v) ? runtime : v + runtime);
    }

    public Map<String, Long> getTimes() {
        return timeSum;
    }
}
