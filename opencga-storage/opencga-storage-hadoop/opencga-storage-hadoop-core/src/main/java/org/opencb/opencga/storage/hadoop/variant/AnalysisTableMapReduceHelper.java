package org.opencb.opencga.storage.hadoop.variant;

import com.google.common.collect.BiMap;
import org.apache.hadoop.mapreduce.Mapper;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseStudyConfigurationDBAdaptor;
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
public class AnalysisTableMapReduceHelper implements AutoCloseable {
    public static final String COUNTER_GROUP_NAME = "OPENCGA.HBASE";
    private final Logger logger = LoggerFactory.getLogger(AnalysisTableMapReduceHelper.class);

    private final Mapper.Context context;
    private final VariantTableHelper helper;
    private final StudyConfiguration studyConfiguration;
    private final HBaseToVariantConverter hbaseToVariantConverter;
    private final long timestamp;
    private final BiMap<String, Integer> indexedSamples;
    private final Map<String, Long> timeSum = new ConcurrentHashMap<>();
    private final AtomicLong lastTime = new AtomicLong(0);
    private final HBaseManager hBaseManager;


    public AnalysisTableMapReduceHelper(Mapper.Context context) throws IOException {
        this.context = context;

        Thread.currentThread().setName(context.getTaskAttemptID().toString());
        logger.debug("Setup configuration");

        // Setup configurationHBaseToVariantConverter// Setup configuration
        helper = new VariantTableHelper(context.getConfiguration());
        this.studyConfiguration = getHelper().readStudyConfiguration(); // Variant meta

        hBaseManager = new HBaseManager(context.getConfiguration());
        StudyConfigurationManager scm = new StudyConfigurationManager(
                new HBaseStudyConfigurationDBAdaptor(getHelper().getAnalysisTableAsString(), context.getConfiguration(),
                        new ObjectMap(), hBaseManager));
        hbaseToVariantConverter = new HBaseToVariantConverter(getHelper(), scm)
                .setFailOnEmptyVariants(true)
                .setSimpleGenotypes(false);
        this.indexedSamples = StudyConfiguration.getIndexedSamples(this.studyConfiguration);
//        timestamp = HConstants.LATEST_TIMESTAMP;
        timestamp = context.getConfiguration().getLong(AbstractAnalysisTableDriver.TIMESTAMP, -1);
        if (timestamp == -1) {
            throw new IllegalArgumentException("Missing TimeStamp");
        }
    }

    public VariantTableHelper getHelper() {
        return helper;
    }

    public HBaseManager getHBaseManager() {
        return hBaseManager;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public BiMap<String, Integer> getIndexedSamples() {
        return indexedSamples;
    }

    public StudyConfiguration getStudyConfiguration() {
        return studyConfiguration;
    }

    public HBaseToVariantConverter getHbaseToVariantConverter() {
        return hbaseToVariantConverter;
    }

    @Override
    public void close() throws IOException {
        if (hBaseManager != null) {
            hBaseManager.close();
        }
    }

    /**
     * Sets the lastTime value to the {@link System#nanoTime}.
     */
    public void startStep() {
        lastTime.set(System.nanoTime());
    }

    /**
     * Calculates the delay between the last saved time and the current {@link System#nanoTime}
     * Resets the last time.
     *
     * @param name Name of the last code block
     */
    public void endStep(String name) {
        long time = System.nanoTime();
        addStepDuration(name, time - lastTime.get());
        lastTime.set(time);
    }

    public void addStepDuration(String name, long nanotime) {
        timeSum.compute(name, (k, v) -> Objects.isNull(v) ? nanotime : v + nanotime);
    }

    public void addTimesAsCounters() {
        for (Map.Entry<String, Long> entry : timeSum.entrySet()) {
            context.getCounter(AnalysisTableMapReduceHelper.COUNTER_GROUP_NAME,
                    "VCF_TIMER_" + entry.getKey().replace(' ', '_')).increment(entry.getValue());
        }
        timeSum.clear();
    }
}
