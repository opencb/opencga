package org.opencb.opencga.storage.hadoop.variant;

import com.google.common.collect.BiMap;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
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
import java.util.*;

/**
 * Created by mh719 on 06/12/2016.
 */
public abstract class AbstractHBaseMapReduce<KEYOUT, VALUEOUT> extends TableMapper<KEYOUT, VALUEOUT> {
    private Logger LOG = LoggerFactory.getLogger(this.getClass());

    private VariantTableHelper helper;
    private StudyConfiguration studyConfiguration;
    private HBaseToVariantConverter hbaseToVariantConverter;
    private long timestamp = HConstants.LATEST_TIMESTAMP;
    private BiMap<String, Integer> indexedSamples;
    private Map<String, Long> timeSum = new HashMap<>();
    private long lastTime;

    @Override
    protected void setup(Mapper<ImmutableBytesWritable, Result, KEYOUT, VALUEOUT>.Context context) throws IOException,
            InterruptedException {
        super.setup(context);
        Thread.currentThread().setName(context.getTaskAttemptID().toString());
        getLog().debug("Setup configuration");

        // Setup configurationHBaseToVariantConverter// Setup configuration
        helper = new VariantTableHelper(context.getConfiguration());
        this.studyConfiguration = getHelper().loadMeta(); // Variant meta

        hbaseToVariantConverter = new HBaseToVariantConverter(this.getHelper(),
                new HBaseStudyConfigurationManager(this.getHelper(), this.getHelper().getOutputTableAsString(),
                        this.getHelper().getConf(), new ObjectMap())).setFailOnEmptyVariants(true).setSimpleGenotypes(false);
        this.indexedSamples = StudyConfiguration.getIndexedSamples(this.studyConfiguration);
        timestamp = context.getConfiguration().getLong(AbstractVariantTableDriver.TIMESTAMP, -1);
        if (timestamp == -1) {
            throw new IllegalArgumentException("Missing TimeStamp");
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        if (null != this.getHelper()) {
            this.getHelper().close();
        }
        super.cleanup(context);
    }

    protected Logger getLog() {
        return LOG;
    }

    public VariantTableHelper getHelper() {
        return helper;
    }

    public void setHelper(VariantTableHelper helper) {
        this.helper = helper;
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
    protected void startTime() {
        lastTime = System.nanoTime();
    }

    /**
     * Calculates the delay between the last saved time and the current {@link System#currentTimeMillis}
     * Resets the last time.
     *
     * @param name Name of the last code block
     */
    protected void endTime(String name) {
        long time = System.nanoTime();
        timeSum.put(name, time - lastTime);
        lastTime = time;
    }

    public Map<String, Long> getTimes() {
        return timeSum;
    }
}
