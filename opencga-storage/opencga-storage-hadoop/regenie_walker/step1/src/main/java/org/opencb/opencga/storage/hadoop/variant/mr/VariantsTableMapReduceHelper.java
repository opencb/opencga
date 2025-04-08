/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.hadoop.variant.mr;

import com.google.common.collect.BiMap;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.hadoop.utils.AbstractHBaseDriver;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseVariantConverterConfiguration;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantStorageMetadataDBAdaptorFactory;
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
public class VariantsTableMapReduceHelper implements AutoCloseable {
    public static final String COUNTER_GROUP_NAME = AbstractHBaseDriver.COUNTER_GROUP_NAME;
    private final Logger logger = LoggerFactory.getLogger(VariantsTableMapReduceHelper.class);

    private final TaskAttemptContext context;
    private final VariantTableHelper helper;
    private StudyMetadata studyMetadata; // Lazy initialisation
    private final HBaseToVariantConverter<Result> hbaseToVariantConverter;
    private final long timestamp;
    private BiMap<String, Integer> indexedSamples; // Lazy initialisation
    private final Map<String, Long> timeSum = new ConcurrentHashMap<>();
    private final AtomicLong lastTime = new AtomicLong(0);
    private final HBaseManager hBaseManager;
    private final VariantStorageMetadataManager metadataManager;


    public VariantsTableMapReduceHelper(TaskAttemptContext context) throws IOException {
        this.context = context;

        Thread.currentThread().setName(context.getTaskAttemptID().toString());
        logger.debug("Setup configuration");

        // Setup configurationHBaseToVariantConverter// Setup configuration
        helper = new VariantTableHelper(context.getConfiguration());

        hBaseManager = new HBaseManager(context.getConfiguration());
        metadataManager = new VariantStorageMetadataManager(
                new HBaseVariantStorageMetadataDBAdaptorFactory(hBaseManager, helper.getMetaTableAsString(), context.getConfiguration()));

        hbaseToVariantConverter = HBaseToVariantConverter.fromResult(metadataManager);
        hbaseToVariantConverter.configure(HBaseVariantConverterConfiguration.builder()
                .setFailOnEmptyVariants(true)
                .setSimpleGenotypes(false)
                .build());
//        timestamp = HConstants.LATEST_TIMESTAMP;
        if (AbstractVariantsTableDriver.NONE_TIMESTAMP.equals(context.getConfiguration().get(AbstractVariantsTableDriver.TIMESTAMP))) {
            timestamp = -1;
        } else {
            timestamp = context.getConfiguration().getLong(AbstractVariantsTableDriver.TIMESTAMP, -1);
            if (timestamp == -1) {
                throw new IllegalArgumentException("Missing TimeStamp");
            }
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
        if (indexedSamples == null) {
            this.indexedSamples = metadataManager.getIndexedSamplesMap(studyMetadata.getId());
        }

        return indexedSamples;
    }

    public StudyMetadata getStudyMetadata() {
        if (studyMetadata == null) {
            studyMetadata = metadataManager.getStudyMetadata(getStudyId()); // Variant meta
        }

        return studyMetadata;
    }

    public int getStudyId() {
        return helper.getStudyId();
    }

    public VariantStorageMetadataManager getMetadataManager() {
        return metadataManager;
    }

    public HBaseToVariantConverter<Result> getHbaseToVariantConverter() {
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
            context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME,
                    "VCF_TIMER_" + entry.getKey().replace(' ', '_')).increment(entry.getValue());
        }
        timeSum.clear();
    }
}
