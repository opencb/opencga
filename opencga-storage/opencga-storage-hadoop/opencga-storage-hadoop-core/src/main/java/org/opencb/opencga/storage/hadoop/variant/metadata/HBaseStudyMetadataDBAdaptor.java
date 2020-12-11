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

package org.opencb.opencga.storage.hadoop.variant.metadata;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.util.StopWatch;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.CompressionUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.adaptors.StudyMetadataDBAdaptor;
import org.opencb.opencga.storage.core.metadata.models.Lock;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantTableHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.DataFormatException;

import static org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantMetadataUtils.*;

/**
 * Created on 12/11/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseStudyMetadataDBAdaptor extends AbstractHBaseDBAdaptor implements StudyMetadataDBAdaptor {

    private static Logger logger = LoggerFactory.getLogger(HBaseStudyMetadataDBAdaptor.class);


    public HBaseStudyMetadataDBAdaptor(VariantTableHelper helper) {
        this(null, helper.getMetaTableAsString(), helper.getConf());
    }

    public HBaseStudyMetadataDBAdaptor(HBaseManager hBaseManager, String metaTableName, Configuration configuration) {
        super(hBaseManager, metaTableName, configuration);
    }


    @Override
    public DataResult<StudyConfiguration> getStudyConfiguration(String studyName, Long timeStamp, QueryOptions options) {
        logger.debug("Get StudyConfiguration " + studyName + " from DB " + tableName);
        BiMap<String, Integer> studies = getStudies(options);
        Integer studyId = studies.get(studyName);
        if (studyId == null) {
//            throw VariantQueryException.studyNotFound(studyName, studies.keySet());
            return new DataResult<>(0, Collections.emptyList(), 0, Collections.emptyList(), 0);
        }

//        if (StringUtils.isEmpty(studyName)) {
//            return new DataResult<>("", (int) watch.getTime(),
//                    studyConfigurationList.size(), studyConfigurationList.size(), "", "", studyConfigurationList);
//        }

        return getStudyConfiguration(studyId, timeStamp, options);
    }

    @Override
    public Lock lock(int studyId, long lockDuration, long timeout, String lockName) throws StorageEngineException {
        return lock(getStudyMetadataRowKey(studyId), getLockColumn(lockName), lockDuration, timeout);
    }

    @Override
    public DataResult<StudyConfiguration> getStudyConfiguration(int studyId, Long timeStamp, QueryOptions options) {
        StopWatch watch = new StopWatch().start();
        String error = null;
        List<StudyConfiguration> studyConfigurationList = Collections.emptyList();
        Get get = new Get(getStudyConfigurationRowKey(studyId));
        get.addColumn(family, getValueColumn());
        logger.debug("Get StudyConfiguration {} from DB {}", studyId, tableName);

        if (timeStamp != null) {
            try {
                get.setTimeRange(timeStamp + 1, Long.MAX_VALUE);
            } catch (IOException e) {
                //This should not happen ever.
                throw new IllegalArgumentException(e);
            }
        }

        try {
            if (tableExists()) {
                studyConfigurationList = hBaseManager.act(tableName, table -> {
                    Result result = table.get(get);
                    if (result.isEmpty()) {
                        return Collections.emptyList();
                    } else {
                        byte[] value = result.getValue(family, getValueColumn());
                        // Try to decompress value.
                        try {
                            value = CompressionUtils.decompress(value);
                        } catch (DataFormatException e) {
                            if (value[0] == '{') {
                                logger.debug("StudyConfiguration was not compressed", e);
                            } else {
                                throw new IllegalStateException("Problem reading StudyConfiguration "
                                        + studyId + " from table " + tableName, e);
                            }
                        }
                        StudyConfiguration studyConfiguration = objectMapper.readValue(value, StudyConfiguration.class);
                        return Collections.singletonList(studyConfiguration);
                    }
                });
            }
        } catch (IOException e) {
            throw new IllegalStateException("Problem reading StudyConfiguration " + studyId + " from table " + tableName, e);
        }
        return new DataResult<>((int) watch.now(TimeUnit.MILLISECONDS), StringUtils.isEmpty(error) ? Collections.emptyList()
                : Collections.singletonList(new Event(Event.Type.ERROR, error)), studyConfigurationList.size(), studyConfigurationList,
                studyConfigurationList.size());
    }

    @Override
    public DataResult updateStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options) {
        long startTime = System.currentTimeMillis();
        String error = "";
        logger.info("Update StudyConfiguration {}", studyConfiguration.getName());
        updateStudiesSummary(studyConfiguration.getName(), studyConfiguration.getId(), options);
        updateStudyMetadata(new StudyMetadata(studyConfiguration));

        studyConfiguration.getHeaders().clear(); // REMOVE: stored as VariantFileMetadata

        try {
            hBaseManager.act(tableName, table -> {
                byte[] bytes = objectMapper.writeValueAsBytes(studyConfiguration);
                // Compress json
                // Avoid "java.lang.IllegalArgumentException: KeyValue size too large"
                bytes = CompressionUtils.compress(bytes);
                Put put = new Put(getStudyConfigurationRowKey(studyConfiguration.getId()));
                put.addColumn(family, getValueColumn(), studyConfiguration.getTimeStamp(), bytes);
                put.addColumn(family, getTypeColumn(), studyConfiguration.getTimeStamp(),
                        Type.STUDY_CONFIGURATION.bytes());
                table.put(put);
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return new DataResult().setTime(((int) (System.currentTimeMillis() - startTime)));
    }

    @Override
    public StudyMetadata getStudyMetadata(int id, Long timeStamp) {
        return readValue(getStudyMetadataRowKey(id), StudyMetadata.class, timeStamp);
    }

    @Override
    public void updateStudyMetadata(StudyMetadata sm) {
        sm.setTimeStamp(System.currentTimeMillis());
        updateStudiesSummary(sm.getName(), sm.getId(), null);
        putValue(getStudyMetadataRowKey(sm.getId()), Type.STUDY, sm, sm.getTimeStamp());
    }

    @Override
    public BiMap<String, Integer> getStudies(QueryOptions options) {
        Map<String, Integer> studies = readValue(getStudiesSummaryRowKey(), Map.class);
        if (studies == null) {
            return HashBiMap.create();
        } else {
            return HashBiMap.create(studies);
        }
    }

    private void updateStudiesSummary(String study, Integer studyId, QueryOptions options) {
        BiMap<String, Integer> studiesSummary = getStudies(options);
        if (study.isEmpty()) {
            throw new IllegalStateException("Can't save an study with empty StudyName");
        }
        if (studiesSummary.getOrDefault(study, Integer.MIN_VALUE).equals(studyId)) {
            //Nothing to update
            return;
        } else {
            studiesSummary.put(study, studyId);
            updateStudiesSummary(studiesSummary);
        }
    }

    private void updateStudiesSummary(BiMap<String, Integer> studies) {
        putValue(getStudiesSummaryRowKey(), Type.STUDIES, studies, null);
    }

    @Override
    public void close() throws IOException {
        try {
            hBaseManager.close();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

}
