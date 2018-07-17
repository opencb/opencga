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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StopWatch;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.CompressionUtils;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.adaptors.StudyConfigurationAdaptor;
import org.opencb.opencga.storage.hadoop.utils.HBaseLock;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.zip.DataFormatException;

import static org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantMetadataUtils.*;

/**
 * Created on 12/11/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseStudyConfigurationDBAdaptor extends AbstractHBaseDBAdaptor implements StudyConfigurationAdaptor {

    private static Logger logger = LoggerFactory.getLogger(HBaseStudyConfigurationDBAdaptor.class);

    private final HBaseLock lock;

    public HBaseStudyConfigurationDBAdaptor(VariantTableHelper helper) {
        this(null, helper.getMetaTableAsString(), helper.getConf());
    }

    public HBaseStudyConfigurationDBAdaptor(HBaseManager hBaseManager, String metaTableName, Configuration configuration) {
        super(hBaseManager, metaTableName, configuration);
        lock = new HBaseLock(this.hBaseManager, this.tableName, family, null);
    }


    @Override
    public QueryResult<StudyConfiguration> getStudyConfiguration(String studyName, Long timeStamp, QueryOptions options) {
        logger.debug("Get StudyConfiguration " + studyName + " from DB " + tableName);
        BiMap<String, Integer> studies = getStudies(options);
        Integer studyId = studies.get(studyName);
        if (studyId == null) {
//            throw VariantQueryException.studyNotFound(studyName, studies.keySet());
            return new QueryResult<>("", 0, 0, 0, "", "", Collections.emptyList());
        }

//        if (StringUtils.isEmpty(studyName)) {
//            return new QueryResult<>("", (int) watch.getTime(),
//                    studyConfigurationList.size(), studyConfigurationList.size(), "", "", studyConfigurationList);
//        }

        return getStudyConfiguration(studyId, timeStamp, options);
    }

    @Override
    public long lockStudy(int studyId, long lockDuration, long timeout, String lockName) throws InterruptedException, TimeoutException {
        return lockStudy(studyId, lockDuration, timeout, StringUtils.isEmpty(lockName) ? getLockColumn() : Bytes.toBytes(lockName));
    }

    private long lockStudy(int studyId, long lockDuration, long timeout, byte[] lockName) throws InterruptedException, TimeoutException {
        try {
            ensureTableExists();
            return lock.lock(getStudyConfigurationRowKey(studyId), lockName, lockDuration, timeout);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void unLockStudy(int studyId, long lockToken, String lockName) {
        try {
            byte[] column = StringUtils.isEmpty(lockName) ? getLockColumn() : Bytes.toBytes(lockName);
            lock.unlock(getStudyConfigurationRowKey(studyId), column, lockToken);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public QueryResult<StudyConfiguration> getStudyConfiguration(int studyId, Long timeStamp, QueryOptions options) {
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
            if (hBaseManager.tableExists(tableName)) {
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
        return new QueryResult<>("", (int) watch.now(TimeUnit.MILLISECONDS),
                studyConfigurationList.size(), studyConfigurationList.size(), "", error, studyConfigurationList);
    }

    @Override
    public QueryResult updateStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options) {
        long startTime = System.currentTimeMillis();
        String error = "";
        logger.info("Update StudyConfiguration {}", studyConfiguration.getStudyName());
        updateStudiesSummary(studyConfiguration.getStudyName(), studyConfiguration.getStudyId(), options);

        studyConfiguration.getHeaders().clear(); // REMOVE: stored as VariantFileMetadata

        try {
            hBaseManager.act(tableName, table -> {
                byte[] bytes = objectMapper.writeValueAsBytes(studyConfiguration);
                // Compress json
                // Avoid "java.lang.IllegalArgumentException: KeyValue size too large"
                bytes = CompressionUtils.compress(bytes);
                Put put = new Put(getStudyConfigurationRowKey(studyConfiguration));
                put.addColumn(family, getValueColumn(), studyConfiguration.getTimeStamp(), bytes);
                put.addColumn(family, getTypeColumn(), studyConfiguration.getTimeStamp(),
                        Type.STUDY_CONFIGURATION.bytes());
                table.put(put);
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return new QueryResult<>("", (int) (System.currentTimeMillis() - startTime), 0, 0, "", error, Collections.emptyList());
    }

    @Override
    public BiMap<String, Integer> getStudies(QueryOptions options) {
        Get get = new Get(getStudiesSummaryRowKey());
        try {
            if (!hBaseManager.tableExists(tableName)) {
                logger.debug("Get StudyConfiguration summary TABLE_NO_EXISTS");
                return HashBiMap.create();
            }
            return hBaseManager.act(tableName, table -> {
                Result result = table.get(get);
                if (result.isEmpty()) {
                    logger.debug("Get StudyConfiguration summary EMPTY");
                    return HashBiMap.create();
                } else {
                    byte[] value = result.getValue(family, getValueColumn());
                    Map<String, Integer> map = objectMapper.readValue(value, Map.class);
                    logger.debug("Get StudyConfiguration summary {}", map);

                    return HashBiMap.create(map);
                }
            });
        } catch (IOException e) {
            logger.warn("Get StudyConfiguration summary ERROR", e);
            throw new UncheckedIOException(e);
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
            updateStudiesSummary(studiesSummary, options);
        }
    }

    private void updateStudiesSummary(BiMap<String, Integer> studies, QueryOptions options) {
        try {
            ensureTableExists();
            Connection connection = hBaseManager.getConnection();
            try (Table table = connection.getTable(TableName.valueOf(tableName))) {
                byte[] bytes = objectMapper.writeValueAsBytes(studies);
                Put put = new Put(getStudiesSummaryRowKey());
                put.addColumn(family, getValueColumn(), bytes);
                put.addColumn(family, getTypeColumn(), Type.STUDIES.bytes());
                table.put(put);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
