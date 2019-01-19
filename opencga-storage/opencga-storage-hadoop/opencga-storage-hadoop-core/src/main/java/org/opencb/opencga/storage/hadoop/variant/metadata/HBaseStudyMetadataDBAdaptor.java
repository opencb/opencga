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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StopWatch;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.CompressionUtils;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.adaptors.StudyMetadataDBAdaptor;
import org.opencb.opencga.storage.core.metadata.models.*;
import org.opencb.opencga.storage.hadoop.utils.HBaseLock;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.zip.DataFormatException;

import static org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantMetadataUtils.*;

/**
 * Created on 12/11/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseStudyMetadataDBAdaptor extends AbstractHBaseDBAdaptor implements StudyMetadataDBAdaptor {

    private static Logger logger = LoggerFactory.getLogger(HBaseStudyMetadataDBAdaptor.class);

    private final HBaseLock lock;

    public HBaseStudyMetadataDBAdaptor(VariantTableHelper helper) {
        this(null, helper.getMetaTableAsString(), helper.getConf());
    }

    public HBaseStudyMetadataDBAdaptor(HBaseManager hBaseManager, String metaTableName, Configuration configuration) {
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
        return new QueryResult<>("", (int) watch.now(TimeUnit.MILLISECONDS),
                studyConfigurationList.size(), studyConfigurationList.size(), "", error, studyConfigurationList);
    }

    @Override
    public QueryResult updateStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options) {
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
    public LinkedHashSet<Integer> getIndexedFiles(int studyId) {
        // FIXME!
        LinkedHashSet<Integer> indexedFiles = new LinkedHashSet<>();
        fileIterator(studyId).forEachRemaining(file -> {
            if (file.isIndexed()) {
                indexedFiles.add(file.getId());
            }
        });

        return indexedFiles;
    }

    @Override
    public Iterator<FileMetadata> fileIterator(int studyId) {
        return iterator(getFileMetadataRowKeyPrefix(studyId), FileMetadata.class, false);
    }

    @Override
    public FileMetadata getFileMetadata(int studyId, int fileId, Long timeStamp) {
        return readValue(getFileMetadataRowKey(studyId, fileId), FileMetadata.class, timeStamp);
    }

    @Override
    public void updateFileMetadata(int studyId, FileMetadata file, Long timeStamp) {
        putValue(getFileNameIndexRowKey(studyId, file.getName()), Type.INDEX, file.getId(), timeStamp);
        putValue(getFileMetadataRowKey(studyId, file.getId()), Type.FILE, file, timeStamp);
    }

    @Override
    public Integer getFileId(int studyId, String fileName) {
        return readValue(getFileNameIndexRowKey(studyId, fileName), Integer.class, null);
    }

    @Override
    public SampleMetadata getSampleMetadata(int studyId, int sampleId, Long timeStamp) {
        return readValue(getSampleMetadataRowKey(studyId, sampleId), SampleMetadata.class, timeStamp);
    }

    @Override
    public void updateSampleMetadata(int studyId, SampleMetadata sample, Long timeStamp) {
        putValue(getSampleNameIndexRowKey(studyId, sample.getName()), Type.INDEX, sample.getId(), timeStamp);
        putValue(getSampleMetadataRowKey(studyId, sample.getId()), Type.SAMPLE, sample, timeStamp);
    }

    @Override
    public Iterator<SampleMetadata> sampleMetadataIterator(int studyId) {
        return iterator(getSampleMetadataRowKeyPrefix(studyId), SampleMetadata.class, false);
    }

    @Override
    public List<Integer> getIndexedSamples(int studyId) {
        // FIXME!
        Set<Integer> set = new LinkedHashSet<>();
        for (Integer indexedFile : getIndexedFiles(studyId)) {
            set.addAll(getFileMetadata(studyId, indexedFile, null).getSamples());
        }
        return new ArrayList<>(set);
    }

    @Override
    public BiMap<String, Integer> getIndexedSamplesMap(int studyId) {
        // FIXME!
        BiMap<String, Integer> map = HashBiMap.create();
        for (Integer indexedFile : getIndexedFiles(studyId)) {
            for (Integer sampleId : getFileMetadata(studyId, indexedFile, null).getSamples()) {
                if (!map.containsValue(sampleId)) {
                    map.put(getSampleMetadata(studyId, sampleId, null).getName(), sampleId);
                }
            }
        }
        return map;
    }

    @Override
    public Integer getSampleId(int studyId, String sampleName) {
        return readValue(getSampleNameIndexRowKey(studyId, sampleName), Integer.class, null);
    }

    @Override
    public CohortMetadata getCohortMetadata(int studyId, int cohortId, Long timeStamp) {
        return readValue(getCohortMetadataRowKey(studyId, cohortId), CohortMetadata.class, timeStamp);
    }

    @Override
    public void updateCohortMetadata(int studyId, CohortMetadata cohort, Long timeStamp) {
        putValue(getCohortNameIndexRowKey(studyId, cohort.getName()), Type.INDEX, cohort.getId(), timeStamp);
        putValue(getCohortMetadataRowKey(studyId, cohort.getId()), Type.COHORT, cohort, timeStamp);
    }

    @Override
    public Integer getCohortId(int studyId, String cohortName) {
        return readValue(getCohortNameIndexRowKey(studyId, cohortName), Integer.class, null);
    }

    @Override
    public Iterator<CohortMetadata> cohortIterator(int studyId) {
        return iterator(getCohortMetadataRowKeyPrefix(studyId), CohortMetadata.class, false);
    }

    @Override
    public BatchFileTask getTask(int studyId, int taskId, Long timeStamp) {
        return readValue(getTaskRowKey(studyId, taskId), BatchFileTask.class, timeStamp);
    }

    @Override
    public Iterator<BatchFileTask> taskIterator(int studyId, boolean reversed) {
        return iterator(getTaskRowKeyPrefix(studyId), BatchFileTask.class, reversed);
    }

    @Override
    public void updateTask(int studyId, BatchFileTask task, Long timeStamp) {
        putValue(getTaskRowKey(studyId, task.getId()), Type.TASK, task, timeStamp);

        BatchFileTask.Status currentStatus = task.currentStatus();
        HashSet<BatchFileTask.Status> allStatus = new HashSet<>(task.getStatus().values());
        for (BatchFileTask.Status status : allStatus) {
            if (currentStatus.equals(status)) {
                putValue(getTaskStatusIndexRowKey(studyId, currentStatus, task.getId()), Type.INDEX, task.getId(), timeStamp);
            } else {
                deleteRow(getTaskStatusIndexRowKey(studyId, status, task.getId()));
            }
        }
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
