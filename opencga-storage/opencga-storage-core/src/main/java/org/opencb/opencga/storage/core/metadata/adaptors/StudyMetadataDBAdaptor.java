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

package org.opencb.opencga.storage.core.metadata.adaptors;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.models.*;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeoutException;

/**
 * Created on 30/03/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface StudyMetadataDBAdaptor extends AutoCloseable {

    default long lockStudy(int studyId, long lockDuration, long timeout, String lockName) throws InterruptedException, TimeoutException {
        LoggerFactory.getLogger(StudyMetadataDBAdaptor.class).warn("Ignoring lock");
        return 0;
    }

    default void unLockStudy(int studyId, long lockId, String lockName) {
        LoggerFactory.getLogger(StudyMetadataDBAdaptor.class).warn("Ignoring unLock");
    }

    @Deprecated
    QueryResult<StudyConfiguration> getStudyConfiguration(String studyName, Long time, QueryOptions options);

    @Deprecated
    QueryResult<StudyConfiguration> getStudyConfiguration(int studyId, Long timeStamp, QueryOptions options);

    @Deprecated
    QueryResult updateStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options);

    default StudyMetadata getStudyMetadata(int id, Long timeStamp) {
        return new StudyMetadata(getStudyConfiguration(id, timeStamp, null).first());
    }

    default void updateStudyMetadata(StudyMetadata sm) {
        StudyConfiguration sc = getStudyConfiguration(sm.getId(), null, null).first();
        if (sc == null) {
            sc = new StudyConfiguration(sm.getId(), sm.getName());
        }
        sc.setStudyId(sm.getId());
        sc.setStudyName(sm.getName());
        sc.setAggregation(sm.getAggregation());
        sc.setVariantHeader(sm.getVariantHeader());
        sc.setTimeStamp(sm.getTimeStamp());
        sc.setAttributes(sm.getAttributes());

        updateStudyConfiguration(sc, null);
    }

    default LinkedHashSet<Integer> getIndexedFiles(int studyId) {
        return getStudyConfiguration(studyId, null, null).first().getIndexedFiles();
    }

    default Iterator<FileMetadata> fileIterator(int studyId) {
        throw new UnsupportedOperationException("TODO");
    }

    default void updateIndexedFiles(int studyId, LinkedHashSet<Integer> indexedFiles) {
        StudyConfiguration sc = getStudyConfiguration(studyId, null, null).first();
        sc.setIndexedFiles(indexedFiles);
        updateStudyConfiguration(sc, null);
    }

    default FileMetadata getFileMetadata(int studyId, int fileId, Long timeStamp) {
        throw new UnsupportedOperationException("TODO");
    }

    default void updateFileMetadata(int studyId, FileMetadata file, Long timeStamp) {
        throw new UnsupportedOperationException("TODO");
    }

    default Integer getFileId(int studyId, String fileName) {
        throw new UnsupportedOperationException("TODO");
    }

    default SampleMetadata getSampleMetadata(int studyId, int sampleId, Long timeStamp) {
        throw new UnsupportedOperationException("TODO");
    }

    default void updateSampleMetadata(int studyId, SampleMetadata sample, Long timeStamp) {
        throw new UnsupportedOperationException("TODO");
    }

    default Iterator<SampleMetadata> sampleMetadataIterator(int studyId) {
        throw new UnsupportedOperationException("TODO");
    }

    default BiMap<String, Integer> getIndexedSamplesMap(int studyId) {
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

    default List<Integer> getIndexedSamples(int studyId) {
        // FIXME!
        Set<Integer> set = new LinkedHashSet<>();
        for (Integer indexedFile : getIndexedFiles(studyId)) {
            set.addAll(getFileMetadata(studyId, indexedFile, null).getSamples());
        }
        return new ArrayList<>(set);
    }

    default Integer getSampleId(int studyId, String sampleName) {
        throw new UnsupportedOperationException("TODO");
    }

    default CohortMetadata getCohortMetadata(int studyId, int cohortId, Long timeStamp) {
        throw new UnsupportedOperationException("TODO");
    }

    default void updateCohortMetadata(int studyId, CohortMetadata cohort, Long timeStamp) {
        throw new UnsupportedOperationException("TODO");
    }

    default Integer getCohortId(int studyId, String cohortName) {
        throw new UnsupportedOperationException("TODO");
    }

    default Iterator<CohortMetadata> cohortIterator(int studyId) {
        throw new UnsupportedOperationException("TODO");
    }

    default BatchFileTask getTask(int studyId, int taskId, Long timeStamp) {
        throw new UnsupportedOperationException("TODO");
    }

    default Iterator<BatchFileTask> taskIterator(int studyId, boolean reversed) {
        throw new UnsupportedOperationException("TODO");
    }

    default void updateTask(int studyId, BatchFileTask task, Long timeStamp) {
        throw new UnsupportedOperationException("TODO");
    }

    Map<String, Integer> getStudies(QueryOptions options);

    default List<String> getStudyNames(QueryOptions options) {
        return new ArrayList<>(getStudies(options).keySet());
    }

    default List<Integer> getStudyIds(QueryOptions options) {
        return new ArrayList<>(getStudies(options).values());
    }

    @Override
    default void close() throws IOException {
    }
}
