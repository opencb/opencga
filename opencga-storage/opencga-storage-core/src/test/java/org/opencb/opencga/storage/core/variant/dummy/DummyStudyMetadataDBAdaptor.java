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

package org.opencb.opencga.storage.core.variant.dummy;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SequenceWriter;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.adaptors.CohortMetadataDBAdaptor;
import org.opencb.opencga.storage.core.metadata.adaptors.SampleMetadataDBAdaptor;
import org.opencb.opencga.storage.core.metadata.adaptors.StudyMetadataDBAdaptor;
import org.opencb.opencga.storage.core.metadata.adaptors.TaskMetadataDBAdaptor;
import org.opencb.opencga.storage.core.metadata.models.*;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Created on 28/11/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class DummyStudyMetadataDBAdaptor implements StudyMetadataDBAdaptor, SampleMetadataDBAdaptor, CohortMetadataDBAdaptor, TaskMetadataDBAdaptor {

    public static Map<String, StudyConfiguration> STUDY_CONFIGURATIONS_BY_NAME = new ConcurrentHashMap<>();
    public static Map<Integer, StudyConfiguration> STUDY_CONFIGURATIONS_BY_ID = new ConcurrentHashMap<>();
    public static Map<Integer, StudyMetadata> STUDY_METADATA_MAP = new ConcurrentHashMap<>();
    public static Map<Integer, Map<Integer, SampleMetadata>> SAMPLE_METADATA_MAP = new ConcurrentHashMap<>();
    public static Map<Integer, Map<Integer, CohortMetadata>> COHORT_METADATA_MAP = new ConcurrentHashMap<>();
    public static Map<Integer, Map<Integer, TaskMetadata>> TASK_METADATA_MAP = new ConcurrentHashMap<>();

    private static Map<Integer, Lock> LOCK_STUDIES = new ConcurrentHashMap<>();
    private static AtomicInteger NUM_PRINTS = new AtomicInteger();

    @Override
    public List<String> getStudyNames(QueryOptions options) {
        return STUDY_METADATA_MAP.values().stream().map(StudyMetadata::getName).collect(Collectors.toList());
    }

    @Override
    public List<Integer> getStudyIds(QueryOptions options) {
        return new ArrayList<>(STUDY_METADATA_MAP.keySet());
    }

    @Override
    public Map<String, Integer> getStudies(QueryOptions options) {
        return STUDY_METADATA_MAP.values().stream().collect(Collectors.toMap(StudyMetadata::getName, StudyMetadata::getId));
    }

    @Override
    public DataResult<StudyConfiguration> getStudyConfiguration(String studyName, Long time, QueryOptions options) {
        if (STUDY_CONFIGURATIONS_BY_NAME.containsKey(studyName)) {
            return new DataResult<>(0, Collections.emptyList(), 1,
                    Collections.singletonList(STUDY_CONFIGURATIONS_BY_NAME.get(studyName).newInstance()), 1);
        } else {
            return new DataResult<>(0, Collections.emptyList(), 0, Collections.emptyList(), 0);
        }
    }

    @Override
    public DataResult<StudyConfiguration> getStudyConfiguration(int studyId, Long timeStamp, QueryOptions options) {
        if (STUDY_CONFIGURATIONS_BY_ID.containsKey(studyId)) {
            return new DataResult<>(0, Collections.emptyList(), 1,
                    Collections.singletonList(STUDY_CONFIGURATIONS_BY_ID.get(studyId).newInstance()), 1);
        } else {
            return new DataResult<>(0, Collections.emptyList(), 0, Collections.emptyList(), 0);
        }
    }

    @Override
    public DataResult updateStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options) {
        STUDY_CONFIGURATIONS_BY_ID.put(studyConfiguration.getId(), studyConfiguration.newInstance());
        STUDY_CONFIGURATIONS_BY_NAME.put(studyConfiguration.getName(), studyConfiguration.newInstance());

        return new DataResult();

    }

    @Override
    public Locked lock(int studyId, long lockDuration, long timeout, String lockName) throws StorageEngineException {
        if (!LOCK_STUDIES.containsKey(studyId)) {
            LOCK_STUDIES.put(studyId, new ReentrantLock());
        }
        try {
            LOCK_STUDIES.get(studyId).tryLock(timeout, TimeUnit.MILLISECONDS);
            return LOCK_STUDIES.get(studyId)::unlock;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StorageEngineException("", e);
        }
    }

    @Override
    public synchronized long lockStudy(int studyId, long lockDuration, long timeout, String lockName) throws StorageEngineException {
        if (!LOCK_STUDIES.containsKey(studyId)) {
            LOCK_STUDIES.put(studyId, new ReentrantLock());
        }
        try {
            LOCK_STUDIES.get(studyId).tryLock(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StorageEngineException("", e);
        }

        return studyId;
    }

    @Override
    public void unLockStudy(int studyId, long lockId, String lockName) {
        LOCK_STUDIES.get(studyId).unlock();
    }

    @Override
    public StudyMetadata getStudyMetadata(int id, Long timeStamp) {
        return STUDY_METADATA_MAP.get(id);
    }

    @Override
    public void updateStudyMetadata(StudyMetadata sm) {
        STUDY_METADATA_MAP.put(sm.getId(), sm);
    }

    @Override
    public SampleMetadata getSampleMetadata(int studyId, int sampleId, Long timeStamp) {
        return SAMPLE_METADATA_MAP.getOrDefault(studyId, Collections.emptyMap()).get(sampleId);
    }

    @Override
    public void updateSampleMetadata(int studyId, SampleMetadata sample, Long timeStamp) {
        SAMPLE_METADATA_MAP.computeIfAbsent(studyId, s -> new ConcurrentHashMap<>()).put(sample.getId(), sample);
    }

    @Override
    public Iterator<SampleMetadata> sampleMetadataIterator(int studyId) {
        return SAMPLE_METADATA_MAP.getOrDefault(studyId, Collections.emptyMap()).values().iterator();
    }

    @Override
    public Integer getSampleId(int studyId, String sampleName) {
        return SAMPLE_METADATA_MAP.getOrDefault(studyId, Collections.emptyMap()).values()
                .stream()
                .filter(f->f.getName().equals(sampleName))
                .map(SampleMetadata::getId)
                .findFirst()
                .orElse(null);
    }

    @Override
    public Locked lock(int studyId, int id, long lockDuration, long timeout) {
        return () -> {};
    }

    @Override
    public CohortMetadata getCohortMetadata(int studyId, int cohortId, Long timeStamp) {
        return COHORT_METADATA_MAP.getOrDefault(studyId, Collections.emptyMap()).get(cohortId);
    }

    @Override
    public void updateCohortMetadata(int studyId, CohortMetadata cohort, Long timeStamp) {
        COHORT_METADATA_MAP.computeIfAbsent(studyId, s -> new ConcurrentHashMap<>()).put(cohort.getId(), cohort);
    }

    @Override
    public Integer getCohortId(int studyId, String cohortName) {
        return COHORT_METADATA_MAP.getOrDefault(studyId, Collections.emptyMap()).values()
                .stream()
                .filter(f->f.getName().equals(cohortName))
                .map(CohortMetadata::getId)
                .findFirst()
                .orElse(null);
    }

    @Override
    public Iterator<CohortMetadata> cohortIterator(int studyId) {
        return COHORT_METADATA_MAP.getOrDefault(studyId, Collections.emptyMap()).values().iterator();
    }

    @Override
    public TaskMetadata getTask(int studyId, int taskId, Long timeStamp) {
        return TASK_METADATA_MAP.getOrDefault(studyId, Collections.emptyMap()).get(taskId);
    }

    @Override
    public Iterator<TaskMetadata> taskIterator(int studyId, boolean reversed) {
        TreeSet<TaskMetadata> t;
        if (reversed) {
            t = new TreeSet<>(Comparator.comparingInt(TaskMetadata::getId).reversed());
        } else {
            t = new TreeSet<>(Comparator.comparingInt(TaskMetadata::getId));
        }

        t.addAll(TASK_METADATA_MAP.getOrDefault(studyId, Collections.emptyMap()).values());
        return t.iterator();
    }

    @Override
    public void updateTask(int studyId, TaskMetadata task, Long timeStamp) {
        TASK_METADATA_MAP.computeIfAbsent(studyId, s -> new ConcurrentHashMap<>()).put(task.getId(), task);
    }

    public static void writeAll(Path path) {
        ObjectMapper objectMapper = new ObjectMapper(new JsonFactory()).configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        String prefix = "study_" + NUM_PRINTS.incrementAndGet() + "_";
        try {
            for (StudyMetadata sm : DummyStudyMetadataDBAdaptor.STUDY_METADATA_MAP.values()) {
                try (OutputStream os = new FileOutputStream(path.resolve(prefix + sm.getName() + ".json").toFile())) {
                    objectMapper.writerWithDefaultPrettyPrinter().writeValue(os, sm);
                }
                writeAll(path, objectMapper, prefix + "samples_", sm, SAMPLE_METADATA_MAP.getOrDefault(sm.getId(), Collections.emptyMap()).values());
                writeAll(path, objectMapper, prefix + "cohort_", sm, COHORT_METADATA_MAP.getOrDefault(sm.getId(), Collections.emptyMap()).values());
                writeAll(path, objectMapper, prefix + "task_", sm, TASK_METADATA_MAP.getOrDefault(sm.getId(), Collections.emptyMap()).values());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected static void writeAll(Path path, ObjectMapper objectMapper, String prefix, StudyMetadata sm, Collection<?> values) throws IOException {
        try (OutputStream os = new FileOutputStream(path.resolve(prefix + sm.getName() + ".json").toFile())) {
            SequenceWriter sequenceWriter = objectMapper.writerWithDefaultPrettyPrinter().writeValues(os);
            sequenceWriter.writeAll(values);
        }
    }

    public static void clear() {
        STUDY_CONFIGURATIONS_BY_NAME.clear();
        STUDY_CONFIGURATIONS_BY_ID.clear();
        STUDY_METADATA_MAP.clear();
        SAMPLE_METADATA_MAP.clear();
        COHORT_METADATA_MAP.clear();
        TASK_METADATA_MAP.clear();
        LOCK_STUDIES.clear();
    }

    public static synchronized void writeAndClear(Path path) {
        writeAll(path);
        clear();
    }

    @Override
    public void close() {
    }
}
