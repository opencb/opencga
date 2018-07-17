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
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.adaptors.StudyConfigurationAdaptor;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Created on 28/11/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class DummyStudyConfigurationAdaptor implements StudyConfigurationAdaptor {

    public static Map<String, StudyConfiguration> STUDY_CONFIGURATIONS_BY_NAME = new ConcurrentHashMap<>();
    public static Map<Integer, StudyConfiguration> STUDY_CONFIGURATIONS_BY_ID = new ConcurrentHashMap<>();
    private static Map<Integer, Lock> LOCK_STUDIES = new ConcurrentHashMap<>();
    private static AtomicInteger NUM_PRINTS = new AtomicInteger();

    @Override
    public List<String> getStudyNames(QueryOptions options) {
        return new ArrayList<>(STUDY_CONFIGURATIONS_BY_NAME.keySet());
    }

    @Override
    public List<Integer> getStudyIds(QueryOptions options) {
        return new ArrayList<>(STUDY_CONFIGURATIONS_BY_ID.keySet());
    }

    @Override
    public Map<String, Integer> getStudies(QueryOptions options) {
        return STUDY_CONFIGURATIONS_BY_NAME.values().stream().collect(Collectors.toMap(StudyConfiguration::getStudyName, StudyConfiguration::getStudyId));
    }

    @Override
    public QueryResult<StudyConfiguration> getStudyConfiguration(String studyName, Long time, QueryOptions options) {
        if (STUDY_CONFIGURATIONS_BY_NAME.containsKey(studyName)) {
            return new QueryResult<>("", 0, 1, 1, "", "", Collections.singletonList(STUDY_CONFIGURATIONS_BY_NAME.get(studyName).newInstance()));
        } else {
            return new QueryResult<>("", 0, 0, 0, "", "", Collections.emptyList());
        }
    }

    @Override
    public QueryResult<StudyConfiguration> getStudyConfiguration(int studyId, Long timeStamp, QueryOptions options) {
        if (STUDY_CONFIGURATIONS_BY_ID.containsKey(studyId)) {
            return new QueryResult<>("", 0, 1, 1, "", "", Collections.singletonList(STUDY_CONFIGURATIONS_BY_ID.get(studyId).newInstance()));
        } else {
            return new QueryResult<>("", 0, 0, 0, "", "", Collections.emptyList());
        }
    }

    @Override
    public QueryResult updateStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options) {
        STUDY_CONFIGURATIONS_BY_ID.put(studyConfiguration.getStudyId(), studyConfiguration.newInstance());
        STUDY_CONFIGURATIONS_BY_NAME.put(studyConfiguration.getStudyName(), studyConfiguration.newInstance());

        return new QueryResult();

    }

    @Override
    public synchronized long lockStudy(int studyId, long lockDuration, long timeout, String lockName) throws InterruptedException, TimeoutException {
        if (!LOCK_STUDIES.containsKey(studyId)) {
            LOCK_STUDIES.put(studyId, new ReentrantLock());
        }
        LOCK_STUDIES.get(studyId).tryLock(timeout, TimeUnit.MILLISECONDS);

        return studyId;
    }

    @Override
    public void unLockStudy(int studyId, long lockId, String lockName) {
        LOCK_STUDIES.get(studyId).unlock();
    }

    public static void writeAll(Path path) {
        ObjectMapper objectMapper = new ObjectMapper(new JsonFactory()).configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        String prefix = "storage_configuration_" + NUM_PRINTS.incrementAndGet() + "_";
        for (StudyConfiguration studyConfiguration : DummyStudyConfigurationAdaptor.STUDY_CONFIGURATIONS_BY_NAME.values()) {
            try (OutputStream os = new FileOutputStream(path.resolve(prefix + studyConfiguration.getStudyName() + ".json").toFile())) {
                objectMapper.writerWithDefaultPrettyPrinter().writeValue(os, studyConfiguration);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public static void clear() {
        STUDY_CONFIGURATIONS_BY_NAME.clear();
        STUDY_CONFIGURATIONS_BY_ID.clear();
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
