package org.opencb.opencga.storage.core.variant.dummy;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;

import java.io.*;
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
public class DummyStudyConfigurationManager extends StudyConfigurationManager {
    public DummyStudyConfigurationManager() {
        super(new ObjectMap());
    }

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
    protected QueryResult<StudyConfiguration> internalGetStudyConfiguration(String studyName, Long time, QueryOptions options) {
        if (STUDY_CONFIGURATIONS_BY_NAME.containsKey(studyName)) {
            return new QueryResult<>("", 0, 1, 1, "", "", Collections.singletonList(STUDY_CONFIGURATIONS_BY_NAME.get(studyName).newInstance()));
        } else {
            return new QueryResult<>("", 0, 0, 0, "", "", Collections.emptyList());
        }
    }

    @Override
    protected QueryResult<StudyConfiguration> internalGetStudyConfiguration(int studyId, Long timeStamp, QueryOptions options) {
        if (STUDY_CONFIGURATIONS_BY_ID.containsKey(studyId)) {
            return new QueryResult<>("", 0, 1, 1, "", "", Collections.singletonList(STUDY_CONFIGURATIONS_BY_ID.get(studyId).newInstance()));
        } else {
            return new QueryResult<>("", 0, 0, 0, "", "", Collections.emptyList());
        }
    }

    @Override
    protected QueryResult internalUpdateStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options) {
        STUDY_CONFIGURATIONS_BY_ID.put(studyConfiguration.getStudyId(), studyConfiguration.newInstance());
        STUDY_CONFIGURATIONS_BY_NAME.put(studyConfiguration.getStudyName(), studyConfiguration.newInstance());

        return new QueryResult();

    }

    @Override
    public synchronized long lockStudy(int studyId, long lockDuration, long timeout) throws InterruptedException, TimeoutException {
        if (!LOCK_STUDIES.containsKey(studyId)) {
            LOCK_STUDIES.put(studyId, new ReentrantLock());
        }
        LOCK_STUDIES.get(studyId).tryLock(timeout, TimeUnit.MILLISECONDS);

        return studyId;
    }

    @Override
    public void unLockStudy(int studyId, long lockId) {
        LOCK_STUDIES.get(studyId).unlock();
    }

    public static synchronized void writeAndClear(Path path) {
        ObjectMapper objectMapper = new ObjectMapper(new JsonFactory());
        String prefix = "storage_configuration_" + NUM_PRINTS.incrementAndGet() + "_";
        for (StudyConfiguration studyConfiguration : DummyStudyConfigurationManager.STUDY_CONFIGURATIONS_BY_NAME.values()) {
            try (OutputStream os = new FileOutputStream(path.resolve(prefix + studyConfiguration.getStudyName()).toFile())) {
                objectMapper.writerWithDefaultPrettyPrinter().writeValue(os, studyConfiguration);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        STUDY_CONFIGURATIONS_BY_NAME.clear();
        STUDY_CONFIGURATIONS_BY_ID.clear();
        LOCK_STUDIES.clear();
    }

    public static void clear() {
        STUDY_CONFIGURATIONS_BY_NAME.clear();
        STUDY_CONFIGURATIONS_BY_ID.clear();
        LOCK_STUDIES.clear();
    }
}
