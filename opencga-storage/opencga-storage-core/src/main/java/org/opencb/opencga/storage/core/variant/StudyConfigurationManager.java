/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.storage.core.variant;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.TimeoutException;

/**
 * @author Jacobo Coll <jacobo167@gmail.com>
 */
public abstract class StudyConfigurationManager implements AutoCloseable {
    public static final String CACHED = "cached";
    public static final String READ_ONLY = "ro";
    protected static Logger logger = LoggerFactory.getLogger(StudyConfigurationManager.class);

    private final Map<String, StudyConfiguration> stringStudyConfigurationMap = new HashMap<>();
    private final Map<Integer, StudyConfiguration> intStudyConfigurationMap = new HashMap<>();

    public interface LockCloseable extends AutoCloseable {
        @Override
        void close();
    }

    public StudyConfigurationManager(ObjectMap objectMap) {
    }

    protected abstract QueryResult<StudyConfiguration> internalGetStudyConfiguration(String studyName, Long time, QueryOptions options);

    protected abstract QueryResult<StudyConfiguration> internalGetStudyConfiguration(int studyId, Long timeStamp, QueryOptions options);

    public LockCloseable closableLockStudy(int studyId) throws StorageManagerException {
        long lock = lockStudy(studyId);
        return () -> unLockStudy(studyId, lock);
    }

    public long lockStudy(int studyId) throws StorageManagerException {
        try {
            return lockStudy(studyId, 10000, 20000);
        } catch (InterruptedException | TimeoutException e) {
            throw new StorageManagerException("Unable to lock the Study " + studyId, e);
        }
    }

    public long lockStudy(int studyId, long lockDuration, long timeout) throws InterruptedException, TimeoutException {
        logger.warn("Ignoring lock");
        return 0;
    }

    public void unLockStudy(int studyId, long lockId) {
        logger.warn("Ignoring unLock");
    }

    protected abstract QueryResult internalUpdateStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options);

    public final QueryResult<StudyConfiguration> getStudyConfiguration(String studyName, QueryOptions options) {
        QueryResult<StudyConfiguration> result;
        final boolean cached = options != null && options.getBoolean(CACHED, false);
        final boolean readOnly = options != null && options.getBoolean(READ_ONLY, false);
        if (stringStudyConfigurationMap.containsKey(studyName)) {
            if (cached) {
                StudyConfiguration studyConfiguration = stringStudyConfigurationMap.get(studyName);
                if (!readOnly) {
                    studyConfiguration = studyConfiguration.newInstance();
                }
                return new QueryResult<>(studyConfiguration.getStudyName(), 0, 1, 1, "", "", Collections.singletonList(studyConfiguration));
            }
            result = internalGetStudyConfiguration(studyName, stringStudyConfigurationMap.get(studyName).getTimeStamp(), options);
            if (result.getNumTotalResults() == 0) { //No changes. Return old value
                StudyConfiguration studyConfiguration = stringStudyConfigurationMap.get(studyName);
                if (!readOnly) {
                    studyConfiguration = studyConfiguration.newInstance();
                }
                return new QueryResult<>(studyName, 0, 1, 1, "", "", Collections.singletonList(studyConfiguration));
            }
        } else {
            result = internalGetStudyConfiguration(studyName, null, options);
        }

        StudyConfiguration studyConfiguration = result.first();
        if (studyConfiguration != null) {
            intStudyConfigurationMap.put(studyConfiguration.getStudyId(), studyConfiguration);
            stringStudyConfigurationMap.put(studyConfiguration.getStudyName(), studyConfiguration);
            if (studyName != null && !studyName.equals(studyConfiguration.getStudyName())) {
                stringStudyConfigurationMap.put(studyName, studyConfiguration);
            }
            if (!readOnly) {
                result.setResult(Collections.singletonList(studyConfiguration.newInstance()));
            }
        }
        return result;

    }

    public final QueryResult<StudyConfiguration> getStudyConfiguration(int studyId, QueryOptions options) {
        QueryResult<StudyConfiguration> result;
        final boolean cached = options != null && options.getBoolean(CACHED, false);
        final boolean readOnly = options != null && options.getBoolean(READ_ONLY, false);
        if (intStudyConfigurationMap.containsKey(studyId)) {
            if (cached) {
                StudyConfiguration studyConfiguration = intStudyConfigurationMap.get(studyId);
                if (!readOnly) {
                    studyConfiguration = studyConfiguration.newInstance();
                }
                return new QueryResult<>(studyConfiguration.getStudyName(), 0, 1, 1, "", "", Collections.singletonList(studyConfiguration));
            }
            result = internalGetStudyConfiguration(studyId, intStudyConfigurationMap.get(studyId).getTimeStamp(), options);
            if (result.getNumTotalResults() == 0) { //No changes. Return old value
                StudyConfiguration studyConfiguration = intStudyConfigurationMap.get(studyId);
                if (!readOnly) {
                    studyConfiguration = studyConfiguration.newInstance();
                }
                return new QueryResult<>(studyConfiguration.getStudyName(), 0, 1, 1, "", "", Collections.singletonList(studyConfiguration));
            }
        } else {
            result = internalGetStudyConfiguration(studyId, null, options);
        }

        StudyConfiguration studyConfiguration = result.first();
        if (studyConfiguration != null) {
            intStudyConfigurationMap.put(studyConfiguration.getStudyId(), studyConfiguration);
            stringStudyConfigurationMap.put(studyConfiguration.getStudyName(), studyConfiguration);
            if (!readOnly) {
                result.setResult(Collections.singletonList(studyConfiguration.newInstance()));
            }
        }
        return result;

    }

    public List<String> getStudyNames(QueryOptions options) {
        return new ArrayList<>(getStudies(options).keySet());
    }

    public List<Integer> getStudyIds(QueryOptions options) {
        return new ArrayList<>(getStudies(options).values());
    }

    public Map<String, Integer> getStudies(QueryOptions options) {
        return Collections.emptyMap();
    }

    public final QueryResult updateStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options) {
        studyConfiguration.setTimeStamp(System.currentTimeMillis());
        Map<Integer, String> headers = studyConfiguration.getHeaders();

        studyConfiguration.setHeaders(null);
        logger.debug("Updating studyConfiguration : {}", studyConfiguration.toJson());
        studyConfiguration.setHeaders(headers);

        stringStudyConfigurationMap.put(studyConfiguration.getStudyName(), studyConfiguration);
        intStudyConfigurationMap.put(studyConfiguration.getStudyId(), studyConfiguration);
        return internalUpdateStudyConfiguration(studyConfiguration, options);
    }

    public static StudyConfigurationManager build(String className, ObjectMap params)
            throws ReflectiveOperationException {
        try {
            Class<?> clazz = Class.forName(className);

            if (StudyConfigurationManager.class.isAssignableFrom(clazz)) {
                return (StudyConfigurationManager) clazz.getConstructor(ObjectMap.class).newInstance(params);
            } else {
                throw new ReflectiveOperationException("Clazz " + className + " is not a subclass of StudyConfigurationManager");
            }

        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException
                | InvocationTargetException e) {
            logger.error("Unable to create StudyConfigurationManager");
            throw e;
        }
    }

    @Override
    public void close() throws IOException { }
}
