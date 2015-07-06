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

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Jacobo Coll <jacobo167@gmail.com>
 */
public abstract class StudyConfigurationManager {
    protected static Logger logger = LoggerFactory.getLogger(StudyConfigurationManager.class);

    private final Map<String, StudyConfiguration> stringStudyConfigurationMap = new HashMap<>();
    private final Map<Integer, StudyConfiguration> intStudyConfigurationMap = new HashMap<>();

    public StudyConfigurationManager(ObjectMap objectMap) {}

    protected abstract QueryResult<StudyConfiguration> _getStudyConfiguration(String studyName, Long timeStamp, QueryOptions options);
    protected abstract QueryResult<StudyConfiguration> _getStudyConfiguration(int studyId, Long timeStamp, QueryOptions options);

    protected abstract QueryResult _updateStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options);

    public final QueryResult<StudyConfiguration> getStudyConfiguration(String studyName, QueryOptions options) {
        QueryResult<StudyConfiguration> result;
        if (stringStudyConfigurationMap.containsKey(studyName)) {
            result = _getStudyConfiguration(studyName, stringStudyConfigurationMap.get(studyName).getTimeStamp(), options);
            if (result.getNumTotalResults() == 0) { //No changes. Return old value
                StudyConfiguration clone = stringStudyConfigurationMap.get(studyName).clone();
                return new QueryResult<>(studyName, 0, 1, 1, "", "", Collections.singletonList(clone));
            }
        } else {
            result = _getStudyConfiguration(studyName, null, options);
        }

        stringStudyConfigurationMap.put(studyName, result.first());
        StudyConfiguration clone = stringStudyConfigurationMap.get(studyName).clone();
        result.setResult(Collections.singletonList(clone));
        return result;

    }

    public final QueryResult<StudyConfiguration> getStudyConfiguration(int studyId, QueryOptions options) {
        QueryResult<StudyConfiguration> result;
        if (intStudyConfigurationMap.containsKey(studyId)) {
            result = _getStudyConfiguration(studyId, intStudyConfigurationMap.get(studyId).getTimeStamp(), options);
            if (result.getNumTotalResults() == 0) { //No changes. Return old value
                StudyConfiguration clone = intStudyConfigurationMap.get(studyId).clone();
                return new QueryResult<>(clone.getStudyName(), 0, 1, 1, "", "", Collections.singletonList(clone));
            }
        } else {
            result = _getStudyConfiguration(studyId, null, options);
        }

        intStudyConfigurationMap.put(studyId, result.first());
        StudyConfiguration clone = intStudyConfigurationMap.get(studyId).clone();
        result.setResult(Collections.singletonList(clone));
        return result;
    }

    public final QueryResult updateStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options) {
        studyConfiguration.setTimeStamp(System.currentTimeMillis());
        stringStudyConfigurationMap.put(studyConfiguration.getStudyName(), studyConfiguration);
        return _updateStudyConfiguration(studyConfiguration, options);
    }

    static public StudyConfigurationManager build(String className, ObjectMap params)
            throws ReflectiveOperationException {
        try {
            Class<?> clazz = Class.forName(className);

            if (StudyConfigurationManager.class.isAssignableFrom(clazz)) {
                return (StudyConfigurationManager) clazz.getConstructor(ObjectMap.class).newInstance(params);
            } else {
                throw new ReflectiveOperationException("Clazz " + className + " is not a subclass of StudyConfigurationManager");
            }

        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            logger.error("Unable to create StudyConfigurationManager");
            throw e;
        }
    }
}
