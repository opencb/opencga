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

/**
 * Created by hpccoll1 on 23/03/15.
 */
public abstract class StudyConfigurationManager {
    protected static Logger logger = LoggerFactory.getLogger(StudyConfigurationManager.class);

    public StudyConfigurationManager(ObjectMap objectMap) {}

    public abstract QueryResult<StudyConfiguration> getStudyConfiguration(int studyId, QueryOptions options);

    public abstract QueryResult updateStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options);

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
