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
