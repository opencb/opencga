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

import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.models.Locked;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created on 30/03/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface StudyMetadataDBAdaptor extends AutoCloseable {

    Locked lock(int studyId, long lockDuration, long timeout, String lockName) throws StorageEngineException;

    @Deprecated
    DataResult<StudyConfiguration> getStudyConfiguration(String studyName, Long time, QueryOptions options);

    @Deprecated
    DataResult<StudyConfiguration> getStudyConfiguration(int studyId, Long timeStamp, QueryOptions options);

    @Deprecated
    DataResult updateStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options);

    StudyMetadata getStudyMetadata(int id, Long timeStamp);

    void updateStudyMetadata(StudyMetadata sm);

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
