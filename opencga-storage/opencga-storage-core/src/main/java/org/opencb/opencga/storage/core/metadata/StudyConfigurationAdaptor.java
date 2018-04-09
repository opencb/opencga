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

package org.opencb.opencga.storage.core.metadata;

import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Created on 30/03/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class StudyConfigurationAdaptor implements AutoCloseable {
    private static Logger logger = LoggerFactory.getLogger(StudyConfigurationAdaptor.class);

    protected long lockStudy(int studyId, long lockDuration, long timeout, String lockName) throws InterruptedException, TimeoutException {
        logger.warn("Ignoring lock");
        return 0;
    }

    protected void unLockStudy(int studyId, long lockId, String lockName) {
        logger.warn("Ignoring unLock");
    }

    protected abstract QueryResult<StudyConfiguration> getStudyConfiguration(String studyName, Long time, QueryOptions options);

    protected abstract QueryResult<StudyConfiguration> getStudyConfiguration(int studyId, Long timeStamp, QueryOptions options);

    protected abstract QueryResult updateStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options);

    protected abstract Map<String, Integer> getStudies(QueryOptions options);

    protected List<String> getStudyNames(QueryOptions options) {
        return new ArrayList<>(getStudies(options).keySet());
    }

    protected List<Integer> getStudyIds(QueryOptions options) {
        return new ArrayList<>(getStudies(options).values());
    }

    @Override
    public void close() throws IOException {

    }
}
