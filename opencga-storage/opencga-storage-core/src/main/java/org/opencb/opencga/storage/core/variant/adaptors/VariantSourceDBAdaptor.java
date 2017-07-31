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

package org.opencb.opencga.storage.core.variant.adaptors;

import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.stats.VariantSourceStats;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public interface VariantSourceDBAdaptor extends AutoCloseable {

    enum VariantSourceQueryParam implements QueryParam {
        STUDY_ID("studyId", Type.INTEGER_ARRAY),
        FILE_ID("fileId", Type.INTEGER_ARRAY);

        private final String key;
        private final Type type;

        VariantSourceQueryParam(String key, Type type) {
            this.key = key;
            this.type = type;
        }

        @Override
        public String key() {
            return key;
        }

        @Override
        public Type type() {
            return type;
        }

        @Override
        public String description() {
            return "";
        }
    }

    QueryResult<Long> count();

    void updateVariantSource(VariantSource variantSource) throws StorageEngineException;

    default QueryResult<VariantSource> get(String fileId, QueryOptions options) throws StorageEngineException {
        StopWatch stopWatch = StopWatch.createStarted();
        Iterator<VariantSource> iterator;
        try {
            iterator = iterator(new Query(VariantSourceQueryParam.FILE_ID.key(), fileId), options);
        } catch (IOException e) {
            throw new StorageEngineException("Error reading VariantSourceDBAdaptor", e);
        }
        if (iterator.hasNext()) {
            VariantSource variantSource = iterator.next();
            return new QueryResult<>("", ((int) stopWatch.getTime(TimeUnit.MILLISECONDS)), 1, 1, null, null,
                    Collections.singletonList(variantSource));
        } else {
            return new QueryResult<>("", ((int) stopWatch.getTime(TimeUnit.MILLISECONDS)), 0, 0, null, null, Collections.emptyList());
        }
    }

    Iterator<VariantSource> iterator(Query query, QueryOptions options) throws IOException;

//    QueryResult<String> getSamplesBySource(String fileId, QueryOptions options);

//    QueryResult<String> getSamplesBySources(List<String> fileIds, QueryOptions options);

    QueryResult updateSourceStats(VariantSourceStats variantSourceStats, StudyConfiguration studyConfiguration, QueryOptions queryOptions);

    void delete(int study, int file) throws IOException;

    void close() throws IOException;

}
