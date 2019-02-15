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

import com.google.common.collect.Iterators;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.stats.VariantSourceStats;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.metadata.models.Locked;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public interface FileMetadataDBAdaptor extends AutoCloseable {

    enum VariantFileMetadataQueryParam implements QueryParam {
        STUDY_ID("studyId", Type.INTEGER_ARRAY),
        FILE_ID("fileId", Type.INTEGER_ARRAY);

        private final String key;
        private final Type type;

        VariantFileMetadataQueryParam(String key, Type type) {
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


    FileMetadata getFileMetadata(int studyId, int fileId, Long timeStamp);

    Iterator<FileMetadata> fileIterator(int studyId);

    void updateFileMetadata(int studyId, FileMetadata file, Long timeStamp);

    Integer getFileId(int studyId, String fileName);

    LinkedHashSet<Integer> getIndexedFiles(int studyId);

    default void addIndexedFiles(int studyId, List<Integer> fileIds) {}

    default void removeIndexedFiles(int studyId, Collection<Integer> fileIds) {};

    default QueryResult<Long> count() {
        return count(new Query());
    }

    QueryResult<Long> count(Query query);

    default void updateVariantFileMetadata(Number studyId, VariantFileMetadata metadata) throws StorageEngineException {
        updateVariantFileMetadata(studyId.toString(), metadata);
    }

    void updateVariantFileMetadata(String studyId, VariantFileMetadata metadata) throws StorageEngineException;

    default QueryResult<VariantFileMetadata> getVariantFileMetadata(int studyId, int fileId, QueryOptions options)
            throws StorageEngineException {
        StopWatch stopWatch = StopWatch.createStarted();
        Iterator<VariantFileMetadata> iterator;
        try {
            Query query = new Query(VariantFileMetadataQueryParam.FILE_ID.key(), fileId)
                    .append(VariantFileMetadataQueryParam.STUDY_ID.key(), studyId);
            iterator = iterator(query, options);
        } catch (IOException e) {
            throw new StorageEngineException("Error reading from FileMetadataDBAdaptor", e);
        }
        VariantFileMetadata metadata = Iterators.getOnlyElement(iterator, null);
        if (metadata != null) {
            return new QueryResult<>("", ((int) stopWatch.getTime(TimeUnit.MILLISECONDS)), 1, 1, null, null,
                    Collections.singletonList(metadata));
        } else {
            return new QueryResult<>("", ((int) stopWatch.getTime(TimeUnit.MILLISECONDS)), 0, 0, null, null, Collections.emptyList());
        }
    }

    Iterator<VariantFileMetadata> iterator(Query query, QueryOptions options) throws IOException;

//    QueryResult<String> getSamplesBySource(String fileId, QueryOptions options);

//    QueryResult<String> getSamplesBySources(List<String> fileIds, QueryOptions options);

    QueryResult updateStats(VariantSourceStats variantSourceStats, StudyConfiguration studyConfiguration, QueryOptions queryOptions);

    void removeVariantFileMetadata(int study, int file) throws IOException;

    void close() throws IOException;

    Locked lock(int studyId, int id, long lockDuration, long timeout) throws StorageEngineException;
}
