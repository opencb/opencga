/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.catalog.db.mongodb.iterators;

import com.mongodb.client.ClientSession;
import org.bson.Document;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.ExecutionDBAdaptor;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.ExecutionMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.FileMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.JobMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.ExecutionConverter;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.core.models.job.Execution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ExecutionCatalogMongoDBIterator extends BatchedCatalogMongoDBIterator<Execution> {

    private final long studyUid;
    private final String user;

    private final ExecutionMongoDBAdaptor executionDBAdaptor;
    private final JobMongoDBAdaptor jobDBAdaptor;
    private final QueryOptions executionQueryOptions = new QueryOptions(QueryOptions.INCLUDE,
            Arrays.asList(ExecutionDBAdaptor.QueryParams.ID.key(), ExecutionDBAdaptor.QueryParams.UID.key(),
                    ExecutionDBAdaptor.QueryParams.UUID.key(), ExecutionDBAdaptor.QueryParams.CREATION_DATE.key(),
                    ExecutionDBAdaptor.QueryParams.STUDY_UID.key(), ExecutionDBAdaptor.QueryParams.INTERNAL_STATUS.key(),
//                    ExecutionDBAdaptor.QueryParams.STUDY.key(), ExecutionDBAdaptor.QueryParams.TOOL.key(),
                    ExecutionDBAdaptor.QueryParams.PRIORITY.key()));
    private final FileMongoDBAdaptor fileDBAdaptor;
    private final QueryOptions fileQueryOptions = FileManager.INCLUDE_FILE_URI_PATH;

    private Logger logger = LoggerFactory.getLogger(ExecutionCatalogMongoDBIterator.class);

    public ExecutionCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, ClientSession clientSession, ExecutionConverter converter,
                                           ExecutionMongoDBAdaptor executionDBAdaptor, JobMongoDBAdaptor jobDBAdaptor,
                                           FileMongoDBAdaptor fileDBAdaptor, QueryOptions options) {
        this(mongoCursor, clientSession, converter, executionDBAdaptor, jobDBAdaptor, fileDBAdaptor, options, 0, null);
    }

    public ExecutionCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, ClientSession clientSession, ExecutionConverter converter,
                                           ExecutionMongoDBAdaptor executionDBAdaptor, JobMongoDBAdaptor jobDBAdaptor,
                                           FileMongoDBAdaptor fileDBAdaptor, QueryOptions options, long studyUid, String user) {
        super(mongoCursor, clientSession, converter, null, options);
        this.executionDBAdaptor = executionDBAdaptor;
        this.jobDBAdaptor = jobDBAdaptor;
        this.fileDBAdaptor = fileDBAdaptor;

        this.user = user;
        this.studyUid = studyUid;
    }

    @Override
    protected void fetchNextBatch(Queue<Document> buffer, int bufferSize) {
        Set<Long> fileUids = new HashSet<>();
        Map<Long, List<Long>> studyUidJobUidMap = new HashMap<>();  // Map studyUid - jobUid
        while (mongoCursor.hasNext() && buffer.size() < bufferSize) {
            Document job = mongoCursor.next();
            buffer.add(job);

//            if (!options.getBoolean(NATIVE_QUERY)) {
//                getDocumentUids(fileUids, job, ExecutionDBAdaptor.QueryParams.INPUT);
//                getDocumentUids(fileUids, job, ExecutionDBAdaptor.QueryParams.OUTPUT);
//                getFile(fileUids, job, ExecutionDBAdaptor.QueryParams.OUT_DIR);
//                getFile(fileUids, job, ExecutionDBAdaptor.QueryParams.STDOUT);
//                getFile(fileUids, job, ExecutionDBAdaptor.QueryParams.STDERR);
//                getStudyUidUidMap(studyUidJobUidMap, job, ExecutionDBAdaptor.QueryParams.DEPENDS_ON);
//            }
        }

//        if (!fileUids.isEmpty()) {
//            Query query = new Query(FileDBAdaptor.QueryParams.UID.key(), new ArrayList<>(fileUids));
//            List<Document> fileDocuments;
//            try {
//                if (user == null) {
//                    fileDocuments = fileDBAdaptor.nativeGet(query, fileQueryOptions).getResults();
//                } else {
//                    query.put(FileDBAdaptor.QueryParams.STUDY_UID.key(), this.studyUid);
//                    fileDocuments = fileDBAdaptor.nativeGet(this.studyUid, query, fileQueryOptions, user).getResults();
//                }
//            } catch (CatalogDBException | CatalogAuthorizationException | CatalogParameterException e) {
//                logger.warn("Could not obtain the files associated to the jobs: {}", e.getMessage(), e);
//                return;
//            }
//
//            // Map each fileId uid to the file entry
//            Map<Long, Document> fileMap = fileDocuments
//                    .stream()
//                    .collect(Collectors.toMap(d -> ((Number) d.get(FileDBAdaptor.QueryParams.UID.key())).longValue(), d -> d));
//
//            buffer.forEach(job -> {
//                setDocuments(fileMap, job, ExecutionDBAdaptor.QueryParams.INPUT);
//                setDocuments(fileMap, job, ExecutionDBAdaptor.QueryParams.OUTPUT);
//                setFile(fileMap, job, ExecutionDBAdaptor.QueryParams.OUT_DIR);
//                setFile(fileMap, job, ExecutionDBAdaptor.QueryParams.STDOUT);
//                setFile(fileMap, job, ExecutionDBAdaptor.QueryParams.STDERR);
//            });
//        }
//
//        if (!studyUidJobUidMap.isEmpty()) {
//            List<Document> jobDocuments = new LinkedList<>();
//            for (Long studyUid : studyUidJobUidMap.keySet()) {
//                Query query = new Query(ExecutionDBAdaptor.QueryParams.UID.key(), new ArrayList<>(studyUidJobUidMap.get(studyUid)));
//                try {
//                    if (user == null) {
//                        jobDocuments.addAll(ExecutionDBAdaptor.nativeGet(query, executionQueryOptions).getResults());
//                    } else {
//                        query.put(ExecutionDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
//                        jobDocuments.addAll(ExecutionDBAdaptor.nativeGet(studyUid, query, executionQueryOptions, user).getResults());
//                    }
//                } catch (CatalogDBException | CatalogAuthorizationException | CatalogParameterException e) {
//                    logger.warn("Could not obtain the jobs the original jobs depend on: {}", e.getMessage(), e);
//                    return;
//                }
//            }
//            // Map each job uid to the job entry
//            Map<Long, Document> jobMap = jobDocuments
//                    .stream()
//                    .collect(Collectors.toMap(d -> ((Number) d.get(ExecutionDBAdaptor.QueryParams.UID.key())).longValue(), d -> d));
//
//            buffer.forEach(job -> {
//                setDocuments(jobMap, job, ExecutionDBAdaptor.QueryParams.DEPENDS_ON);
//            });
//        }
    }

    private void getFile(Set<Long> fileUids, Document job, ExecutionDBAdaptor.QueryParams param) {
        Object file = job.get(param.key());
        if (file != null) {
            Number fileUid = ((Number) ((Document) file).get(FileDBAdaptor.QueryParams.UID.key()));
            if (fileUid != null && fileUid.longValue() > 0) {
                fileUids.add(fileUid.longValue());
            }
        }
    }

    private void setFile(Map<Long, Document> fileMap, Document job, ExecutionDBAdaptor.QueryParams param) {
        Object file = job.get(param.key());
        if (file != null) {
            Number fileUid = ((Number) ((Document) file).get(FileDBAdaptor.QueryParams.UID.key()));
            if (fileUid != null && fileUid.longValue() > 0) {
                job.put(param.key(), fileMap.get(fileUid.longValue()));
            }
        }
    }

    private void getDocumentUids(Set<Long> fileUids, Document job, ExecutionDBAdaptor.QueryParams param) {
        Object files = job.get(param.key());
        if (files != null) {
            for (Object file : ((Collection) files)) {
                Number fileUid = ((Number) ((Document) file).get(ExecutionDBAdaptor.QueryParams.UID.key()));
                if (fileUid != null && fileUid.longValue() > 0) {
                    fileUids.add(fileUid.longValue());
                }
            }
        }
    }

    private void getStudyUidUidMap(Map<Long, List<Long>> jobStudyMap, Document job, ExecutionDBAdaptor.QueryParams param) {
        Object files = job.get(param.key());
        if (files != null) {
            for (Object file : ((Collection) files)) {
                Number fileUid = ((Number) ((Document) file).get(ExecutionDBAdaptor.QueryParams.UID.key()));
                Number studyUid = ((Number) ((Document) file).get(ExecutionDBAdaptor.QueryParams.STUDY_UID.key()));
                if (fileUid != null && fileUid.longValue() > 0 && studyUid != null && studyUid.longValue() > 0) {
                    if (!jobStudyMap.containsKey(studyUid.longValue())) {
                        jobStudyMap.put(studyUid.longValue(), new LinkedList<>());
                    }
                    jobStudyMap.get(studyUid.longValue()).add(fileUid.longValue());
                }
            }
        }
    }

    private void setDocuments(Map<Long, Document> fileMap, Document job, ExecutionDBAdaptor.QueryParams param) {
        Object files = job.get(param.key());
        if (files != null) {
            List<Document> updatedfiles = new ArrayList<>(((Collection) files).size());
            for (Object file : ((Collection) files)) {
                Number fileUid = ((Number) ((Document) file).get(ExecutionDBAdaptor.QueryParams.UID.key()));
                if (fileUid != null && fileUid.longValue() > 0) {
                    updatedfiles.add(fileMap.get(fileUid.longValue()));
                }
            }
            job.put(param.key(), updatedfiles);
        }
    }
}
