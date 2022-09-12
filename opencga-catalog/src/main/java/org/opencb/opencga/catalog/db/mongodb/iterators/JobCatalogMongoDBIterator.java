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
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.FileMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.JobMongoDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.managers.FileManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor.NATIVE_QUERY;

public class JobCatalogMongoDBIterator<T> extends BatchedCatalogMongoDBIterator<T> {

    private final long studyUid;
    private final String user;

    private final JobMongoDBAdaptor jobDBAdaptor;
    private final QueryOptions jobQueryOptions = new QueryOptions(QueryOptions.INCLUDE,
            Arrays.asList(JobDBAdaptor.QueryParams.ID.key(), JobDBAdaptor.QueryParams.UID.key(), JobDBAdaptor.QueryParams.UUID.key(),
                    JobDBAdaptor.QueryParams.CREATION_DATE.key(), JobDBAdaptor.QueryParams.STUDY_UID.key(),
                    JobDBAdaptor.QueryParams.INTERNAL_STATUS.key(), JobDBAdaptor.QueryParams.STUDY.key(),
                    JobDBAdaptor.QueryParams.TOOL.key(), JobDBAdaptor.QueryParams.PRIORITY.key()));
    private final FileMongoDBAdaptor fileDBAdaptor;
    private final QueryOptions fileQueryOptions = FileManager.INCLUDE_FILE_URI_PATH;

    private Logger logger = LoggerFactory.getLogger(JobCatalogMongoDBIterator.class);

    public JobCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, ClientSession clientSession,
                                     GenericDocumentComplexConverter<T> converter, JobMongoDBAdaptor jobDBAdaptor,
                                     FileMongoDBAdaptor fileDBAdaptor, QueryOptions options) {
        this(mongoCursor, clientSession, converter, jobDBAdaptor, fileDBAdaptor, options, 0, null);
    }

    public JobCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, ClientSession clientSession,
                                     GenericDocumentComplexConverter<T> converter, JobMongoDBAdaptor jobDBAdaptor,
                                     FileMongoDBAdaptor fileDBAdaptor, QueryOptions options, long studyUid, String user) {
        super(mongoCursor, clientSession, converter, null, options);
        this.fileDBAdaptor = fileDBAdaptor;
        this.jobDBAdaptor = jobDBAdaptor;
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

            if (!options.getBoolean(NATIVE_QUERY)) {
                getDocumentUids(fileUids, job, JobDBAdaptor.QueryParams.INPUT);
                getDocumentUids(fileUids, job, JobDBAdaptor.QueryParams.OUTPUT);
                getFile(fileUids, job, JobDBAdaptor.QueryParams.OUT_DIR);
                getFile(fileUids, job, JobDBAdaptor.QueryParams.STDOUT);
                getFile(fileUids, job, JobDBAdaptor.QueryParams.STDERR);
                getStudyUidUidMap(studyUidJobUidMap, job, JobDBAdaptor.QueryParams.DEPENDS_ON);
            }
        }

        if (!fileUids.isEmpty()) {
            Query query = new Query(FileDBAdaptor.QueryParams.UID.key(), new ArrayList<>(fileUids));
            List<Document> fileDocuments;
            try {
                if (user == null) {
                    fileDocuments = fileDBAdaptor.nativeGet(clientSession, query, fileQueryOptions).getResults();
                } else {
                    query.put(FileDBAdaptor.QueryParams.STUDY_UID.key(), this.studyUid);
                    fileDocuments = fileDBAdaptor.nativeGet(clientSession, this.studyUid, query, fileQueryOptions, user).getResults();
                }
            } catch (CatalogDBException | CatalogAuthorizationException | CatalogParameterException e) {
                logger.warn("Could not obtain the files associated to the jobs: {}", e.getMessage(), e);
                return;
            }

            // Map each fileId uid to the file entry
            Map<Long, Document> fileMap = fileDocuments
                    .stream()
                    .collect(Collectors.toMap(d -> ((Number) d.get(FileDBAdaptor.QueryParams.UID.key())).longValue(), d -> d));

            buffer.forEach(job -> {
                setDocuments(fileMap, job, JobDBAdaptor.QueryParams.INPUT);
                setDocuments(fileMap, job, JobDBAdaptor.QueryParams.OUTPUT);
                setFile(fileMap, job, JobDBAdaptor.QueryParams.OUT_DIR);
                setFile(fileMap, job, JobDBAdaptor.QueryParams.STDOUT);
                setFile(fileMap, job, JobDBAdaptor.QueryParams.STDERR);
            });
        }

        if (!studyUidJobUidMap.isEmpty()) {
            List<Document> jobDocuments = new LinkedList<>();
            for (Long studyUid : studyUidJobUidMap.keySet()) {
                Query query = new Query(JobDBAdaptor.QueryParams.UID.key(), new ArrayList<>(studyUidJobUidMap.get(studyUid)));
                try {
                    if (user == null) {
                        jobDocuments.addAll(jobDBAdaptor.nativeGet(clientSession, query, jobQueryOptions).getResults());
                    } else {
                        query.put(JobDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
                        jobDocuments.addAll(jobDBAdaptor.nativeGet(clientSession, studyUid, query, jobQueryOptions, user).getResults());
                    }
                } catch (CatalogDBException | CatalogAuthorizationException | CatalogParameterException e) {
                    logger.warn("Could not obtain the jobs the original jobs depend on: {}", e.getMessage(), e);
                    return;
                }
            }
            // Map each job uid to the job entry
            Map<Long, Document> jobMap = jobDocuments
                    .stream()
                    .collect(Collectors.toMap(d -> ((Number) d.get(JobDBAdaptor.QueryParams.UID.key())).longValue(), d -> d));

            buffer.forEach(job -> {
                setDocuments(jobMap, job, JobDBAdaptor.QueryParams.DEPENDS_ON);
            });
        }
    }

    private void getFile(Set<Long> fileUids, Document job, JobDBAdaptor.QueryParams param) {
        Object file = job.get(param.key());
        if (file != null) {
            Number fileUid = ((Number) ((Document) file).get(FileDBAdaptor.QueryParams.UID.key()));
            if (fileUid != null && fileUid.longValue() > 0) {
                fileUids.add(fileUid.longValue());
            }
        }
    }

    private void setFile(Map<Long, Document> fileMap, Document job, JobDBAdaptor.QueryParams param) {
        Object file = job.get(param.key());
        if (file != null) {
            Number fileUid = ((Number) ((Document) file).get(FileDBAdaptor.QueryParams.UID.key()));
            if (fileUid != null && fileUid.longValue() > 0) {
                job.put(param.key(), fileMap.get(fileUid.longValue()));
            }
        }
    }

    private void getDocumentUids(Set<Long> fileUids, Document job, JobDBAdaptor.QueryParams param) {
        Object files = job.get(param.key());
        if (files != null) {
            for (Object file : ((Collection) files)) {
                Number fileUid = ((Number) ((Document) file).get(JobDBAdaptor.QueryParams.UID.key()));
                if (fileUid != null && fileUid.longValue() > 0) {
                    fileUids.add(fileUid.longValue());
                }
            }
        }
    }

    private void getStudyUidUidMap(Map<Long, List<Long>> jobStudyMap, Document job, JobDBAdaptor.QueryParams param) {
        Object files = job.get(param.key());
        if (files != null) {
            for (Object file : ((Collection) files)) {
                Number fileUid = ((Number) ((Document) file).get(JobDBAdaptor.QueryParams.UID.key()));
                Number studyUid = ((Number) ((Document) file).get(JobDBAdaptor.QueryParams.STUDY_UID.key()));
                if (fileUid != null && fileUid.longValue() > 0 && studyUid != null && studyUid.longValue() > 0) {
                    if (!jobStudyMap.containsKey(studyUid.longValue())) {
                        jobStudyMap.put(studyUid.longValue(), new LinkedList<>());
                    }
                    jobStudyMap.get(studyUid.longValue()).add(fileUid.longValue());
                }
            }
        }
    }

    private void setDocuments(Map<Long, Document> fileMap, Document job, JobDBAdaptor.QueryParams param) {
        Object files = job.get(param.key());
        if (files != null) {
            List<Document> updatedfiles = new ArrayList<>(((Collection) files).size());
            for (Object file : ((Collection) files)) {
                Number fileUid = ((Number) ((Document) file).get(JobDBAdaptor.QueryParams.UID.key()));
                if (fileUid != null && fileUid.longValue() > 0 && fileMap.containsKey(fileUid.longValue())) {
                    updatedfiles.add(fileMap.get(fileUid.longValue()));
                }
            }
            job.put(param.key(), updatedfiles);
        }
    }
}
