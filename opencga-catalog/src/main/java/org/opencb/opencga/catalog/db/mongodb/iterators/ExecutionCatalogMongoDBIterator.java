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
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.ExecutionDBAdaptor;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.ExecutionMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.FileMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.JobMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.ExecutionConverter;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.core.models.job.Execution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor.NATIVE_QUERY;

public class ExecutionCatalogMongoDBIterator extends BatchedCatalogMongoDBIterator<Execution> {

    private final long studyUid;
    private final String user;

    private final ExecutionMongoDBAdaptor executionDBAdaptor;
    private final JobMongoDBAdaptor jobDBAdaptor;
    private final FileMongoDBAdaptor fileDBAdaptor;

    private final QueryOptions executionQueryOptions;
    private final QueryOptions fileQueryOptions;
    private final QueryOptions jobQueryOptions;

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

        this.executionQueryOptions = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(ExecutionDBAdaptor.QueryParams.ID.key(), ExecutionDBAdaptor.QueryParams.UID.key(),
                        ExecutionDBAdaptor.QueryParams.UUID.key(), ExecutionDBAdaptor.QueryParams.CREATION_DATE.key(),
                        ExecutionDBAdaptor.QueryParams.MODIFICATION_DATE.key(), ExecutionDBAdaptor.QueryParams.STUDY_UID.key(),
                        ExecutionDBAdaptor.QueryParams.INTERNAL.key(), ExecutionDBAdaptor.QueryParams.PRIORITY.key()))
                .append(NATIVE_QUERY, true);
        this.fileQueryOptions = FileManager.INCLUDE_FILE_URI_PATH;
        this.jobQueryOptions = createJobQueryOptions();

        this.user = user;
        this.studyUid = studyUid;
    }

    @Override
    protected void fetchNextBatch(Queue<Document> buffer, int bufferSize) {
        Set<Long> fileUids = new HashSet<>();
        Set<Long> jobUids = new HashSet<>();
        Map<Long, List<Long>> studyUidExecutionUidMap = new HashMap<>();  // Map studyUid - executionUid
        while (mongoCursor.hasNext() && buffer.size() < bufferSize) {
            Document job = mongoCursor.next();
            buffer.add(job);

            if (!options.getBoolean(NATIVE_QUERY)) {
                getDocumentUids(jobUids, job, ExecutionDBAdaptor.QueryParams.JOBS);
                getDocumentUids(fileUids, job, ExecutionDBAdaptor.QueryParams.INPUT);
                getDocumentUids(fileUids, job, ExecutionDBAdaptor.QueryParams.OUTPUT);
                getFile(fileUids, job, ExecutionDBAdaptor.QueryParams.OUT_DIR);
                getFile(fileUids, job, ExecutionDBAdaptor.QueryParams.STDOUT);
                getFile(fileUids, job, ExecutionDBAdaptor.QueryParams.STDERR);
                getStudyUidUidMap(studyUidExecutionUidMap, job, ExecutionDBAdaptor.QueryParams.DEPENDS_ON);
            }
        }

        // Map each job uid to the job entry
        Map<Long, Document> jobMap = null;
        if (!jobUids.isEmpty()) {
            Query query = new Query(JobDBAdaptor.QueryParams.UID.key(), new ArrayList<>(jobUids));
            List<Document> jobDocuments;
            try {
                if (user == null) {
                    jobDocuments = jobDBAdaptor.nativeGet(query, jobQueryOptions).getResults();
                } else {
                    query.put(JobDBAdaptor.QueryParams.STUDY_UID.key(), this.studyUid);
                    jobDocuments = jobDBAdaptor.nativeGet(this.studyUid, query, jobQueryOptions, user).getResults();
                }
            } catch (CatalogDBException | CatalogAuthorizationException | CatalogParameterException e) {
                logger.warn("Could not obtain the jobs associated to the executions: {}", e.getMessage(), e);
                return;
            }

            jobMap = jobDocuments
                    .stream()
                    .collect(Collectors.toMap(j -> ((Number) j.get(JobDBAdaptor.QueryParams.UID.key())).longValue(), j -> j));
        }

        // Map each fileId uid to the file entry
        Map<Long, Document> fileMap = null;
        if (!fileUids.isEmpty()) {
            Query query = new Query(FileDBAdaptor.QueryParams.UID.key(), new ArrayList<>(fileUids));
            List<Document> fileDocuments;
            try {
                if (user == null) {
                    fileDocuments = fileDBAdaptor.nativeGet(query, fileQueryOptions).getResults();
                } else {
                    query.put(FileDBAdaptor.QueryParams.STUDY_UID.key(), this.studyUid);
                    fileDocuments = fileDBAdaptor.nativeGet(this.studyUid, query, fileQueryOptions, user).getResults();
                }
            } catch (CatalogDBException | CatalogAuthorizationException | CatalogParameterException e) {
                logger.warn("Could not obtain the files associated to the jobs: {}", e.getMessage(), e);
                return;
            }

            fileMap = fileDocuments
                    .stream()
                    .collect(Collectors.toMap(d -> ((Number) d.get(FileDBAdaptor.QueryParams.UID.key())).longValue(), d -> d));
        }

        // Map each execution uid to the execution entry
        Map<Long, Document> executionMap = null;
        if (!studyUidExecutionUidMap.isEmpty()) {
            List<Document> executionDocuments = new LinkedList<>();
            for (Long studyUid : studyUidExecutionUidMap.keySet()) {
                Query query = new Query(ExecutionDBAdaptor.QueryParams.UID.key(), new ArrayList<>(studyUidExecutionUidMap.get(studyUid)));
                try {
                    if (user == null) {
                        executionDocuments.addAll(executionDBAdaptor.nativeGet(query, executionQueryOptions).getResults());
                    } else {
                        query.put(ExecutionDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
                        executionDocuments.addAll(executionDBAdaptor.nativeGet(studyUid, query, executionQueryOptions, user).getResults());
                    }
                } catch (CatalogDBException | CatalogAuthorizationException | CatalogParameterException e) {
                    logger.warn("Could not obtain the executions the original executions depend on: {}", e.getMessage(), e);
                    return;
                }
            }
            // Map each execution uid to the job entry
            executionMap = executionDocuments
                    .stream()
                    .collect(Collectors.toMap(d -> ((Number) d.get(ExecutionDBAdaptor.QueryParams.UID.key())).longValue(), d -> d));
        }

        if (jobMap != null || executionMap != null || fileMap != null) {
            for (Document execution : buffer) {
                if (jobMap != null) {
                    setDocuments(jobMap, execution, ExecutionDBAdaptor.QueryParams.JOBS);
                }
                if (fileMap != null) {
                    setDocuments(fileMap, execution, ExecutionDBAdaptor.QueryParams.INPUT);
                    setDocuments(fileMap, execution, ExecutionDBAdaptor.QueryParams.OUTPUT);
                    setFile(fileMap, execution, ExecutionDBAdaptor.QueryParams.OUT_DIR);
                    setFile(fileMap, execution, ExecutionDBAdaptor.QueryParams.STDOUT);
                    setFile(fileMap, execution, ExecutionDBAdaptor.QueryParams.STDERR);
                }
                if (executionMap != null) {
                    setDocuments(executionMap, execution, ExecutionDBAdaptor.QueryParams.DEPENDS_ON);
                }
            }
        }
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

    private void getDocumentUids(Set<Long> fileUids, Document execution, ExecutionDBAdaptor.QueryParams param) {
        Object files = execution.get(param.key());
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

    private void setDocuments(Map<Long, Document> documentMap, Document job, ExecutionDBAdaptor.QueryParams param) {
        Object files = job.get(param.key());
        if (files != null) {
            List<Document> updatedfiles = new ArrayList<>(((Collection) files).size());
            for (Object file : ((Collection) files)) {
                Number fileUid = ((Number) ((Document) file).get(ExecutionDBAdaptor.QueryParams.UID.key()));
                if (fileUid != null && fileUid.longValue() > 0) {
                    updatedfiles.add(documentMap.get(fileUid.longValue()));
                }
            }
            job.put(param.key(), updatedfiles);
        }
    }

    private QueryOptions createJobQueryOptions() {
//        QueryOptions queryOptions = new QueryOptions(NATIVE_QUERY, true);
        QueryOptions queryOptions = new QueryOptions();

        if (options.containsKey(QueryOptions.INCLUDE)) {
            List<String> currentIncludeList = options.getAsStringList(QueryOptions.INCLUDE);
            List<String> includeList = new ArrayList<>();
            for (String include : currentIncludeList) {
                if (include.startsWith(ExecutionDBAdaptor.QueryParams.JOBS.key() + ".")) {
                    includeList.add(include.replace(ExecutionDBAdaptor.QueryParams.JOBS.key() + ".", ""));
                }
            }
            if (!includeList.isEmpty()) {
                // If we only have include uid there is no need for an additional query so we will set current options to native query
                boolean includeAdditionalFields = includeList.stream().anyMatch(
                        field -> !field.equals(JobDBAdaptor.QueryParams.UID.key())
                );
                if (includeAdditionalFields) {
                    includeList.add(JobDBAdaptor.QueryParams.UID.key());
                    queryOptions.put(QueryOptions.INCLUDE, includeList);
                } else {
                    // User wants to include fields already retrieved
                    options.put(NATIVE_QUERY, true);
                }
            }
        }
        if (options.containsKey(QueryOptions.EXCLUDE)) {
            List<String> currentExcludeList = options.getAsStringList(QueryOptions.EXCLUDE);
            List<String> excludeList = new ArrayList<>();
            for (String exclude : currentExcludeList) {
                if (exclude.startsWith(ExecutionDBAdaptor.QueryParams.JOBS.key() + ".")) {
                    String replace = exclude.replace(ExecutionDBAdaptor.QueryParams.JOBS.key() + ".", "");
                    if (!JobDBAdaptor.QueryParams.UID.key().equals(replace)) {
                        excludeList.add(replace);
                    }
                }
            }
            if (!excludeList.isEmpty()) {
                queryOptions.put(QueryOptions.EXCLUDE, excludeList);
            }
        }

        return queryOptions;
    }
}
