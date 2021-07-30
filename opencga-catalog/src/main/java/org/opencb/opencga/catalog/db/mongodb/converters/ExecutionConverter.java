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

package org.opencb.opencga.catalog.db.mongodb.converters;

import org.apache.avro.generic.GenericRecord;
import org.bson.Document;
import org.opencb.opencga.catalog.db.api.ExecutionDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor;
import org.opencb.opencga.core.models.common.GenericRecordAvroJsonMixin;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.job.Execution;

import java.util.*;

/**
 * Created by pfurio on 30/07/21.
 */
public class ExecutionConverter extends OpenCgaMongoConverter<Execution> {

    public ExecutionConverter() {
        super(Execution.class);
        getObjectMapper().addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
    }

    @Override
    public Document convertToStorageType(Execution object) {
        Document document = super.convertToStorageType(object);
        document.put(MongoDBAdaptor.PRIVATE_UID, object.getUid());
        document.put(MongoDBAdaptor.PRIVATE_STUDY_UID, object.getStudyUid());
        document.put(ExecutionDBAdaptor.QueryParams.OUT_DIR.key(), convertFileToDocument(object.getOutDir()));
//        document.put(ExecutionDBAdaptor.QueryParams.INPUT.key(), convertFilesToDocument(object.getInput()));
//        document.put(ExecutionDBAdaptor.QueryParams.OUTPUT.key(), convertFilesToDocument(object.getOutput()));
        document.put(ExecutionDBAdaptor.QueryParams.STDOUT.key(), convertFileToDocument(object.getStdout()));
        document.put(ExecutionDBAdaptor.QueryParams.STDERR.key(), convertFileToDocument(object.getStderr()));
        document.put(ExecutionDBAdaptor.QueryParams.DEPENDS_ON.key(), convertExecutionsOrJobsToDocument(object.getDependsOn()));
        document.put(ExecutionDBAdaptor.QueryParams.JOBS.key(), convertExecutionsOrJobsToDocument(object.getJobs()));
        return document;
    }

    public Document convertFileToDocument(Object file) {
        if (file == null) {
            return new Document(MongoDBAdaptor.PRIVATE_UID, -1L);
        }
        if (file instanceof File) {
            return new Document(MongoDBAdaptor.PRIVATE_UID, ((File) file).getUid());
        } else if (file instanceof Map) {
            return new Document(MongoDBAdaptor.PRIVATE_UID,
                    Long.valueOf(String.valueOf(((Map) file).get(MongoDBAdaptor.PRIVATE_UID))));
        } else {
            return new Document(MongoDBAdaptor.PRIVATE_UID, -1L);
        }
    }

    public List<Document> convertFilesToDocument(Object fileList) {
        if (!(fileList instanceof Collection)) {
            return Collections.emptyList();
        }
        List<Object> myList = (List<Object>) fileList;
        if (myList.isEmpty()) {
            return Collections.emptyList();
        }
        List<Document> files = new ArrayList(myList.size());
        for (Object file : myList) {
            files.add(convertFileToDocument(file));
        }
        return files;
    }

    public Document convertExecutionOrJobToDocument(Object execution) {
        if (execution == null) {
            return new Document()
                    .append(MongoDBAdaptor.PRIVATE_UID, -1L)
                    .append(MongoDBAdaptor.PRIVATE_STUDY_UID, -1L);
        }
        if (execution instanceof Execution) {
            return new Document()
                    .append(MongoDBAdaptor.PRIVATE_UID, ((Execution) execution).getUid())
                    .append(MongoDBAdaptor.PRIVATE_STUDY_UID, ((Execution) execution).getStudyUid());
        } else if (execution instanceof Map) {
            return new Document()
                    .append(MongoDBAdaptor.PRIVATE_UID,
                            Long.valueOf(String.valueOf(((Map) execution).get(MongoDBAdaptor.PRIVATE_UID))))
                    .append(MongoDBAdaptor.PRIVATE_STUDY_UID,
                            Long.valueOf(String.valueOf(((Map) execution).get(MongoDBAdaptor.PRIVATE_STUDY_UID))));
        } else {
            return new Document()
                    .append(MongoDBAdaptor.PRIVATE_UID, -1L)
                    .append(MongoDBAdaptor.PRIVATE_STUDY_UID, -1L);
        }
    }

    public List<Document> convertExecutionsOrJobsToDocument(Object executionList) {
        if (!(executionList instanceof Collection)) {
            return Collections.emptyList();
        }
        List<Object> myList = (List<Object>) executionList;
        if (myList.isEmpty()) {
            return Collections.emptyList();
        }
        List<Document> executions = new ArrayList(myList.size());
        for (Object execution : myList) {
            executions.add(convertExecutionOrJobToDocument(execution));
        }
        return executions;
    }
}
