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
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.core.models.common.GenericRecordAvroJsonMixin;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.job.Job;

import java.util.*;

/**
 * Created by pfurio on 19/01/16.
 */
public class JobConverter extends OpenCgaMongoConverter<Job> {

    public JobConverter() {
        super(Job.class);
        getObjectMapper().addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
    }

    @Override
    public Document convertToStorageType(Job object) {
        Document document = super.convertToStorageType(object);
        document.put(JobDBAdaptor.QueryParams.UID.key(), object.getUid());
        document.put(JobDBAdaptor.QueryParams.STUDY_UID.key(), object.getStudyUid());
        document.put(JobDBAdaptor.QueryParams.OUT_DIR.key(), convertFileToDocument(object.getOutDir()));
//        document.put(JobDBAdaptor.QueryParams.TMP_DIR.key(), convertFileToDocument(object.getTmpDir()));
        document.put(JobDBAdaptor.QueryParams.INPUT.key(), convertFilesToDocument(object.getInput()));
        document.put(JobDBAdaptor.QueryParams.OUTPUT.key(), convertFilesToDocument(object.getOutput()));
        document.put(JobDBAdaptor.QueryParams.STDOUT.key(), convertFileToDocument(object.getStdout()));
        document.put(JobDBAdaptor.QueryParams.STDERR.key(), convertFileToDocument(object.getStderr()));
        document.put(JobDBAdaptor.QueryParams.DEPENDS_ON.key(), convertJobsToDocument(object.getDependsOn()));
        return document;
    }

    public Document convertFileToDocument(Object file) {
        if (file == null) {
            return new Document(JobDBAdaptor.QueryParams.UID.key(), -1L);
        }
        if (file instanceof File) {
            return new Document(JobDBAdaptor.QueryParams.UID.key(), ((File) file).getUid());
        } else if (file instanceof Map) {
            return new Document(JobDBAdaptor.QueryParams.UID.key(),
                    Long.valueOf(String.valueOf(((Map) file).get(JobDBAdaptor.QueryParams.UID.key()))));
        } else {
            return new Document(JobDBAdaptor.QueryParams.UID.key(), -1L);
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

    public Document convertJobToDocument(Object job) {
        if (job == null) {
            return new Document()
                    .append(JobDBAdaptor.QueryParams.UID.key(), -1L)
                    .append(JobDBAdaptor.QueryParams.STUDY_UID.key(), -1L);
        }
        if (job instanceof Job) {
            return new Document()
                    .append(JobDBAdaptor.QueryParams.UID.key(), ((Job) job).getUid())
                    .append(JobDBAdaptor.QueryParams.STUDY_UID.key(), ((Job) job).getStudyUid());
        } else if (job instanceof Map) {
            return new Document()
                    .append(JobDBAdaptor.QueryParams.UID.key(),
                            Long.valueOf(String.valueOf(((Map) job).get(JobDBAdaptor.QueryParams.UID.key()))))
                    .append(JobDBAdaptor.QueryParams.STUDY_UID.key(),
                            Long.valueOf(String.valueOf(((Map) job).get(JobDBAdaptor.QueryParams.STUDY_UID.key()))));
        } else {
            return new Document()
                    .append(JobDBAdaptor.QueryParams.UID.key(), -1L)
                    .append(JobDBAdaptor.QueryParams.STUDY_UID.key(), -1L);
        }
    }

    public List<Document> convertJobsToDocument(Object jobList) {
        if (!(jobList instanceof Collection)) {
            return Collections.emptyList();
        }
        List<Object> myList = (List<Object>) jobList;
        if (myList.isEmpty()) {
            return Collections.emptyList();
        }
        List<Document> jobs = new ArrayList(myList.size());
        for (Object job : myList) {
            jobs.add(convertJobToDocument(job));
        }
        return jobs;
    }
}
