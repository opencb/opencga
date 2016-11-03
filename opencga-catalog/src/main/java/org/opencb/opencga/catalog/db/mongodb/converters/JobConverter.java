/*
 * Copyright 2015-2016 OpenCB
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.bson.Document;
import org.opencb.opencga.catalog.models.Job;

import java.io.IOException;

import static org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter.replaceDots;
import static org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter.restoreDots;

/**
 * Created by pfurio on 19/01/16.
 */
public class JobConverter extends GenericConverter<Job, Document> {

    private ObjectWriter jobWriter;

    public JobConverter() {
        objectReader = objectMapper.reader(Job.class);
        jobWriter = objectMapper.writerFor(Job.class);
    }

    @Override
    public Job convertToDataModelType(Document object) {
        Job job = null;
        try {
            object = restoreDots(object);
            job = objectReader.readValue(objectWriter.writeValueAsString(object));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return job;
    }

    @Override
    public Document convertToStorageType(Job object) {
        Document document = null;
        try {
            document = Document.parse(jobWriter.writeValueAsString(object));
            document.put("id", document.getInteger("id").longValue());
            document = replaceDots(document);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return document;
    }
}
