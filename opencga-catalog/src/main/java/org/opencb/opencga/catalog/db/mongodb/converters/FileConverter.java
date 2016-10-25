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
import org.opencb.opencga.catalog.models.File;

import java.io.IOException;
import java.util.List;

/**
 * Created by pfurio on 19/01/16.
 */
public class FileConverter extends GenericConverter<File, Document> {

    private ObjectWriter fileWriter;

    public FileConverter() {
        objectReader = objectMapper.reader(File.class);
        fileWriter = objectMapper.writerFor(File.class);
    }

    @Override
    public File convertToDataModelType(Document document) {
        File file = null;
        try {
            if (document.get("job") != null && document.get("job") instanceof List) {
                document.put("job", ((List) document.get("job")).get(0));
            }
            if (document.get("experiment") != null && document.get("experiment") instanceof List) {
                document.put("experiment", ((List) document.get("experiment")).get(0));
            }
            file = objectReader.readValue(objectWriter.writeValueAsString(document));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return file;
    }

    @Override
    public Document convertToStorageType(File file) {
        Document document = null;
        try {
            document = Document.parse(fileWriter.writeValueAsString(file));
            document.put("id", document.getInteger("id").longValue());

            long jobId = file.getJob() != null ? (file.getJob().getId() == 0 ? -1L : file.getJob().getId()) : -1L;
            document.put("job", new Document("id", jobId));

            long experimentId = file.getExperiment() != null
                    ? (file.getExperiment().getId() == 0 ? -1L : file.getExperiment().getId())
                    : -1L;
            document.put("experiment", new Document("id", experimentId));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return document;
    }
}
