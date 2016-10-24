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
import org.opencb.opencga.catalog.models.Sample;

import java.io.IOException;
import java.util.List;

/**
 * Created by pfurio on 19/01/16.
 */
public class SampleConverter extends GenericConverter<Sample, Document> {

    private ObjectWriter sampleWriter;

    public SampleConverter() {
        objectReader = objectMapper.reader(Sample.class);
        sampleWriter = objectMapper.writerFor(Sample.class);
    }

    @Override
    public Sample convertToDataModelType(Document object) {
        Sample sample = null;
        try {
            if (object.get("individual") instanceof List) {
                object.put("individual", ((List) object.get("individual")).get(0));
            }
            sample = objectReader.readValue(objectWriter.writeValueAsString(object));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sample;
    }

    @Override
    public Document convertToStorageType(Sample object) {
        Document document = null;
        try {
            document = Document.parse(sampleWriter.writeValueAsString(object));
            document.put("id", document.getInteger("id").longValue());
            long individualId = object.getIndividual() != null
                    ? (object.getIndividual().getId() == 0 ? -1L : object.getIndividual().getId()) : -1L;
            document.put("individual", new Document("id", individualId));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return document;
    }
}
