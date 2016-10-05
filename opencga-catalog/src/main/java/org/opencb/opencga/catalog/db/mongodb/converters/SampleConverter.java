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
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return document;
    }
}
