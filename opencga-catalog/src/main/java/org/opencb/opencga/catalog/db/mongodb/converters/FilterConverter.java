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
import org.opencb.opencga.catalog.models.QueryFilter;

import java.io.IOException;
/**
 * Created by pfurio on 13/04/16.
 */
public class FilterConverter extends GenericConverter<QueryFilter, Document> {

    private ObjectWriter fileWriter;

    public FilterConverter() {
        objectReader = objectMapper.reader(QueryFilter.class);
        fileWriter = objectMapper.writerFor(QueryFilter.class);
    }

    @Override
    public QueryFilter convertToDataModelType(Document object) {
        QueryFilter queryFilter = null;
        try {
            Document filters = (Document) ((Document) object.get("configs")).get("opencga-filters");
            queryFilter = objectReader.readValue(objectWriter.writeValueAsString(filters));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return queryFilter;
    }

    @Override
    public Document convertToStorageType(QueryFilter object) {
        Document document = null;
        try {
            document = Document.parse(fileWriter.writeValueAsString(object));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return document;
    }
}
