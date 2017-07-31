/*
 * Copyright 2015-2017 OpenCB
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

import org.bson.Document;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.opencga.catalog.models.Dataset;
/**
 * Created by pfurio on 04/05/16.
 */
public class DatasetConverter extends GenericDocumentComplexConverter<Dataset> {

    public DatasetConverter() {
        super(Dataset.class);
    }

    @Override
    public Document convertToStorageType(Dataset object) {
        Document document = super.convertToStorageType(object);
        document.put("id", document.getInteger("id").longValue());
        return document;
    }

}
