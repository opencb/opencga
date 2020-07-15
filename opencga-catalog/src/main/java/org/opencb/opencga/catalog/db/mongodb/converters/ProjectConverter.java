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
import org.opencb.opencga.core.models.common.GenericRecordAvroJsonMixin;
import org.opencb.opencga.core.models.project.Project;

/**
 * Created by pfurio on 18/01/16.
 */
public class ProjectConverter extends OpenCgaMongoConverter<Project> {

    public ProjectConverter() {
        super(Project.class);
        getObjectMapper().addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
    }

    @Override
    public Project convertToDataModelType(Document object) {
        Document projects = (Document) object.get("projects");
        return super.convertToDataModelType(projects);
    }

    @Override
    public Document convertToStorageType(Project object) {
        Document document = super.convertToStorageType(object);
        document.put("uid", document.getInteger("uid").longValue());
        return document;
    }
}
