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
import org.opencb.opencga.core.models.common.mixins.GenericRecordAvroJsonMixin;
import org.opencb.opencga.core.models.study.Variable;
import org.opencb.opencga.core.models.study.VariableMixin;
import org.opencb.opencga.core.models.study.VariableSet;

/**
 * Created by pfurio on 04/04/16.
 */
public class VariableSetConverter extends OpenCgaMongoConverter<VariableSet> {

    public VariableSetConverter() {
        super(VariableSet.class);
        getObjectMapper().addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
        getObjectMapper().addMixIn(Variable.class, VariableMixin.class);
    }

    @Override
    public Document convertToStorageType(VariableSet object) {
        Document document = super.convertToStorageType(object);
        document.put("uid", document.getInteger("uid").longValue());
        return document;
    }
}
