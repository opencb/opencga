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

import com.fasterxml.jackson.databind.*;
import org.bson.Document;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by pfurio on 18/01/16.
 */
@Deprecated
public abstract class GenericConverter<M, D> implements ComplexTypeConverter<M, D> {

    protected final ObjectMapper objectMapper;
    protected final ObjectWriter objectWriter;
    protected ObjectReader objectReader;

    protected Logger logger;

    public GenericConverter() {
        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
        objectMapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        objectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);

        objectWriter = objectMapper.writerFor(Document.class);

        logger = LoggerFactory.getLogger(this.getClass());
    }

}
