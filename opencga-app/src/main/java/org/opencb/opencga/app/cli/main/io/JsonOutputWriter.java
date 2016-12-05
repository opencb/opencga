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

package org.opencb.opencga.app.cli.main.io;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.avro.generic.GenericRecord;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.storage.core.variant.io.json.mixin.GenericRecordAvroJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.mixin.GenotypeJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.mixin.VariantSourceJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.mixin.VariantStatsJsonMixin;

import java.io.IOException;

/**
 * Created by pfurio on 28/07/16.
 */
public class JsonOutputWriter extends AbstractOutputWriter {

    private ObjectMapper objectMapper;

    public JsonOutputWriter() {
        super();
        initObjectMapper();
    }

    public JsonOutputWriter(WriterConfiguration writerConfiguration) {
        super(writerConfiguration);
        initObjectMapper();
    }

    private void initObjectMapper() {
        // Same options as in OpenCGAWSServer
        objectMapper = new ObjectMapper();
        objectMapper.addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
        objectMapper.addMixIn(VariantSource.class, VariantSourceJsonMixin.class);
        objectMapper.addMixIn(VariantStats.class, VariantStatsJsonMixin.class);
        objectMapper.addMixIn(Genotype.class, GenotypeJsonMixin.class);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
    }

    @Override
    public void print(QueryResponse queryResponse) {
        if (checkErrors(queryResponse)) {
            return;
        }

        ObjectWriter objectWriter;
        if (writerConfiguration.isPretty()) {
            objectWriter = objectMapper.writerWithDefaultPrettyPrinter();
        } else {
            objectWriter = objectMapper.writer();
        }
        Object toPrint = queryResponse;
        if (!writerConfiguration.isMetadata()) {
            toPrint = queryResponse.getResponse();
        }
        try {
            ps.println(objectWriter.writeValueAsString(toPrint));
        } catch (IOException e) {
            System.err.println(ANSI_RED + "ERROR: Could not parse the queryResponse to print as "
                            + (writerConfiguration.isPretty() ? "a beautiful" : "") + " JSON");
            System.err.println(e.getMessage() + ANSI_RESET);
        }
    }
}
