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

package org.opencb.opencga.app.cli.main.io;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.avro.generic.GenericRecord;
import org.opencb.biodata.models.variant.Genotype;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.opencga.app.cli.main.utils.CommandLineUtils;
import org.opencb.opencga.core.models.common.GenericRecordAvroJsonMixin;
import org.opencb.opencga.core.response.RestResponse;
import org.opencb.opencga.storage.core.variant.io.json.mixin.GenotypeJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.mixin.VariantStatsJsonMixin;

import java.io.IOException;

/**
 * Created by imedina on 01/12/16.
 */
public class YamlOutputWriter extends AbstractOutputWriter {

    private ObjectMapper objectMapper;

    public YamlOutputWriter() {
        this(new WriterConfiguration());
    }

    public YamlOutputWriter(WriterConfiguration writerConfiguration) {
        super(writerConfiguration);
        initObjectMapper();
    }

    private void initObjectMapper() {
        // Same options as in OpenCGAWSServer
        objectMapper = new ObjectMapper(new YAMLFactory());
        objectMapper.addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
        objectMapper.addMixIn(VariantStats.class, VariantStatsJsonMixin.class);
        objectMapper.addMixIn(Genotype.class, GenotypeJsonMixin.class);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
    }

    @Override
    public void print(RestResponse queryResponse) {
        print(queryResponse, true);
    }

    public void print(RestResponse queryResponse, boolean checkErrors) {
        if (checkErrors && checkErrors(queryResponse) && queryResponse.allResultsSize() == 0) {
            return;
        }

        ObjectWriter objectWriter = objectMapper.writer();
        Object toPrint = queryResponse;
        if (!writerConfiguration.isMetadata()) {
            toPrint = queryResponse.getResponses();
        }

        try {
            ps.println(objectWriter.writeValueAsString(toPrint));
        } catch (IOException e) {
            CommandLineUtils.error("Could not parse the queryResponse to print as "
                    + (writerConfiguration.isPretty() ? "a beautiful" : "") + " JSON", e);
        }
    }
}
