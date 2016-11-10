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
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.storage.core.alignment.json.AlignmentDifferenceJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.GenericRecordAvroJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.GenotypeJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.VariantSourceJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.VariantStatsJsonMixin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;

/**
 * Created by pfurio on 28/07/16.
 */
public class JsonWriter implements IWriter {

    protected Logger logger = LoggerFactory.getLogger(JsonWriter.class);
    private final ObjectMapper objectMapper;

    public JsonWriter() {
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
    public void print(QueryResponse queryResponse, boolean beauty) {
        if (!checkErrors(queryResponse)) {
            generalPrint(queryResponse, beauty, System.out);
        }
    }

    @Override
    public void writeToFile(QueryResponse queryResponse, Path filePath, boolean beauty) {
        if (checkErrors(queryResponse)) {
            return;
        }

        try (FileOutputStream fos = new FileOutputStream(filePath.toFile())) {
            PrintStream ps = new PrintStream(fos);
            generalPrint(queryResponse, beauty, ps);
//            System.setOut(ps);
        } catch (IOException e) {
            // TODO: Throw exception?
            e.printStackTrace();
        }
    }

    /**
     * Print errors or warnings and return true if any error was found.
     *
     * @param queryResponse queryResponse object
     * @return true if the query gave an error.
     */
    private boolean checkErrors(QueryResponse queryResponse) {
        boolean errors = false;
        if (StringUtils.isNotEmpty(queryResponse.getError())) {
            logger.error(queryResponse.getError());
            errors = true;
        }

        // Print warnings
        if (StringUtils.isNotEmpty(queryResponse.getWarning())) {
            logger.warn(queryResponse.getWarning());
        }

        return errors;
    }

    private void generalPrint(QueryResponse queryResponse, boolean beauty, PrintStream out) {
        ObjectWriter objectWriter;
        if (beauty) {
            objectWriter = objectMapper.writerWithDefaultPrettyPrinter();
        } else {
            objectWriter = objectMapper.writer();
        }
        try {
            out.println(objectWriter.writeValueAsString(queryResponse.getResponse()));
        } catch (IOException e) {
            logger.error("Error parsing the queryResponse to print as " + (beauty ? "a beautiful" : "") + " JSON", e);
        }
    }
}
