/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.storage.core.variant;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFactory;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.opencb.biodata.models.variant.exceptions.NotAVariantException;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.core.runner.StringDataWriter;
import org.opencb.opencga.storage.core.variant.io.json.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Jacobo Coll <jacobo167@gmail.com>
 */
class VariantJsonTransformTask implements ParallelTaskRunner.Task<String, String> {

    final VariantFactory factory;
    final ObjectWriter objectWriter;
    private final VariantSource source;
    private final ObjectMapper jsonObjectMapper;
    private final Path outputFileJsonFile;
    protected static Logger logger = LoggerFactory.getLogger(VariantJsonTransformTask.class);
    private boolean includeSrc;

    public VariantJsonTransformTask(VariantFactory factory, VariantSource source, Path outputFileJsonFile) {
        this.factory = factory;
        this.source = source;
        this.outputFileJsonFile = outputFileJsonFile;

        JsonFactory jsonFactory = new JsonFactory();
        ObjectMapper jsonObjectMapper = new ObjectMapper(jsonFactory);
        jsonObjectMapper.addMixIn(VariantSourceEntry.class, VariantSourceEntryJsonMixin.class);
        jsonObjectMapper.addMixIn(Genotype.class, GenotypeJsonMixin.class);
        jsonObjectMapper.addMixIn(VariantStats.class, VariantStatsJsonMixin.class);
        jsonObjectMapper.addMixIn(VariantSource.class, VariantSourceJsonMixin.class);
        jsonObjectMapper.addMixIn(VariantAnnotation.class, VariantAnnotationMixin.class);

        this.jsonObjectMapper = jsonObjectMapper;
        this.objectWriter = jsonObjectMapper.writerFor(Variant.class);
    }

    public void setIncludeSrc(boolean includeSrc) {
        this.includeSrc = includeSrc;
    }

    @Override
    public void post() {
        ObjectWriter variantSourceObjectWriter = jsonObjectMapper.writerFor(VariantSource.class);
        try {
            String sourceJsonString = variantSourceObjectWriter.writeValueAsString(source);
            StringDataWriter.write(outputFileJsonFile, Collections.singletonList(sourceJsonString));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<String> apply(List<String> batch) {
        List<String> outputBatch = new ArrayList<>(batch.size());
//            logger.info("batch.size() = " + batch.size());
        try {
            for (String line : batch) {
                if (line.startsWith("#") || line.trim().isEmpty()) {
                    continue;
                }
                List<Variant> variants = null;
                try {
                    variants = factory.create(source, line);
                } catch (NotAVariantException e) {
                    variants = Collections.emptyList();
                } catch (Exception e) {
                    logger.error("Error parsing line: {}", line);
                    throw e;
                }
                for (Variant variant : variants) {
                    try {
                        if (!includeSrc) {
                            for (VariantSourceEntry variantSourceEntry : variant.getSourceEntries().values()) {
                                if (variantSourceEntry.getAttributes().containsKey("src")) {
                                    variantSourceEntry.getAttributes().remove("src");
                                }
                            }
                        }
                        String e = objectWriter.writeValueAsString(variant);
                        outputBatch.add(e + "\n");
                    } catch (IOException e) {
                        logger.error("Error parsing line: {}", line);
                        throw new IllegalStateException("Error parsing line: " + line , e);
                    } catch (Exception e) {
                        logger.error("Error parsing line: {}", line);
                        throw e;
                    }
                }
            }
//            logger.info("outputBatch.size() = " + outputBatch.size());
            batch.clear();
            batch.addAll(outputBatch);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return batch;
    }
}