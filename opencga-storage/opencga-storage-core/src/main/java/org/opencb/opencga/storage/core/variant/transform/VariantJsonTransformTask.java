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

package org.opencb.opencga.storage.core.variant.transform;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.avro.generic.GenericRecord;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.*;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.exceptions.NotAVariantException;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.biodata.tools.variant.stats.VariantGlobalStatsCalculator;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.opencga.storage.core.runner.StringDataWriter;
import org.opencb.opencga.storage.core.variant.io.json.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Jacobo Coll <jacobo167@gmail.com>
 */
public class VariantJsonTransformTask implements ParallelTaskRunner.Task<String, String> {

    private final VariantFactory factory;
    private final ObjectWriter objectWriter;
    private final VariantSource source;
    private final ObjectMapper jsonObjectMapper;
    private final Path outputFileJsonFile;
    protected static Logger logger = LoggerFactory.getLogger(VariantJsonTransformTask.class);
    @Deprecated
    private boolean includeSrc = false;
    private final VariantGlobalStatsCalculator variantStatsTask;

    public VariantJsonTransformTask(VariantFactory factory, VariantSource source, Path outputFileJsonFile) {
        this.factory = factory;
        this.source = source;
        this.outputFileJsonFile = outputFileJsonFile;

        JsonFactory jsonFactory = new JsonFactory();
        ObjectMapper jsonObjectMapper = new ObjectMapper(jsonFactory);
        jsonObjectMapper.addMixIn(StudyEntry.class, VariantSourceEntryJsonMixin.class);
        jsonObjectMapper.addMixIn(Genotype.class, GenotypeJsonMixin.class);
        jsonObjectMapper.addMixIn(VariantStats.class, VariantStatsJsonMixin.class);
        jsonObjectMapper.addMixIn(VariantSource.class, VariantSourceJsonMixin.class);
        jsonObjectMapper.addMixIn(VariantAnnotation.class, VariantAnnotationMixin.class);
        jsonObjectMapper.addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);

        this.jsonObjectMapper = jsonObjectMapper;
        this.objectWriter = jsonObjectMapper.writerFor(Variant.class);
        variantStatsTask = new VariantGlobalStatsCalculator(source);
    }

    public void setIncludeSrc(boolean includeSrc) {
        this.includeSrc = includeSrc;
    }

    @Override
    public void pre() {
        variantStatsTask.pre();
    }

    @Override
    public void post() {
        variantStatsTask.post();
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
                        for (StudyEntry studyEntry : variant.getStudies()) {
                            for (FileEntry fileEntry : studyEntry.getFiles()) {
                                if (fileEntry.getAttributes().containsKey(VariantVcfFactory.SRC)) {
                                    fileEntry.getAttributes().remove(VariantVcfFactory.SRC);
                                }
                            }
                        }
                    }
                    String e = variant.toJson();
                    outputBatch.add(e);
                    outputBatch.add("\n");
                } catch (Exception e) {
                    logger.error("Error parsing line: {}", line);
                    throw e;
                }
            }

            variantStatsTask.apply(variants);

        }
//            logger.info("outputBatch.size() = " + outputBatch.size());
        batch.clear();
        batch.addAll(outputBatch);
        return batch;
    }

}
