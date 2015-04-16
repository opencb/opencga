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
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.core.runner.StringDataWriter;
import org.opencb.opencga.storage.core.variant.io.json.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by hpccoll1 on 26/02/15.
 */
class VariantJsonTransformTask extends Task<String> {

    final VariantFactory factory;
    final ObjectWriter objectWriter;
    private final VariantSource source;
    private final ObjectMapper jsonObjectMapper;
    private final Path outputFileJsonFile;

    public VariantJsonTransformTask(VariantFactory factory, VariantSource source, Path outputFileJsonFile) {
        this.factory = factory;
        this.source = source;
        this.outputFileJsonFile = outputFileJsonFile;

        JsonFactory jsonFactory = new JsonFactory();
        ObjectMapper jsonObjectMapper = new ObjectMapper(jsonFactory);
        jsonObjectMapper.addMixInAnnotations(VariantSourceEntry.class, VariantSourceEntryJsonMixin.class);
        jsonObjectMapper.addMixInAnnotations(Genotype.class, GenotypeJsonMixin.class);
        jsonObjectMapper.addMixInAnnotations(VariantStats.class, VariantStatsJsonMixin.class);
        jsonObjectMapper.addMixInAnnotations(VariantSource.class, VariantSourceJsonMixin.class);
        jsonObjectMapper.addMixInAnnotations(VariantAnnotation.class, VariantAnnotationMixin.class);

        this.jsonObjectMapper = jsonObjectMapper;
        this.objectWriter = jsonObjectMapper.writerWithType(Variant.class);
    }

    @Override
    public boolean pre() {
        return super.pre();
    }

    @Override
    public boolean post() {
        ObjectWriter variantSourceObjectWriter = jsonObjectMapper.writerWithType(VariantSource.class);
        try {
            String sourceJsonString = variantSourceObjectWriter.writeValueAsString(source);
            StringDataWriter.write(outputFileJsonFile, Collections.singletonList(sourceJsonString));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return false;
        }
        return super.post();
    }

    @Override
    public boolean apply(List<String> batch) {
        List<String> outputBatch = new ArrayList<>(batch.size());
//            logger.info("batch.size() = " + batch.size());
        try {
            for (String line : batch) {
                if (line.startsWith("#") || line.trim().isEmpty()) {
                    continue;
                }
                List<Variant> variants = factory.create(source, line);
                for (Variant variant : variants) {
                    try {
                        String e = objectWriter.writeValueAsString(variant);
                        outputBatch.add(e + "\n");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
//            logger.info("outputBatch.size() = " + outputBatch.size());
            batch.clear();
            batch.addAll(outputBatch);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}