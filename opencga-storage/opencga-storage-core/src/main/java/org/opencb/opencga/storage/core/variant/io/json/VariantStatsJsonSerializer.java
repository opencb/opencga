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

package org.opencb.opencga.storage.core.variant.io.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.opencb.biodata.models.variant.stats.VariantStats;

import java.io.IOException;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class VariantStatsJsonSerializer extends JsonSerializer<VariantStats> {

    @Override
    public void serialize(VariantStats value, JsonGenerator generator, SerializerProvider provider) throws IOException {
        generator.writeStartObject();
        generator.writeStringField("refAllele", value.getRefAllele());
        generator.writeStringField("altAllele", value.getAltAllele());
        generator.writeStringField("variantType", value.getVariantType().toString());

        generator.writeNumberField("refAlleleCount", value.getRefAlleleCount());
        generator.writeNumberField("altAlleleCount", value.getAltAlleleCount());
        generator.writeNumberField("refAlleleFreq", value.getRefAlleleFreq());
        generator.writeNumberField("altAlleleFreq", value.getAltAlleleFreq());

        generator.writeObjectField("genotypesCount", value.getGenotypesCount());
        generator.writeObjectField("genotypesFreq", value.getGenotypesFreq());

        generator.writeNumberField("missingAlleles", value.getMissingAlleles());
        generator.writeNumberField("missingGenotypes", value.getMissingGenotypes());

        if (value.getMaf() >= 0) {
            generator.writeNumberField("maf", value.getMaf());
            generator.writeStringField("mafAllele", value.getMafAllele());
        }

        if (value.getMgf() >= 0) {
            generator.writeNumberField("mgf", value.getMgf());
            generator.writeStringField("mgfGenotype", value.getMgfGenotype());
        }

        if (value.getMendelianErrors() >= 0) {
            generator.writeNumberField("mendelianErrors", value.getMendelianErrors());
        }

        if (value.getCasesPercentDominant() >= 0) {
            generator.writeNumberField("casesPercentDominant", value.getCasesPercentDominant());
        }

        if (value.getControlsPercentDominant() >= 0) {
            generator.writeNumberField("controlsPercentDominant", value.getControlsPercentDominant());
        }

        if (value.getCasesPercentRecessive() >= 0) {
            generator.writeNumberField("casesPercentRecessive", value.getCasesPercentRecessive());
        }

        if (value.getControlsPercentRecessive() >= 0) {
            generator.writeNumberField("controlsPercentRecessive", value.getControlsPercentRecessive());
        }

        if (value.getQuality() >= 0) {
            generator.writeNumberField("quality", value.getQuality());
        }

        generator.writeNumberField("numSamples", value.getNumSamples());
        generator.writeEndObject();
    }

}
