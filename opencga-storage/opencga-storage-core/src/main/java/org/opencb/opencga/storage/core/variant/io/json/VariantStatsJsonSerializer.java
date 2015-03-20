package org.opencb.opencga.storage.core.variant.io.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import org.opencb.biodata.models.variant.stats.VariantStats;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class VariantStatsJsonSerializer extends JsonSerializer<VariantStats> {
    
    @Override
    public void serialize(VariantStats value, JsonGenerator generator, SerializerProvider provider)
      throws IOException, JsonProcessingException {
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
