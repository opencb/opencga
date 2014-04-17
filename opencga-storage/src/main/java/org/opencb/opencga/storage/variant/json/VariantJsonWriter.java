package org.opencb.opencga.storage.variant.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;
import org.opencb.biodata.formats.variant.vcf4.io.VariantWriter;
import org.opencb.biodata.models.variant.ArchivedVariantFile;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.stats.VariantStats;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class VariantJsonWriter implements VariantWriter {
    
    private VariantSource source;
    
    protected ObjectMapper jsonObjectMapper;
    protected ObjectWriter jsonObjectWriter;
    private BufferedWriter writer;

    
    public VariantJsonWriter(VariantSource source, String species) {
        this.source = source;
        this.jsonObjectMapper = new ObjectMapper();
    }

    @Override
    public boolean open() {
        try {
            writer = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(source.getName() + ".json.gz"))));
        } catch (IOException ex) {
            Logger.getLogger(VariantJsonWriter.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return true;
    }

    @Override
    public boolean pre() {
        jsonObjectMapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
        jsonObjectMapper.addMixInAnnotations(ArchivedVariantFile.class, ArchivedVariantFileJsonMixin.class);
        jsonObjectMapper.addMixInAnnotations(VariantStats.class, VariantStatsJsonMixin.class);
        
        String[] ignorableFieldNames = { "sampleNames", "indel", "snp", "altAlleles" };  
        FilterProvider filters = new SimpleFilterProvider().addFilter("filter properties by name",   
              SimpleBeanPropertyFilter.serializeAllExcept(ignorableFieldNames));
        
        jsonObjectWriter = jsonObjectMapper.writer();
        jsonObjectWriter.with(filters);
        
        return true;
    }

    @Override
    public boolean write(Variant variant) {
        try {
            writer.write(jsonObjectWriter.writeValueAsString(variant) + "\n");
        } catch (IOException ex) {
            Logger.getLogger(VariantJsonWriter.class.getName()).log(Level.SEVERE, variant.getChromosome() + ":" + variant.getStart(), ex);
            return false;
        }
        return true;
    }

    @Override
    public boolean write(List<Variant> batch) {
        for (Variant variant : batch) {
            try {
                writer.write(jsonObjectWriter.writeValueAsString(variant) + "\n");
            } catch (IOException ex) {
                Logger.getLogger(VariantJsonWriter.class.getName()).log(Level.SEVERE, variant.getChromosome() + ":" + variant.getStart(), ex);
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean post() {
        return true;
    }

    @Override
    public boolean close() {
        try {
            writer.close();
        } catch (IOException ex) {
            Logger.getLogger(VariantJsonWriter.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return true;
    }


    @Override
    public void includeStats(boolean stats) { }

    @Override
    public void includeSamples(boolean samples) { }

    @Override
    public void includeEffect(boolean effect) { }

}
