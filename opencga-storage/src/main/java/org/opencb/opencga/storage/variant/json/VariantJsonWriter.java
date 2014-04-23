package org.opencb.opencga.storage.variant.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
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
    private Path outdir;
    
    protected JsonFactory factory;
    protected ObjectMapper jsonObjectMapper;
    protected JsonGenerator generator;
    
    private BufferedWriter writer;

    private long numVariantsWritten;
    
    public VariantJsonWriter(VariantSource source, Path outdir) {
        this.source = source;
        this.outdir = (outdir != null) ? outdir : Paths.get("").toAbsolutePath(); 
        this.factory = new JsonFactory();
        this.jsonObjectMapper = new ObjectMapper(this.factory);
        this.numVariantsWritten = 0;
    }

    @Override
    public boolean open() {
        try {
            writer = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(
                    Paths.get(outdir.toString(), source.getFilename()).toAbsolutePath().toString() + ".json.gz"))));
        } catch (IOException ex) {
            Logger.getLogger(VariantJsonWriter.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return true;
    }

    @Override
    public boolean pre() {
        jsonObjectMapper.addMixInAnnotations(ArchivedVariantFile.class, ArchivedVariantFileJsonMixin.class);
        jsonObjectMapper.addMixInAnnotations(VariantStats.class, VariantStatsJsonMixin.class);
        
        try {
            generator = factory.createGenerator(writer);
            generator.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
        } catch (IOException ex) {
            Logger.getLogger(VariantJsonWriter.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        
        return true;
    }

    @Override
    public boolean write(Variant variant) {
        try {
            generator.writeObject(variant);
            generator.writeRaw('\n');
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
                generator.writeObject(variant);
                generator.writeRaw('\n');
            } catch (IOException ex) {
                Logger.getLogger(VariantJsonWriter.class.getName()).log(Level.SEVERE, variant.getChromosome() + ":" + variant.getStart(), ex);
                return false;
            }
        }
        
        try {
            generator.flush();
        } catch (IOException ex) {
            Logger.getLogger(VariantJsonWriter.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        
        numVariantsWritten += batch.size();
        Variant lastVariantInBatch = batch.get(batch.size()-1);
        Logger.getLogger(VariantJsonWriter.class.getName()).log(Level.INFO, "{0}\tvariants written upto position {1}:{2}", 
                new Object[]{numVariantsWritten, lastVariantInBatch.getChromosome(), lastVariantInBatch.getStart()});
        
        return true;
    }

    @Override
    public boolean post() {
        return true;
    }

    @Override
    public boolean close() {
        try {
            generator.close();
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
