package org.opencb.opencga.storage.core.variant.io.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;
import org.opencb.biodata.formats.variant.io.VariantWriter;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class VariantJsonWriter implements VariantWriter {
    
    private VariantSource source;
    private Path outdir;
    
    protected JsonFactory factory;
    protected ObjectMapper jsonObjectMapper;
    
    protected JsonGenerator variantsGenerator;
    protected JsonGenerator fileGenerator;
    
    private OutputStream variantsStream;
    private OutputStream fileStream;

    private long numVariantsWritten;
    private VariantStorageManager.IncludeSrc includeSrc;
    private boolean includeStats;
    private boolean includeSamples;

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
            variantsStream = new GZIPOutputStream(new FileOutputStream(
                    Paths.get(outdir.toString(), source.getFileName()).toAbsolutePath().toString() + ".variants.json.gz"));
            fileStream = new GZIPOutputStream(new FileOutputStream(
                    Paths.get(outdir.toString(), source.getFileName()).toAbsolutePath().toString() + ".file.json.gz"));
        } catch (IOException ex) {
            Logger.getLogger(VariantJsonWriter.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return true;
    }

    @Override
    public boolean pre() {
        jsonObjectMapper.addMixInAnnotations(VariantSourceEntry.class, VariantSourceEntryJsonMixin.class);
        jsonObjectMapper.addMixInAnnotations(Genotype.class, GenotypeJsonMixin.class);
        jsonObjectMapper.addMixInAnnotations(VariantStats.class, VariantStatsJsonMixin.class);
        jsonObjectMapper.addMixInAnnotations(VariantSource.class, VariantSourceJsonMixin.class);
        jsonObjectMapper.addMixInAnnotations(VariantAnnotation.class, VariantAnnotationMixin.class);

        try {
            variantsGenerator = factory.createGenerator(variantsStream);
            fileGenerator = factory.createGenerator(fileStream);
        } catch (IOException ex) {
            Logger.getLogger(VariantJsonWriter.class.getName()).log(Level.SEVERE, null, ex);
            close();
            return false;
        }
        
        return true;
    }

    @Override
    public boolean write(Variant variant) {
        try {
            variantsGenerator.writeObject(variant);
            variantsGenerator.writeRaw('\n');
        } catch (IOException ex) {
            Logger.getLogger(VariantJsonWriter.class.getName()).log(Level.SEVERE, variant.getChromosome() + ":" + variant.getStart(), ex);
            close();
            return false;
        }
        return true;
    }

    @Override
    public boolean write(List<Variant> batch) {
        for (Variant variant : batch) {
            try {
                for (VariantSourceEntry variantSourceEntry : variant.getSourceEntries().values()) {
                    if (!includeSrc.equals(VariantStorageManager.IncludeSrc.NO)) {  // avoid parsing the source line here
                        if (variantSourceEntry.getAttributes().containsKey("src")) {
                            variantSourceEntry.getAttributes().remove("src");
                        }
                    }
                    if (!includeSamples) {
                        variantSourceEntry.getSamplesData().clear();
                    }
                    if (!includeStats) {
                        variantSourceEntry.setStats(null);
                    }
                }
                variantsGenerator.writeObject(variant);
                variantsGenerator.writeRaw('\n');
            } catch (IOException ex) {
                Logger.getLogger(VariantJsonWriter.class.getName()).log(Level.SEVERE, variant.getChromosome() + ":" + variant.getStart(), ex);
            close();
                return false;
            }
        }
        
        numVariantsWritten += batch.size();
        if (numVariantsWritten % 1000 == 0) {
            Variant lastVariantInBatch = batch.get(batch.size()-1);
            Logger.getLogger(VariantJsonWriter.class.getName()).log(Level.INFO, "{0}\tvariants written upto position {1}:{2}", 
                    new Object[]{numVariantsWritten, lastVariantInBatch.getChromosome(), lastVariantInBatch.getStart()});
        }
        
        return true;
    }

    @Override
    public boolean post() {
        try {
            variantsStream.flush();
            variantsGenerator.flush();
            
            fileGenerator.writeObject(source);
            fileStream.flush();
            fileGenerator.flush();
        } catch (IOException ex) {
            Logger.getLogger(VariantJsonWriter.class.getName()).log(Level.SEVERE, null, ex);
            close();
            return false;
        }
        return true;
    }

    @Override
    public boolean close() {
        try {
            variantsGenerator.close();
            fileGenerator.close();
        } catch (IOException ex) {
            Logger.getLogger(VariantJsonWriter.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return true;
    }


    @Override
    public void includeStats(boolean stats) {
        this.includeStats = stats;
    }

    public void includeSrc(VariantStorageManager.IncludeSrc src) {
        this.includeSrc = src;
    }

    @Override
    public void includeSamples(boolean samples) {
        this.includeSamples = samples;
    }

    @Override
    public void includeEffect(boolean effect) { }

}
