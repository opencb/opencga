package org.opencb.opencga.storage.core.variant.io.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.formats.variant.vcf4.io.VariantVcfReader;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.stats.VariantStats;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class VariantJsonReader implements VariantReader {

    protected JsonFactory factory;
    protected ObjectMapper jsonObjectMapper;
    private JsonParser variantsParser;
    private JsonParser globalParser;
    
    private InputStream variantsStream;
    private InputStream globalStream;
    
    private String variantFilename;
    private String globalFilename;
    
    private Path variantsPath;
    private Path globalPath;
    
    private VariantSource source;
    
//    public VariantJsonReader(String variantFilename, String globalFilename) {
    public VariantJsonReader(VariantSource source, String variantFilename, String globalFilename) {
        this.source = source;
        this.variantFilename = variantFilename;
        this.globalFilename = globalFilename;
        this.factory = new JsonFactory();
        this.jsonObjectMapper = new ObjectMapper(this.factory);
    }

    @Override
    public boolean open() {
        try {
            this.variantsPath = Paths.get(this.variantFilename);
//            this.variantsPath = Paths.get(source.getFileName());
            this.globalPath = Paths.get(this.globalFilename);
            
            Files.exists(this.variantsPath);
            Files.exists(this.globalPath);

            if (variantsPath.toFile().getName().endsWith(".gz")) {
                this.variantsStream = new GZIPInputStream(new FileInputStream(variantsPath.toFile()));
            } else {
                this.variantsStream = new FileInputStream(variantsPath.toFile());
            }
            
            if (globalPath.toFile().getName().endsWith(".gz")) {
                this.globalStream = new GZIPInputStream(new FileInputStream(globalPath.toFile()));
            } else {
                this.globalStream = new FileInputStream(globalPath.toFile());
            }
            
        } catch (IOException ex) {
            Logger.getLogger(VariantJsonReader.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }

        return true;
    }

    @Override
    public boolean pre() {
        jsonObjectMapper.addMixInAnnotations(VariantSourceEntry.class, VariantSourceEntryJsonMixin.class);
        jsonObjectMapper.addMixInAnnotations(Genotype.class, GenotypeJsonMixin.class);
        jsonObjectMapper.addMixInAnnotations(VariantStats.class, VariantStatsJsonMixin.class);
        try {
            variantsParser = factory.createParser(variantsStream);
            globalParser = factory.createParser(globalStream);
            // TODO Optimizations for memory management?
            
            // Read global JSON file and copy its info into the already available VariantSource object
            VariantSource readSource = globalParser.readValueAs(VariantSource.class);
            source.setFileName(readSource.getFileName());
            source.setFileId(readSource.getFileId());
            source.setStudyName(readSource.getStudyName());
            source.setStudyId(readSource.getStudyId());
            source.setAggregation(readSource.getAggregation());
            source.setMetadata(readSource.getMetadata());
            source.setPedigree(readSource.getPedigree());
            source.setSamplesPosition(readSource.getSamplesPosition());
            source.setStats(readSource.getStats());
            source.setType(readSource.getType());
        } catch (IOException ex) {
            Logger.getLogger(VariantJsonReader.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        
        return true;
    }

    @Override
    public List<Variant> read() {
        try {
            if (variantsParser.nextToken() != null) {
                Variant variant = variantsParser.readValueAs(Variant.class);
                return Arrays.asList(variant);
            }
        } catch (IOException ex) {
            Logger.getLogger(VariantVcfReader.class.getName()).log(Level.SEVERE, null, ex);
        }

        return null;
    }

    @Override
    public List<Variant> read(int batchSize) {
        List<Variant> listRecords = new ArrayList<>(batchSize);
        
        try {
            for (int i = 0; i < batchSize && variantsParser.nextToken() != null; i++) {
                Variant variant = variantsParser.readValueAs(Variant.class);
                listRecords.add(variant);
            }
        } catch (IOException ex) {
            Logger.getLogger(VariantVcfReader.class.getName()).log(Level.SEVERE, null, ex);
        }

        return listRecords;
    }
    
    @Override
    public boolean post() {
        return true;
    }

    @Override
    public boolean close() {
        try {
            variantsParser.close();
            globalParser.close();
        } catch (IOException ex) {
            Logger.getLogger(VariantJsonReader.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        
        return true;
    }

    @Override
    public List<String> getSampleNames() {
        return source.getSamples();
    }

    @Override
    public String getHeader() {
        return source.getMetadata().get("variantFileHeader").toString();
    }

}
