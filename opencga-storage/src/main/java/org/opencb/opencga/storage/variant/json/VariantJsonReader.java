package org.opencb.opencga.storage.variant.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.formats.variant.vcf4.io.VariantVcfReader;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.ArchivedVariantFile;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.stats.VariantStats;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class VariantJsonReader implements VariantReader {

    protected JsonFactory factory;
    protected ObjectMapper jsonObjectMapper;
    private JsonParser parser;
    
    private InputStream stream;
    private Path path;
    private String filename;
    
    public VariantJsonReader(String fileName) {
        this.filename = fileName;
        this.factory = new JsonFactory();
        this.jsonObjectMapper = new ObjectMapper(this.factory);
    }

    @Override
    public boolean open() {
        try {
            this.path = Paths.get(this.filename);
            Files.exists(this.path);

            if (path.toFile().getName().endsWith(".gz")) {
                this.stream = new GZIPInputStream(new FileInputStream(path.toFile()));
            } else {
                this.stream = new FileInputStream(path.toFile());
            }

        } catch (IOException ex) {
            Logger.getLogger(VariantJsonReader.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }

        return true;
    }

    @Override
    public boolean pre() {
        jsonObjectMapper.addMixInAnnotations(ArchivedVariantFile.class, ArchivedVariantFileJsonMixin.class);
        jsonObjectMapper.addMixInAnnotations(Genotype.class, GenotypeJsonMixin.class);
        jsonObjectMapper.addMixInAnnotations(VariantStats.class, VariantStatsJsonMixin.class);
        try {
            parser = factory.createParser(stream);
            // TODO Optimizations for memory management?
        } catch (IOException ex) {
            Logger.getLogger(VariantJsonReader.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        
        return true;
    }

    @Override
    public Variant read() {
        try {
            if (parser.nextToken() != null) {
                Variant variant = parser.readValueAs(Variant.class);
                return variant;
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
            for (int i = 0; i < batchSize && parser.nextToken() != null; i++) {
                Variant variant = parser.readValueAs(Variant.class);
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
            parser.close();
        } catch (IOException ex) {
            Logger.getLogger(VariantJsonReader.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        
        return true;
    }

    @Override
    public List<String> getSampleNames() {
        return new ArrayList<>();
    }

    @Override
    public String getHeader() {
        return "";
    }

}
