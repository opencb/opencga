/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.opencb.opencga.storage.alignment.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;
import org.opencb.biodata.formats.alignment.io.AlignmentDataWriter;
import org.opencb.biodata.formats.alignment.io.AlignmentRegionDataWriter;
import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.biodata.models.alignment.AlignmentHeader;
import org.opencb.biodata.models.alignment.AlignmentRegion;
import org.opencb.biodata.models.alignment.stats.MeanCoverage;
import org.opencb.biodata.models.alignment.stats.RegionCoverage;
import org.opencb.commons.io.DataWriter;

/**
 *
 * @author jacobo
 */
public class AlignmentCoverageJsonDataWriter implements DataWriter<AlignmentRegion>{

    private final String filename;
    private final JsonFactory factory;
    private final ObjectMapper jsonObjectMapper;
    private final boolean gzip;
    
    private OutputStream coverageOutputStream;
    private JsonGenerator coverageGenerator;
    
    
    public AlignmentCoverageJsonDataWriter(String coverageFilename) {
        this.filename = coverageFilename;
        this.gzip = filename.endsWith(".gz");
        this.factory = new JsonFactory();
        this.jsonObjectMapper = new ObjectMapper(this.factory);
    }
    
    public AlignmentCoverageJsonDataWriter(String baseFilename, boolean gzip) {
        this.filename = baseFilename + ".coverage" + (gzip ? ".json.gz" : ".json");
        this.gzip = gzip;
        this.factory = new JsonFactory();
        this.jsonObjectMapper = new ObjectMapper(this.factory);
    }

    @Override
    public boolean open() {
        try {
            coverageOutputStream = new FileOutputStream(filename);
            
            if (gzip) {
                coverageOutputStream = new GZIPOutputStream(coverageOutputStream);
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return false;
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return false;
        }
    
        return true;
    }

    @Override
    public boolean pre() {
        jsonObjectMapper.addMixInAnnotations(Alignment.AlignmentDifference.class, AlignmentDifferenceJsonMixin.class);
        try {
            coverageGenerator = factory.createGenerator(coverageOutputStream);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            close();
            return false;
        }
        return true;
    }

    @Override
    public boolean post() {
        try {
            coverageGenerator.flush();
        } catch (IOException ex) {
            Logger.getLogger(AlignmentCoverageJsonDataWriter.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return true;
    }

    @Override
    public boolean close() {
        try {
            coverageGenerator.close();
        } catch (IOException ex) {
            Logger.getLogger(AlignmentCoverageJsonDataWriter.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return true;
    }

    @Override
    public boolean write(AlignmentRegion elem) {
        RegionCoverage coverage = elem.getCoverage();
        List<MeanCoverage> meanCoverage = elem.getMeanCoverage();
        
        try {
            coverageGenerator.writeObject(coverage);
            coverageGenerator.writeObject(meanCoverage);
            coverageGenerator.writeRaw("\n");
        } catch (IOException ex) {
            Logger.getLogger(AlignmentCoverageJsonDataWriter.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return true;
    }

    @Override
    public boolean write(List<AlignmentRegion> batch) {
        for(AlignmentRegion ar : batch){
            if(!write(ar)){
                return false;
            }
        }
        return true;
    }

    
}
