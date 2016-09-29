/*
 * Copyright 2015-2016 OpenCB
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

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.opencb.opencga.storage.alignment.tasks;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import org.opencb.biodata.models.alignment.AlignmentRegion;
import org.opencb.biodata.models.alignment.stats.MeanCoverage;
import org.opencb.biodata.models.alignment.stats.RegionCoverage;
import org.opencb.biodata.models.feature.Region;
import org.opencb.commons.run.Task;

/**
 *
 * @author jacobo
 */
public class AlignmentRegionCoverageFromJsonTask extends Task<AlignmentRegion>{
    private final String filename;

    private final JsonFactory factory;
    private final ObjectMapper jsonObjectMapper;
    
    private JsonParser coverageParser;
    private InputStream coveragesStream;
    
    private RegionCoverage actualRC;
    private List<MeanCoverage> meanCoverage;
    
    public AlignmentRegionCoverageFromJsonTask(String filename) {
        this.filename = filename;
        this.factory = new JsonFactory();
        this.jsonObjectMapper = new ObjectMapper(this.factory);
    }

    @Override
    public boolean pre() {
        try {
            coveragesStream = new FileInputStream(Paths.get(filename).toAbsolutePath().toString());
            
            if (filename.endsWith(".gz")) {
                coveragesStream = new GZIPInputStream(coveragesStream);
            }
            
            coverageParser = factory.createParser(coveragesStream);
            return super.pre();
            
        } catch (FileNotFoundException ex) {
            Logger.getLogger(AlignmentRegionCoverageFromJsonTask.class.getName()).log(Level.SEVERE, null, ex);            
        } catch (IOException ex) {
            Logger.getLogger(AlignmentRegionCoverageFromJsonTask.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }
    
    @Override
    public boolean post() {
        try {
            coverageParser.close();
            return super.post();
        } catch (IOException ex) {
            Logger.getLogger(AlignmentRegionCoverageFromJsonTask.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }

    
    
    @Override
    public boolean apply(List<AlignmentRegion> batch) throws IOException {

        
        for(AlignmentRegion ar : batch){
            getRegionCoverage(ar);
        }
        
        return true;
    }

    
    private void getRegionCoverage(AlignmentRegion alignmentRegion) {
        /*
        *         actualRC
        *     _______|_______
        *     |              |
        *  ---a----b---------c-------d----    <--coverage
        *          |_________________| 
        *                   |
        *                region
        *  offset = b-a
        *  length = d-b
        * 
        */
        Region region = alignmentRegion.getRegion();
       // RegionCoverage regionCoverage = new RegionCoverage(region.getEnd()-region.getStart());
        
        read();
        if(meanCoverage == null || actualRC == null){
            System.out.println("NULL in " + region.getStart() + " " + region.getEnd());
        }
        alignmentRegion.setMeanCoverage(meanCoverage);
        alignmentRegion.setCoverage(actualRC);
        
        meanCoverage = null;
        actualRC = null;
        return;

//        
//        int index = 0;
//        int length = region.getEnd()-region.getStart();
//        int offset;
//        while(index < length){
//            if(actualRC == null) {
//                actualRC = read();
//            }
//            if(region.getChromosome().equals(actualRC.getChromosome())){
//                offset = (int)(region.getStart() - actualRC.getStart());
//                //assert(offset >= 0);
//                if(offset < 0){
//                    region.setStart((int)actualRC.getStart());
//                    length = region.getEnd()-region.getStart();
//                    offset = 0;
//                }
//
//                for(; index < length; index++){
//                    if(index + offset >= actualRC.getA().length){
//                        actualRC = null;
//                        break;
//                    }
//                    regionCoverage.getA()[index]   = actualRC.getA()[index + offset];
//                    regionCoverage.getC()[index]   = actualRC.getC()[index + offset];
//                    regionCoverage.getT()[index]   = actualRC.getT()[index + offset];
//                    regionCoverage.getG()[index]   = actualRC.getG()[index + offset];
//                    regionCoverage.getAll()[index] = actualRC.getAll()[index + offset];
//                }
//            } else {
//                actualRC = null;
//            }
//        }
//        
//        return;
        //return regionCoverage;
    }
    
    
    private void read() {
        try {
            if (coverageParser.nextToken() == null) {
                System.out.println("[ERROR] Corrupted file " + filename);
            }
            actualRC = coverageParser.readValueAs(RegionCoverage.class);
            meanCoverage = coverageParser.readValueAs(new TypeReference<List<MeanCoverage>>() { });

        } catch (IOException ex) {
            Logger.getLogger(AlignmentRegionCoverageFromJsonTask.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
