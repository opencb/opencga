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

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.opencb.opencga.storage.core.alignment.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.biodata.models.alignment.AlignmentRegion;
import org.opencb.biodata.models.alignment.stats.MeanCoverage;
import org.opencb.biodata.models.alignment.stats.RegionCoverage;
import org.opencb.commons.io.DataWriter;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

/**
 * @author Jacobo Coll Moragon <jcoll@ebi.ac.uk>
 *         <p>
 *         CoverageFileName     : <name>.coverage.json.gz
 *         MeanCoverageFileName : <name>.mean-coverage.json.gz
 */
public class AlignmentCoverageJsonDataWriter implements DataWriter<AlignmentRegion> {

    public static final int DEFAULT_CHUNK_SIZE = 1000;

    private final String coverageFilename;
    private final String meanCoverageFilename;
    private final JsonFactory factory;
    private final ObjectMapper jsonObjectMapper;
    private final boolean gzip;

    private int chunkSize = DEFAULT_CHUNK_SIZE;

    private OutputStream coverageOutputStream;
    private OutputStream meanCoverageOutputStream;
    private JsonGenerator coverageGenerator;
    private JsonGenerator meanCoverageGenerator;

    private RegionCoverage bufferedCoverage;
    private boolean writeMeanCoverage;
    private boolean writeCoverage;

    public AlignmentCoverageJsonDataWriter(String coverageFilename) {
        this.coverageFilename = coverageFilename;
        this.meanCoverageFilename = null;
        this.gzip = this.coverageFilename.endsWith(".gz");
        this.factory = new JsonFactory();
        this.jsonObjectMapper = new ObjectMapper(this.factory);
        chunkSize = DEFAULT_CHUNK_SIZE;
    }

    public AlignmentCoverageJsonDataWriter(String baseFilename, boolean writeCoverage, boolean writeMeanCoverage, boolean gzip) {
        this.coverageFilename = baseFilename + ".coverage" + (gzip ? ".json.gz" : ".json");
        this.meanCoverageFilename = baseFilename + ".mean-coverage" + (gzip ? ".json.gz" : ".json");
        this.gzip = gzip;
        this.factory = new JsonFactory();
        this.jsonObjectMapper = new ObjectMapper(this.factory);
        chunkSize = DEFAULT_CHUNK_SIZE;

        this.writeMeanCoverage = writeMeanCoverage;
        this.writeCoverage = writeCoverage;
        if (!this.writeCoverage && !this.writeMeanCoverage) {
            throw new IllegalStateException("Writer needs to write region coverage or mean coverage.");
        }
    }

    @Override
    public boolean open() {
        bufferedCoverage = new RegionCoverage(chunkSize);
        bufferedCoverage.setChromosome("");

        try {
            if (writeCoverage) {
                coverageOutputStream = new FileOutputStream(coverageFilename);

                if (gzip) {
                    coverageOutputStream = new GZIPOutputStream(coverageOutputStream);
                }
            }

            if (writeMeanCoverage) {
                if (meanCoverageFilename == null) {
                    meanCoverageOutputStream = coverageOutputStream;
                } else {
                    meanCoverageOutputStream = new FileOutputStream(meanCoverageFilename);
                    if (gzip) {
                        meanCoverageOutputStream = new GZIPOutputStream(meanCoverageOutputStream);
                    }
                }
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
        jsonObjectMapper.addMixIn(Alignment.AlignmentDifference.class, AlignmentDifferenceJsonMixin.class);
        try {
            coverageGenerator = factory.createGenerator(coverageOutputStream);
            meanCoverageGenerator = factory.createGenerator(meanCoverageOutputStream);
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
            writeRegionCoverageJson(bufferedCoverage);
            if (writeCoverage) {
                coverageGenerator.flush();
            }
            if (writeMeanCoverage) {
                meanCoverageGenerator.flush();
            }
        } catch (IOException ex) {
            Logger.getLogger(AlignmentCoverageJsonDataWriter.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return true;
    }

    @Override
    public boolean close() {
        try {
            if (writeCoverage) {
                coverageGenerator.close();
            }
            if (writeMeanCoverage) {
                meanCoverageGenerator.close();
            }
        } catch (IOException ex) {
            Logger.getLogger(AlignmentCoverageJsonDataWriter.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return true;
    }

    private void writeRegionCoverageJson(RegionCoverage coverage) throws IOException {
        boolean empty = true;
        for (int i = 0; i < chunkSize; i++) {
            if (coverage.getAll()[i] != 0) {
                empty = false;
                break;
            }
        }
        if (!empty) {
            coverageGenerator.writeObject(coverage);
            coverageGenerator.writeRaw("\n");
        }
    }

    private void writeMeanCoverageJson(List<MeanCoverage> meanCoverage) throws IOException {
        Collections.sort(meanCoverage, (o1, o2) -> {
            if (o1.getRegion().getChromosome().equals(o2.getRegion().getChromosome())) {
                return o1.getRegion().getStart() - o2.getRegion().getStart();
            } else {
                return o1.getRegion().getChromosome().compareTo(o2.getRegion().getChromosome());
            }
        });

        for (MeanCoverage mc : meanCoverage) {
            meanCoverageGenerator.writeObject(mc);
            meanCoverageGenerator.writeRaw("\n");
        }
    }

    /**
     * Writes coverage in batches.
     *
     * @param elem AlignmentRegion which contains the RegionCoverage
     * @return Exit or fail at writing
     */
    @Override
    public boolean write(AlignmentRegion elem) {

        final RegionCoverage coverage = elem.getCoverage(); //Current RegionCoverage to be written.
        int coverageIndex = 0;  //Index over the current coverage

        if (writeMeanCoverage) {
            try {
                writeMeanCoverageJson(elem.getMeanCoverage());
            } catch (IOException ex) {
                Logger.getLogger(AlignmentCoverageJsonDataWriter.class.getName()).log(Level.SEVERE, null, ex);
                return false;
            }
        }

        if (writeCoverage) {
            if (coverage.getStart() - bufferedCoverage.getStart() > chunkSize
                    || !bufferedCoverage.getChromosome().equals(coverage.getChromosome())) {
                //Current coverage is out of the bufferedCoverage region.
                //Write all the bufferedCoverage.
                if (bufferedCoverage.getChromosome() != null) {  //If it's a valid coverage
                    try {
                        writeRegionCoverageJson(bufferedCoverage);
                    } catch (IOException ex) {
                        Logger.getLogger(AlignmentCoverageJsonDataWriter.class.getName()).log(Level.SEVERE, null, ex);
                        return false;
                    }
                }
                bufferedCoverage.setChromosome(coverage.getChromosome());
                bufferedCoverage.setStart((coverage.getStart() - 1) / chunkSize * chunkSize + 1);   //1-based position
                bufferedCoverage.setEnd(bufferedCoverage.getStart() + chunkSize - 1);
                Arrays.fill(bufferedCoverage.getAll(), (short) 0);
                Arrays.fill(bufferedCoverage.getA(), (short) 0);
                Arrays.fill(bufferedCoverage.getC(), (short) 0);
                Arrays.fill(bufferedCoverage.getG(), (short) 0);
                Arrays.fill(bufferedCoverage.getT(), (short) 0);
            }

            int offset = (int) (coverage.getStart() - bufferedCoverage.getStart()); //Difference between the bufferedCoverage and the
            // current coverage
            int lim = coverage.getAll().length;
            if (coverageIndex < -offset) {
                coverageIndex = -offset;
            }
            for (; coverageIndex < lim; coverageIndex++) {
                if (coverageIndex + offset == chunkSize) {  //Buffer filled. Write and move start and end of the region.
                    try {
                        writeRegionCoverageJson(bufferedCoverage);
                    } catch (IOException ex) {
                        Logger.getLogger(AlignmentCoverageJsonDataWriter.class.getName()).log(Level.SEVERE, null, ex);
                        return false;
                    }
                    bufferedCoverage.setStart(bufferedCoverage.getStart() + chunkSize);
                    bufferedCoverage.setEnd(bufferedCoverage.getEnd() + chunkSize);
                    offset = (int) (coverage.getStart() - bufferedCoverage.getStart());
                }
                //Copy coverage to the buffer
                bufferedCoverage.getAll()[coverageIndex + offset] = coverage.getAll()[coverageIndex];
                bufferedCoverage.getA()[coverageIndex + offset] = coverage.getA()[coverageIndex];
                bufferedCoverage.getC()[coverageIndex + offset] = coverage.getC()[coverageIndex];
                bufferedCoverage.getG()[coverageIndex + offset] = coverage.getG()[coverageIndex];
                bufferedCoverage.getT()[coverageIndex + offset] = coverage.getT()[coverageIndex];
            }

            Arrays.fill(bufferedCoverage.getAll(), coverageIndex + offset, chunkSize, (short) 0);
            Arrays.fill(bufferedCoverage.getA(), coverageIndex + offset, chunkSize, (short) 0);
            Arrays.fill(bufferedCoverage.getC(), coverageIndex + offset, chunkSize, (short) 0);
            Arrays.fill(bufferedCoverage.getG(), coverageIndex + offset, chunkSize, (short) 0);
            Arrays.fill(bufferedCoverage.getT(), coverageIndex + offset, chunkSize, (short) 0);
        }
        return true;
    }

//    public boolean write_old(AlignmentRegion elem) {
//        RegionCoverage coverage = elem.getCoverage();
//        List<MeanCoverage> meanCoverage = elem.getMeanCoverage();
//
//        try {
//            coverageGenerator.writeObject(coverage);
//            coverageGenerator.writeObject(meanCoverage);
//            coverageGenerator.writeRaw("\n");
//        } catch (IOException ex) {
//            Logger.getLogger(AlignmentCoverageJsonDataWriter.class.getName()).log(Level.SEVERE, null, ex);
//            return false;
//        }
//        return true;
//    }

    @Override
    public boolean write(List<AlignmentRegion> batch) {
        for (AlignmentRegion ar : batch) {
            if (!write(ar)) {
                return false;
            }
        }
        return true;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    public String getMeanCoverageFilename() {
        return meanCoverageFilename;
    }

    public String getCoverageFilename() {
        return coverageFilename;
    }
}
