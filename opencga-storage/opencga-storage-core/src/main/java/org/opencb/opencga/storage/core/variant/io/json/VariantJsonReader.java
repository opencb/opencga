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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.formats.variant.vcf4.io.VariantVcfReader;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.biodata.tools.variant.VariantFileUtils;
import org.xerial.snappy.SnappyInputStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

/**
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
    private LinkedHashMap<String, Integer> samplesPosition;

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

            String name = variantsPath.toFile().getName();
            if (name.endsWith(".gz")) {
                this.variantsStream = new GZIPInputStream(new FileInputStream(variantsPath.toFile()));
            } else if (name.endsWith(".snz") || name.endsWith(".snappy")) {
                this.variantsStream = new SnappyInputStream(new FileInputStream(variantsPath.toFile()));
            } else {
                this.variantsStream = new FileInputStream(variantsPath.toFile());
            }

            String globalFileName = globalPath.toFile().getName();
            if (globalFileName.endsWith(".gz")) {
                this.globalStream = new GZIPInputStream(new FileInputStream(globalPath.toFile()));
            } else if (globalFileName.endsWith(".snz") || name.endsWith(".snappy")) {
                this.globalStream = new SnappyInputStream(new FileInputStream(globalPath.toFile()));
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
        jsonObjectMapper.addMixIn(StudyEntry.class, VariantSourceEntryJsonMixin.class);
        jsonObjectMapper.addMixIn(Genotype.class, GenotypeJsonMixin.class);
        jsonObjectMapper.addMixIn(VariantStats.class, VariantStatsJsonMixin.class);
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

        Map<String, Integer> samplesPosition = source.getSamplesPosition();
        this.samplesPosition = new LinkedHashMap<>(samplesPosition.size());
        String[] samples = new String[samplesPosition.size()];
        for (Map.Entry<String, Integer> entry : samplesPosition.entrySet()) {
            samples[entry.getValue()] = entry.getKey();
        }
        for (int i = 0; i < samples.length; i++) {
            this.samplesPosition.put(samples[i], i);
        }

        return true;
    }

    @Override
    public List<Variant> read() {
        try {
            if (variantsParser.nextToken() != null) {
                Variant variant = variantsParser.readValueAs(Variant.class);
                variant.getStudy(source.getStudyId()).setSamplesPosition(samplesPosition);
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
                variant.getStudy(source.getStudyId()).setSamplesPosition(samplesPosition);
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
        return new ArrayList<>(source.getSamplesPosition().keySet());
    }

    @Override
    public String getHeader() {
        return source.getMetadata().get(VariantFileUtils.VARIANT_FILE_HEADER).toString();
    }

}
