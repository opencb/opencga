/*
 * Copyright 2015-2017 OpenCB
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
import org.opencb.biodata.formats.variant.vcf4.io.VariantVcfReader;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.opencga.storage.core.variant.io.AbstractVariantReader;
import org.opencb.opencga.storage.core.variant.io.json.mixin.GenotypeJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.mixin.VariantSourceEntryJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.mixin.VariantStatsJsonMixin;
import org.xerial.snappy.SnappyInputStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class VariantJsonReader extends AbstractVariantReader {

    protected JsonFactory factory;
    protected ObjectMapper jsonObjectMapper;
    private JsonParser variantsParser;

    private InputStream variantsStream;

    private String variantFilename;

    private VariantSource source;

    //    public VariantJsonReader(String variantFilename, String globalFilename) {
    public VariantJsonReader(VariantSource source, String variantFilename, String globalFilename) {
        super(Paths.get(globalFilename), source);
        this.source = source;
        this.variantFilename = variantFilename;
        this.factory = new JsonFactory();
        this.jsonObjectMapper = new ObjectMapper(this.factory);
    }

    @Override
    public boolean open() {
        try {
            Path variantsPath = Paths.get(this.variantFilename);
//            this.variantsPath = Paths.get(source.getFileName());

            Files.exists(variantsPath);

            String name = variantsPath.toFile().getName();
            if (name.endsWith(".gz")) {
                this.variantsStream = new GZIPInputStream(new FileInputStream(variantsPath.toFile()));
            } else if (name.endsWith(".snz") || name.endsWith(".snappy")) {
                this.variantsStream = new SnappyInputStream(new FileInputStream(variantsPath.toFile()));
            } else {
                this.variantsStream = new FileInputStream(variantsPath.toFile());
            }

        } catch (IOException ex) {
            Logger.getLogger(VariantJsonReader.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }

        return true;
    }

    @Override
    public boolean pre() {
        super.pre();

        jsonObjectMapper.addMixIn(StudyEntry.class, VariantSourceEntryJsonMixin.class);
        jsonObjectMapper.addMixIn(Genotype.class, GenotypeJsonMixin.class);
        jsonObjectMapper.addMixIn(VariantStats.class, VariantStatsJsonMixin.class);
        try {
            variantsParser = factory.createParser(variantsStream);
            // TODO Optimizations for memory management?
        } catch (IOException ex) {
            Logger.getLogger(VariantJsonReader.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }

        return true;
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
        return addSamplesPosition(listRecords);
    }

    @Override
    public boolean post() {
        return true;
    }

    @Override
    public boolean close() {
        try {
            variantsParser.close();
        } catch (IOException ex) {
            Logger.getLogger(VariantJsonReader.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }

        return true;
    }


}
