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
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.formats.variant.io.VariantWriter;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantVcfFactory;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

/**
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
    private boolean includeSrc = false;
    private boolean includeStats;
    private boolean includeSamples = true;

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
            String output = Paths.get(outdir.toString(), source.getFileName()).toAbsolutePath().toString()
                    + "." + VariantReaderUtils.VARIANTS_FILE + ".json.gz";
            variantsStream = new GZIPOutputStream(new FileOutputStream(output));
            fileStream = new GZIPOutputStream(new FileOutputStream(VariantReaderUtils.getMetaFromTransformedFile(output)));
        } catch (IOException ex) {
            Logger.getLogger(VariantJsonWriter.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return true;
    }

    @Override
    public boolean pre() {
        jsonObjectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        jsonObjectMapper.addMixIn(StudyEntry.class, VariantSourceEntryJsonMixin.class);
        jsonObjectMapper.addMixIn(Genotype.class, GenotypeJsonMixin.class);
        jsonObjectMapper.addMixIn(VariantStats.class, VariantStatsJsonMixin.class);
        jsonObjectMapper.addMixIn(VariantSource.class, VariantSourceJsonMixin.class);
        jsonObjectMapper.addMixIn(VariantAnnotation.class, VariantAnnotationMixin.class);

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
                for (StudyEntry studyEntry : variant.getStudies()) {
                    if (!includeSrc) {
                        for (FileEntry fileEntry : studyEntry.getFiles()) {
                            if (fileEntry.getAttributes().containsKey(VariantVcfFactory.SRC)) {
                                fileEntry.getAttributes().remove(VariantVcfFactory.SRC);
                            }
                        }
                    }
                    if (!includeSamples) {
                        studyEntry.getSamplesData().clear();
                    }
                    if (!includeStats) {
                        studyEntry.setStats(Collections.emptyMap());
                    }
                }
                variantsGenerator.writeObject(variant);
                variantsGenerator.writeRaw('\n');
            } catch (IOException ex) {
                Logger.getLogger(VariantJsonWriter.class.getName()).log(Level.SEVERE, variant.getChromosome() + ":" + variant.getStart(),
                        ex);
                close();
                return false;
            }
        }

        numVariantsWritten += batch.size();
        if (numVariantsWritten % 1000 == 0) {
            Variant lastVariantInBatch = batch.get(batch.size() - 1);
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

    public void includeSrc(boolean src) {
        this.includeSrc = src;
    }

    @Override
    public void includeSamples(boolean samples) {
        this.includeSamples = samples;
    }

    @Override
    public void includeEffect(boolean effect) {
    }

}
