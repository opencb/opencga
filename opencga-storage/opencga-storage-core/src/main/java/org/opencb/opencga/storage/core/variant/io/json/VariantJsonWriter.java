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
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.formats.variant.io.VariantWriter;
import org.opencb.biodata.formats.variant.vcf4.VariantVcfFactory;
import org.opencb.biodata.models.variant.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.opencga.core.models.common.mixins.GenotypeJsonMixin;
import org.opencb.opencga.core.models.common.mixins.VariantStatsJsonMixin;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.core.variant.io.json.mixin.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.zip.GZIPOutputStream;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class VariantJsonWriter implements VariantWriter {

    private final VariantFileMetadata fileMetadata;
    // Null if OutputStreams were directly provided
    private final Path outdir;

    protected JsonFactory factory;
    protected ObjectMapper jsonObjectMapper;

    protected JsonGenerator variantsGenerator;
    protected JsonGenerator fileGenerator;

    private OutputStream variantsStream;
    private OutputStream fileStream;

    private Logger logger = LoggerFactory.getLogger(VariantJsonWriter.class);

    private long numVariantsWritten;
    private boolean includeSrc = false;
    private boolean includeStats = true;
    private boolean includeSamples = true;
    private boolean closeStreams;

    public VariantJsonWriter(VariantFileMetadata fileMetadata, @Nullable Path outdir) {
        Objects.requireNonNull(fileMetadata, "VariantFileMetadata can not be null");
        this.fileMetadata = fileMetadata;
        this.outdir = (outdir != null) ? outdir : Paths.get("").toAbsolutePath();
        this.factory = new JsonFactory();
        this.jsonObjectMapper = new ObjectMapper(this.factory);
        this.numVariantsWritten = 0;
        closeStreams = true;
    }

    public VariantJsonWriter(OutputStream variantsStream) {
        this(null, variantsStream, null);
    }

    public VariantJsonWriter(VariantFileMetadata fileMetadata, OutputStream variantsStream, OutputStream fileStream) {
        this.fileMetadata = fileMetadata;
        this.outdir = null;
        this.variantsStream = variantsStream;
        this.fileStream = fileStream;
        this.factory = new JsonFactory();
        this.jsonObjectMapper = new ObjectMapper(this.factory);
        this.numVariantsWritten = 0;
        closeStreams = false;
    }

    @Override
    public boolean open() {
        try {
            if (outdir != null) {
                String filePath = fileMetadata.getPath();
                String fileName = Paths.get(filePath).getFileName().toString();
                String output = Paths.get(outdir.toString(), fileName).toAbsolutePath().toString()
                        + "." + VariantReaderUtils.VARIANTS_FILE + ".json.gz";
                variantsStream = new GZIPOutputStream(new FileOutputStream(output));
                fileStream = new GZIPOutputStream(new FileOutputStream(VariantReaderUtils.getMetaFromTransformedFile(output)));
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
        return true;
    }

    @Override
    public boolean pre() {
        jsonObjectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        jsonObjectMapper.addMixIn(StudyEntry.class, VariantSourceEntryJsonMixin.class);
        jsonObjectMapper.addMixIn(Genotype.class, GenotypeJsonMixin.class);
        jsonObjectMapper.addMixIn(VariantStats.class, VariantStatsJsonMixin.class);
        jsonObjectMapper.addMixIn(VariantAnnotation.class, VariantAnnotationMixin.class);
        jsonObjectMapper.addMixIn(ConsequenceType.class, ConsequenceTypeMixin.class);

        try {
            variantsGenerator = factory.createGenerator(variantsStream);
            if (fileStream != null || fileMetadata != null) {
                fileGenerator = factory.createGenerator(fileStream);
            }
        } catch (IOException ex) {
            close();
            throw new UncheckedIOException(ex);
        }

        return true;
    }

    @Override
    public boolean write(List<Variant> batch) {
        for (Variant variant : batch) {
            write(variant);
        }
        return true;
    }

    @Override
    public boolean write(Variant variant) {
        try {
            for (StudyEntry studyEntry : variant.getStudies()) {
                if (!includeSrc) {
                    for (FileEntry fileEntry : studyEntry.getFiles()) {
                        fileEntry.getData().remove(VariantVcfFactory.SRC);
                    }
                }
                if (!includeSamples) {
                    studyEntry.getSamples().clear();
                }
                if (!includeStats) {
                    studyEntry.setStats(Collections.emptyList());
                }
            }
            variantsGenerator.writeObject(variant);
            variantsGenerator.writeRaw('\n');
            numVariantsWritten++;
        } catch (IOException ex) {
            logger.error(variant.toString(), ex);
            close();
            throw new UncheckedIOException(ex);
        }
        return true;
    }

    @Override
    public boolean post() {
        try {
            variantsStream.flush();
            variantsGenerator.flush();

            if (fileGenerator != null) {
                fileGenerator.writeObject(fileMetadata);
                fileStream.flush();
                fileGenerator.flush();
            }
        } catch (IOException ex) {
            close();
            throw new UncheckedIOException(ex);
        }
        return true;
    }

    @Override
    public boolean close() {
        try {
            if (closeStreams) {
                variantsGenerator.close();
                if (fileGenerator != null) {
                    fileGenerator.close();
                }
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
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
