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
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SequenceWriter;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.storage.core.variant.io.json.mixin.VariantAnnotationMixin;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.zip.GZIPOutputStream;

/**
 * Created on 09/11/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantAnnotationJsonDataWriter implements DataWriter<VariantAnnotation> {

    private Path outputPath;
    private boolean gzip;
    private SequenceWriter sequenceWriter;
    private OutputStream outputStream;

    public VariantAnnotationJsonDataWriter(Path outputPath, boolean gzip) {
        this.outputPath = Objects.requireNonNull(outputPath);
        this.gzip = gzip;
    }

    public VariantAnnotationJsonDataWriter(OutputStream outputStream) {
        this.outputStream = Objects.requireNonNull(outputStream);
        this.outputPath = null;
        this.gzip = false;
    }

    @Override
    public boolean open() {
        /** Open output stream **/
        OutputStreamWriter writer;
        try {
            if (outputStream == null) {
                outputStream = gzip
                        ? new GZIPOutputStream(new FileOutputStream(outputPath.toFile()))
                        : new FileOutputStream(outputPath.toFile());
            }
            writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        /** Initialize Json serializer**/


        JsonFactory factory = new JsonFactory();
        factory.setRootValueSeparator("\n");
        ObjectMapper jsonObjectMapper = new ObjectMapper(factory);
        jsonObjectMapper.addMixIn(VariantAnnotation.class, VariantAnnotationMixin.class);
        jsonObjectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        try {
            sequenceWriter = jsonObjectMapper.writerFor(VariantAnnotation.class).writeValues(writer);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }

    @Override
    public boolean write(List<VariantAnnotation> variantAnnotationList) {
        try {
            sequenceWriter.writeAll(variantAnnotationList);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    @Override
    public boolean close() {
        try {
            sequenceWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return true;
    }
}
