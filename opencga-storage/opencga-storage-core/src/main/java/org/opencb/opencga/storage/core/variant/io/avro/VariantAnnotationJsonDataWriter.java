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

package org.opencb.opencga.storage.core.variant.io.avro;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.storage.core.variant.io.json.VariantAnnotationMixin;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/**
 * Created on 09/11/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantAnnotationJsonDataWriter implements DataWriter<VariantAnnotation> {

    private OutputStream outputStream;
    private ObjectWriter jsonWriter;
    private Path outputPath;
    private boolean gzip;

    public VariantAnnotationJsonDataWriter(Path outputPath, boolean gzip) {
        this.outputPath = outputPath;
        this.gzip = gzip;
    }

    @Override
    public boolean open() {

        /** Open output stream **/
        try {
            if (gzip) {
                outputStream = new GZIPOutputStream(new FileOutputStream(outputPath.toFile()));
            } else {
                outputStream = new FileOutputStream(outputPath.toFile());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    @Override
    public boolean pre() {
        /** Initialize Json serializer**/

        JsonFactory factory = new JsonFactory();
        ObjectMapper jsonObjectMapper = new ObjectMapper(factory);

        jsonObjectMapper.addMixIn(VariantAnnotation.class, VariantAnnotationMixin.class);
        jsonObjectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        jsonWriter = jsonObjectMapper.writerFor(VariantAnnotation.class);
        return true;
    }

    @Override
    public boolean write(List<VariantAnnotation> variantAnnotationList) {
        try {
            for (VariantAnnotation variantAnnotation : variantAnnotationList) {
                outputStream.write(jsonWriter.writeValueAsString(variantAnnotation).getBytes());
                outputStream.write('\n');
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    @Override
    public boolean close() {
        try {
            outputStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return true;
    }
}
