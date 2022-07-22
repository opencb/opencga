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
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.io.DataReader;
import org.opencb.opencga.storage.core.variant.io.json.mixin.ConsequenceTypeMixin;
import org.opencb.opencga.storage.core.variant.io.json.mixin.VariantAnnotationMixin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Created on 09/11/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantAnnotationJsonDataReader implements DataReader<VariantAnnotation> {
    private InputStream inputStream = null;
    private JsonParser parser;
    private int readsCounter;
    private File file;
    protected Logger logger = LoggerFactory.getLogger(this.getClass().toString());

    public VariantAnnotationJsonDataReader(File file) {
        readsCounter = 0;
        this.file = file;
    }

    public VariantAnnotationJsonDataReader(InputStream inputStream) {
        this.inputStream = inputStream;
        readsCounter = 0;
    }

    @Override
    public boolean open() {

        /** Open input stream **/
        if (inputStream == null) {
            try {
                inputStream = new FileInputStream(file);
                inputStream = new GZIPInputStream(inputStream);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        /** Innitialice Json parse**/
        JsonFactory factory = new JsonFactory();
        ObjectMapper jsonObjectMapper = new ObjectMapper(factory);

        jsonObjectMapper.addMixIn(VariantAnnotation.class, VariantAnnotationMixin.class);
        jsonObjectMapper.addMixIn(ConsequenceType.class, ConsequenceTypeMixin.class);
        jsonObjectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        try {
            parser = factory.createParser(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return true;
    }

    @Override
    public List<VariantAnnotation> read(int batchSize) {
        List<VariantAnnotation> batch = new ArrayList<>(batchSize);
        try {
            for (int i = 0; i < batchSize && parser.nextToken() != null; i++) {
                batch.add(parser.readValueAs(VariantAnnotation.class));
                readsCounter++;
                if (readsCounter % 1000 == 0) {
                    logger.debug("Element {}", readsCounter);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return batch;
    }

    @Override
    public boolean close() {

        try {
            inputStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return true;
    }
}
