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

package org.opencb.opencga.storage.core.io.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.commons.io.DataReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created on 02/12/16.
 *
 * Generic Json writer from an input stream.
 *
 * TODO: Move to Java Common Libs
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class JsonDataReader<T>  implements DataReader<T> {
    private final JsonFactory jsonFactory;
    private final ObjectMapper objectMapper;
    private final Class<T> clazz;
    private final InputStream inputStream;
    private JsonParser parser;
    private long numReads;

    public JsonDataReader(Class<T> clazz, InputStream inputStream) {
        this.clazz = clazz;
        this.inputStream = inputStream;
        jsonFactory = new JsonFactory();
        objectMapper = new ObjectMapper(jsonFactory);
    }

    @Override
    public boolean open() {
        numReads = 0;
        try {
            parser = jsonFactory.createParser(inputStream);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }

    @Override
    public List<T> read(int batchSize) {
        List<T> batch = new ArrayList<>(batchSize);
        try {
            while (parser.nextToken() != null && batch.size() < batchSize) {
                numReads++;
                batch.add(parser.readValueAs(clazz));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return batch;
    }

    @Override
    public boolean close() {
        try {
            parser.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }

    public void addMixIn(Class<?> target, Class<?> mixinSource) {
        objectMapper.addMixIn(target, mixinSource);
    }

    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    public long getNumReads() {
        return numReads;
    }
}
