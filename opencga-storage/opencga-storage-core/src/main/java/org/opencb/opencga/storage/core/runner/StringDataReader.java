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

package org.opencb.opencga.storage.core.runner;

import org.opencb.commons.io.DataReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Created by jacobo on 25/02/15.
 */
public class StringDataReader implements DataReader<String> {

    protected BufferedReader reader;
    protected final Path path;
    protected long readLines = 0L;

    protected static Logger logger = LoggerFactory.getLogger(StringDataReader.class);

    public StringDataReader(Path path) {
        this.path = path;
    }

    @Override
    public boolean open() {
        try {
            String fileName = path.toFile().getName();
            if (fileName.endsWith(".gz")) {
                logger.info("Gzip input compress");
                this.reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(path.toFile()))));
            } else if (fileName.endsWith(".snappy") || fileName.endsWith(".snz")) {
                logger.info("Snappy input compress");
                this.reader = new BufferedReader(new InputStreamReader(new SnappyInputStream(new FileInputStream(path.toFile()))));
            } else {
                logger.info("Plain input compress");
                this.reader = Files.newBufferedReader(path, Charset.defaultCharset());
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean close() {
        try {
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean pre() {
        return true;
    }

    @Override
    public boolean post() {
        return true;
    }

    @Override
    public List<String> read() {
        try {
            if (++readLines % 1000 == 0) {
                logger.info("read lines = " + readLines);
            }
            return Collections.singletonList(reader.readLine());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<String> read(int batchSize) {
        List<String> batch = new ArrayList<>(batchSize);
        try {
            for (int i = 0; i < batchSize; i++) {
                String line = reader.readLine();
                if (line == null) {
                    return batch;
                }
                batch.add(line);
                if (++readLines % 1000 == 0) {
                    logger.info("read lines = " + readLines);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return batch;
    }
}
