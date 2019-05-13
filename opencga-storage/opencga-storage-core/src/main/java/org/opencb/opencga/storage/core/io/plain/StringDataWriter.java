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

package org.opencb.opencga.storage.core.io.plain;

import org.opencb.commons.io.DataWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyOutputStream;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.zip.GZIPOutputStream;

/**
 * Created by jacobo on 25/02/15.
 */
public class StringDataWriter implements DataWriter<String> {

    protected OutputStream os;
    protected final Path path;
    protected final boolean endLine;
    protected long writtenLines = 0L;
    protected final boolean closeOutputStream;

    protected static Logger logger = LoggerFactory.getLogger(StringDataWriter.class);

    public StringDataWriter(Path path) {
        this(path, false);
    }

    public StringDataWriter(Path path, boolean endLine) {
        this.path = Objects.requireNonNull(path);
        this.os = null;
        this.endLine = endLine;
        this.closeOutputStream = true;
    }

    public StringDataWriter(OutputStream os, boolean endLine) {
        this(os, endLine, false);
    }

    public StringDataWriter(OutputStream os, boolean endLine, boolean closeOutputStream) {
        this.path = null;
        this.os = Objects.requireNonNull(os);
        this.endLine = endLine;
        this.closeOutputStream = closeOutputStream;
    }

    public static void write(Path path, List<String> batch) {
        StringDataWriter writer = new StringDataWriter(path);
        writer.open();
        writer.pre();

        writer.write(batch);

        writer.post();
        writer.close();
    }

    @Override
    public boolean open() {
        if (os != null) {
            // Nothing to do!
            return true;
        }
        try {
            String fileName = path.toFile().getName();
            if (fileName.endsWith(".gz")) {
                logger.debug("Gzip output compress");
                os = new GZIPOutputStream(new FileOutputStream(path.toAbsolutePath().toString()));
            } else if (fileName.endsWith(".snappy") || fileName.endsWith(".snz")) {
                logger.debug("Snappy output compress");
                os = new SnappyOutputStream(new FileOutputStream(path.toAbsolutePath().toString()));
            } else {
                logger.debug("Plain output");
                os = new FileOutputStream(path.toAbsolutePath().toString());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return true;
    }

    @Override
    public boolean close() {
        try {
            os.flush();
            if (closeOutputStream) {
                os.close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
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
    public boolean write(String elem) {
        try {
            if (++writtenLines % 1000 == 0) {
                logger.debug("written lines = {}", writtenLines);
            }
            os.write(elem.getBytes());
            if (endLine) {
                os.write('\n');
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }

    @Override
    public boolean write(List<String> batch) {
        try {
            long start = System.currentTimeMillis();
            for (String b : batch) {
                if (++writtenLines % 1000 == 0) {
                    logger.debug("written lines = {}", writtenLines);
                }
                os.write(b.getBytes());
                if (endLine) {
                    os.write('\n');
                }
            }
            logger.debug("another batch of {} elements written. time: {}ms", batch.size(), System.currentTimeMillis() - start);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }
}
