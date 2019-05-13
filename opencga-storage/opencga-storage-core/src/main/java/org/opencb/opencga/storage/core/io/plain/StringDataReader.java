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

import org.opencb.commons.io.DataReader;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.io.managers.IOManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.zip.GZIPInputStream;

/**
 * Created by jacobo on 25/02/15.
 */
public class StringDataReader implements DataReader<String> {

    private final URI uri;
    private final IOManager ioManager;
    protected BufferedReader reader;
    protected final Path path;
    protected static Logger logger = LoggerFactory.getLogger(StringDataReader.class);
    protected long readLines = 0L;
    protected long lastAvailable = 0;
    private SizeInputStream sizeInputStream;
    private BiConsumer<Long, Long> readBytesListener;
    private BiConsumer<Long, Long> readLinesListener;
    private final InputStream is;
    private final boolean closeReader;

    public StringDataReader(Path path) {
        this.path = Objects.requireNonNull(path);
        this.is = null;
        closeReader = true;
        this.uri = null;
        this.ioManager = null;
    }

    public StringDataReader(InputStream is) {
        this(is, false);
    }

    public StringDataReader(InputStream is, boolean closeStream) {
        this.is = Objects.requireNonNull(is);
        this.closeReader = closeStream;
        this.path = null;
        this.uri = null;
        this.ioManager = null;
    }

    public StringDataReader(URI uri, IOManager ioManager) {
        this.uri = Objects.requireNonNull(uri);
        this.ioManager = Objects.requireNonNull(ioManager);
        this.is = null;
        this.path = null;
        this.closeReader = true;
    }

    @Override
    public boolean open() {
        try {
            if (is != null) {
                sizeInputStream = new SizeInputStream(is, 0);
                this.reader = new BufferedReader(new InputStreamReader(sizeInputStream));
            } else {
                String fileName;
                if (uri != null) {
                    fileName = UriUtils.fileName(uri);
                    lastAvailable = ioManager.size(uri);
                    sizeInputStream = new SizeInputStream(ioManager.newInputStreamRaw(uri), lastAvailable);
                } else {
                    fileName = path.toFile().getName();
                    lastAvailable = getFileSize();
                    sizeInputStream = new SizeInputStream(new FileInputStream(path.toFile()), lastAvailable);
                }
                if (fileName.endsWith(".gz")) {
                    logger.debug("Gzip input compress");
                    this.reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(sizeInputStream)));
                } else if (fileName.endsWith(".snappy") || fileName.endsWith(".snz")) {
                    logger.debug("Snappy input compress");
                    this.reader = new BufferedReader(new InputStreamReader(new SnappyInputStream(sizeInputStream)));
                } else {
                    logger.debug("Plain input compress");
//                this.reader = Files.newBufferedReader(path, Charset.defaultCharset());
                    this.reader = new BufferedReader(new InputStreamReader(sizeInputStream));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }

    @Override
    public boolean close() {
        try {
            if (closeReader) {
                reader.close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }

    @Override
    public List<String> read() {
        try {
            String line = reader.readLine();
            if (line == null) {
                return Collections.emptyList();
            } else {
                onReadBytes();
                onReadLine();
                return Collections.singletonList(line);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public List<String> read(int batchSize) {
        List<String> batch = new ArrayList<>(batchSize);
        try {
            for (int i = 0; i < batchSize; i++) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                batch.add(line);
                onReadLine();
            }
            onReadBytes();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return batch;
    }

    protected void onReadLine() {
        if (++readLines % 100000 == 0) {
            logger.debug("read lines = " + readLines);
        }
        if (readLinesListener != null) {
            readLinesListener.accept(readLines, 1L);
        }
    }

    private void onReadBytes() throws IOException {
        long newAvailable = sizeInputStream.availableLong();
        if (readBytesListener != null) {
            readBytesListener.accept(sizeInputStream.size() - newAvailable, lastAvailable - newAvailable);
        }
//        logger.info((sizeInputStream.size - newAvailable) + "/" + sizeInputStream.size + " : " + (lastAvailable - newAvailable));
        lastAvailable = newAvailable;
        this.readLines += readLines;
    }

    public StringDataReader setReadBytesListener(BiConsumer<Long, Long> readBytesListener) {
        this.readBytesListener = readBytesListener;
        return this;
    }

    public StringDataReader setReadLinesListener(BiConsumer<Long, Long> readLinesListener) {
        this.readLinesListener = readLinesListener;
        return this;
    }

    public long getFileSize() throws IOException {
        if (path != null) {
            return Files.size(path);
        } else {
            return -1;
        }
    }

    private static class SizeInputStream extends InputStream {
        // The InputStream to read bytes from
        private InputStream in = null;

        // The number of bytes that can be read from the InputStream
        private long size = 0;

        // The number of bytes that have been read from the InputStream
        private long bytesRead = 0;

        SizeInputStream(InputStream in, long size) {
            super();
            this.in = in;
            this.size = size;
        }

        /**
         * Do not overwrite {@link InputStream#available()}.
         * Return long instead of int.
         * @return An estimate of the number of bytes that can be read
         */
        public long availableLong() {
            return (size - bytesRead);
        }

        public long size() {
            return size;
        }

        @Override
        public int read() throws IOException {
            int b = in.read();
            if (b != -1) {
                bytesRead++;
            }
            return b;
        }

        @Override
        public int read(byte[] b) throws IOException {
            int read = in.read(b);
            bytesRead += read;
            return read;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int read = in.read(b, off, len);
            bytesRead += read;
            return read;
        }

        @Override
        public long skip(long n) throws IOException {
            return in.skip(n);
        }

        @Override
        public int available() throws IOException {
            return in.available();
        }

        @Override
        public void close() throws IOException {
            in.close();
        }

        @Override
        public synchronized void mark(int readlimit) {
            in.mark(readlimit);
        }

        @Override
        public synchronized void reset() throws IOException {
            in.reset();
        }

        @Override
        public boolean markSupported() {
            return in.markSupported();
        }

    }

}
