package org.opencb.opencga.storage.core.io.proto;


import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opencb.commons.io.DataWriter;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by mh719 on 11/05/2016.
 */
public class ProtoFileWriter<T extends com.google.protobuf.GeneratedMessageV3> implements DataWriter<T> {
    private String compression = StringUtils.EMPTY;
    protected Logger logger = LogManager.getLogger(this.getClass().toString());

    private OutputStream outputStream = null;
    private Path input = null;
    private final boolean closeStream;
    private final AtomicLong timeWrite = new AtomicLong(0);

    public ProtoFileWriter(OutputStream outputStream) {
        this(outputStream, false);
    }

    public ProtoFileWriter(OutputStream outputStream, boolean closeStream) {
        this.outputStream = Objects.requireNonNull(outputStream);
        this.closeStream = closeStream;
    }

    public ProtoFileWriter(Path input) {
        this.input = Objects.requireNonNull(input);
        closeStream = true;
    }

    public ProtoFileWriter(Path input, String compression) {
        this(input);
        this.compression = Objects.requireNonNull(compression);
    }

    @Override
    public boolean open() {
        if (outputStream == null) {
            try {
                OutputStream out = new BufferedOutputStream(new FileOutputStream(input.toFile()));
                if (StringUtils.equalsIgnoreCase(compression, "gzip") || StringUtils.equalsIgnoreCase(compression, "gz")) {
                    out = new GzipCompressorOutputStream(out);
                } else if (StringUtils.isNotBlank(compression)) {
                    throw new NotImplementedException("Proto compression not implemented yet: " + compression);
                }
                outputStream = out;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return true;
    }

    @Override
    public boolean close() {
        try {
            this.outputStream.flush();
        } catch (IOException e) {
            logger.error("Problems flushing outputstream", e);
            return false;
        }
        if (closeStream) {
            try {
                this.outputStream.close();
            } catch (IOException e) {
                logger.error("Problems closing outputstream", e);
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean pre() {
        return false;
    }

    @Override
    public boolean post() {
        return false;
    }

    @Override
    public boolean write(T elem) {
        try {
            elem.writeDelimitedTo(this.outputStream);
        } catch (IOException e) {
            throw new IllegalStateException("Problems writing Proto element ot output stream!!! " + elem, e);
        }
        return true;
    }

    @Override
    public boolean write(List<T> batch) {
        batch.forEach(e -> write(e));
        return true;
    }
}
