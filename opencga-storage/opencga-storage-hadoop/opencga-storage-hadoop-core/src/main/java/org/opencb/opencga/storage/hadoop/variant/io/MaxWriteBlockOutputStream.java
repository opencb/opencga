package org.opencb.opencga.storage.hadoop.variant.io;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * MaxWriteBlockOutputStream is a {@link FilterOutputStream} that writes blocks of a maximum size.
 * <p>
 * If the block size is greater than the maximum block size, it will split the block in smaller blocks of the maximum size.
 * <p>
 *  This class is used to avoid writing large blocks into Azure Blob Storage. Azure Blob Storage has a limit of 4MB per block.
 *  See <a href="https://learn.microsoft.com/en-us/troubleshoot/azure/azure-storage/blobs/connectivity/request-body-large">
 *      Request body too large</a>.
 */
public class MaxWriteBlockOutputStream extends FilterOutputStream {

    private final int maxBlockSize;

    public MaxWriteBlockOutputStream(OutputStream out) {
        this(out, 1024 * 1024 * 2);
    }

    public MaxWriteBlockOutputStream(OutputStream out, int maxBlockSize) {
        super(out);
        this.maxBlockSize = maxBlockSize;
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) throws IOException {
        if (len > maxBlockSize) {
            int start = 0;
            while (start < len) {
                int blockLength = Math.min(maxBlockSize, len - start);
                out.write(b, off + start, blockLength);
                start += blockLength;
            }
        } else {
            out.write(b, off, len);
        }
    }
}
