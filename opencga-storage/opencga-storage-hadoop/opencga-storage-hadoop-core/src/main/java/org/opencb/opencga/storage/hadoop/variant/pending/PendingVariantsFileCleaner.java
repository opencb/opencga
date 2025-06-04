package org.opencb.opencga.storage.hadoop.variant.pending;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.io.DataWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedHashSet;
import java.util.List;

public final class PendingVariantsFileCleaner implements DataWriter<Variant> {
    private final LinkedHashSet<String> pathsToClean = new LinkedHashSet<>();
    private final PendingVariantsFileBasedDescriptor descriptor;
    private final FileSystem fs;

    private final Logger logger = LoggerFactory.getLogger(PendingVariantsFileCleaner.class);
    private final Path pendingVariantsDir;

    private int deletedFiles = 0;

    PendingVariantsFileCleaner(PendingVariantsFileBasedDescriptor descriptor, FileSystem fs, URI pendingVariantsDir) {
        this.descriptor = descriptor;
        this.fs = fs;
        this.pendingVariantsDir = new Path(pendingVariantsDir);
    }

    @Override
    public boolean write(List<Variant> list) {
        for (Variant variant : list) {
            pathsToClean.add(descriptor.buildFileName(variant));
        }

        cleanFiles(5);
        return true;
    }

    @Override
    public boolean close() {
        // Do not delete any more files, as close method might be invoked even if it fails
        return true;
    }

    public void success() {
        // Successfully finished processing pending variants. Delete all files
        cleanFiles(0);
        logger.info("Deleted " + deletedFiles + " files with pending variants");
    }

    public void abort() {
        // Something went wrong. Do not delete any more files
        if (!pathsToClean.isEmpty()) {
            logger.info("Aborting pending variants file cleaner. Avoid deleting {} pending files", pathsToClean.size());
            pathsToClean.clear();
        }
    }

    private void cleanFiles(int filesLeft) {
        while (pathsToClean.size() > filesLeft) {
            String next = pathsToClean.iterator().next();
            pathsToClean.remove(next);
            try {
                logger.debug("Deleting file {}", next);
                fs.delete(new Path(pendingVariantsDir, next), true);
            } catch (IOException e) {
                throw new RuntimeException("Unable to delete file " + next, e);
            }
            deletedFiles++;
        }
    }
}
