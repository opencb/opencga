package org.opencb.opencga.storage.hadoop.variant.pending;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.common.IOUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Abstract class for managing pending variants in a file-based system.
 *
 * The pending variants are stored in a directory specified by the {@code pendingVariantsDir} URI.
 * Each file in the directory contains a set of pending variants for a specific region.
 * The whole genome is divided into regions of 1Mb, and each file contains the pending variants for a specific region.
 */
public abstract class PendingVariantsFileBasedManager {

    private final PendingVariantsFileBasedDescriptor descriptor;
    private final URI pendingVariantsDir;
    private final FileSystem fs;
    private final Logger logger = LoggerFactory.getLogger(PendingVariantsFileBasedManager.class);

    protected PendingVariantsFileBasedManager(URI pendingVariantsDir, PendingVariantsFileBasedDescriptor descriptor, Configuration conf)
            throws IOException {
        this.pendingVariantsDir = pendingVariantsDir;
        this.descriptor = descriptor;
        fs = FileSystem.get(pendingVariantsDir, conf);
    }

    public PendingVariantsFileReader reader(Query query) {
        List<String> regions = query.getAsStringList(VariantQueryParam.REGION.key());
        return new PendingVariantsFileReader(descriptor, fs, pendingVariantsDir, regions);
    }

    public VariantDBIterator iterator(Query query) {
        return VariantDBIterator.wrapper(reader(query).iterator());
    }

    public PendingVariantsFileCleaner cleaner() {
        return new PendingVariantsFileCleaner(descriptor, fs, pendingVariantsDir);
    }

    public ObjectMap discoverPending(MRExecutor mrExecutor, String variantsTable, boolean overwrite, ObjectMap options)
            throws StorageEngineException {
        return discoverPending(mrExecutor, variantsTable, new QueryOptions(options)
                .append(DiscoverPendingVariantsDriver.OVERWRITE, overwrite));
    }

    public ObjectMap discoverPending(MRExecutor mrExecutor, String variantsTable, ObjectMap options) throws StorageEngineException {
        options = new ObjectMap(options);
        URI outdir = pendingVariantsDir.resolve("scratch/" + TimeUtils.getTime() + "-" + RandomStringUtils.randomAlphanumeric(5) + "/");
        options.put(DiscoverPendingVariantsDriver.OUTPUT_PARAM, outdir.toString());
        ObjectMap result = mrExecutor.run(DiscoverPendingVariantsDriver.class,
                DiscoverPendingVariantsDriver.buildArgs(
                        variantsTable, descriptor.getClass(), options),
                "Discover pending " + descriptor.name() + " variants");

        try {
            // Move the pending variants to the pending variants directory
            logger.info("Moving pending variants from " + outdir + " to " + pendingVariantsDir);
            Set<String> newFiles = new HashSet<>();
            int moved = 0;
            long size = 0;
            FileStatus[] files = fs.listStatus(new Path(outdir));
            for (FileStatus file : files) {
                if (!descriptor.isPendingVariantsFile(file)) {
                    continue;
                }
                try {
                    moved++;
                    size += file.getLen();
                    String name = file.getPath().getName();
                    String cleanName = name.substring(0, name.indexOf(".json")) + ".json.gz";
                    // Check for duplicated new files
                    // Should not be any duplicated file name, as the MRInputStream is aligned to a specific region size
                    // If it were not aligned, multiple mappers might write the same file
                    if (!newFiles.add(cleanName)) {
                        for (FileStatus fileStatus : files) {
                            logger.info(" - " + fileStatus.getPath() + " (" + fileStatus.getLen() + " bytes)"
                                    + (fileStatus.isDirectory() ? " [directory]" : ""));
                        }
                        throw new StorageEngineException("Duplicated file name " + cleanName + " in pending variants directory");
                    }
                    Path dst = new Path(pendingVariantsDir.resolve(UriUtils.toUriRelative(cleanName)));
                    if (fs.exists(dst)) {
                        fs.delete(dst, true);
                    }
                    if (!fs.rename(file.getPath(), dst)) {
                        throw new StorageEngineException("Unable to move file " + file.getPath() + " to " + dst);
                    }
                } catch (IOException e) {
                    throw new StorageEngineException("Unable to move file " + file.getPath() + " to " + pendingVariantsDir, e);
                }
            }
            logger.info("Moved " + moved + " pending variant files to " + pendingVariantsDir);
            logger.info(" - size: " + IOUtils.humanReadableByteCount(size, false));
            logger.info("Deleting temporary directory " + outdir);
            fs.delete(new Path(outdir), true);
        } catch (IOException e) {
            throw new StorageEngineException("Unable to list files in pending variants directory", e);
        }

        return result;
    }

    public boolean exists() throws StorageEngineException {
        try {
            return fs.exists(new Path(pendingVariantsDir));
        } catch (IOException e) {
            throw new StorageEngineException("Unable to check if pending variants directory exists", e);
        }
    }

    public void delete() throws StorageEngineException {
        try {
            fs.delete(new Path(pendingVariantsDir), true);
        } catch (IOException e) {
            throw new StorageEngineException("Unable to delete pending variants directory", e);
        }
    }

    public boolean checkFilesIntegrity() throws IOException {
        // Ensure that all pending files are valid
        Path pendingPath = new Path(pendingVariantsDir);
        if (!fs.exists(pendingPath)) {
            logger.warn("Pending variants directory does not exist: " + pendingPath);
            return false;
        }
        FileStatus[] fileStatuses = fs.listStatus(pendingPath);
        for (FileStatus fileStatus : fileStatuses) {
            if (!descriptor.isPendingVariantsFile(fileStatus)) {
                // Ignore other files
                continue;
            }
            try {
                // Check if the file is readable
                fs.open(fileStatus.getPath()).close();
            } catch (IOException e) {
                logger.error("Error reading pending variants file: " + fileStatus.getPath(), e);
                return false;
            }
        }
        return true;
    }

    public URI getPendingVariantsDir() {
        return pendingVariantsDir;
    }
}
