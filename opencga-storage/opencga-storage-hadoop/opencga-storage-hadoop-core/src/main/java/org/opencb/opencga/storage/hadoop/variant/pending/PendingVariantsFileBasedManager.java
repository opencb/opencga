package org.opencb.opencga.storage.hadoop.variant.pending;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.core.common.IOUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.io.json.VariantJsonReader;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantLocusKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.zip.GZIPInputStream;

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

    public DataReader<Variant> reader(Query query) {
        List<String> regions = query.getAsStringList(VariantQueryParam.REGION.key());
        return new DataReader<Variant>() {

            private List<Path> pathsToRead;
            private DataReader<Variant> fileReader = null;

            @Override
            public boolean open() {
                pathsToRead = getPendingVariantFiles(regions);
                return true;
            }

            @Override
            public boolean pre() {
                nextFile();
                return true;
            }

            private void nextFile() {
                if (fileReader != null) {
                    fileReader.post();
                    fileReader.close();
                }
                if (pathsToRead.isEmpty()) {
                    fileReader = null;
                    return;
                }
                Path path = pathsToRead.remove(0);
                try {
                    fileReader = new VariantJsonReader(new GZIPInputStream(fs.open(path)));
                    fileReader.open();
                    fileReader.pre();
                } catch (IOException e) {
                    throw new RuntimeException("Unable to open file " + path, e);
                }
            }

            @Override
            public List<Variant> read(int batch) {
                if (fileReader == null) {
                    return Collections.emptyList();
                }
                List<Variant> variants = new ArrayList<>(batch);
                while (variants.size() < batch) {
                    List<Variant> read = fileReader.read(batch - variants.size());
                    if (read.isEmpty()) {
                        nextFile();
                        if (fileReader == null) {
                            break;
                        }
                    } else {
                        variants.addAll(read);
                    }
                }
                return variants;
            }

            @Override
            public boolean close() {
                if (fileReader != null) {
                    fileReader.post();
                    fileReader.close();
                }
                return true;
            }
        };
    }

    private List<Path> getPendingVariantFiles(List<String> regionsStr) {
        Set<Region> regions = new HashSet<>();
        for (String region : regionsStr) {
            regions.add(Region.parseRegion(region));
        }
        List<Path> pathsToRead = new ArrayList<>();
        try {
            FileStatus[] fileStatuses = fs.listStatus(new Path(pendingVariantsDir));
            Arrays.sort(fileStatuses, Comparator.comparing(FileStatus::getPath));
            for (FileStatus fileStatus : fileStatuses) {
                if (!descriptor.isPendingVariantsFile(fileStatus)) {
                    // Ignore other files
                    continue;
                }
                if (!regions.isEmpty()) {
                    String name = fileStatus.getPath().getName();

                    // Extract region
                    VariantLocusKey locusKey = descriptor.getLocusFromFileName(name);
                    boolean anyMatch = false;
                    for (Region region : regions) {
                        if (region.contains(locusKey.getChromosome(), locusKey.getPosition())) {
                            // Found a matching region
                            anyMatch = true;
                            break;
                        }
                    }
                    if (anyMatch) {
                        pathsToRead.add(fileStatus.getPath());
                    }
                } else {
                    pathsToRead.add(fileStatus.getPath());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to list files in pending variants directory", e);
        }
        return pathsToRead;
    }

    public VariantDBIterator iterator(Query query) {
        return VariantDBIterator.wrapper(reader(query).iterator());
    }

    public PendingVariantsFileCleaner cleaner() {
        return new PendingVariantsFileCleaner();
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
                    fs.rename(file.getPath(), new Path(pendingVariantsDir.resolve(cleanName)));
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

    public final class PendingVariantsFileCleaner implements DataWriter<Variant> {
        private final LinkedHashSet<String> pathsToClean = new LinkedHashSet<>();
        private int deletedFiles = 0;

        private PendingVariantsFileCleaner() {
        }

        @Override
        public boolean write(List<Variant> list) {
            Set<String> files = new HashSet<>();
            for (Variant variant : list) {
                files.addAll(descriptor.buildFileName(variant.getChromosome(), variant.getStart(), variant.getEnd()));
            }
            pathsToClean.addAll(files);

            cleanFiles(5);
            return true;
        }

        @Override
        public boolean close() {
            cleanFiles(0);
            logger.info("Deleted " + deletedFiles + " files with pending variants");
            return true;
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
//                        logger.info("Deleting file " + next);
                    fs.delete(new Path(new Path(pendingVariantsDir), next), true);
                    deletedFiles++;
                } catch (IOException e) {
                    throw new RuntimeException("Unable to delete file " + next, e);
                }
            }
        }
    }
}
