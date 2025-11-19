package org.opencb.opencga.storage.hadoop.variant.pending;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.io.DataReader;
import org.opencb.opencga.storage.core.variant.io.json.VariantJsonReader;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantLocusKey;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class PendingVariantsFileReader implements DataReader<Variant> {

    private final PendingVariantsFileBasedDescriptor descriptor;
    private final FileSystem fs;
    private final Path pendingVariantsDir;
    private final List<String> regions;
    private List<Path> pathsToRead;
    private DataReader<Variant> fileReader;

    public PendingVariantsFileReader(PendingVariantsFileBasedDescriptor descriptor, FileSystem fs, URI pendingVariantsDir,
                                     List<String> regions) {
        this.descriptor = descriptor;
        this.fs = fs;
        this.pendingVariantsDir = new Path(pendingVariantsDir);
        this.regions = regions;
        fileReader = null;
    }

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
            fileReader = new VariantJsonReader(new GZIPInputStream(fs.open(path)), 10 * 1024 * 1024);
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
        while (variants.isEmpty()) {
            List<Variant> read = fileReader.read(batch);
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


    private List<Path> getPendingVariantFiles(List<String> regionsStr) {
        Set<Region> regions = new HashSet<>();
        for (String region : regionsStr) {
            regions.add(Region.parseRegion(region));
        }
        List<Path> pathsToRead = new ArrayList<>();
        try {
            FileStatus[] fileStatuses = fs.listStatus(pendingVariantsDir);
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

}
