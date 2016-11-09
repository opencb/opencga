package org.opencb.opencga.storage.core.alignment.local;

import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.tools.alignment.BamManager;
import org.opencb.biodata.tools.alignment.BamUtils;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.storage.core.StorageETL;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by pfurio on 31/10/16.
 */
public class LocalAlignmentStorageETL implements StorageETL {

    public LocalAlignmentStorageETL() {
        super();
    }

    @Override
    public URI extract(URI input, URI ouput) throws StorageManagerException {
        return input;
    }

    @Override
    public URI preTransform(URI input) throws IOException, FileFormatException, StorageManagerException {
        // Check if a BAM file is passed and it is sorted.
        // Only binaries and sorted BAM files are accepted at this point.
        Path inputPath = Paths.get(input.getRawPath());
        BamUtils.checkBamOrCramFile(new FileInputStream(inputPath.toFile()), inputPath.getFileName().toString(), true);
        return input;
    }

    @Override
    public URI transform(URI input, URI pedigree, URI output) throws Exception {
        Path path = Paths.get(input.getRawPath());
        FileUtils.checkFile(path);

        Path workspace = Paths.get(output.getRawPath());
        FileUtils.checkDirectory(workspace);

        // 1. Check if the bai does not exist and create it
        BamManager bamManager = new BamManager(path);
        if (!path.getParent().resolve(path.getFileName().toString() + ".bai").toFile().exists()) {
            bamManager.createIndex();
        }
        return input;
    }

    @Override
    public URI postTransform(URI input) throws Exception {
        return input;
    }

    @Override
    public URI preLoad(URI input, URI output) throws IOException, StorageManagerException {
        return null;
    }

    @Override
    public URI load(URI input) throws IOException, StorageManagerException {
        return null;
    }

    @Override
    public URI postLoad(URI input, URI output) throws IOException, StorageManagerException {
        return null;
    }
}
