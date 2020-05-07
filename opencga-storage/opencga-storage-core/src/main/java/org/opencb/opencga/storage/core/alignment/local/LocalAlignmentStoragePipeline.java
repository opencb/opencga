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

package org.opencb.opencga.storage.core.alignment.local;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.tools.alignment.BamManager;
import org.opencb.biodata.tools.alignment.BamUtils;
import org.opencb.biodata.tools.alignment.stats.AlignmentGlobalStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.storage.core.StoragePipeline;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by pfurio on 31/10/16.
 */
@Deprecated
public class LocalAlignmentStoragePipeline implements StoragePipeline {

    private final ObjectMap configuration;

    public LocalAlignmentStoragePipeline(ObjectMap configuration) {
        super();
        this.configuration = configuration;
    }

    @Override
    public URI extract(URI input, URI output) throws StorageEngineException {
        return input;
    }

    @Override
    public URI preTransform(URI input) throws IOException, FileFormatException, StorageEngineException {
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

        // 1) Check if the bai does not exist and create it
        BamManager bamManager = new BamManager(path);
        if (!path.getParent().resolve(path.getFileName().toString() + ".bai").toFile().exists()) {
            bamManager.createIndex();
        }

        // 2) Calculate stats and store in a file
        Path statsPath = workspace.resolve(path.getFileName() + ".stats");
        if (!statsPath.toFile().exists()) {
            AlignmentGlobalStats stats = bamManager.stats();
            ObjectMapper objectMapper = new ObjectMapper();
            ObjectWriter objectWriter = objectMapper.writerFor(AlignmentGlobalStats.class);
            objectWriter.writeValue(statsPath.toFile(), stats);
        }

        // 3) Create the BigWig file containing the coverage using the bamCoverage from the DeepTools package
        Path bwPath = workspace.resolve(path.getFileName() + BamManager.COVERAGE_BIGWIG_EXTENSION);
        int windowSize = configuration.getInt(AlignmentStorageOptions.BIG_WIG_WINDOWS_SIZE.key(),
                AlignmentStorageOptions.BIG_WIG_WINDOWS_SIZE.defaultValue());
        bamManager.calculateBigWigCoverage(bwPath, windowSize);

        return input;
    }

    @Override
    public URI postTransform(URI input) throws Exception {
        return input;
    }

    @Override
    public URI preLoad(URI input, URI output) throws IOException, StorageEngineException {
        return null;
    }

    @Override
    public URI load(URI input, URI outdir) throws IOException, StorageEngineException {
        return null;
    }

    @Override
    public URI postLoad(URI input, URI output) throws IOException, StorageEngineException {
        return null;
    }
}
