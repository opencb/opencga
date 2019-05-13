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

package org.opencb.opencga.storage.core.variant.io;

import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.commons.utils.FileUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Created on 08/12/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class AbstractVariantReader implements VariantReader {

    private Path metadataPath;
    private final Map<String, LinkedHashMap<String, Integer>> samplesPositions;
    private InputStream metadataInputStream;
    private LinkedHashMap<String, Integer> samplesPosition;
    private VariantFileMetadata fileMetadata;

    public AbstractVariantReader(Path metadataPath, VariantStudyMetadata metadata) {
        this.metadataPath = metadataPath;
        this.samplesPositions = null;
        if (metadata.getFiles().isEmpty()) {
            fileMetadata = new VariantFileMetadata("", "");
            metadata.getFiles().add(fileMetadata.getImpl());
        } else {
            fileMetadata = new VariantFileMetadata(metadata.getFiles().get(0));
        }
    }

    public AbstractVariantReader(InputStream metadataInputStream, VariantStudyMetadata metadata) {
        this.metadataInputStream = metadataInputStream;
        this.samplesPositions = null;
        if (metadata.getFiles().isEmpty()) {
            fileMetadata = new VariantFileMetadata("", "");
            metadata.getFiles().add(fileMetadata.getImpl());
        } else {
            fileMetadata = new VariantFileMetadata(metadata.getFiles().get(0));
        }
    }

    public AbstractVariantReader(Map<String, LinkedHashMap<String, Integer>> samplesPositions) {
        this.metadataPath = null;
        this.samplesPositions = samplesPositions;
    }

    @Override
    public boolean pre() {

        try {
            if (metadataPath != null) {
                if (!Files.exists(metadataPath)) {
                    throw new FileNotFoundException(metadataPath.toString());
                }
                this.metadataInputStream = FileUtils.newInputStream(metadataPath);
            }
            if (metadataInputStream != null) {
                // Read global JSON file and copy its info into the already available VariantSource object
                VariantFileMetadata readMetadata = VariantReaderUtils.readVariantFileMetadataFromJson(metadataInputStream);

                fileMetadata.setId(readMetadata.getId());
                fileMetadata.setPath(readMetadata.getPath());
                fileMetadata.setHeader(readMetadata.getHeader());
                fileMetadata.setSamplesPosition(readMetadata.getSamplesPosition());
                fileMetadata.setStats(readMetadata.getStats());
                metadataInputStream.close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        if (fileMetadata != null) {
            Map<String, Integer> samplesPosition = fileMetadata.getSamplesPosition();
            this.samplesPosition = new LinkedHashMap<>(samplesPosition.size());
            String[] samples = new String[samplesPosition.size()];
            for (Map.Entry<String, Integer> entry : samplesPosition.entrySet()) {
                samples[entry.getValue()] = entry.getKey();
            }

            for (int i = 0; i < samples.length; i++) {
                this.samplesPosition.put(samples[i], i);
            }
        }
        return true;
    }

    protected List<Variant> addSamplesPosition(List<Variant> variants) {
        if (samplesPositions != null) {
            for (Variant variant : variants) {
                for (StudyEntry studyEntry : variant.getStudies()) {
                    LinkedHashMap<String, Integer> samplesPosition = samplesPositions.get(studyEntry.getStudyId());
                    if (samplesPosition != null) {
                        studyEntry.setSortedSamplesPosition(samplesPosition);
                    }
                }
            }
        } else {
            for (Variant variant : variants) {
                if (variant.getStudies().size() == 1) {
                    variant.getStudies().get(0).setSortedSamplesPosition(samplesPosition);
                }
            }
        }

        return variants;

    }


    @Override
    public List<String> getSampleNames() {
        return new ArrayList<>(fileMetadata.getSamplesPosition().keySet());
    }

//    @Override
//    public String getHeader() {
//        return metadata.getMetadata().get(VariantFileUtils.VARIANT_FILE_HEADER).toString();
//    }

    @Override
    public VariantFileMetadata getVariantFileMetadata() {
        return fileMetadata;
    }
}
