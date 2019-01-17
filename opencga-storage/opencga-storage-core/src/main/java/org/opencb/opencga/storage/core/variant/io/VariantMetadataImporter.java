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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.VariantMetadataConverter;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * Created on 12/12/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantMetadataImporter {

    public VariantMetadata importMetaData(URI inputUri, VariantStorageMetadataManager scm) throws IOException {

        // Check if can be loaded
        Map<String, Integer> studies = scm.getStudies(QueryOptions.empty());
        if (!studies.isEmpty()) {
            throw new IllegalStateException("Unable to import data if there are other loaded studies: " + studies);
        }

        // Load metadata
        VariantMetadata metadata = readMetadata(inputUri);
        List<StudyConfiguration> studyConfigurations = new VariantMetadataConverter(scm).toStudyConfigurations(metadata);

        for (StudyConfiguration studyConfiguration : studyConfigurations) {
            processStudyConfiguration(studyConfiguration);
            scm.updateStudyConfiguration(studyConfiguration, QueryOptions.empty());
        }

        return metadata;
    }

    protected void processStudyConfiguration(StudyConfiguration studyConfiguration) {

        // Remove non indexed files
        LinkedHashSet<Integer> indexedFiles = studyConfiguration.getIndexedFiles();
        studyConfiguration.getFileIds().values().removeIf(fileId -> !indexedFiles.contains(fileId));

        studyConfiguration.getSamplesInFiles().keySet().removeIf(fileId -> !indexedFiles.contains(fileId));

    }

    public static VariantMetadata readMetadata(URI inputUri) throws IOException {
        try (InputStream is = FileUtils.newInputStream(Paths.get(inputUri.getPath() + VariantExporter.METADATA_FILE_EXTENSION))) {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(is, VariantMetadata.class);
        }
    }

}
