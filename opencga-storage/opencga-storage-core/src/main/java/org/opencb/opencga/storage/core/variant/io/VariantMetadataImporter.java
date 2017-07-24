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
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.storage.core.metadata.ExportMetadata;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created on 12/12/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantMetadataImporter {

    public ExportMetadata importMetaData(URI inputUri, StudyConfigurationManager scm) throws IOException {

        // Check if can be loaded
        Map<String, Integer> studies = scm.getStudies(QueryOptions.empty());
        if (!studies.isEmpty()) {
            throw new IllegalStateException("Unable to import data if there are other loaded studies: " + studies);
        }

        // Load metadata
        ExportMetadata exportMetadata = readMetadata(inputUri);

        // Get list of returned samples
        Map<Integer, List<Integer>> returnedSamples = getReturnedSamplesMap(exportMetadata);

        for (StudyConfiguration studyConfiguration : exportMetadata.getStudies()) {
            processStudyConfiguration(returnedSamples, studyConfiguration);
            scm.updateStudyConfiguration(studyConfiguration, QueryOptions.empty());
        }

        return exportMetadata;
    }

    protected void processStudyConfiguration(Map<Integer, List<Integer>> returnedSamples, StudyConfiguration studyConfiguration) {

        // Remove non indexed files
        LinkedHashSet<Integer> indexedFiles = studyConfiguration.getIndexedFiles();
        for (Iterator<Integer> iterator = studyConfiguration.getFileIds().values().iterator(); iterator.hasNext();) {
            Integer fileId = iterator.next();
            if (!indexedFiles.contains(fileId)) {
                iterator.remove();
            }
        }

        for (Iterator<Integer> iterator = studyConfiguration.getSamplesInFiles().keySet().iterator(); iterator.hasNext();) {
            Integer fileId = iterator.next();
            if (!indexedFiles.contains(fileId)) {
                iterator.remove();
            }
        }


        if (returnedSamples != null) {
            List<Integer> samples = returnedSamples.get(studyConfiguration.getStudyId());
            // Remove missing samples from StudyConfiguration
            if (samples != null) {
                Iterator<Map.Entry<String, Integer>> iterator = studyConfiguration.getSampleIds().entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, Integer> entry = iterator.next();
                    if (!samples.contains(entry.getValue())) {
                        iterator.remove();
                        for (LinkedHashSet<Integer> samplesInFile : studyConfiguration.getSamplesInFiles().values()) {
                            samplesInFile.remove(entry.getValue());
                        }
                        for (Set<Integer> samplesInCohort : studyConfiguration.getCohorts().values()) {
                            samplesInCohort.remove(entry.getValue());
                        }
                    }
                }
            }
        }
    }

    protected Map<Integer, List<Integer>> getReturnedSamplesMap(ExportMetadata exportMetadata) {
        Map<Integer, List<Integer>> returnedSamples;
        if (exportMetadata.getQuery() != null) {
            Map<Integer, StudyConfiguration> studyConfigurationMap = new HashMap<>();
            for (StudyConfiguration studyConfiguration : exportMetadata.getStudies()) {
                studyConfigurationMap.put(studyConfiguration.getStudyId(), studyConfiguration);
            }

            returnedSamples = VariantQueryUtils.getReturnedSamples(exportMetadata.getQuery(), exportMetadata.getQueryOptions(),
                    studyConfigurationMap.keySet(), studyConfigurationMap::get);
        } else {
            returnedSamples = null;
        }
        return returnedSamples;
    }

    public static ExportMetadata readMetadata(URI inputUri) throws IOException {
        ExportMetadata exportMetadata;
        try (InputStream is = FileUtils.newInputStream(Paths.get(inputUri.getPath() + VariantExporter.METADATA_FILE_EXTENSION))) {
            ObjectMapper objectMapper = new ObjectMapper();
            exportMetadata = objectMapper.readValue(is, ExportMetadata.class);
        }
        return exportMetadata;
    }

}
