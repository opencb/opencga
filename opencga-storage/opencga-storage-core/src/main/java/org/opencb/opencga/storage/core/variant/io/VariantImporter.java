package org.opencb.opencga.storage.core.variant.io;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.metadata.ExportMetadata;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created on 07/12/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class VariantImporter {

    public abstract void importData(URI input) throws StorageManagerException, IOException;

    protected ExportMetadata importMetaData(URI inputUri, StudyConfigurationManager scm) throws IOException {

        // Check if can be loaded
        Map<String, Integer> studies = scm.getStudies(QueryOptions.empty());
        if (!studies.isEmpty()) {
            throw new IllegalStateException("Unable to import data if there are other loaded studies: " + studies);
        }

        // Load metadata
        ExportMetadata exportMetadata;
        try (InputStream is = FileUtils.newInputStream(Paths.get(inputUri.getPath() + VariantExporter.METADATA_FILE_EXTENSION))) {
            ObjectMapper objectMapper = new ObjectMapper();
            exportMetadata = objectMapper.readValue(is, ExportMetadata.class);
        }

        // Get list of returned samples
        Map<Integer, List<Integer>> returnedSamples;
        if (exportMetadata.getQuery() != null) {
            Map<Integer, StudyConfiguration> studyConfigurationMap = new HashMap<>();
            for (StudyConfiguration studyConfiguration : exportMetadata.getStudies()) {
                studyConfigurationMap.put(studyConfiguration.getStudyId(), studyConfiguration);
            }

            returnedSamples = VariantDBAdaptorUtils.getReturnedSamples(exportMetadata.getQuery(), exportMetadata.getQueryOptions(),
                    studyConfigurationMap.keySet(), studyConfigurationMap::get);
        } else {
            returnedSamples = null;
        }

        for (StudyConfiguration studyConfiguration : exportMetadata.getStudies()) {
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
//            scm.lockAndUpdate(studyConfiguration.getStudyId(), sc -> studyConfiguration);
            scm.updateStudyConfiguration(studyConfiguration, QueryOptions.empty());
        }

        return exportMetadata;
    }
}
