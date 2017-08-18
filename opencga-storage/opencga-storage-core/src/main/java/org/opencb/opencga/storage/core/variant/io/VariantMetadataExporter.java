package org.opencb.opencga.storage.core.variant.io;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.models.variant.metadata.VariantDatasetMetadata;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.metadata.VariantMetadataFactory;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantFileMetadataDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

/**
 * Created on 17/08/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantMetadataExporter {

    protected final StudyConfigurationManager scm;
    private final VariantFileMetadataDBAdaptor fileMetadataDBAdaptor;

    public VariantMetadataExporter(StudyConfigurationManager studyConfigurationManager, VariantFileMetadataDBAdaptor fileDBAdaptor) {
        scm = studyConfigurationManager;
        this.fileMetadataDBAdaptor = fileDBAdaptor;
    }

    protected void exportMetaData(Query query, QueryOptions queryOptions, String output)
            throws IOException, StorageEngineException {

        Map<Integer, List<Integer>> returnedSamples = VariantQueryUtils.getReturnedSamples(query, queryOptions, scm);
        Map<Integer, List<Integer>> returnedFiles = new HashMap<>(returnedSamples.size());
        List<StudyConfiguration> studyConfigurations = new ArrayList<>(returnedSamples.size());
        Set<VariantField> fields = VariantField.getReturnedFields(queryOptions);

        for (Integer studyId : returnedSamples.keySet()) {
            studyConfigurations.add(scm.getStudyConfiguration(studyId, QueryOptions.empty()).first());
            returnedFiles.put(studyId, VariantQueryUtils.getReturnedFiles(query, queryOptions, fields, scm));
        }

        VariantMetadata variantMetadata = generateVariantMetadata(studyConfigurations, returnedSamples, returnedFiles);
        writeMetadata(variantMetadata, output);

    }

    protected VariantMetadata generateVariantMetadata(List<StudyConfiguration> studyConfigurations,
                                                    Map<Integer, List<Integer>> returnedSamples,
                                                    Map<Integer, List<Integer>> returnedFiles) throws StorageEngineException {
        VariantMetadata metadata = new VariantMetadataFactory().toVariantMetadata(studyConfigurations, returnedSamples, returnedFiles);

        Map<String, StudyConfiguration> studyConfigurationMap = studyConfigurations.stream()
                .collect(Collectors.toMap(StudyConfiguration::getStudyName, Function.identity()));

        for (VariantDatasetMetadata datasetMetadata : metadata.getDatasets()) {
            StudyConfiguration studyConfiguration = studyConfigurationMap.get(datasetMetadata.getId());
            List<Integer> fileIds = datasetMetadata.getFiles().stream()
                    .map(fileMetadata -> {
                        Integer fileId = studyConfiguration.getFileIds().get(fileMetadata.getId());
                        if (fileId == null) {
                            fileId = studyConfiguration.getFileIds().get(fileMetadata.getAlias());
                        }
                        return fileId;
                    }).collect(Collectors.toList());
            Query query = new Query()
                    .append(VariantFileMetadataDBAdaptor.VariantFileMetadataQueryParam.STUDY_ID.key(), studyConfiguration.getStudyId())
                    .append(VariantFileMetadataDBAdaptor.VariantFileMetadataQueryParam.FILE_ID.key(), fileIds);
            try {
                fileMetadataDBAdaptor.iterator(query, new QueryOptions()).forEachRemaining(fileMetadata -> {
                    datasetMetadata.getFiles().removeIf(file -> file.getId().equals(fileMetadata.getId()));
                    datasetMetadata.getFiles().add(fileMetadata.getImpl());
                });
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return metadata;
    }

    protected void writeMetadata(VariantMetadata metadata, String output) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper().configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        File file = Paths.get(output).toFile();
        try (OutputStream os = new GZIPOutputStream(new FileOutputStream(file))) {
            objectMapper.writeValue(os, metadata);
        }
    }
}
