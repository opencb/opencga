package org.opencb.opencga.storage.core.metadata;

import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantFileMetadataDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Creates a VariantMetadata from other metadata information stored in the database.
 *
 * Created on 17/08/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantMetadataFactory {

    protected final StudyConfigurationManager scm;
    private final VariantFileMetadataDBAdaptor fileMetadataDBAdaptor;

    public VariantMetadataFactory(StudyConfigurationManager studyConfigurationManager, VariantFileMetadataDBAdaptor fileDBAdaptor) {
        scm = studyConfigurationManager;
        this.fileMetadataDBAdaptor = fileDBAdaptor;
    }

    public VariantMetadata makeVariantMetadata() throws StorageEngineException {
        return makeVariantMetadata(new Query(), new QueryOptions());
    }

    public VariantMetadata makeVariantMetadata(Query query, QueryOptions queryOptions) throws StorageEngineException {
        Map<Integer, List<Integer>> returnedSamples = VariantQueryUtils.getReturnedSamples(query, queryOptions, scm);
        Map<Integer, List<Integer>> returnedFiles = new HashMap<>(returnedSamples.size());
        List<StudyConfiguration> studyConfigurations = new ArrayList<>(returnedSamples.size());
        Set<VariantField> fields = VariantField.getReturnedFields(queryOptions);

        for (Integer studyId : returnedSamples.keySet()) {
            studyConfigurations.add(scm.getStudyConfiguration(studyId, QueryOptions.empty()).first());
            returnedFiles.put(studyId, VariantQueryUtils.getReturnedFiles(query, queryOptions, fields, scm));
        }

        return makeVariantMetadata(studyConfigurations, returnedSamples, returnedFiles);
    }

    protected VariantMetadata makeVariantMetadata(List<StudyConfiguration> studyConfigurations,
                                                  Map<Integer, List<Integer>> returnedSamples,
                                                  Map<Integer, List<Integer>> returnedFiles) throws StorageEngineException {
        VariantMetadata metadata = new VariantMetadataConverter().toVariantMetadata(studyConfigurations, returnedSamples, returnedFiles);

        Map<String, StudyConfiguration> studyConfigurationMap = studyConfigurations.stream()
                .collect(Collectors.toMap(StudyConfiguration::getStudyName, Function.identity()));

        for (VariantStudyMetadata studyMetadata : metadata.getStudies()) {
            StudyConfiguration studyConfiguration = studyConfigurationMap.get(studyMetadata.getId());
            List<Integer> fileIds = studyMetadata.getFiles().stream()
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
                    studyMetadata.getFiles().removeIf(file -> file.getId().equals(fileMetadata.getId()));
                    studyMetadata.getFiles().add(fileMetadata.getImpl());
                });
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return metadata;
    }

}
