package org.opencb.opencga.storage.core.metadata;

import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.adaptors.FileMetadataDBAdaptor;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjection;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjectionParser;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Creates a VariantMetadata from other metadata information stored in the database.
 *
 * Created on 17/08/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantMetadataFactory {

    protected final VariantStorageMetadataManager metadataManager;

    public VariantMetadataFactory(VariantStorageMetadataManager variantStorageMetadataManager) {
        metadataManager = variantStorageMetadataManager;
    }

    public VariantMetadata makeVariantMetadata() throws StorageEngineException {
        return makeVariantMetadata(new Query(), new QueryOptions());
    }

    public VariantMetadata makeVariantMetadata(Query query, QueryOptions queryOptions) throws StorageEngineException {
        return makeVariantMetadata(query, queryOptions, false);
    }

    public VariantMetadata makeVariantMetadata(Query query, QueryOptions queryOptions, boolean skipFiles) throws StorageEngineException {
        // Parse projection BEFORE clearing INCLUDE_FILE, so that FILE→sample derivation is preserved.
        // Setting INCLUDE_FILE=NONE before parsing would break the chain that resolves samples from files.
        VariantQueryProjection queryFields = VariantQueryProjectionParser.parseVariantQueryFields(query, queryOptions, metadataManager);
        if (skipFiles) {
            // Clear file entries from the projection — the VCF writer only needs sample metadata, not file metadata.
            for (VariantQueryProjection.StudyVariantQueryProjection study : queryFields.getStudies().values()) {
                study.setFiles(Collections.emptyList());
            }
        }

        return makeVariantMetadata(queryFields, queryOptions);
    }

    protected VariantMetadata makeVariantMetadata(VariantQueryProjection queryFields, QueryOptions queryOptions)
            throws StorageEngineException {
        VariantMetadata metadata = new VariantMetadataConverter(metadataManager)
                .toVariantMetadata(queryFields);

        Map<String, StudyMetadata> studyConfigurationMap = queryFields.getStudies().values().stream()
                .collect(Collectors.toMap(VariantQueryProjection.StudyVariantQueryProjection::getName,
                        VariantQueryProjection.StudyVariantQueryProjection::getStudyMetadata));

        for (VariantStudyMetadata variantStudyMetadata : metadata.getStudies()) {
            StudyMetadata studyMetadata = studyConfigurationMap.get(variantStudyMetadata.getId());
            List<Integer> fileIds = queryFields.getStudy(studyMetadata.getId()).getFileIds();
            if (fileIds != null && !fileIds.isEmpty()) {
                if (fileIds.size() < 50) {
                    Query query = new Query()
                            .append(FileMetadataDBAdaptor.VariantFileMetadataQueryParam.STUDY_ID.key(), studyMetadata.getId())
                            .append(FileMetadataDBAdaptor.VariantFileMetadataQueryParam.FILE_ID.key(), fileIds);
                    metadataManager.variantFileMetadataIterator(query, new QueryOptions()).forEachRemaining(fileMetadata -> {
                        variantStudyMetadata.getFiles().removeIf(file -> file.getId().equals(fileMetadata.getId()));
                        variantStudyMetadata.getFiles().add(fileMetadata.getImpl());
                    });
                } // else { /* Do not add VariantFileMetadata if there are more than 50 files! */ }
            }
        }
        return metadata;
    }

}
