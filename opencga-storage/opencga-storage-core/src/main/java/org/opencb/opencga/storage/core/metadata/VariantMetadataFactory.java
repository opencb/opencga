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

import java.util.List;
import java.util.Map;
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

    protected final VariantStorageMetadataManager scm;

    public VariantMetadataFactory(VariantStorageMetadataManager variantStorageMetadataManager) {
        scm = variantStorageMetadataManager;
    }

    public VariantMetadata makeVariantMetadata() throws StorageEngineException {
        return makeVariantMetadata(new Query(), new QueryOptions());
    }

    public VariantMetadata makeVariantMetadata(Query query, QueryOptions queryOptions) throws StorageEngineException {
        VariantQueryProjection queryFields = VariantQueryProjectionParser.parseVariantQueryFields(query, queryOptions, scm);

        return makeVariantMetadata(queryFields, queryOptions);
    }

    protected VariantMetadata makeVariantMetadata(VariantQueryProjection queryFields, QueryOptions queryOptions)
            throws StorageEngineException {
        VariantMetadata metadata = new VariantMetadataConverter(scm)
                .toVariantMetadata(queryFields);

        Map<String, StudyMetadata> studyConfigurationMap = queryFields.getStudyMetadatas().values().stream()
                .collect(Collectors.toMap(StudyMetadata::getName, Function.identity()));

        for (VariantStudyMetadata variantStudyMetadata : metadata.getStudies()) {
            StudyMetadata studyMetadata = studyConfigurationMap.get(variantStudyMetadata.getId());
            List<Integer> fileIds = queryFields.getFiles().get(studyMetadata.getId());
            if (fileIds != null && !fileIds.isEmpty()) {
                Query query = new Query()
                        .append(FileMetadataDBAdaptor.VariantFileMetadataQueryParam.STUDY_ID.key(), studyMetadata.getId())
                        .append(FileMetadataDBAdaptor.VariantFileMetadataQueryParam.FILE_ID.key(), fileIds);
                scm.variantFileMetadataIterator(query, new QueryOptions()).forEachRemaining(fileMetadata -> {
                    variantStudyMetadata.getFiles().removeIf(file -> file.getId().equals(fileMetadata.getId()));
                    variantStudyMetadata.getFiles().add(fileMetadata.getImpl());
                });
            }
        }
        return metadata;
    }

}
