package org.opencb.opencga.storage.core.variant.search;

import org.apache.solr.client.solrj.beans.DocumentObjectBinder;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.commons.Converter;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.search.solr.SolrInputDocumentDataWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class VariantToSolrBeanConverterTask implements Converter<Variant, VariantSearchUpdateDocument> {

    private final VariantSearchToVariantConverter converter;
    private final DocumentObjectBinder binder;
    private final VariantSecondaryIndexFilter filter;

    public VariantToSolrBeanConverterTask(DocumentObjectBinder binder, VariantStorageMetadataManager metadataManager) {
        this.binder = binder;
        this.filter = new VariantSecondaryIndexFilter(metadataManager.getStudies());
        this.converter = new VariantSearchToVariantConverter(metadataManager);
    }

    @Override
    public List<VariantSearchUpdateDocument> apply(List<Variant> from) {
        List<VariantSearchUpdateDocument> convertedBatch = new ArrayList<>(from.size());

        for (Variant item : from) {
            VariantSearchUpdateDocument convert = this.convert(item);
            if (convert != null) {
                convertedBatch.add(convert);
            }
        }

        return convertedBatch;
    }

    @Override
    public VariantSearchUpdateDocument convert(Variant variant) {
        VariantSearchSyncInfo.Status status = filter.getSyncStatus(variant);

        List<String> studies = variant.getStudies().stream()
                .map(StudyEntry::getStudyId)
                .collect(Collectors.toList());

        switch (status) {
            case SYNCHRONIZED:
                // Nothing to sync!
                return null;
            case NOT_SYNCHRONIZED:
                final SolrInputDocument insertDocument;
                // Need to sync main document
                // This should contain all the fields, including stats
                insertDocument = binder.toSolrInputDocument(converter.convertToStorageType(variant, true, true));

                return new VariantSearchUpdateDocument(variant, studies, insertDocument, true, status);
            case STATS_NOT_SYNC: {
                final SolrInputDocument updateDocument;
                // Need to sync stats document
                // This should contain only the stats fields, as will be merged with the main document with partial solr updates
                updateDocument = binder.toSolrInputDocument(converter.convertToStorageType(variant, true, false));

                for (String fieldName : new ArrayList<>(updateDocument.getFieldNames())) {
                    SolrInputField field = updateDocument.getField(fieldName);
                    if (fieldName.equals("id")) {
                        // uniqueKey field should not be modified.
                        continue;
                    } else {
                        SolrInputDocumentDataWriter.toSetValue(field);
                    }
                }

                return new VariantSearchUpdateDocument(variant, studies, updateDocument, false, status);
            }
            case STATS_NOT_SYNC_AND_STUDIES_UNKNOWN:
            case STUDIES_UNKNOWN_SYNC:
                // No uncertainty is expected at this point, so we should not reach here.
            default:
                throw new IllegalStateException("Unexpected value: " + filter.getSyncStatus(variant));
        }
    }

}
