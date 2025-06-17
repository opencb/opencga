package org.opencb.opencga.storage.core.variant.search;

import org.apache.solr.client.solrj.beans.DocumentObjectBinder;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.commons.Converter;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.project.SearchIndexMetadata;
import org.opencb.opencga.storage.core.variant.search.solr.SolrInputDocumentDataWriter;

import java.util.ArrayList;
import java.util.List;

public class VariantToSolrBeanConverterTask implements Converter<Variant, VariantSearchUpdateDocument> {

    private final VariantSearchToVariantConverter converter;
    private final DocumentObjectBinder binder;
    private final VariantSecondaryIndexFilter filter;

    public VariantToSolrBeanConverterTask(DocumentObjectBinder binder, SearchIndexMetadata indexMetadata,
                                          VariantStorageMetadataManager metadataManager) {
        this.binder = binder;
        this.filter = new VariantSecondaryIndexFilter(metadataManager, indexMetadata);
        this.converter = new VariantSearchToVariantConverter(metadataManager, indexMetadata);
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
        VariantSearchSyncInfo syncInfo = VariantSecondaryIndexFilter.readSearchSyncInfoFromAnnotation(variant.getAnnotation());
        VariantSearchSyncInfo resolvedSyncInfo = filter.getResolvedStatus(variant, syncInfo);


        if (resolvedSyncInfo.getStatus() == VariantSearchSyncInfo.Status.SYNCHRONIZED) {
            // If the variant is synchronized, we do not need to update it
            return null;
        }


        switch (resolvedSyncInfo.getStatus()) {
            case NOT_SYNCHRONIZED:
                final SolrInputDocument insertDocument;
                // Need to sync main document
                // This should contain all the fields, including stats
                insertDocument = binder.toSolrInputDocument(converter.convertToStorageType(variant, true, true));

                return new VariantSearchUpdateDocument(variant, resolvedSyncInfo, insertDocument, true);
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

                return new VariantSearchUpdateDocument(variant, resolvedSyncInfo, updateDocument, false);
            }
            case STATS_AND_STUDIES_UNKNOWN:
            case STATS_UNKNOWN:
            case STUDIES_UNKNOWN:
                // No uncertainty is expected at this point, so we should not reach here.
            default:
                throw new IllegalStateException("Unexpected value: " + resolvedSyncInfo.getStatus());
        }
    }

}
