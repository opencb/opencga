package org.opencb.opencga.storage.core.variant.search;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.solr.client.solrj.beans.DocumentObjectBinder;
import org.apache.solr.common.SolrInputDocument;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.commons.Converter;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;

import java.util.ArrayList;
import java.util.List;

public class VariantToSolrBeanConverterTask implements Converter<Variant, Pair<SolrInputDocument, SolrInputDocument>> {

    private final VariantSearchToVariantConverter converter;
    private final DocumentObjectBinder binder;
    private final VariantSecondaryIndexFilter filter;

    public VariantToSolrBeanConverterTask(DocumentObjectBinder binder, VariantStorageMetadataManager metadataManager) {
        this.binder = binder;
        this.filter = new VariantSecondaryIndexFilter(metadataManager.getStudies());
        this.converter = new VariantSearchToVariantConverter(metadataManager);
    }

    @Override
    public List<Pair<SolrInputDocument, SolrInputDocument>> apply(List<Variant> from) {
        List<Pair<SolrInputDocument, SolrInputDocument>> convertedBatch = new ArrayList<>(from.size());

        for (Variant item : from) {
            Pair<SolrInputDocument, SolrInputDocument> convert = this.convert(item);
            if (convert != null) {
                convertedBatch.add(convert);
            }
        }

        return convertedBatch;
    }

    @Override
    public Pair<SolrInputDocument, SolrInputDocument> convert(Variant variant) {
        VariantStorageEngine.SyncStatus status = filter.getSyncStatus(variant);
        switch (status) {
            case SYNCHRONIZED:
                // Nothing to sync!
                return null;
            case NOT_SYNCHRONIZED:
                final SolrInputDocument mainDocument;
                // Need to sync main document
                // This should contain all the fields, including stats
                mainDocument = binder.toSolrInputDocument(converter.convertToStorageType(variant, true, true));

                return Pair.of(mainDocument, null);
            case STATS_NOT_SYNC: {
                final SolrInputDocument statsDocument;
                // Need to sync stats document
                // This should contain only the stats fields, as will be merged with the main document with partial solr updates
                statsDocument = binder.toSolrInputDocument(converter.convertToStorageType(variant, true, false));

                return Pair.of(null, statsDocument);
            }
            case STATS_NOT_SYNC_AND_STUDIES_UNKNOWN:
            case STUDIES_UNKNOWN_SYNC:
                // No uncertainty is expected at this point, so we should not reach here.
            default:
                throw new IllegalStateException("Unexpected value: " + filter.getSyncStatus(variant));
        }
    }

}
