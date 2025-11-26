package org.opencb.opencga.storage.core.variant.search;

import org.apache.solr.common.SolrInputDocument;
import org.opencb.biodata.models.variant.Variant;

public class VariantSearchUpdateDocument {

    private final Variant variant;
    private final VariantSearchSyncInfo syncInfo;
    private final SolrInputDocument document;
    private final boolean insert;

    public VariantSearchUpdateDocument(Variant variant, VariantSearchSyncInfo syncInfo, SolrInputDocument document,
                                       boolean insert) {
        this.variant = new Variant(variant.toString());
        this.syncInfo = syncInfo;
        this.document = document;
        this.insert = insert;
    }

    public Variant getVariant() {
        return variant;
    }

    public SolrInputDocument getDocument() {
        return document;
    }

    public boolean isInsert() {
        return insert;
    }

    public VariantSearchSyncInfo.Status getStatus() {
        return syncInfo.getStatus();
    }

    public VariantSearchSyncInfo getSyncInfo() {
        return syncInfo;
    }


}
