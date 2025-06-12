package org.opencb.opencga.storage.core.variant.search;

import org.apache.solr.common.SolrInputDocument;
import org.opencb.biodata.models.variant.Variant;

import java.util.List;

public class VariantSearchUpdateDocument {

    private final Variant variant;
    private final List<String> studies;
    private final SolrInputDocument document;
    private final boolean insert;
    private final VariantSearchSyncInfo.Status status;

    public VariantSearchUpdateDocument(Variant variant, List<String> studies, SolrInputDocument document,
                                       boolean insert, VariantSearchSyncInfo.Status status) {
        this.variant = new Variant(variant.toString());
        this.studies = studies;
        this.document = document;
        this.insert = insert;
        this.status = status;
    }

    public Variant getVariant() {
        return variant;
    }

    public List<String> getStudies() {
        return studies;
    }

    public SolrInputDocument getDocument() {
        return document;
    }

    public boolean isInsert() {
        return insert;
    }

    public VariantSearchSyncInfo.Status getStatus() {
        return status;
    }
}
