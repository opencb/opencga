package org.opencb.opencga.storage.mongodb.variant.search;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.storage.core.metadata.models.project.SearchIndexMetadata;
import org.opencb.opencga.storage.core.variant.search.VariantSearchUpdateDocument;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSolrInputDocumentDataWriter;
import org.opencb.opencga.storage.mongodb.variant.converters.VariantStringIdConverter;

import java.util.ArrayList;
import java.util.List;

/**
 * Extends the base Solr writer to also write sync status back to MongoDB after indexing.
 * Mirrors the HBase pattern in {@code HadoopVariantSearchDataWriter}.
 */
public class MongoDBVariantSearchDataWriter extends VariantSolrInputDocumentDataWriter {

    private final MongoDBCollection variantsCollection;
    private final VariantStringIdConverter idConverter = new VariantStringIdConverter();
    private final List<Document> queries = new ArrayList<>();
    private final List<Bson> updates = new ArrayList<>();

    public MongoDBVariantSearchDataWriter(VariantSearchManager variantSearchManager,
                                          SearchIndexMetadata indexMetadata,
                                          MongoDBCollection variantsCollection) {
        super(variantSearchManager, indexMetadata);
        this.variantsCollection = variantsCollection;
    }

    @Override
    public boolean write(List<VariantSearchUpdateDocument> batch) {
        super.write(batch);

        for (VariantSearchUpdateDocument doc : batch) {
            String id = idConverter.buildId(doc.getVariant());
            queries.add(new Document("_id", id));
            updates.add(MongoDBVariantSearchIndexUtils.getSetSyncStatus(
                    doc.getSyncInfo().getStudies(), doc.getSyncInfo().getStatsHash()));
        }
        return true;
    }

    @Override
    protected void flush() {
        super.flush();
        if (!queries.isEmpty()) {
            variantsCollection.update(queries, updates, null);
            queries.clear();
            updates.clear();
        }
    }

    @Override
    public boolean post() {
        super.post();
        flush();
        return true;
    }
}
