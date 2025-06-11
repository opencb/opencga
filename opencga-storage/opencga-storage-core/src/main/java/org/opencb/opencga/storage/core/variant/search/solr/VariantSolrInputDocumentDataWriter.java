package org.opencb.opencga.storage.core.variant.search.solr;

import org.apache.solr.common.SolrInputDocument;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.storage.core.metadata.models.project.SearchIndexMetadata;
import org.opencb.opencga.storage.core.variant.search.VariantSearchUpdateDocument;

import java.util.ArrayList;
import java.util.List;

public class VariantSolrInputDocumentDataWriter implements DataWriter<VariantSearchUpdateDocument> {

    private final SolrInputDocumentDataWriter mainWriter;
    private int updatedDocuments = 0;
    private int insertedDocuments = 0;
    private int partiallyUpdatedDocuments = 0;

    public VariantSolrInputDocumentDataWriter(VariantSearchManager variantSearchManager, SearchIndexMetadata indexMetadata) {
        mainWriter = new SolrInputDocumentDataWriter(
                variantSearchManager.buildCollectionName(indexMetadata),
                variantSearchManager.getSolrClient(),
                variantSearchManager.getInsertBatchSize()
        );
    }

    @Override
    public boolean open() {
        mainWriter.open();
        return true;
    }

    @Override
    public boolean pre() {
        mainWriter.pre();
        return true;
    }

    @Override
    public boolean write(List<VariantSearchUpdateDocument> batch) {
        List<SolrInputDocument> documents = new ArrayList<>(batch.size());

        boolean commit;
        for (VariantSearchUpdateDocument updateDocument : batch) {
            updatedDocuments++;
            documents.add(updateDocument.getDocument());
            if (updateDocument.isInsert()) {
                insertedDocuments++;
            } else {
                partiallyUpdatedDocuments++;
            }
        }
        commit = mainWriter.write(documents);

        if (commit) {
            flush();
            return true;
        } else {
            return false;
        }
    }

    protected void flush() {
    }

    @Override
    public boolean post() {
        mainWriter.post();
        return true;
    }

    @Override
    public boolean close() {
        mainWriter.close();
        return true;
    }

    public int getUpdatedDocuments() {
        return updatedDocuments;
    }

    public int getInsertedDocuments() {
        return insertedDocuments;
    }

    public int getPartiallyUpdatedDocuments() {
        return partiallyUpdatedDocuments;
    }

}
