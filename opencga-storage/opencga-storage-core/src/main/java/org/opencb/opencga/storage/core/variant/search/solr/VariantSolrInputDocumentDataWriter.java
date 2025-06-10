package org.opencb.opencga.storage.core.variant.search.solr;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.solr.common.SolrInputDocument;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.storage.core.metadata.models.project.SearchIndexMetadata;

import java.util.ArrayList;
import java.util.List;

public class VariantSolrInputDocumentDataWriter implements DataWriter<Pair<SolrInputDocument, SolrInputDocument>> {

    private final SolrInputDocumentDataWriter mainWriter;
    private int insertedDocuments = 0;
    private int mainInsertedDocuments = 0;
    private int statsPartialUpdatedDocuments = 0;

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
    public boolean write(List<Pair<SolrInputDocument, SolrInputDocument>> batch) {
        List<SolrInputDocument> mainDocuments = new ArrayList<>(batch.size());
        List<SolrInputDocument> statsDocuments = new ArrayList<>(batch.size());
        boolean commit = false;
        for (Pair<SolrInputDocument, SolrInputDocument> pair : batch) {
            insertedDocuments++;
            if (pair.getLeft() != null) {
                mainDocuments.add(pair.getLeft());
            }
            if (pair.getRight() != null) {
                statsDocuments.add(pair.getRight());
            }
        }
        if (!mainDocuments.isEmpty()) {
            commit = mainWriter.write(mainDocuments);
            mainInsertedDocuments += mainDocuments.size();
        }
        if (!statsDocuments.isEmpty()) {
            commit |= mainWriter.update(statsDocuments);
            statsPartialUpdatedDocuments += statsDocuments.size();
        }
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

    public int getInsertedDocuments() {
        return insertedDocuments;
    }

    public int getMainInsertedDocuments() {
        return mainInsertedDocuments;
    }

    public int getStatsPartialUpdatedDocuments() {
        return statsPartialUpdatedDocuments;
    }

}
