package org.opencb.opencga.storage.core.variant.search.solr;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.solr.common.SolrInputDocument;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.storage.core.metadata.models.project.SearchIndexMetadata;

import java.util.ArrayList;
import java.util.List;

public class VariantSolrInputDocumentDataWriter implements DataWriter<Pair<SolrInputDocument, SolrInputDocument>> {

    private final SolrInputDocumentDataWriter mainWriter;
    private final SolrInputDocumentDataWriter statsWriter;
    private int insertedDocuments = 0;
    private int mainInsertedDocuments = 0;
    private int statsInsertedDocuments = 0;

    public VariantSolrInputDocumentDataWriter(SolrInputDocumentDataWriter mainWriter, SolrInputDocumentDataWriter statsWriter) {
        this.mainWriter = mainWriter;
        this.statsWriter = statsWriter;
    }

    public VariantSolrInputDocumentDataWriter(VariantSearchManager variantSearchManager, SearchIndexMetadata indexMetadata) {
        mainWriter = new SolrInputDocumentDataWriter(
                variantSearchManager.buildCollectionName(indexMetadata),
                variantSearchManager.getSolrClient(),
                variantSearchManager.getInsertBatchSize()
        );

        if (variantSearchManager.isStatsCollectionEnabled(indexMetadata)) {
            statsWriter = new SolrInputDocumentDataWriter(
                    variantSearchManager.buildStatsCollectionName(indexMetadata),
                    variantSearchManager.getSolrClient(),
                    variantSearchManager.getInsertBatchSize()
            );
        } else {
            statsWriter = null;
        }
    }

    @Override
    public boolean open() {
        mainWriter.open();
        if (statsWriter != null) {
            statsWriter.open();
        }
        return true;
    }

    @Override
    public boolean pre() {
        mainWriter.pre();
        if (statsWriter != null) {
            statsWriter.pre();
        }
        return true;
    }

    @Override
    public boolean write(List<Pair<SolrInputDocument, SolrInputDocument>> batch) {
        List<SolrInputDocument> mainDocuments = new ArrayList<>(batch.size());
        List<SolrInputDocument> statsDocuments = new ArrayList<>(batch.size());
        boolean mainCommit = false;
        boolean statsCommit = false;
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
            mainCommit = mainWriter.write(mainDocuments);
            mainInsertedDocuments += mainDocuments.size();
        }
        if (!statsDocuments.isEmpty()) {
            // TODO: If statsWriter is null, should update the main collection
            statsCommit = statsWriter.write(statsDocuments);
            statsInsertedDocuments += statsDocuments.size();
        }
        if (mainCommit || statsCommit) {
            flush(mainCommit, statsCommit);
            return true;
        } else {
            return false;
        }
    }

    protected void flush(boolean mainCommit, boolean statsCommit) {
        try {
            if (mainCommit || statsCommit) {
                if (!mainCommit) {
                    mainWriter.commit();
                }
                if (!statsCommit && statsWriter != null) {
                    statsWriter.commit();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error flushing Solr documents", e);
        }
    }

    @Override
    public boolean post() {
        mainWriter.post();
        if (statsWriter != null) {
            statsWriter.post();
        }
        return true;
    }

    @Override
    public boolean close() {
        mainWriter.close();
        if (statsWriter != null) {
            statsWriter.close();
        }
        return true;
    }

    public int getInsertedDocuments() {
        return insertedDocuments;
    }

    public int getMainInsertedDocuments() {
        return mainInsertedDocuments;
    }

    public int getStatsInsertedDocuments() {
        return statsInsertedDocuments;
    }

}
