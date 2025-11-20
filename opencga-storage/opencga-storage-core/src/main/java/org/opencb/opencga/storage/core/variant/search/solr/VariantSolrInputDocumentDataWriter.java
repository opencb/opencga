package org.opencb.opencga.storage.core.variant.search.solr;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.solr.common.SolrInputDocument;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.metadata.models.project.SearchIndexMetadata;
import org.opencb.opencga.storage.core.variant.search.VariantSearchUpdateDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class VariantSolrInputDocumentDataWriter implements DataWriter<VariantSearchUpdateDocument> {

    private final Logger logger = LoggerFactory.getLogger(VariantSolrInputDocumentDataWriter.class);
    private final SolrInputDocumentDataWriter mainWriter;
    private int updatedDocuments = 0;
    private int insertedDocuments = 0;
    private int partiallyUpdatedDocuments = 0;
    private StopWatch startTime;

    public VariantSolrInputDocumentDataWriter(VariantSearchManager variantSearchManager, SearchIndexMetadata indexMetadata) {
        mainWriter = new SolrInputDocumentDataWriter(
                variantSearchManager.buildCollectionName(indexMetadata),
                variantSearchManager.getSolrClient(),
                variantSearchManager.getInsertBatchSize()
        );
    }

    @Override
    public boolean open() {
        startTime = StopWatch.createStarted();
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
        logger.info("Variant Search loading done. Collection '{}' updated.", mainWriter.getCollection());
        printStats();
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

    public long getCommitTimeMs() {
        return mainWriter.getCommitTimeMs();
    }

    public long getAddTimeMs() {
        return mainWriter.getAddTimeMs();
    }

    public void printStats() {
        logger.info(" - Processed variants: " + getUpdatedDocuments());
        logger.info(" - Inserted documents: " + getInsertedDocuments());
        logger.info(" - Partially updated documents: " + getPartiallyUpdatedDocuments());
        logger.info(" - Push (add) time: {}", TimeUtils.durationToString(getAddTimeMs()));
        logger.info(" - Commit time: {}", TimeUtils.durationToString(getCommitTimeMs()));

        float ratePerHour = getUpdatedDocuments() / (startTime.getTime() / 1000f) * 3600;
        logger.info(" - Insert/Update rate: " + String.format("%.2f", ratePerHour) + " variants/hour "
                + "(" + String.format("%.2f", ratePerHour / 1000000) + " M/h)");
    }

}
