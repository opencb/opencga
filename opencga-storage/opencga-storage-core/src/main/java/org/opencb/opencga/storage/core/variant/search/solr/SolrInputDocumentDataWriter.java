package org.opencb.opencga.storage.core.variant.search.solr;

import com.google.common.base.Throwables;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SolrInputDocumentDataWriter implements DataWriter<SolrInputDocument> {

    private final String collection;
    private final SolrClient solrClient;
    private final int insertBatchSize;
    private int serverBufferSize = 0;
    private int insertedDocuments = 0;
    private long addTimeMs = 0;
    private long commitTimeMs = 0;
    private final Logger logger = LoggerFactory.getLogger(SolrInputDocumentDataWriter.class);

    public SolrInputDocumentDataWriter(String collection, SolrClient solrClient, int insertBatchSize) {
        this.collection = collection;
        this.solrClient = solrClient;
        this.insertBatchSize = insertBatchSize;
    }

    @Override
    public final boolean write(List<SolrInputDocument> batch) {
        try {
            add(batch);
            if (serverBufferSize > insertBatchSize) {
                commit();
            }
        } catch (Exception e) {
            Throwables.propagate(e);
        }
        return true;
    }

    @Override
    public boolean post() {
        try {
            commit();
        } catch (Exception e) {
            Throwables.propagate(e);
        }
        logger.info("Finish Solr Bulk Load: {} inserted documents.", insertedDocuments);
        logger.info("Push (add) time: {}", TimeUtils.durationToString(addTimeMs));
        logger.info("Commit time: {}", TimeUtils.durationToString(commitTimeMs));
        return true;
    }

    protected void add(List<SolrInputDocument> batch) throws Exception {
        UpdateResponse response = solrClient.add(collection, batch);
        addTimeMs += response.getElapsedTime();
        if (response.getException() != null) {
            // FIXME: Is this even possible?
            throw response.getException();
        }
        serverBufferSize += batch.size();
    }

    protected void commit() throws Exception {
        UpdateResponse response = solrClient.commit(collection, true, true, false);
        commitTimeMs += response.getElapsedTime();
        if (response.getException() != null) {
            // FIXME: Is this even possible?
            throw response.getException();
        }
        insertedDocuments += serverBufferSize;
        serverBufferSize = 0;
    }

}
