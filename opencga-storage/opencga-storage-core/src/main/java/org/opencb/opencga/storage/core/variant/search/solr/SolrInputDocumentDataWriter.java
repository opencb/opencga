package org.opencb.opencga.storage.core.variant.search.solr;

import com.google.common.base.Throwables;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class SolrInputDocumentDataWriter implements DataWriter<SolrInputDocument> {

    public static final int MAX_RETRIES = 5;
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
            commit(true);
        } catch (Exception e) {
            Throwables.propagate(e);
        }
        logger.info("Finish Solr Bulk Load: {} inserted documents.", insertedDocuments);
        logger.info("Push (add) time: {}", TimeUtils.durationToString(addTimeMs));
        logger.info("Commit time: {}", TimeUtils.durationToString(commitTimeMs));
        return true;
    }

    protected void add(List<SolrInputDocument> batch) throws Exception {
        UpdateResponse response = retry(() -> solrClient.add(collection, batch));
        addTimeMs += response.getElapsedTime();
        serverBufferSize += batch.size();
    }

    protected void commit() throws Exception {
        commit(false);
    }

    protected void commit(boolean openSearcher) throws Exception {
        UpdateResponse response = retry(() -> new UpdateRequest()
                .setAction(UpdateRequest.ACTION.COMMIT, true, false, 1, false, false, openSearcher)
//                .setAction(UpdateRequest.ACTION.COMMIT, true, true)
                .process(solrClient, collection));
        commitTimeMs += response.getElapsedTime();
        insertedDocuments += serverBufferSize;
        serverBufferSize = 0;
    }

    @FunctionalInterface
    public interface SolrClientFunction<T, E extends Exception> {
        T apply() throws E;
    }

    private <E extends Exception> UpdateResponse retry(SolrClientFunction<UpdateResponse, E> f) throws Exception {
        int retry = 0;
        List<Exception> suppressed = null;
        while (true) {
            try {
                UpdateResponse apply = f.apply();
                if (apply.getException() != null) {
                    // FIXME: Is this even possible?
                    throw apply.getException();
                }
                return apply;
            } catch (Exception e) {
                logger.warn("Error while writing to Solr: {}. Retrying... (attempt {}/{})", e.getMessage(), retry, MAX_RETRIES);
                if (retry++ > MAX_RETRIES) {
                    for (Exception s : suppressed) {
                        e.addSuppressed(s);
                    }
                    throw e;
                } else {
                    if (suppressed == null) {
                        suppressed = new ArrayList<>(MAX_RETRIES);
                    }
                    suppressed.add(e);
                }
                try {
                    Thread.sleep(1000 * retry);
                } catch (InterruptedException ex) {
                    // Stop retrying.
                    throw e;
                }
            }
        }
    }

}
