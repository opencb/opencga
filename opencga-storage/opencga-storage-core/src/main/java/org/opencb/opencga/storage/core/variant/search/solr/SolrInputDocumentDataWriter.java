package org.opencb.opencga.storage.core.variant.search.solr;

import com.google.common.base.Throwables;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.core.common.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class SolrInputDocumentDataWriter implements DataWriter<SolrInputDocument> {

    public static final int MAX_RETRIES = 30;
    private final String collection;
    private final SolrClient solrClient;
    private final int insertBatchSize;
    private int serverBufferSize = 0;
    private int serverBufferSizeBytes = 0;
    private int insertedDocuments = 0;
    private long addTimeMs = 0;
    private long commitTimeMs = 0;
    private final Logger logger = LoggerFactory.getLogger(SolrInputDocumentDataWriter.class);
    private final String uniqueKey;

    private static final String SET = "set";

    public SolrInputDocumentDataWriter(String collection, SolrClient solrClient, int insertBatchSize) {
        this(collection, solrClient, insertBatchSize, "id");
    }

    public SolrInputDocumentDataWriter(String collection, SolrClient solrClient, int insertBatchSize, String uniqueKey) {
        this.collection = collection;
        this.solrClient = solrClient;
        this.insertBatchSize = insertBatchSize;
        this.uniqueKey = uniqueKey;
    }

    public final boolean update(List<SolrInputDocument> batch) {
        batch.forEach(doc -> {
            for (String fieldName : new ArrayList<>(doc.getFieldNames())) {
                SolrInputField field = doc.getField(fieldName);
                if (fieldName.equals(uniqueKey)) {
                    // uniqueKey field should not be modified.
                    continue;
                } else {
                    toSetValue(field);
                }
            }
        });
        return write(batch);
    }

    public static void toSetValue(SolrInputField field) {
        field.setValue(Collections.singletonMap(SET, field.getValue()));
    }

    public static <T> T readSetValue(SolrInputField field) {
        if (isSetValue(field)) {
            return (T) ((Map<?, ?>) field.getValue()).get(SET);
        } else {
            throw new IllegalArgumentException("Field value is not a Map: " + field.getValue());
        }
    }

    public static boolean isSetValue(SolrInputField field) {
        return field.getValue() instanceof Map && ((Map<?, ?>) field.getValue()).containsKey(SET);
    }

    @Override
    public final boolean write(List<SolrInputDocument> batch) {
        try {
            add(batch);
            if (serverBufferSize >= insertBatchSize || serverBufferSizeBytes >= (300 * 1024 * 1024) /* 300 MB */) {
                // If the buffer size exceeds the insert batch size or 300 MB, commit the changes
                commit();
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public boolean post() {
        try {
            commit(true);
        } catch (Exception e) {
            Throwables.propagate(e);
        }
        return true;
    }

    protected void add(List<SolrInputDocument> batch) throws Exception {
        UpdateResponse response = retry(() -> solrClient.add(collection, batch));
        for (SolrInputDocument document : batch) {
            long docSize = getSize(document);
            serverBufferSizeBytes += docSize;
        }
        addTimeMs += response.getElapsedTime();
        serverBufferSize += batch.size();
    }

    /**
     * Get an approximation of the size of the value in bytes.
     * @param value The value to estimate the size of.
     *             It can be a String, Number, Collection, SolrInputDocument, Map
     * @return The estimated size in bytes.
     */
    private static long getSize(Object value) {
        long size = 0;

        if (value instanceof Number || value instanceof String) {
            size += value.toString().length();
        } else if (value instanceof Collection) {
            for (Object item : ((Collection) value)) {
                size += getSize(item);
            }
        } else if (value instanceof SolrInputDocument) {
            SolrInputDocument document = (SolrInputDocument) value;
            for (SolrInputField field : document.values()) {
                if (field == null || field.getValue() == null) {
                    continue;
                }
                size += field.getName().length(); // Field name length
                size += getSize(field.getValue());
            }
        } else if (value instanceof Map) {
            for (Map.Entry entry : ((Map<?, ?>) value).entrySet()) {
                size += getSize(entry.getKey());
                size += getSize(entry.getValue());
            }
        } else {
            size += value.toString().length(); // Fallback for other types
        }
        return size;
    }

    public final void commit() throws Exception {
        commit(false);
    }

    public void commit(boolean openSearcher) throws Exception {
        boolean waitSearcher = openSearcher;
        StopWatch stopWatch = StopWatch.createStarted();
        UpdateResponse response = retry(() -> new UpdateRequest()
                .setAction(UpdateRequest.ACTION.COMMIT, true, waitSearcher, 1, false, false, openSearcher)
                .process(solrClient, collection));
        commitTimeMs += response.getElapsedTime();
        insertedDocuments += serverBufferSize;
        if (logger.isDebugEnabled()) {
            logger.debug("Committed {} documents to Solr in {} s. Buffer size : {}",
                    serverBufferSize,
                    (stopWatch.getTime(TimeUnit.MILLISECONDS)) / 1000.0,
                    IOUtils.humanReadableByteCount(serverBufferSizeBytes, false));
        }
        serverBufferSize = 0;
        serverBufferSizeBytes = 0;
    }

    public long getCommitTimeMs() {
        return commitTimeMs;
    }

    public long getAddTimeMs() {
        return addTimeMs;
    }

    public int getInsertedDocuments() {
        return insertedDocuments;
    }

    public String getCollection() {
        return collection;
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
                String message = e.getMessage();
                // Remove duplicated lines from "message"
               message = removeDuplicates(message, " | ");

                logger.warn("Error while writing to Solr: {}. Retrying... (attempt {}/{})", message, retry, MAX_RETRIES);
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

    public static String removeDuplicates(String message, String lineBreak) {
        String[] lines = message.split("\n");
        String prevLine = lines[0];
        StringBuilder sb = new StringBuilder();

        int duplicateCount = 0;
        for (int i = 1; i < lines.length; i++) {
            String line = lines[i];
            if (line.equals(prevLine)) {
                // Skip duplicate line
                duplicateCount++;
                continue;
            }
            if (sb.length() > 0) {
                sb.append(lineBreak);
            }
            sb.append(prevLine);
            if (duplicateCount > 0) {
                sb.append(" (repeated ").append(duplicateCount + 1).append(" times)");
                duplicateCount = 0; // Reset count after appending
            }
            prevLine = line;
        }
        // Append the last line
        if (sb.length() > 0) {
            sb.append(lineBreak);
        }
        sb.append(prevLine);
        if (duplicateCount > 0) {
            sb.append(" (repeated ").append(duplicateCount + 1).append(" times)");
        }

        return sb.toString();
    }
}
