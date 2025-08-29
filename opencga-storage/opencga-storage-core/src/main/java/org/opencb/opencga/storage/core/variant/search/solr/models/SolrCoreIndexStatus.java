package org.opencb.opencga.storage.core.variant.search.solr.models;

import org.opencb.commons.datastore.core.ObjectMap;

public class SolrCoreIndexStatus {

    private final String name;
    private final long numDocs;
    private final long maxDoc;
    private final long deletedDocs;
    private final long indexHeapUsageBytes;
    private final long version;
    private final long segmentCount;
    private final boolean hasDeletions;
    private final boolean current;

    private final long sizeInBytes;
    private final String lastModified;

    public SolrCoreIndexStatus(String name, ObjectMap index) {
        this.name = name;
        this.numDocs = index.getLong("numDocs", 0);
        this.sizeInBytes = index.getLong("sizeInBytes", 0);
        this.lastModified = index.getString("lastModified", "N/A");
        this.maxDoc = index.getLong("maxDoc", 0);
        this.deletedDocs = index.getLong("deletedDocs", 0);
        this.indexHeapUsageBytes = index.getLong("indexHeapUsageBytes", 0);
        this.version = index.getLong("version", 0);
        this.segmentCount = index.getLong("segmentCount", 0);
        this.hasDeletions = index.getBoolean("hasDeletions", false);
        this.current = index.getBoolean("current", false);

    }

    public String getName() {
        return name;
    }

    public long getNumDocs() {
        return numDocs;
    }

    public long getMaxDoc() {
        return maxDoc;
    }

    public long getDeletedDocs() {
        return deletedDocs;
    }

    public long getIndexHeapUsageBytes() {
        return indexHeapUsageBytes;
    }

    public long getVersion() {
        return version;
    }

    public long getSegmentCount() {
        return segmentCount;
    }

    public boolean isHasDeletions() {
        return hasDeletions;
    }

    public boolean isCurrent() {
        return current;
    }

    public long getSizeInBytes() {
        return sizeInBytes;
    }

    public String getLastModified() {
        return lastModified;
    }
}
