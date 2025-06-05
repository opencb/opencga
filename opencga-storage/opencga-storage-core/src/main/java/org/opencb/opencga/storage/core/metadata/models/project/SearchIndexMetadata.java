package org.opencb.opencga.storage.core.metadata.models.project;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.Date;

public class SearchIndexMetadata {

    /**
     * Version of the index.
     * Starting from 1.
     */
    private int version;
    /**
     * Date when the index was created.
     */
    private Date creationDate;
    /**
     * The date when the index was last updated. This field captures the timestamp
     * of the start of the most recent update process for the index.
     */
    private Date lastUpdateDate;
    /**
     * Status of the index.
     * <ul>
     * <li>STAGING: Index being built. Not ready. Not to be used.</li>
     * <li>ACTIVE: Index ready to be used (if present)</li>
     * <li>DEPRECATED: Index marked to be removed.</li>
     * <li>REMOVED: Index no longer exists.</li>
     * </ul>
     */
    private Status status;

    private String configSetId;
    /**
     * Suffix to be added to the collection name.
     * <p>
     * This is used to create different collections for different versions of the same index.
     * The final collection name will be:
     *     collectionName = dbName
     *     if (collectionNameSuffix is not empty) {
     *          collectionName += "_" + collectionNameSuffix;
     * @see org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager#buildCollectionName(SearchIndexMetadata)
     * </p>
     */
    private String collectionNameSuffix;
//    private String collectionName;

    private ObjectMap attributes;

    public enum Status {
        STAGING, // Index being built. Not ready. Not to be used.
        ACTIVE, // Index ready to be used (if present)
        DEPRECATED, // Index marked to be removed.
        REMOVED // Index no longer exists.
    }

    protected SearchIndexMetadata() {
    }

    public SearchIndexMetadata(int version, Date creationDate, Date lastUpdateDate, Status status, String configSetId,
                               String collectionNameSuffix, ObjectMap attributes) {
        this.version = version;
        this.creationDate = creationDate;
        this.lastUpdateDate = lastUpdateDate;
        this.status = status;
        this.configSetId = configSetId;
        this.collectionNameSuffix = collectionNameSuffix;
        this.attributes = attributes;
    }

    public int getVersion() {
        return version;
    }

    public SearchIndexMetadata setVersion(int version) {
        this.version = version;
        return this;
    }

    public Date getCreationDate() {
        return creationDate;
    }

    public SearchIndexMetadata setCreationDate(Date creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public Date getLastUpdateDate() {
        return lastUpdateDate;
    }

    public long getLastUpdateDateTimestamp() {
        return lastUpdateDate == null ? 0 : lastUpdateDate.toInstant().toEpochMilli();
    }

    public SearchIndexMetadata setLastUpdateDate(Date lastUpdateDate) {
        this.lastUpdateDate = lastUpdateDate;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public SearchIndexMetadata setStatus(Status status) {
        this.status = status;
        return this;
    }

    public String getCollectionNameSuffix() {
        return collectionNameSuffix;
    }

    public SearchIndexMetadata setCollectionNameSuffix(String collectionNameSuffix) {
        this.collectionNameSuffix = collectionNameSuffix;
        return this;
    }

    public String getConfigSetId() {
        return configSetId;
    }

    public SearchIndexMetadata setConfigSetId(String configSetId) {
        this.configSetId = configSetId;
        return this;
    }

    public ObjectMap getAttributes() {
        return attributes;
    }

    public SearchIndexMetadata setAttributes(ObjectMap attributes) {
        this.attributes = attributes;
        return this;
    }
}
