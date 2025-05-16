package org.opencb.opencga.storage.core.metadata.models.project;

import org.opencb.commons.datastore.core.ObjectMap;

import java.time.Instant;

public class SearchIndexMetadata {

    /**
     * Version of the index.
     * Starting from 1.
     */
    private int version;
    /**
     * Date when the index was created.
     */
    private Instant creationDate;
    /**
     * Date when the index was last modified. Because the modification process takes
     * some time, this date will record the date when the update process started.
     */
    private Instant modificationDate;
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

    public SearchIndexMetadata(int version, Instant creationDate, Instant modificationDate, Status status, String configSetId,
                               String collectionNameSuffix, ObjectMap attributes) {
        this.version = version;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
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

    public Instant getCreationDate() {
        return creationDate;
    }

    public SearchIndexMetadata setCreationDate(Instant creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public Instant getModificationDate() {
        return modificationDate;
    }

    public SearchIndexMetadata setModificationDate(Instant modificationDate) {
        this.modificationDate = modificationDate;
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
