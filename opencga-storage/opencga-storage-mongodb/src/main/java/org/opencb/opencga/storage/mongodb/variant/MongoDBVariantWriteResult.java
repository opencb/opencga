package org.opencb.opencga.storage.mongodb.variant;

/**
 * Created on 30/10/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoDBVariantWriteResult {

    private long newDocuments;
    private long updatedObjects;
    private long skippedVariants;
    private long nonInsertedVariants;

    public MongoDBVariantWriteResult() {
    }

    public MongoDBVariantWriteResult(long newDocuments, long updatedObjects, long skippedVariants, long nonInsertedVariants) {
        this.newDocuments = newDocuments;
        this.updatedObjects = updatedObjects;
        this.skippedVariants = skippedVariants;
        this.nonInsertedVariants = nonInsertedVariants;
    }

    public void merge(MongoDBVariantWriteResult... others) {
        for (MongoDBVariantWriteResult other : others) {
            newDocuments += other.newDocuments;
            updatedObjects += other.updatedObjects;
            skippedVariants += other.skippedVariants;
            nonInsertedVariants += other.nonInsertedVariants;
        }
    }

    public long getNewDocuments() {
        return newDocuments;
    }

    public MongoDBVariantWriteResult setNewDocuments(long newDocuments) {
        this.newDocuments = newDocuments;
        return this;
    }

    public long getUpdatedObjects() {
        return updatedObjects;
    }

    public MongoDBVariantWriteResult setUpdatedObjects(long updatedObjects) {
        this.updatedObjects = updatedObjects;
        return this;
    }

    public long getSkippedVariants() {
        return skippedVariants;
    }

    public MongoDBVariantWriteResult setSkippedVariants(long skippedVariants) {
        this.skippedVariants = skippedVariants;
        return this;
    }

    public long getNonInsertedVariants() {
        return nonInsertedVariants;
    }

    public MongoDBVariantWriteResult setNonInsertedVariants(long nonInsertedVariants) {
        this.nonInsertedVariants = nonInsertedVariants;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MongoDBVariantWriteResult{");
        sb.append("newDocuments=").append(newDocuments);
        sb.append(", updatedObjects=").append(updatedObjects);
        sb.append(", skippedVariants=").append(skippedVariants);
        sb.append(", nonInsertedVariants=").append(nonInsertedVariants);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MongoDBVariantWriteResult)) {
            return false;
        }

        MongoDBVariantWriteResult that = (MongoDBVariantWriteResult) o;
        if (newDocuments != that.newDocuments) {
            return false;
        }
        if (updatedObjects != that.updatedObjects) {
            return false;
        }
        if (skippedVariants != that.skippedVariants) {
            return false;
        }
        return nonInsertedVariants == that.nonInsertedVariants;
    }

    @Override
    public int hashCode() {
        int result = (int) (newDocuments ^ (newDocuments >>> 32));
        result = 31 * result + (int) (updatedObjects ^ (updatedObjects >>> 32));
        result = 31 * result + (int) (skippedVariants ^ (skippedVariants >>> 32));
        result = 31 * result + (int) (nonInsertedVariants ^ (nonInsertedVariants >>> 32));
        return result;
    }

}
