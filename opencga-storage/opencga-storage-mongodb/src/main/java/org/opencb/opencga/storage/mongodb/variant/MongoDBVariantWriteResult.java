package org.opencb.opencga.storage.mongodb.variant;

/**
 * Created on 30/10/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoDBVariantWriteResult {

    private long newDocuments;
    private long updatedObjects;
    private long skippedVariants;
    private long nonInsertedVariants;
    private long newVariantsNanoTime;
    private long existingVariantsNanoTime;
    private long fillGapsNanoTime;

    public MongoDBVariantWriteResult() {
    }

    public MongoDBVariantWriteResult(long newDocuments, long updatedObjects, long skippedVariants, long nonInsertedVariants) {
        this.newDocuments = newDocuments;
        this.updatedObjects = updatedObjects;
        this.skippedVariants = skippedVariants;
        this.nonInsertedVariants = nonInsertedVariants;
    }

    public void merge(MongoDBVariantWriteResult ... others) {
        for (MongoDBVariantWriteResult other : others) {
            newDocuments += other.newDocuments;
            updatedObjects += other.updatedObjects;
            skippedVariants += other.skippedVariants;
            nonInsertedVariants += other.nonInsertedVariants;
            newVariantsNanoTime += other.newVariantsNanoTime;
            existingVariantsNanoTime += other.existingVariantsNanoTime;
            fillGapsNanoTime += other.fillGapsNanoTime;
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

    public long getNewVariantsNanoTime() {
        return newVariantsNanoTime;
    }

    public MongoDBVariantWriteResult setNewVariantsNanoTime(long newVariantsNanoTime) {
        this.newVariantsNanoTime = newVariantsNanoTime;
        return this;
    }

    public long getExistingVariantsNanoTime() {
        return existingVariantsNanoTime;
    }

    public MongoDBVariantWriteResult setExistingVariantsNanoTime(long existingVariantsNanoTime) {
        this.existingVariantsNanoTime = existingVariantsNanoTime;
        return this;
    }

    public long getFillGapsNanoTime() {
        return fillGapsNanoTime;
    }

    public MongoDBVariantWriteResult setFillGapsNanoTime(long fillGapsNanoTime) {
        this.fillGapsNanoTime = fillGapsNanoTime;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MongoDBVariantWriteResult)) return false;

        MongoDBVariantWriteResult that = (MongoDBVariantWriteResult) o;

        if (newDocuments != that.newDocuments) return false;
        if (updatedObjects != that.updatedObjects) return false;
        if (skippedVariants != that.skippedVariants) return false;
        if (nonInsertedVariants != that.nonInsertedVariants) return false;
        if (newVariantsNanoTime != that.newVariantsNanoTime) return false;
        if (existingVariantsNanoTime != that.existingVariantsNanoTime) return false;
        return fillGapsNanoTime == that.fillGapsNanoTime;

    }

    @Override
    public int hashCode() {
        int result = (int) (newDocuments ^ (newDocuments >>> 32));
        result = 31 * result + (int) (updatedObjects ^ (updatedObjects >>> 32));
        result = 31 * result + (int) (skippedVariants ^ (skippedVariants >>> 32));
        result = 31 * result + (int) (nonInsertedVariants ^ (nonInsertedVariants >>> 32));
        result = 31 * result + (int) (newVariantsNanoTime ^ (newVariantsNanoTime >>> 32));
        result = 31 * result + (int) (existingVariantsNanoTime ^ (existingVariantsNanoTime >>> 32));
        result = 31 * result + (int) (fillGapsNanoTime ^ (fillGapsNanoTime >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "MongoDBVariantWriteResult{" +
                "newDocuments=" + newDocuments +
                ", updatedObjects=" + updatedObjects +
                ", skippedVariants=" + skippedVariants +
                ", nonInsertedVariants=" + nonInsertedVariants +
                ", newVariantsTime=" + newVariantsNanoTime / 1000000000.0 + "s" +
                ", existingVariantsTime=" + existingVariantsNanoTime / 1000000000.0 + "s" +
                ", fillGapsTime=" + fillGapsNanoTime / 1000000000.0 + "s" +
                '}';
    }

}
