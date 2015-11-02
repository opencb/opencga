package org.opencb.opencga.storage.mongodb.variant;

/**
 * Created on 30/10/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoDBVariantWriteResult {

    private int newDocuments;
    private int updatedObjects;
    private int skippedVariants;
    private int nonInsertedVariants;

    public MongoDBVariantWriteResult() {
    }

    public MongoDBVariantWriteResult(int newDocuments, int updatedObjects, int skippedVariants, int nonInsertedVariants) {
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
        }
    }

    public int getNewDocuments() {
        return newDocuments;
    }

    public MongoDBVariantWriteResult setNewDocuments(int newDocuments) {
        this.newDocuments = newDocuments;
        return this;
    }

    public int getUpdatedObjects() {
        return updatedObjects;
    }

    public MongoDBVariantWriteResult setUpdatedObjects(int updatedObjects) {
        this.updatedObjects = updatedObjects;
        return this;
    }

    public int getSkippedVariants() {
        return skippedVariants;
    }

    public MongoDBVariantWriteResult setSkippedVariants(int skippedVariants) {
        this.skippedVariants = skippedVariants;
        return this;
    }

    public int getNonInsertedVariants() {
        return nonInsertedVariants;
    }

    public MongoDBVariantWriteResult setNonInsertedVariants(int nonInsertedVariants) {
        this.nonInsertedVariants = nonInsertedVariants;
        return this;
    }

    @Override
    public String toString() {
        return "MongoDBVariantWriteResult{" +
                "newDocuments=" + newDocuments +
                ", updatedObjects=" + updatedObjects +
                ", skippedVariants=" + skippedVariants +
                ", nonInsertedVariants=" + nonInsertedVariants +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MongoDBVariantWriteResult)) return false;

        MongoDBVariantWriteResult that = (MongoDBVariantWriteResult) o;

        if (newDocuments != that.newDocuments) return false;
        if (updatedObjects != that.updatedObjects) return false;
        if (skippedVariants != that.skippedVariants) return false;
        return nonInsertedVariants == that.nonInsertedVariants;

    }

    @Override
    public int hashCode() {
        int result = newDocuments;
        result = 31 * result + updatedObjects;
        result = 31 * result + skippedVariants;
        result = 31 * result + nonInsertedVariants;
        return result;
    }
}
