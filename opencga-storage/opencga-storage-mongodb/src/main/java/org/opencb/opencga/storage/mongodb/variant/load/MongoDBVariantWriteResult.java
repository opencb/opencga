/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.mongodb.variant.load;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Created on 30/10/15.
 *
 *                +------+------+
 *                | File |  DB  |
 *  +-------------+------+------+
 *  | 1:100:A:C   | DATA | ---- | <-- NewVariants. Insert new document
 *  +-------------+------+------+
 *  | 1:150:G:T   | ---- | DATA | <-- Missing Variants. Update with "?/?". FillGaps
 *  +-------------+------+------+
 *  | 1:200:T:C   | DATA | DATA | <-- Update Variants
 *  +-------------+------+------+
 *  | 1:200:T:G   | ---- | DATA | <-- Overlapped Variant. Not in the input file, but present in the DB. Have to write twice.
 *  +-------------+------+------+
 *  | 1:250:G:GT  | DATA | ???? | <--\
 *  +-------------+------+------+     |-- Normalize into the same variant, duplicated information : 1:251:-:T
 *  | 1:251:C:TC  | DATA | ???? | <--/
 *  +-------------+------+------+
 *  | 1:300:GCT:G | DATA | ???? | <--\
 *  +-------------+------+------+     |-- Overlapped variants in the same document. Potentially inconsistent information
 *  | 1:301:C:T   | DATA | ???? | <--/
 *  +-------------+------+------+
 *
 *  @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoDBVariantWriteResult {

    /** Number of new variants inserted in the Database. Variants never seen in any other study */
    private long newVariants;
//    /** Number of new variants for this study. Variants already seen in other studies, but first time in the current study*/
//    private long newStudy;
    /** Number of existing variants, updated with new information. */
    private long updatedVariants;
    /** Number of existing variants that were not present in the loaded variants. Missing variants. */
    private long updatedMissingVariants;
    /** New overlapped variants, not in the input file. */
    private long overlappedVariants;
    /**
     * Ignored variants.
     * @see org.opencb.biodata.models.variant.avro.VariantType#SYMBOLIC
     * @see org.opencb.biodata.models.variant.avro.VariantType#NO_VARIATION
     * */
    private long skippedVariants;
    /** Non inserted variants, due to duplicated or overlapped variants in the same file. */
    private long nonInsertedVariants;

    /** Time in nanoseconds into inserting the new variants. */
    private long newVariantsNanoTime;
    /** Time in nanoseconds into updating the existing variants. */
    private long existingVariantsNanoTime;
    /** Time in nanoseconds into updating the missing variants. */
    private long fillGapsNanoTime;

    /** List of Genotypes seen in all loaded variants. */
    private Set<String> genotypes;

    public MongoDBVariantWriteResult() {
        genotypes = new HashSet<>();
    }

    public MongoDBVariantWriteResult(long newVariants, long updatedVariants, long updatedMissingVariants, long overlappedVariants,
                                     long skippedVariants, long nonInsertedVariants) {
        this.newVariants = newVariants;
        this.updatedVariants = updatedVariants;
        this.updatedMissingVariants = updatedMissingVariants;
        this.overlappedVariants = overlappedVariants;
        this.skippedVariants = skippedVariants;
        this.nonInsertedVariants = nonInsertedVariants;
        this.genotypes = new HashSet<>();
    }


    public MongoDBVariantWriteResult(long newVariants, long updatedVariants, long updatedMissingVariants, long overlappedVariants,
                                     long skippedVariants, long nonInsertedVariants,
                                     long newVariantsNanoTime, long existingVariantsNanoTime, long fillGapsNanoTime,
                                     Set<String> genotypes) {
        this.newVariants = newVariants;
        this.updatedVariants = updatedVariants;
        this.updatedMissingVariants = updatedMissingVariants;
        this.overlappedVariants = overlappedVariants;
        this.skippedVariants = skippedVariants;
        this.nonInsertedVariants = nonInsertedVariants;
        this.newVariantsNanoTime = newVariantsNanoTime;
        this.existingVariantsNanoTime = existingVariantsNanoTime;
        this.fillGapsNanoTime = fillGapsNanoTime;
        this.genotypes = genotypes;
    }

    public void merge(MongoDBVariantWriteResult... others) {
        for (MongoDBVariantWriteResult other : others) {
            newVariants += other.newVariants;
            updatedVariants += other.updatedVariants;
            updatedMissingVariants += other.updatedMissingVariants;
            overlappedVariants += other.overlappedVariants;
            skippedVariants += other.skippedVariants;
            nonInsertedVariants += other.nonInsertedVariants;
            newVariantsNanoTime += other.newVariantsNanoTime;
            existingVariantsNanoTime += other.existingVariantsNanoTime;
            fillGapsNanoTime += other.fillGapsNanoTime;
            genotypes.addAll(other.genotypes);
        }
    }

    public long getNewVariants() {
        return newVariants;
    }

    public MongoDBVariantWriteResult setNewVariants(long newVariants) {
        this.newVariants = newVariants;
        return this;
    }

    public long getUpdatedVariants() {
        return updatedVariants;
    }

    public MongoDBVariantWriteResult setUpdatedVariants(long updatedVariants) {
        this.updatedVariants = updatedVariants;
        return this;
    }

    public long getUpdatedMissingVariants() {
        return updatedMissingVariants;
    }

    public MongoDBVariantWriteResult setUpdatedMissingVariants(long updatedMissingVariants) {
        this.updatedMissingVariants = updatedMissingVariants;
        return this;
    }

    public long getOverlappedVariants() {
        return overlappedVariants;
    }

    public MongoDBVariantWriteResult setOverlappedVariants(long overlappedVariants) {
        this.overlappedVariants = overlappedVariants;
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

    public Set<String> getGenotypes() {
        return genotypes;
    }

    public MongoDBVariantWriteResult setGenotypes(Set<String> genotypes) {
        this.genotypes = genotypes;
        return this;
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
        return newVariants == that.newVariants
                && updatedVariants == that.updatedVariants
                && updatedMissingVariants == that.updatedMissingVariants
                && overlappedVariants == that.overlappedVariants
                && skippedVariants == that.skippedVariants
                && nonInsertedVariants == that.nonInsertedVariants
                && newVariantsNanoTime == that.newVariantsNanoTime
                && existingVariantsNanoTime == that.existingVariantsNanoTime
                && fillGapsNanoTime == that.fillGapsNanoTime
                && Objects.equals(genotypes, that.genotypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                newVariants,
                updatedVariants,
                updatedMissingVariants,
                overlappedVariants,
                skippedVariants,
                nonInsertedVariants,
                newVariantsNanoTime,
                existingVariantsNanoTime,
                fillGapsNanoTime,
                genotypes);
    }

    @Override
    public String toString() {
        return "MongoDBVariantWriteResult{"
                + "newVariants:" + newVariants
                + ", updatedVariants:" + updatedVariants
                + ", updatedMissingVariants:" + updatedMissingVariants
                + ", overlappedVariants:" + overlappedVariants
                + ", skippedVariants:" + skippedVariants
                + ", nonInsertedVariants:" + nonInsertedVariants
                + ", newVariantsTime=" + newVariantsNanoTime / 1000000000.0 + "s"
                + ", existingVariantsTime=" + existingVariantsNanoTime / 1000000000.0 + "s"
                + ", fillGapsTime=" + fillGapsNanoTime / 1000000000.0 + "s"
                + '}';
    }


    public String toJson() {
        return "{\n"
                + "\tnewVariants:" + newVariants + ",\n"
                + "\tupdatedVariants:" + updatedVariants + ",\n"
                + "\tupdatedMissingVariants:" + updatedMissingVariants + ",\n"
                + "\toverlappedVariants:" + overlappedVariants + ",\n"
                + "\tskippedVariants:" + skippedVariants + ",\n"
                + "\tnonInsertedVariants:" + nonInsertedVariants + ",\n"
                + "\tnewVariantsTime:" + newVariantsNanoTime / 1000000000.0 + ",\n"
                + "\texistingVariantsTime:" + existingVariantsNanoTime / 1000000000.0 + ",\n"
                + "\tfillGapsTime:" + fillGapsNanoTime / 1000000000.0 + "\n"
                + '}';
    }

    public String toTSV() {
        return "#newVariants\tupdatedVariants\tupdatedMissingVariants\toverlappedVariants\tskippedVariants\tnonInsertedVariants"
                + "\tnewVariantsTime\texistingVariantsTime\tfillGapsTime\t"
                + "\n"
                + newVariants + '\t'
                + updatedVariants + '\t'
                + updatedMissingVariants + '\t'
                + overlappedVariants + '\t'
                + skippedVariants + '\t'
                + nonInsertedVariants + '\t'
                + newVariantsNanoTime / 1000000000.0 + '\t'
                + existingVariantsNanoTime / 1000000000.0 + '\t'
                + fillGapsNanoTime / 1000000000.0;
    }


}
