package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.opencga.storage.core.io.bit.BitInputStream;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Model representing an entry (row) of the SampleIndex.
 *
 * Use the {@link SampleIndexEntryIterator} to read the variants and the annotation from the entry.
 *
 * Created on 18/04/19.
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexEntry {

    private int sampleId;
    private String chromosome;
    private int batchStart;
    private Map<String, SampleIndexGtEntry> gts;
    private byte[] mendelianVariantsValue;
    private int mendelianVariantsLength;
    private int mendelianVariantsOffset;
    private int discrepancies;

    public SampleIndexEntry(int sampleId, String chromosome, int batchStart) {
        this.sampleId = sampleId;
        this.chromosome = chromosome;
        this.batchStart = batchStart;
        this.gts = new HashMap<>(4);
    }

    public String getChromosome() {
        return chromosome;
    }

    public SampleIndexEntry setChromosome(String chromosome) {
        this.chromosome = chromosome;
        return this;
    }

    public int getBatchStart() {
        return batchStart;
    }

    public SampleIndexEntry setBatchStart(int batchStart) {
        this.batchStart = batchStart;
        return this;
    }

    public Map<String, SampleIndexGtEntry> getGts() {
        return gts;
    }

    public SampleIndexGtEntry getGtEntry(String gt) {
        return gts.computeIfAbsent(gt, SampleIndexGtEntry::new);
    }

    public SampleIndexEntry setGts(Map<String, SampleIndexGtEntry> gts) {
        this.gts = gts;
        return this;
    }

    public byte[] getMendelianVariantsValue() {
        return mendelianVariantsValue;
    }

    public SampleIndexEntry setMendelianVariants(byte[] mendelianVariantsValue) {
        return setMendelianVariants(mendelianVariantsValue, 0, mendelianVariantsValue.length);
    }

    public SampleIndexEntry setMendelianVariants(byte[] mendelianVariantsValue, int offset, int length) {
        this.mendelianVariantsValue = mendelianVariantsValue;
        this.mendelianVariantsLength = length;
        this.mendelianVariantsOffset = offset;
        return this;
    }

    public int getMendelianVariantsLength() {
        return mendelianVariantsLength;
    }

    public SampleIndexEntry setMendelianVariantsLength(int mendelianVariantsLength) {
        this.mendelianVariantsLength = mendelianVariantsLength;
        return this;
    }

    public int getMendelianVariantsOffset() {
        return mendelianVariantsOffset;
    }

    public SampleIndexEntry setMendelianVariantsOffset(int mendelianVariantsOffset) {
        this.mendelianVariantsOffset = mendelianVariantsOffset;
        return this;
    }

    public int getDiscrepancies() {
        return discrepancies;
    }

    public SampleIndexEntry setDiscrepancies(int discrepancies) {
        this.discrepancies = discrepancies;
        return this;
    }

    public int getSampleId() {
        return sampleId;
    }

    public SampleIndexEntry setSampleId(int sampleId) {
        this.sampleId = sampleId;
        return this;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("sampleId", sampleId)
                .append("chromosome", chromosome)
                .append("batchStart", batchStart)
                .append("gts", gts)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SampleIndexEntry that = (SampleIndexEntry) o;
        return sampleId == that.sampleId
                && batchStart == that.batchStart
                && discrepancies == that.discrepancies
                && Objects.equals(chromosome, that.chromosome)
                && Objects.equals(gts, that.gts)
                && Bytes.equals(mendelianVariantsValue, mendelianVariantsOffset, mendelianVariantsLength,
                that.mendelianVariantsValue, that.mendelianVariantsOffset, that.mendelianVariantsLength);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(sampleId, chromosome, batchStart, gts, mendelianVariantsLength, mendelianVariantsOffset);
        result = 31 * result + Arrays.hashCode(mendelianVariantsValue);
        return result;
    }

    public class SampleIndexGtEntry {
        private String gt;

        private int count;

        private byte[] variants;
        private int variantsOffset;
        private int variantsLength;

        private byte[] fileIndex;
        private int fileIndexOffset;
        private int fileIndexLength;

        private byte[] annotationIndex;
        private int annotationIndexOffset;
        private int annotationIndexLength;

        private int[] annotationCounts;

        private byte[] consequenceTypeIndex;
        private int consequenceTypeIndexOffset;
        private int consequenceTypeIndexLength;

        private byte[] biotypeIndex;
        private int biotypeIndexOffset;
        private int biotypeIndexLength;

        private byte[] transcriptFlagIndex;
        private int transcriptFlagIndexOffset;
        private int transcriptFlagIndexLength;

        private byte[] ctBtTfIndex;
        private int ctBtTfIndexOffset;
        private int ctBtTfIndexLength;

        private byte[] populationFrequencyIndex;
        private int populationFrequencyIndexOffset;
        private int populationFrequencyIndexLength;

        private byte[] clinicalIndex;
        private int clinicalIndexOffset;
        private int clinicalIndexLength;

        private byte[] parentsIndex;
        private int parentsIndexOffset;
        private int parentsIndexLength;

        public SampleIndexGtEntry(String gt) {
            this.gt = gt;
        }

        public SampleIndexEntry getEntry() {
            return SampleIndexEntry.this;
        }

        public String getGt() {
            return gt;
        }

        public SampleIndexGtEntry setGt(String gt) {
            this.gt = gt;
            return this;
        }

        public int getCount() {
            return count;
        }

        public SampleIndexGtEntry setCount(int count) {
            this.count = count;
            return this;
        }

        public byte[] getVariants() {
            return variants;
        }

        public SampleIndexGtEntry setVariants(byte[] variants, int offset, int length) {
            this.variants = variants;
            this.variantsLength = length;
            this.variantsOffset = offset;
            return this;
        }

        public SampleIndexGtEntry setVariants(byte[] variants) {
            this.variants = variants;
            this.variantsOffset = 0;
            this.variantsLength = variants.length;
            return this;
        }

        public int getVariantsLength() {
            return variantsLength;
        }

        public SampleIndexGtEntry setVariantsLength(int variantsLength) {
            this.variantsLength = variantsLength;
            return this;
        }

        public int getVariantsOffset() {
            return variantsOffset;
        }

        public SampleIndexGtEntry setVariantsOffset(int variantsOffset) {
            this.variantsOffset = variantsOffset;
            return this;
        }

        public byte[] getFileIndex() {
            return fileIndex;
        }

        public BitInputStream getFileIndexStream() {
            return fileIndex == null ? null : new BitInputStream(fileIndex, fileIndexOffset, fileIndexLength);
        }

        public int getFileIndexOffset() {
            return fileIndexOffset;
        }

        public int getFileIndexLength() {
            return fileIndexLength;
        }

        public SampleIndexGtEntry setFileIndex(byte[] fileIndex) {
            return setFileIndex(fileIndex, 0, fileIndex.length);
        }

        public SampleIndexGtEntry setFileIndex(byte[] fileIndex, int offset, int length) {
            this.fileIndex = fileIndex;
            this.fileIndexOffset = offset;
            this.fileIndexLength = length;
            return this;
        }

        public byte[] getAnnotationIndex() {
            return annotationIndex;
        }

        public byte getAnnotationIndex(int idx) {
            return annotationIndex[idx + annotationIndexOffset];
        }

        public SampleIndexGtEntry setAnnotationIndex(byte[] annotationIndex) {
            return setAnnotationIndex(annotationIndex, 0, annotationIndex.length);
        }

        public SampleIndexGtEntry setAnnotationIndex(byte[] annotationIndex, int offset, int length) {
            this.annotationIndex = annotationIndex;
            this.annotationIndexOffset = offset;
            this.annotationIndexLength = length;
            return this;
        }

        public int[] getAnnotationCounts() {
            return annotationCounts;
        }

        public SampleIndexGtEntry setAnnotationCounts(int[] annotationCounts) {
            this.annotationCounts = annotationCounts;
            return this;
        }

        public byte[] getConsequenceTypeIndex() {
            return consequenceTypeIndex;
        }

        public BitInputStream getConsequenceTypeIndexStream() {
            return consequenceTypeIndex == null
                    ? null
                    : new BitInputStream(consequenceTypeIndex, consequenceTypeIndexOffset, consequenceTypeIndexLength);
        }

        public SampleIndexGtEntry setConsequenceTypeIndex(byte[] consequenceTypeIndex) {
            return setConsequenceTypeIndex(consequenceTypeIndex, 0, consequenceTypeIndex.length);
        }

        public SampleIndexGtEntry setConsequenceTypeIndex(byte[] consequenceTypeIndex, int offset, int length) {
            this.consequenceTypeIndex = consequenceTypeIndex;
            this.consequenceTypeIndexOffset = offset;
            this.consequenceTypeIndexLength = length;
            return this;
        }

        public byte[] getBiotypeIndex() {
            return biotypeIndex;
        }

        public BitInputStream getBiotypeIndexStream() {
            return biotypeIndex == null ? null : new BitInputStream(biotypeIndex, biotypeIndexOffset, biotypeIndexLength);
        }

        public SampleIndexGtEntry setBiotypeIndex(byte[] biotypeIndex) {
            return setBiotypeIndex(biotypeIndex, 0, biotypeIndex.length);
        }

        public SampleIndexGtEntry setBiotypeIndex(byte[] biotypeIndex, int offset, int length) {
            this.biotypeIndex = biotypeIndex;
            this.biotypeIndexOffset = offset;
            this.biotypeIndexLength = length;
            return this;
        }

        public BitInputStream getTranscriptFlagIndexStream() {
            return transcriptFlagIndex == null
                    ? null
                    : new BitInputStream(transcriptFlagIndex, transcriptFlagIndexOffset, transcriptFlagIndexLength);
        }

        public SampleIndexGtEntry setTranscriptFlagIndex(byte[] value) {
            return setTranscriptFlagIndex(value, 0, value.length);
        }

        public SampleIndexGtEntry setTranscriptFlagIndex(byte[] value, int offset, int length) {
            this.transcriptFlagIndex = value;
            this.transcriptFlagIndexOffset = offset;
            this.transcriptFlagIndexLength = length;
            return this;
        }

        public byte[] getCtBtTfIndex() {
            return ctBtTfIndex;
        }

        public BitInputStream getCtBtTfIndexStream() {
            return ctBtTfIndex == null ? null : new BitInputStream(ctBtTfIndex, ctBtTfIndexOffset, ctBtTfIndexLength);
        }

        public int getCtBtTfIndexOffset() {
            return ctBtTfIndexOffset;
        }

        public int getCtBtTfIndexLength() {
            return ctBtTfIndexLength;
        }

        public SampleIndexGtEntry setCtBtTfIndex(byte[] ctBtTfIndex) {
            return setCtBtTfIndex(ctBtTfIndex, 0, ctBtTfIndex.length);
        }

        public SampleIndexGtEntry setCtBtTfIndex(byte[] ctBtTfIndex, int offset, int length) {
            this.ctBtTfIndex = ctBtTfIndex;
            this.ctBtTfIndexOffset = offset;
            this.ctBtTfIndexLength = length;
            return this;
        }

        public byte[] getPopulationFrequencyIndex() {
            return populationFrequencyIndex;
        }

        public BitInputStream getPopulationFrequencyIndexStream() {
            return populationFrequencyIndex == null
                    ? null
                    : new BitInputStream(populationFrequencyIndex, populationFrequencyIndexOffset, populationFrequencyIndexLength);
        }

        public int getPopulationFrequencyIndexOffset() {
            return populationFrequencyIndexOffset;
        }

        public int getPopulationFrequencyIndexLength() {
            return populationFrequencyIndexLength;
        }

        public SampleIndexGtEntry setPopulationFrequencyIndex(byte[] populationFrequencyIndex) {
            return setPopulationFrequencyIndex(populationFrequencyIndex, 0, populationFrequencyIndex.length);
        }

        public SampleIndexGtEntry setPopulationFrequencyIndex(byte[] populationFrequencyIndex, int offset, int length) {
            this.populationFrequencyIndex = populationFrequencyIndex;
            this.populationFrequencyIndexOffset = offset;
            this.populationFrequencyIndexLength = length;
            return this;
        }

        public byte[] getClinicalIndex() {
            return clinicalIndex;
        }

        public BitInputStream getClinicalIndexStream() {
            return clinicalIndex == null
                    ? null
                    : new BitInputStream(clinicalIndex, clinicalIndexOffset, clinicalIndexLength);
        }

        @Deprecated
        public byte getClinicalIndex(int idx) {
            return clinicalIndex[clinicalIndexOffset + idx];
        }

        public SampleIndexGtEntry setClinicalIndex(byte[] clinicalIndex, int offset, int length) {
            this.clinicalIndex = clinicalIndex;
            this.clinicalIndexOffset = offset;
            this.clinicalIndexLength = length;
            return this;
        }

        public byte[] getParentsIndex() {
            return parentsIndex;
        }

        public byte getParentsIndex(int idx) {
            return parentsIndex[parentsIndexOffset + idx];
        }

        public SampleIndexGtEntry setParentsIndex(byte[] parentsIndex) {
            return setParentsIndex(parentsIndex, 0, parentsIndex.length);
        }

        public SampleIndexGtEntry setParentsIndex(byte[] parentsIndex, int offset, int length) {
            this.parentsIndex = parentsIndex;
            this.parentsIndexOffset = offset;
            this.parentsIndexLength = length;
            return this;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("SampleIndexGtEntry{");
            sb.append("gt='").append(gt).append('\'');
            sb.append(", count=").append(count);
            sb.append(", variants=").append(variants == null ? "null"
                    : Bytes.toStringBinary(variants, variantsOffset, variantsLength));
            sb.append(", fileIndex=").append(fileIndex == null ? "null"
                    : Bytes.toStringBinary(fileIndex, fileIndexOffset, fileIndexLength));
            sb.append(", annotationIndex=").append(annotationIndex == null ? "null"
                    : Bytes.toStringBinary(annotationIndex, annotationIndexOffset, annotationIndexLength));
            sb.append(", annotationIndexLength=").append(annotationIndexLength);
            sb.append(", annotationCounts=").append(Arrays.toString(annotationCounts));
            sb.append(", consequenceTypeIndex=").append(consequenceTypeIndex == null ? "null"
                    : Bytes.toStringBinary(consequenceTypeIndex, consequenceTypeIndexOffset, consequenceTypeIndexLength));
            sb.append(", consequenceTypeIndexLength=").append(consequenceTypeIndexLength);
            sb.append(", biotypeIndex=").append(biotypeIndex == null ? "null"
                    : Bytes.toStringBinary(biotypeIndex, biotypeIndexOffset, biotypeIndexLength));
            sb.append(", biotypeIndexLength=").append(biotypeIndexLength);
            sb.append(", transcriptFlagIndex=").append(transcriptFlagIndex == null ? "null"
                    : Bytes.toStringBinary(transcriptFlagIndex, transcriptFlagIndexOffset, transcriptFlagIndexLength));
            sb.append(", transcriptFlagIndexLength=").append(transcriptFlagIndexLength);
            sb.append(", ctBtTfIndex=").append(ctBtTfIndex == null ? "null"
                    : Bytes.toStringBinary(ctBtTfIndex, ctBtTfIndexOffset, ctBtTfIndexLength));
            sb.append(", ctBtTfIndexLength=").append(ctBtTfIndexLength);
            sb.append(", populationFrequencyIndex=").append(populationFrequencyIndex == null ? "null"
                    : Bytes.toStringBinary(populationFrequencyIndex, populationFrequencyIndexOffset, populationFrequencyIndexLength));
            sb.append(", clinicalIndex=").append(clinicalIndex == null ? "null"
                    : Bytes.toStringBinary(clinicalIndex, clinicalIndexOffset, clinicalIndexLength));
            sb.append(", parentsIndex=").append(parentsIndex == null ? "null"
                    : Bytes.toStringBinary(parentsIndex, parentsIndexOffset, parentsIndexLength));
            sb.append('}');
            return sb.toString();
        }

        public String toStringSummary() {
            final StringBuilder sb = new StringBuilder("SampleIndexGtEntry{");
            sb.append("gt='").append(gt).append('\'');
            sb.append(", count=").append(count);
            sb.append(", variants=(")
                    .append(IndexUtils.bytesToSummary(variants, variantsOffset, variantsLength))
                    .append(")");
            sb.append(", fileIndex=(")
                    .append(IndexUtils.bytesToSummary(fileIndex, fileIndexOffset, fileIndexLength))
                    .append(")");
            sb.append(", annotationIndex=(")
                    .append(IndexUtils.bytesToSummary(annotationIndex, annotationIndexOffset, annotationIndexLength))
                    .append(")");
            sb.append(", annotationIndexLength=").append(annotationIndexLength);
            sb.append(", annotationCounts=").append(Arrays.toString(annotationCounts));
            sb.append(", consequenceTypeIndex=(")
                    .append(IndexUtils.bytesToSummary(consequenceTypeIndex, consequenceTypeIndexOffset, consequenceTypeIndexLength))
                    .append(")");
            sb.append(", consequenceTypeIndexLength=").append(consequenceTypeIndexLength);
            sb.append(", biotypeIndex=(")
                    .append(IndexUtils.bytesToSummary(biotypeIndex, biotypeIndexOffset, biotypeIndexLength))
                    .append(")");
            sb.append(", biotypeIndexLength=").append(biotypeIndexLength);
            sb.append(", transcriptFlagIndex=(")
                    .append(IndexUtils.bytesToSummary(transcriptFlagIndex, transcriptFlagIndexOffset, transcriptFlagIndexLength))
                    .append(")");
            sb.append(", transcriptFlagIndexLength=").append(transcriptFlagIndexLength);
            sb.append(", ctBtTfIndex=(")
                    .append(IndexUtils.bytesToSummary(ctBtTfIndex, ctBtTfIndexOffset, ctBtTfIndexLength))
                    .append(")");
            sb.append(", ctBtTfIndexLength=").append(ctBtTfIndexLength);
            sb.append(", populationFrequencyIndex=(")
                    .append(IndexUtils.bytesToSummary(
                            populationFrequencyIndex, populationFrequencyIndexOffset, populationFrequencyIndexLength))
                    .append(")");
            sb.append(", clinicalIndex=(")
                    .append(IndexUtils.bytesToSummary(clinicalIndex, clinicalIndexOffset, clinicalIndexLength))
                    .append(")");
            sb.append(", parentsIndex=(")
                    .append(IndexUtils.bytesToSummary(parentsIndex, parentsIndexOffset, parentsIndexLength))
                    .append(")");
            sb.append('}');
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SampleIndexGtEntry that = (SampleIndexGtEntry) o;
            return count == that.count
                    && Objects.equals(gt, that.gt)
                    && Arrays.equals(annotationCounts, that.annotationCounts)
                    && Bytes.equals(fileIndex, fileIndexOffset, fileIndexLength,
                        that.fileIndex, that.fileIndexOffset, that.fileIndexLength)
                    && Bytes.equals(variants, variantsOffset, variantsLength,
                        that.variants, that.variantsOffset, that.variantsLength)
                    && Bytes.equals(annotationIndex, annotationIndexOffset, annotationIndexLength,
                        that.annotationIndex, that.annotationIndexOffset, that.annotationIndexLength)
                    && Bytes.equals(consequenceTypeIndex, consequenceTypeIndexOffset, consequenceTypeIndexLength,
                        that.consequenceTypeIndex, that.consequenceTypeIndexOffset, that.consequenceTypeIndexLength)
                    && Bytes.equals(biotypeIndex, biotypeIndexOffset, biotypeIndexLength,
                        that.biotypeIndex, that.biotypeIndexOffset, that.biotypeIndexLength)
                    && Bytes.equals(ctBtTfIndex, ctBtTfIndexOffset, ctBtTfIndexLength,
                        that.ctBtTfIndex, that.ctBtTfIndexOffset, that.ctBtTfIndexLength)
                    && Bytes.equals(populationFrequencyIndex, populationFrequencyIndexOffset, populationFrequencyIndexLength,
                        that.populationFrequencyIndex, that.populationFrequencyIndexOffset, that.populationFrequencyIndexLength)
                    && Bytes.equals(clinicalIndex, clinicalIndexOffset, clinicalIndexLength,
                        that.clinicalIndex, that.clinicalIndexOffset, that.clinicalIndexLength)
                    && Bytes.equals(parentsIndex, parentsIndexOffset, parentsIndexLength,
                        that.parentsIndex, that.parentsIndexOffset, that.parentsIndexLength);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(gt, count);
            result = 31 * result + Arrays.hashCode(annotationCounts);
            result = 31 * result + Bytes.hashCode(variants, variantsOffset, variantsLength);
            result = 31 * result + Bytes.hashCode(fileIndex, fileIndexOffset, fileIndexLength);
            result = 31 * result + Bytes.hashCode(annotationIndex, annotationIndexOffset, annotationIndexLength);
            result = 31 * result + Bytes.hashCode(consequenceTypeIndex, consequenceTypeIndexOffset, consequenceTypeIndexLength);
            result = 31 * result + Bytes.hashCode(biotypeIndex, biotypeIndexOffset, biotypeIndexLength);
            result = 31 * result + Bytes.hashCode(ctBtTfIndex, ctBtTfIndexOffset, ctBtTfIndexLength);
            result = 31 * result + Bytes.hashCode(populationFrequencyIndex, populationFrequencyIndexOffset, populationFrequencyIndexLength);
            result = 31 * result + Bytes.hashCode(clinicalIndex, clinicalIndexOffset, clinicalIndexLength);
            result = 31 * result + Bytes.hashCode(parentsIndex, parentsIndexOffset, parentsIndexLength);
            return result;
        }
    }
}
