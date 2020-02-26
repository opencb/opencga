package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.opencga.storage.hadoop.variant.index.family.MendelianErrorSampleIndexEntryIterator;

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
    private SampleIndexConfiguration configuration;

    public SampleIndexEntry(int sampleId, String chromosome, int batchStart, SampleIndexConfiguration configuration) {
        this.sampleId = sampleId;
        this.chromosome = chromosome;
        this.batchStart = batchStart;
        this.gts = new HashMap<>(4);
        this.configuration = configuration;
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

    public SampleIndexConfiguration getConfiguration() {
        return configuration;
    }

    public SampleIndexEntryIterator iterator(String gt) {
        return new SampleIndexVariantBiConverter().toVariantsIterator(this, gt);
    }

    public MendelianErrorSampleIndexEntryIterator mendelianIterator() {
        return new MendelianErrorSampleIndexEntryIterator(this);
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
                && Objects.equals(chromosome, that.chromosome)
                && Objects.equals(gts, that.gts)
                && Bytes.equals(mendelianVariantsValue, mendelianVariantsOffset, mendelianVariantsLength,
                that.mendelianVariantsValue, that.mendelianVariantsOffset, that.mendelianVariantsLength)
                && Objects.equals(configuration, that.configuration);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(sampleId, chromosome, batchStart, gts, mendelianVariantsLength, mendelianVariantsOffset, configuration);
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

        private byte[] ctBtIndex;
        private int ctBtIndexOffset;
        private int ctBtIndexLength;

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

        public SampleIndexEntryIterator iterator() {
            return iterator(false);
        }

        public SampleIndexEntryIterator iterator(boolean onlyCount) {
            if (onlyCount) {
                return new SampleIndexVariantBiConverter().toVariantsCountIterator(SampleIndexEntry.this, gt);
            } else {
                return new SampleIndexVariantBiConverter().toVariantsIterator(SampleIndexEntry.this, gt);
            }
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

        public byte getFileIndex(int idx) {
            return fileIndex[fileIndexOffset + idx];
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

        public short getConsequenceTypeIndex(int nonIntergenicIndex) {
            return Bytes.toShort(consequenceTypeIndex, consequenceTypeIndexOffset + nonIntergenicIndex * Short.BYTES);
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

        public byte getBiotypeIndex(int idx) {
            return biotypeIndex[biotypeIndexOffset + idx];
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

        public byte[] getCtBtIndex() {
            return ctBtIndex;
        }

        public int getCtBtIndexOffset() {
            return ctBtIndexOffset;
        }

        public int getCtBtIndexLength() {
            return ctBtIndexLength;
        }

        public SampleIndexGtEntry setCtBtIndex(byte[] ctBtIndex) {
            return setCtBtIndex(ctBtIndex, 0, ctBtIndex.length);
        }

        public SampleIndexGtEntry setCtBtIndex(byte[] ctBtIndex, int offset, int length) {
            this.ctBtIndex = ctBtIndex;
            this.ctBtIndexOffset = offset;
            this.ctBtIndexLength = length;
            return this;
        }

        public byte[] getPopulationFrequencyIndex() {
            return populationFrequencyIndex;
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
            sb.append(", variants=").append(variants == null ? "null" : Bytes.toStringBinary(variants, variantsOffset, variantsLength));
            sb.append(", fileIndex=").append(fileIndex == null ? "null" : Bytes.toStringBinary(fileIndex, fileIndexOffset, fileIndexLength));
            sb.append(", annotationIndex=").append(annotationIndex == null ? "null" : Bytes.toStringBinary(annotationIndex, annotationIndexOffset, annotationIndexLength));
            sb.append(", annotationCounts=").append(Arrays.toString(annotationCounts));
            sb.append(", consequenceTypeIndex=").append(consequenceTypeIndex == null ? "null" : Bytes.toStringBinary(consequenceTypeIndex, consequenceTypeIndexOffset, consequenceTypeIndexLength));
            sb.append(", biotypeIndex=").append(biotypeIndex == null ? "null" : Bytes.toStringBinary(biotypeIndex, biotypeIndexOffset, biotypeIndexLength));
            sb.append(", ctBtIndex=").append(ctBtIndex == null ? "null" : Bytes.toStringBinary(ctBtIndex, ctBtIndexOffset, ctBtIndexLength));
            sb.append(", populationFrequencyIndex=").append(populationFrequencyIndex == null ? "null" : Bytes.toStringBinary(populationFrequencyIndex, populationFrequencyIndexOffset, populationFrequencyIndexLength));
            sb.append(", clinicalIndex=").append(clinicalIndex == null ? "null" : Bytes.toStringBinary(clinicalIndex, clinicalIndexOffset, clinicalIndexLength));
            sb.append(", parentsIndex=").append(parentsIndex == null ? "null" : Bytes.toStringBinary(parentsIndex, parentsIndexOffset, parentsIndexLength));
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
                    && Bytes.equals(fileIndex, fileIndexOffset, that.fileIndexLength,
                        that.fileIndex, that.fileIndexOffset, that.fileIndexLength)
                    && Bytes.equals(variants, variantsOffset, that.variantsLength,
                        that.variants, that.variantsOffset, that.variantsLength)
                    && Bytes.equals(annotationIndex, annotationIndexOffset, that.annotationIndexLength,
                        that.annotationIndex, that.annotationIndexOffset, that.annotationIndexLength)
                    && Bytes.equals(consequenceTypeIndex, consequenceTypeIndexOffset, that.consequenceTypeIndexLength,
                        that.consequenceTypeIndex, that.consequenceTypeIndexOffset, that.consequenceTypeIndexLength)
                    && Bytes.equals(biotypeIndex, biotypeIndexOffset, that.biotypeIndexLength,
                        that.biotypeIndex, that.biotypeIndexOffset, that.biotypeIndexLength)
                    && Bytes.equals(ctBtIndex, ctBtIndexOffset, that.ctBtIndexLength,
                        that.ctBtIndex, that.ctBtIndexOffset, that.ctBtIndexLength)
                    && Bytes.equals(populationFrequencyIndex, populationFrequencyIndexOffset, that.populationFrequencyIndexLength,
                        that.populationFrequencyIndex, that.populationFrequencyIndexOffset, that.populationFrequencyIndexLength)
                    && Bytes.equals(clinicalIndex, clinicalIndexOffset, that.clinicalIndexLength,
                        that.clinicalIndex, that.clinicalIndexOffset, that.clinicalIndexLength)
                    && Bytes.equals(parentsIndex, parentsIndexOffset, that.parentsIndexLength,
                        that.parentsIndex, that.parentsIndexOffset, that.parentsIndexLength);
        }
    }
}
