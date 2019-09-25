package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.opencga.storage.hadoop.variant.index.family.MendelianErrorSampleIndexEntryIterator;

import java.util.HashMap;
import java.util.Map;

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

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("chromosome", chromosome)
                .append("batchStart", batchStart)
                .append("gts", gts)
                .toString();
    }

    public int getSampleId() {
        return sampleId;
    }

    public SampleIndexEntry setSampleId(int sampleId) {
        this.sampleId = sampleId;
        return this;
    }

    public class SampleIndexGtEntry {
        private String gt;

        private int count;

        private byte[] variants;
        private int variantsLength;
        private int variantsOffset;
        private byte[] fileIndexGt;
        private byte[] annotationIndexGt;
        private int[] annotationCounts;
        private byte[] consequenceTypeIndexGt;
        private byte[] biotypeIndexGt;
        private byte[] ctBtIndexGt;
        private byte[] populationFrequencyIndexGt;
        private byte[] parentsGt;

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

        public byte[] getFileIndexGt() {
            return fileIndexGt;
        }

        public SampleIndexGtEntry setFileIndexGt(byte[] fileIndexGt) {
            this.fileIndexGt = fileIndexGt;
            return this;
        }

        public byte[] getAnnotationIndexGt() {
            return annotationIndexGt;
        }

        public SampleIndexGtEntry setAnnotationIndexGt(byte[] annotationIndexGt) {
            this.annotationIndexGt = annotationIndexGt;
            return this;
        }

        public int[] getAnnotationCounts() {
            return annotationCounts;
        }

        public SampleIndexGtEntry setAnnotationCounts(int[] annotationCounts) {
            this.annotationCounts = annotationCounts;
            return this;
        }

        public byte[] getConsequenceTypeIndexGt() {
            return consequenceTypeIndexGt;
        }

        public short getConsequenceTypeIndexGt(int nonIntergenicIndex) {
            return Bytes.toShort(consequenceTypeIndexGt, nonIntergenicIndex * Short.BYTES);
        }

        public SampleIndexGtEntry setConsequenceTypeIndexGt(byte[] consequenceTypeIndexGt) {
            this.consequenceTypeIndexGt = consequenceTypeIndexGt;
            return this;
        }

        public byte[] getBiotypeIndexGt() {
            return biotypeIndexGt;
        }

        public SampleIndexGtEntry setBiotypeIndexGt(byte[] biotypeIndexGt) {
            this.biotypeIndexGt = biotypeIndexGt;
            return this;
        }

        public byte[] getCtBtIndexGt() {
            return ctBtIndexGt;
        }

        public SampleIndexGtEntry setCtBtIndexGt(byte[] ctBtIndexGt) {
            this.ctBtIndexGt = ctBtIndexGt;
            return this;
        }

        public byte[] getPopulationFrequencyIndexGt() {
            return populationFrequencyIndexGt;
        }

        public SampleIndexGtEntry setPopulationFrequencyIndexGt(byte[] populationFrequencyIndexGt) {
            this.populationFrequencyIndexGt = populationFrequencyIndexGt;
            return this;
        }

        public byte[] getParentsGt() {
            return parentsGt;
        }

        public SampleIndexGtEntry setParentsGt(byte[] parentsGt) {
            this.parentsGt = parentsGt;
            return this;
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("gt", gt)
                    .append("variants", variants)
                    .append("fileIndexGt", fileIndexGt)
                    .append("annotationIndexGt", annotationIndexGt)
                    .append("annotationCounts", annotationCounts)
                    .append("parentsGt", parentsGt)
                    .toString();
        }
    }
}
