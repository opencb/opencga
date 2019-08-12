package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.opencb.opencga.storage.hadoop.variant.index.family.MendelianErrorSampleIndexConverter.MendelianErrorSampleIndexVariantIterator;

import java.util.Map;

/**
 * Created on 18/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexEntry {

    private int sampleId;
    private String chromosome;
    private int batchStart;
    private Map<String, SampleIndexGtEntry> gts;
    private MendelianErrorSampleIndexVariantIterator mendelianVariants;

    public SampleIndexEntry(int sampleId, String chromosome, int batchStart,
                            Map<String, SampleIndexGtEntry> gts, MendelianErrorSampleIndexVariantIterator mendelianVariants) {
        this.sampleId = sampleId;
        this.chromosome = chromosome;
        this.batchStart = batchStart;
        this.gts = gts;
        this.mendelianVariants = mendelianVariants;
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

    public SampleIndexEntry setGts(Map<String, SampleIndexGtEntry> gts) {
        this.gts = gts;
        return this;
    }

    public MendelianErrorSampleIndexVariantIterator getMendelianVariants() {
        return mendelianVariants;
    }

    public SampleIndexEntry setMendelianVariants(MendelianErrorSampleIndexVariantIterator mendelianVariants) {
        this.mendelianVariants = mendelianVariants;
        return this;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("chromosome", chromosome)
                .append("chunkStart", batchStart)
                .append("gts", gts)
                .append("mendelianVariants", mendelianVariants)
                .toString();
    }

    public int getSampleId() {
        return sampleId;
    }

    public SampleIndexEntry setSampleId(int sampleId) {
        this.sampleId = sampleId;
        return this;
    }

    public static class SampleIndexGtEntry {
        private String gt;

        private int count;
        private SampleIndexVariantBiConverter.SampleIndexVariantIterator variants;
        private byte[] fileIndexGt;
        private byte[] annotationIndexGt;
        private int[] annotationCounts;
        private byte[] consequenceTypeIndexGt;
        private byte[] biotypeIndexGt;
        private byte[] populationFrequencyIndexGt;
        private byte[] parentsGt;

        public SampleIndexGtEntry(String gt) {
            this.gt = gt;
            variants = SampleIndexVariantBiConverter.SampleIndexVariantIterator.emptyIterator();
        }

        public SampleIndexGtEntry(SampleIndexVariantBiConverter.SampleIndexVariantIterator variants, byte[] fileIndexGt,
                                  byte[] annotationIndexGt, int[] annotationCounts, byte[] parentsGt) {
            this.setVariants(variants);
            this.setFileIndexGt(fileIndexGt);
            this.setAnnotationIndexGt(annotationIndexGt);
            this.setAnnotationCounts(annotationCounts);
            this.setParentsGt(parentsGt);
            this.count = -1;
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

        public int getApproxNumVariants() {
            if (count > 0) {
                return count;
            } else {
                return variants.getApproxSize();
            }
        }

        public SampleIndexVariantBiConverter.SampleIndexVariantIterator getVariants() {
            return variants;
        }

        public SampleIndexGtEntry setVariants(SampleIndexVariantBiConverter.SampleIndexVariantIterator variants) {
            this.variants = variants;
            if (annotationIndexGt != null) {
                variants.setAnnotationIndex(annotationIndexGt);
            }
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
            if (annotationIndexGt != null && variants != null) {
                variants.setAnnotationIndex(annotationIndexGt);
            }
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
