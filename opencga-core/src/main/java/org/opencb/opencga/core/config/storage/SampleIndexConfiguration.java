package org.opencb.opencga.core.config.storage;

import htsjdk.variant.vcf.VCFConstants;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.avro.ClinicalSignificance;

import java.beans.ConstructorProperties;
import java.util.*;

import static org.opencb.opencga.core.models.variant.VariantAnnotationConstants.*;

public class SampleIndexConfiguration {

    public static final int DEFAULT_FILE_POSITION_SIZE_BITS = 3;
    private static final double[] QUAL_THRESHOLDS = new double[]{10, 20, 30};
    private static final double[] DP_THRESHOLDS = new double[]{5, 10, 15, 20, 30, 40, 50};
    private static final double[] DP_THRESHOLDS_NULLABLE = new double[]{5, 10, 15, 20, 30, 50};
    private final List<PopulationFrequencyRange> populationRanges = new ArrayList<>();
    private final FileIndexConfiguration fileIndexConfiguration = new FileIndexConfiguration();
    private final AnnotationIndexConfiguration annotationIndexConfiguration = new AnnotationIndexConfiguration();

    public static SampleIndexConfiguration backwardCompatibleConfiguration() {
        SampleIndexConfiguration sampleIndexConfiguration = new SampleIndexConfiguration()
                .addPopulationRange(new PopulationFrequencyRange("1kG_phase3", "ALL"))
                .addPopulationRange(new PopulationFrequencyRange("GNOMAD_GENOMES", "ALL"))
                .addFileIndexField(new IndexFieldConfiguration(
                        IndexFieldConfiguration.Source.FILE,
                        StudyEntry.FILTER,
                        IndexFieldConfiguration.Type.CATEGORICAL,
                        VCFConstants.PASSES_FILTERS_v4))
                .addFileIndexField(new IndexFieldConfiguration(
                        IndexFieldConfiguration.Source.FILE, StudyEntry.QUAL, QUAL_THRESHOLDS).setNullable(false))
                .addFileIndexField(new IndexFieldConfiguration(
                        IndexFieldConfiguration.Source.SAMPLE, VCFConstants.DEPTH_KEY, DP_THRESHOLDS).setNullable(false));

        sampleIndexConfiguration.getFileIndexConfiguration().setFilePositionBits(4);

        // Ensure backward compatibility with these two params:
        sampleIndexConfiguration.addFileIndexField(new IndexFieldConfiguration(
                IndexFieldConfiguration.Source.SAMPLE, "padding", IndexFieldConfiguration.Type.CATEGORICAL,
                "add_two_extra_bits", "to_allow_backward", "compatibility"));
        sampleIndexConfiguration.getFileIndexConfiguration().setFixedFieldsFirst(false);

        IndexFieldConfiguration biotypeConfiguration = new IndexFieldConfiguration(IndexFieldConfiguration.Source.ANNOTATION,
                "biotype",
                IndexFieldConfiguration.Type.CATEGORICAL_MULTI_VALUE)
                .setValues(
                        NONSENSE_MEDIATED_DECAY,
                        LINCRNA,
                        MIRNA,
                        RETAINED_INTRON,
                        SNRNA,
                        SNORNA,
                        "other_non_pseudo_gene",
//                        "other",
                        PROTEIN_CODING
                ).setValuesMapping(new HashMap<>());
        biotypeConfiguration.getValuesMapping().put(LINCRNA, Arrays.asList(
                "lncRNA",
                NON_CODING,
                LINCRNA,
                "macro_lncRNA",
                ANTISENSE,
                SENSE_INTRONIC,
                SENSE_OVERLAPPING,
                THREEPRIME_OVERLAPPING_NCRNA,
                "bidirectional_promoter_lncRNA"));
        biotypeConfiguration.getValuesMapping().put("other_non_pseudo_gene", Arrays.asList(
                PROCESSED_TRANSCRIPT,
                NON_STOP_DECAY,
                MISC_RNA,
                RRNA,
                MT_RRNA,
                MT_TRNA,
                IG_C_GENE,
                IG_D_GENE,
                IG_J_GENE,
                IG_V_GENE,
                TR_C_GENE,
                TR_D_GENE,
                TR_J_GENE,
                TR_V_GENE,
                NMD_TRANSCRIPT_VARIANT,
                TRANSCRIBED_UNPROCESSED_PSEUDGENE,
                AMBIGUOUS_ORF,
                KNOWN_NCRNA,
                RETROTRANSPOSED,
                LRG_GENE
        ));
        biotypeConfiguration.setNullable(false);

        sampleIndexConfiguration.getAnnotationIndexConfiguration().setBiotype(biotypeConfiguration);
        IndexFieldConfiguration consequenceType = new IndexFieldConfiguration(
                IndexFieldConfiguration.Source.ANNOTATION,
                "consequenceType",
                IndexFieldConfiguration.Type.CATEGORICAL_MULTI_VALUE)
                .setValues(
                        SPLICE_DONOR_VARIANT,
                        TRANSCRIPT_ABLATION,
                        TRANSCRIPT_AMPLIFICATION,
                        INITIATOR_CODON_VARIANT,
                        SPLICE_REGION_VARIANT,
                        INCOMPLETE_TERMINAL_CODON_VARIANT,
                        "utr",
                        "mirna_tfbs",
                        MISSENSE_VARIANT,
                        FRAMESHIFT_VARIANT,
                        INFRAME_DELETION,
                        INFRAME_INSERTION,
                        START_LOST,
                        STOP_GAINED,
                        STOP_LOST,
                        SPLICE_ACCEPTOR_VARIANT
                ).setValuesMapping(new HashMap<>());
        consequenceType.getValuesMapping().put("mirna_tfbs", Arrays.asList(
                TF_BINDING_SITE_VARIANT,
                MATURE_MIRNA_VARIANT));
        consequenceType.getValuesMapping().put("utr", Arrays.asList(
                THREE_PRIME_UTR_VARIANT,
                FIVE_PRIME_UTR_VARIANT));
        consequenceType.setNullable(false);

        sampleIndexConfiguration.getAnnotationIndexConfiguration().setConsequenceType(consequenceType);

        sampleIndexConfiguration.getAnnotationIndexConfiguration().setClinicalSource(
                new IndexFieldConfiguration(
                        IndexFieldConfiguration.Source.ANNOTATION, "clinicalSource",
                        IndexFieldConfiguration.Type.CATEGORICAL_MULTI_VALUE, "cosmic")
                        .setNullable(false));
        sampleIndexConfiguration.getAnnotationIndexConfiguration().setClinicalSignificance(
                new IndexFieldConfiguration(
                        IndexFieldConfiguration.Source.ANNOTATION, "clinicalSignificance",
                        IndexFieldConfiguration.Type.CATEGORICAL_MULTI_VALUE,
                        ClinicalSignificance.likely_benign.toString(),
                        ClinicalSignificance.uncertain_significance.toString(),
                        ClinicalSignificance.likely_pathogenic.toString(),
                        ClinicalSignificance.pathogenic.toString(),
                        "unused_target_drug",
                        "unused_pgx",
                        "unused_bit8"
                ).setNullable(false));

        return sampleIndexConfiguration;
    }

    public static SampleIndexConfiguration defaultConfiguration() {
        SampleIndexConfiguration sampleIndexConfiguration = new SampleIndexConfiguration()
                .addPopulationRange(new PopulationFrequencyRange("1kG_phase3", "ALL"))
                .addPopulationRange(new PopulationFrequencyRange("GNOMAD_GENOMES", "ALL"))
                .addFileIndexField(new IndexFieldConfiguration(
                        IndexFieldConfiguration.Source.FILE,
                        StudyEntry.FILTER,
                        IndexFieldConfiguration.Type.CATEGORICAL,
                        VCFConstants.PASSES_FILTERS_v4))
                .addFileIndexField(new IndexFieldConfiguration(
                        IndexFieldConfiguration.Source.FILE, StudyEntry.QUAL, QUAL_THRESHOLDS).setNullable(false))
                .addFileIndexField(new IndexFieldConfiguration(
                        IndexFieldConfiguration.Source.SAMPLE, VCFConstants.DEPTH_KEY, DP_THRESHOLDS_NULLABLE).setNullable(true));

        sampleIndexConfiguration.getFileIndexConfiguration()
                .setFilePositionBits(DEFAULT_FILE_POSITION_SIZE_BITS);

        IndexFieldConfiguration biotypeConfiguration = new IndexFieldConfiguration(IndexFieldConfiguration.Source.ANNOTATION,
                "biotype",
                IndexFieldConfiguration.Type.CATEGORICAL_MULTI_VALUE)
                .setValues(
                        NONSENSE_MEDIATED_DECAY,
                        LINCRNA,
                        MIRNA,
                        RETAINED_INTRON,
                        SNRNA,
                        SNORNA,
                        "other_non_pseudo_gene",
//                        "other",
                        PROTEIN_CODING
                ).setValuesMapping(new HashMap<>());
        biotypeConfiguration.getValuesMapping().put(LINCRNA, Arrays.asList(
                "lncRNA",
                NON_CODING,
                LINCRNA,
                "macro_lncRNA",
                ANTISENSE,
                SENSE_INTRONIC,
                SENSE_OVERLAPPING,
                THREEPRIME_OVERLAPPING_NCRNA,
                "bidirectional_promoter_lncRNA"));
        biotypeConfiguration.getValuesMapping().put("other_non_pseudo_gene", Arrays.asList(
                PROCESSED_TRANSCRIPT
        ));

        sampleIndexConfiguration.getAnnotationIndexConfiguration().setBiotype(biotypeConfiguration);
        IndexFieldConfiguration consequenceType = new IndexFieldConfiguration(
                IndexFieldConfiguration.Source.ANNOTATION,
                "consequenceType",
                IndexFieldConfiguration.Type.CATEGORICAL_MULTI_VALUE)
                .setValues(
                        MISSENSE_VARIANT,
                        FRAMESHIFT_VARIANT,
                        INFRAME_DELETION,
                        INFRAME_INSERTION,
                        START_LOST,
                        STOP_GAINED,
                        STOP_LOST,
                        SPLICE_ACCEPTOR_VARIANT,
                        SPLICE_DONOR_VARIANT,
                        TRANSCRIPT_ABLATION,
                        TRANSCRIPT_AMPLIFICATION,
                        INITIATOR_CODON_VARIANT,
                        SPLICE_REGION_VARIANT,
                        INCOMPLETE_TERMINAL_CODON_VARIANT,
                        FEATURE_TRUNCATION,
                        SYNONYMOUS_VARIANT,
                        REGULATORY_REGION_VARIANT,
                        TF_BINDING_SITE_VARIANT,
                        MATURE_MIRNA_VARIANT,
                        UPSTREAM_GENE_VARIANT,
                        DOWNSTREAM_GENE_VARIANT,
                        THREE_PRIME_UTR_VARIANT,
                        FIVE_PRIME_UTR_VARIANT,
                        INTRON_VARIANT
                ).setValuesMapping(new HashMap<>());

        sampleIndexConfiguration.getAnnotationIndexConfiguration().setConsequenceType(consequenceType);

        sampleIndexConfiguration.getAnnotationIndexConfiguration().setClinicalSignificance(
                        new IndexFieldConfiguration(
                                IndexFieldConfiguration.Source.ANNOTATION, "clinicalSignificance",
                                IndexFieldConfiguration.Type.CATEGORICAL_MULTI_VALUE,
                                ClinicalSignificance.benign.toString(),
                                ClinicalSignificance.likely_benign.toString(),
//                                ClinicalSignificance.VUS.toString(),
                                ClinicalSignificance.uncertain_significance.toString(),
                                ClinicalSignificance.likely_pathogenic.toString(),
                                ClinicalSignificance.pathogenic.toString())
                                .setNullable(false));
        sampleIndexConfiguration.getAnnotationIndexConfiguration().setClinicalSource(
                new IndexFieldConfiguration(
                        IndexFieldConfiguration.Source.ANNOTATION, "clinicalSource",
                        IndexFieldConfiguration.Type.CATEGORICAL_MULTI_VALUE,
                        "clinvar",
                        "cosmic")
                        .setNullable(false)
        );

        return sampleIndexConfiguration;
    }

    public void validate() {
        for (IndexFieldConfiguration customField : fileIndexConfiguration.getCustomFields()) {
            customField.validate();
        }
        for (PopulationFrequencyRange populationRange : populationRanges) {
            populationRange.validate();
        }
    }

    public static class FileIndexConfiguration {

        private final List<IndexFieldConfiguration> customFields = new ArrayList<>();
        private int filePositionBits = DEFAULT_FILE_POSITION_SIZE_BITS;
        private boolean fixedFieldsFirst = true;

        public FileIndexConfiguration() {
        }

        public FileIndexConfiguration(int filePositionBits, boolean fixedFieldsFirst) {
            this.filePositionBits = filePositionBits;
            this.fixedFieldsFirst = fixedFieldsFirst;
        }

        public List<IndexFieldConfiguration> getCustomFields() {
            return customFields;
        }

        public int getFilePositionBits() {
            return filePositionBits;
        }

        public FileIndexConfiguration setFilePositionBits(int filePositionBits) {
            this.filePositionBits = filePositionBits;
            return this;
        }

        public boolean isFixedFieldsFirst() {
            return fixedFieldsFirst;
        }

        public FileIndexConfiguration setFixedFieldsFirst(boolean fixedFieldsFirst) {
            this.fixedFieldsFirst = fixedFieldsFirst;
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FileIndexConfiguration that = (FileIndexConfiguration) o;
            return filePositionBits == that.filePositionBits
                    && fixedFieldsFirst == that.fixedFieldsFirst
                    && Objects.equals(customFields, that.customFields);
        }

        @Override
        public int hashCode() {
            return Objects.hash(customFields, filePositionBits, fixedFieldsFirst);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("FileIndexConfiguration{");
            sb.append("customFields=").append(customFields);
            sb.append(", filePositionBits=").append(filePositionBits);
            sb.append(", fixedFieldsFirst=").append(fixedFieldsFirst);
            sb.append('}');
            return sb.toString();
        }
    }

    public static class PopulationFrequencyRange extends IndexFieldConfiguration {
        private static final double[] DEFAULT_THRESHOLDS = new double[]{0.001, 0.005, 0.01};
        private String study;
        private String population;

        public PopulationFrequencyRange(String studyPopulation) {
            super(Source.ANNOTATION, studyPopulation, DEFAULT_THRESHOLDS);
            this.study = studyPopulation.split(":")[0];
            this.population = studyPopulation.split(":")[1];
            setNullable(false);
        }

        public PopulationFrequencyRange(String study, String population) {
            super(Source.ANNOTATION, study + ":" + population, DEFAULT_THRESHOLDS);
            this.study = study;
            this.population = population;
            setNullable(false);
        }

        @ConstructorProperties({"source", "key", "type"})
        protected PopulationFrequencyRange(Source source, String key, Type type) {
            super(source, key, type);
        }

        public String getStudy() {
            return study;
        }

        public PopulationFrequencyRange setStudy(String study) {
            this.study = study;
            return this;
        }

        public String getPopulation() {
            return population;
        }

        public PopulationFrequencyRange setPopulation(String population) {
            this.population = population;
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PopulationFrequencyRange that = (PopulationFrequencyRange) o;
            return super.equals(that) && Objects.equals(study, that.study) && Objects.equals(population, that.population);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), study, population);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("PopulationFrequencyRange{");
            sb.append("study='").append(study).append('\'');
            sb.append(", population='").append(population).append('\'');
            sb.append(", source=").append(source);
            sb.append(", key='").append(key).append('\'');
            sb.append(", type=").append(type);
            sb.append(", thresholds=").append(Arrays.toString(thresholds));
            sb.append(", values=").append(Arrays.toString(values));
            sb.append('}');
            return sb.toString();
        }
    }

    public static class AnnotationIndexConfiguration {

        private List<SampleIndexConfiguration.PopulationFrequencyRange> populationFrequency;

        private IndexFieldConfiguration biotype;

        private IndexFieldConfiguration consequenceType;

        private IndexFieldConfiguration clinicalSignificance;

        private IndexFieldConfiguration clinicalSource;

        public List<PopulationFrequencyRange> getPopulationFrequency() {
            return populationFrequency;
        }

        public AnnotationIndexConfiguration setPopulationFrequency(List<PopulationFrequencyRange> populationFrequency) {
            this.populationFrequency = populationFrequency;
            return this;
        }

        public IndexFieldConfiguration getBiotype() {
            return biotype;
        }

        public AnnotationIndexConfiguration setBiotype(IndexFieldConfiguration biotype) {
            this.biotype = biotype;
            return this;
        }

        public IndexFieldConfiguration getConsequenceType() {
            return consequenceType;
        }

        public AnnotationIndexConfiguration setConsequenceType(IndexFieldConfiguration consequenceType) {
            this.consequenceType = consequenceType;
            return this;
        }

        public IndexFieldConfiguration getClinicalSignificance() {
            return clinicalSignificance;
        }

        public AnnotationIndexConfiguration setClinicalSignificance(IndexFieldConfiguration clinicalSignificance) {
            this.clinicalSignificance = clinicalSignificance;
            return this;
        }

        public IndexFieldConfiguration getClinicalSource() {
            return clinicalSource;
        }

        public AnnotationIndexConfiguration setClinicalSource(IndexFieldConfiguration clinicalSource) {
            this.clinicalSource = clinicalSource;
            return this;
        }
    }

    public AnnotationIndexConfiguration getAnnotationIndexConfiguration() {
        return annotationIndexConfiguration;
    }

    public List<SampleIndexConfiguration.PopulationFrequencyRange> getPopulationRanges() {
        return populationRanges;
    }

    public SampleIndexConfiguration addPopulationRange(SampleIndexConfiguration.PopulationFrequencyRange populationRange) {
        if (populationRanges.contains(populationRange)) {
            throw new IllegalArgumentException("Duplicated population '"
                    + populationRange.getKey() + "' in SampleIndexConfiguration");
        }
        this.populationRanges.add(populationRange);
        return this;
    }

    public FileIndexConfiguration getFileIndexConfiguration() {
        return fileIndexConfiguration;
    }


    public SampleIndexConfiguration addFileIndexField(IndexFieldConfiguration fileIndex) {
        if (fileIndexConfiguration.getCustomFields().contains(fileIndex)) {
            throw new IllegalArgumentException("Duplicated file index '"
                    + fileIndex.getKey() + "' in SampleIndexConfiguration");
        }
        this.fileIndexConfiguration.getCustomFields().add(fileIndex);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SampleIndexConfiguration that = (SampleIndexConfiguration) o;
        return Objects.equals(populationRanges, that.populationRanges)
                && Objects.equals(fileIndexConfiguration, that.fileIndexConfiguration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(populationRanges, fileIndexConfiguration);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleIndexConfiguration{");
        sb.append("populationRanges=").append(populationRanges);
        sb.append(", fileIndexConfiguration=").append(fileIndexConfiguration);
        sb.append('}');
        return sb.toString();
    }
}
