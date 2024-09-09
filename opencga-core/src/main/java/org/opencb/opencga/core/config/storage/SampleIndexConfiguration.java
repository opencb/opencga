package org.opencb.opencga.core.config.storage;

import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.collections4.CollectionUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.avro.ClinicalSignificance;
import org.opencb.opencga.core.api.ParamConstants;

import java.beans.ConstructorProperties;
import java.util.*;

import static org.opencb.opencga.core.models.variant.VariantAnnotationConstants.*;

public class SampleIndexConfiguration {

    public static final int DEFAULT_FILE_POSITION_SIZE_BITS = 3;
    private static final double[] QUAL_THRESHOLDS = new double[]{10, 20, 30};
    private static final double[] DP_THRESHOLDS_NULLABLE = new double[]{5, 10, 15, 20, 30, 50};

    private final FileIndexConfiguration fileIndexConfiguration = new FileIndexConfiguration();
    private final FileDataConfiguration fileDataConfiguration = new FileDataConfiguration();
    private final AnnotationIndexConfiguration annotationIndexConfiguration = new AnnotationIndexConfiguration();

    public static SampleIndexConfiguration defaultConfiguration() {
        return defaultConfiguration(false);
    }

    public static SampleIndexConfiguration defaultConfiguration(boolean cellbaseV4) {
        SampleIndexConfiguration sampleIndexConfiguration = new SampleIndexConfiguration()
                .addPopulation(new Population(cellbaseV4
                        ? ParamConstants.POP_FREQ_1000G_CB_V4
                        : ParamConstants.POP_FREQ_1000G_CB_V5, "ALL"))
                .addPopulation(new Population(ParamConstants.POP_FREQ_GNOMAD_GENOMES, "ALL"))
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
        sampleIndexConfiguration.getFileDataConfiguration()
                .setIncludeOriginalCall(true)
                .setIncludeSecondaryAlternates(true);

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

        sampleIndexConfiguration.getAnnotationIndexConfiguration().setTranscriptFlagIndexConfiguration(
                new IndexFieldConfiguration(
                        IndexFieldConfiguration.Source.ANNOTATION,
                        "transcriptFlag",
                        IndexFieldConfiguration.Type.CATEGORICAL_MULTI_VALUE,
                        "canonical",
                        "MANE Select",
                        "MANE Plus Clinical",
                        "CCDS",
                        "basic",
                        "LRG",
                        "EGLH_HaemOnc",
                        "TSO500"
                        ).setNullable(true));
        sampleIndexConfiguration.getAnnotationIndexConfiguration().setTranscriptCombination(true);

        sampleIndexConfiguration.getAnnotationIndexConfiguration().setClinicalSource(
                new IndexFieldConfiguration(
                        IndexFieldConfiguration.Source.ANNOTATION, "clinicalSource",
                        IndexFieldConfiguration.Type.CATEGORICAL_MULTI_VALUE,
                        "clinvar",
                        "cosmic")
                        .setNullable(false)
        );

        sampleIndexConfiguration.getAnnotationIndexConfiguration().setClinicalSignificance(
                new IndexFieldConfiguration(
                        IndexFieldConfiguration.Source.ANNOTATION, "clinicalSignificance",
                        IndexFieldConfiguration.Type.CATEGORICAL_MULTI_VALUE,
                        "clinvar_" + ClinicalSignificance.benign.toString(),
                        "clinvar_" + ClinicalSignificance.likely_benign.toString(),
                        "clinvar_" + ClinicalSignificance.uncertain_significance.toString(),
                        "clinvar_" + ClinicalSignificance.likely_pathogenic.toString(),
                        "clinvar_" + ClinicalSignificance.pathogenic.toString(),

                        "clinvar_" + ClinicalSignificance.benign.toString() + "_confirmed",
                        "clinvar_" + ClinicalSignificance.likely_benign.toString() + "_confirmed",
                        "clinvar_" + ClinicalSignificance.uncertain_significance.toString() + "_confirmed",
                        "clinvar_" + ClinicalSignificance.likely_pathogenic.toString() + "_confirmed",
                        "clinvar_" + ClinicalSignificance.pathogenic.toString() + "_confirmed",

                        "cosmic_" + ClinicalSignificance.benign.toString(),
                        "cosmic_" + ClinicalSignificance.pathogenic.toString(),

                        "cosmic_" + ClinicalSignificance.benign.toString() + "_confirmed",
                        "cosmic_" + ClinicalSignificance.pathogenic.toString() + "_confirmed")
                        .setNullable(false)
        );

        return sampleIndexConfiguration;
    }

    public void validate(String cellbaseVersion) {
        addMissingValues(defaultConfiguration("v4".equalsIgnoreCase(cellbaseVersion)));

        for (IndexFieldConfiguration customField : fileIndexConfiguration.getCustomFields()) {
            customField.validate();
        }
        for (IndexFieldConfiguration configuration : annotationIndexConfiguration.getPopulationFrequency().toIndexFieldConfiguration()) {
            configuration.validate();
        }
        annotationIndexConfiguration.biotype.validate();
        annotationIndexConfiguration.consequenceType.validate();
        annotationIndexConfiguration.transcriptFlagIndexConfiguration.validate();
        annotationIndexConfiguration.clinicalSignificance.validate();
        annotationIndexConfiguration.clinicalSource.validate();
    }

    public void addMissingValues(SampleIndexConfiguration defaultConfiguration) {
        if (fileIndexConfiguration.getCustomFields().isEmpty()) {
            fileIndexConfiguration.getCustomFields().addAll(defaultConfiguration.fileIndexConfiguration.customFields);
        }
        if (fileDataConfiguration.includeOriginalCall == null) {
            fileDataConfiguration.includeOriginalCall = defaultConfiguration.fileDataConfiguration.includeOriginalCall;
        }
        if (fileDataConfiguration.includeSecondaryAlternates == null) {
            fileDataConfiguration.includeSecondaryAlternates = defaultConfiguration.fileDataConfiguration.includeSecondaryAlternates;
        }

        if (annotationIndexConfiguration.getPopulationFrequency() == null) {
            annotationIndexConfiguration.setPopulationFrequency(defaultConfiguration.annotationIndexConfiguration.populationFrequency);
        }
        if (annotationIndexConfiguration.getPopulationFrequency().getThresholds() == null) {
            annotationIndexConfiguration.getPopulationFrequency()
                    .setThresholds(defaultConfiguration.annotationIndexConfiguration.populationFrequency.thresholds);
        }
        if (CollectionUtils.isEmpty(annotationIndexConfiguration.getPopulationFrequency().getPopulations())) {
            annotationIndexConfiguration.getPopulationFrequency()
                    .setPopulations(defaultConfiguration.annotationIndexConfiguration.populationFrequency.populations);
        }
        if (annotationIndexConfiguration.biotype == null) {
            annotationIndexConfiguration.biotype = defaultConfiguration.annotationIndexConfiguration.biotype;
        }
        if (annotationIndexConfiguration.consequenceType == null) {
            annotationIndexConfiguration.consequenceType = defaultConfiguration.annotationIndexConfiguration.consequenceType;
        }
        if (annotationIndexConfiguration.transcriptFlagIndexConfiguration == null) {
            annotationIndexConfiguration.transcriptFlagIndexConfiguration = defaultConfiguration.annotationIndexConfiguration
                    .transcriptFlagIndexConfiguration;
        }
        if (annotationIndexConfiguration.transcriptCombination == null) {
            annotationIndexConfiguration.transcriptCombination = defaultConfiguration.annotationIndexConfiguration.transcriptCombination;
        }
        if (annotationIndexConfiguration.clinicalSignificance == null) {
            annotationIndexConfiguration.clinicalSignificance = defaultConfiguration.annotationIndexConfiguration.clinicalSignificance;
        }
        if (annotationIndexConfiguration.clinicalSource == null) {
            annotationIndexConfiguration.clinicalSource = defaultConfiguration.annotationIndexConfiguration.clinicalSource;
        }
    }

    public static class FileDataConfiguration {
        private Boolean includeOriginalCall;
        private Boolean includeSecondaryAlternates;

        public FileDataConfiguration() {
            // By default, left as null.
            // The defaultConfiguration will set it to true when constructed.
            this.includeOriginalCall = null;
            this.includeSecondaryAlternates = null;
        }

        public Boolean getIncludeOriginalCall() {
            return includeOriginalCall;
        }

        public FileDataConfiguration setIncludeOriginalCall(Boolean includeOriginalCall) {
            this.includeOriginalCall = includeOriginalCall;
            return this;
        }

        public boolean isIncludeOriginalCall() {
            return includeOriginalCall != null && includeOriginalCall;
        }

        public Boolean getIncludeSecondaryAlternates() {
            return includeSecondaryAlternates;
        }

        public FileDataConfiguration setIncludeSecondaryAlternates(Boolean includeSecondaryAlternates) {
            this.includeSecondaryAlternates = includeSecondaryAlternates;
            return this;
        }

        public boolean isIncludeSecondaryAlternates() {
            return includeSecondaryAlternates != null && includeSecondaryAlternates;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("FileDataConfiguration{");
            sb.append("includeOriginalCall=").append(includeOriginalCall);
            sb.append(", includeSecondaryAlternates=").append(includeSecondaryAlternates);
            sb.append('}');
            return sb.toString();
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

        public IndexFieldConfiguration getCustomField(IndexFieldConfiguration.Source source, String key) {
            for (IndexFieldConfiguration s : customFields) {
                if (s.getKey().equals(key) && s.getSource() == source) {
                    return s;
                }
            }
            return null;
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

    public static class AnnotationIndexConfiguration {
        private PopulationFrequencyIndexConfiguration populationFrequency = new PopulationFrequencyIndexConfiguration();
        private IndexFieldConfiguration biotype;
        private IndexFieldConfiguration consequenceType;
        private IndexFieldConfiguration clinicalSource;
        private IndexFieldConfiguration clinicalSignificance;
        private IndexFieldConfiguration transcriptFlagIndexConfiguration;
        private Boolean transcriptCombination;

        public PopulationFrequencyIndexConfiguration getPopulationFrequency() {
            return populationFrequency;
        }

        public AnnotationIndexConfiguration setPopulationFrequency(PopulationFrequencyIndexConfiguration populationFrequency) {
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

        public IndexFieldConfiguration getTranscriptFlagIndexConfiguration() {
            return transcriptFlagIndexConfiguration;
        }

        public void setTranscriptFlagIndexConfiguration(IndexFieldConfiguration transcriptFlagIndexConfiguration) {
            this.transcriptFlagIndexConfiguration = transcriptFlagIndexConfiguration;
        }

        public IndexFieldConfiguration getClinicalSource() {
            return clinicalSource;
        }

        public AnnotationIndexConfiguration setClinicalSource(IndexFieldConfiguration clinicalSource) {
            this.clinicalSource = clinicalSource;
            return this;
        }

        public IndexFieldConfiguration getClinicalSignificance() {
            return clinicalSignificance;
        }

        public AnnotationIndexConfiguration setClinicalSignificance(IndexFieldConfiguration clinicalSignificance) {
            this.clinicalSignificance = clinicalSignificance;
            return this;
        }

        public Boolean getTranscriptCombination() {
            return transcriptCombination;
        }

        public AnnotationIndexConfiguration setTranscriptCombination(Boolean transcriptCombination) {
            this.transcriptCombination = transcriptCombination;
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o){
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AnnotationIndexConfiguration that = (AnnotationIndexConfiguration) o;
            return Objects.equals(populationFrequency, that.populationFrequency)
                    && Objects.equals(biotype, that.biotype)
                    && Objects.equals(consequenceType, that.consequenceType)
                    && Objects.equals(transcriptFlagIndexConfiguration, that.transcriptFlagIndexConfiguration)
                    && Objects.equals(clinicalSignificance, that.clinicalSignificance)
                    && Objects.equals(clinicalSource, that.clinicalSource);
        }

        @Override
        public int hashCode() {
            return Objects.hash(populationFrequency, biotype, consequenceType, transcriptFlagIndexConfiguration, clinicalSignificance, clinicalSource);
        }
    }


    public static class PopulationFrequencyIndexConfiguration {
        private static final double[] DEFAULT_THRESHOLDS = new double[]{0.0000001, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05};

        private List<Population> populations = new ArrayList<>(5);
        private double[] thresholds = DEFAULT_THRESHOLDS;

        public List<Population> getPopulations() {
            return populations;
        }

        public PopulationFrequencyIndexConfiguration setPopulations(List<Population> populations) {
            this.populations = populations;
            return this;
        }

        public double[] getThresholds() {
            return thresholds;
        }

        public PopulationFrequencyIndexConfiguration setThresholds(double[] thresholds) {
            this.thresholds = thresholds;
            return this;
        }

        public PopulationFrequencyIndexConfiguration addPopulation(String study, String population) {
            addPopulation(new Population(study, population));
            return this;
        }

        public PopulationFrequencyIndexConfiguration addPopulation(Population population) {
            if (populations.contains(population)) {
                throw new IllegalArgumentException("Duplicated population '"
                        + population.getKey() + "' in SampleIndexConfiguration");
            }
            populations.add(population);
            return this;
        }

        public List<IndexFieldConfiguration> toIndexFieldConfiguration() {
            List<IndexFieldConfiguration> indexFieldConfigurations = new ArrayList<>(populations.size());
            for (Population population : populations) {
                indexFieldConfigurations.add(new IndexFieldConfiguration(
                        IndexFieldConfiguration.Source.ANNOTATION,
                        population.getKey(),
                        IndexFieldConfiguration.Type.RANGE_LT)
                        .setNullable(false)
                        .setThresholds(thresholds));
            }
            return indexFieldConfigurations;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PopulationFrequencyIndexConfiguration that = (PopulationFrequencyIndexConfiguration) o;
            return Objects.equals(populations, that.populations)
                    && Arrays.equals(thresholds, that.thresholds);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(populations);
            result = 31 * result + Arrays.hashCode(thresholds);
            return result;
        }
    }

    public static class Population {
        private String study;
        private String population;

        public Population(String studyPopulation) {
            this.study = studyPopulation.split(":")[0];
            this.population = studyPopulation.split(":")[1];
        }

        @ConstructorProperties({"study", "population"})
        public Population(String study, String population) {
            this.study = study;
            this.population = population;
        }

        private String getKey() {
            return study + ":" + population;
        }

        public String getStudy() {
            return study;
        }

        public Population setStudy(String study) {
            this.study = study;
            return this;
        }

        public String getPopulation() {
            return population;
        }

        public Population setPopulation(String population) {
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
            Population that = (Population) o;
            return Objects.equals(study, that.study) && Objects.equals(population, that.population);
        }

        @Override
        public int hashCode() {
            return Objects.hash(study, population);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("PopulationFrequencyRange{");
            sb.append("study='").append(study).append('\'');
            sb.append(", population='").append(population).append('\'');
            sb.append('}');
            return sb.toString();
        }
    }

    public AnnotationIndexConfiguration getAnnotationIndexConfiguration() {
        return annotationIndexConfiguration;
    }

    public SampleIndexConfiguration addPopulation(Population population) {
        getAnnotationIndexConfiguration().getPopulationFrequency().addPopulation(population);
        return this;
    }

    public FileIndexConfiguration getFileIndexConfiguration() {
        return fileIndexConfiguration;
    }

    public FileDataConfiguration getFileDataConfiguration() {
        return fileDataConfiguration;
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
        return Objects.equals(fileIndexConfiguration, that.fileIndexConfiguration)
                && Objects.equals(annotationIndexConfiguration, that.annotationIndexConfiguration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileIndexConfiguration, annotationIndexConfiguration);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleIndexConfiguration{");
        sb.append("fileIndexConfiguration=").append(fileIndexConfiguration);
        sb.append("fileDataConfiguration=").append(fileDataConfiguration);
        sb.append(", annotationIndexConfiguration=").append(annotationIndexConfiguration);
        sb.append('}');
        return sb.toString();
    }
}
