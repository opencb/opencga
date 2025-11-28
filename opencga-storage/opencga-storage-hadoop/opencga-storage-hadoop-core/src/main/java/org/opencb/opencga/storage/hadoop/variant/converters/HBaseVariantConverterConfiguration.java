package org.opencb.opencga.storage.hadoop.variant.converters;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.models.project.SearchIndexMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjection;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;

import java.util.List;

import static org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass.UNKNOWN_GENOTYPE;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.INCLUDE_SAMPLE_ID;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.getIncludeSampleData;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.isValidParam;

public class HBaseVariantConverterConfiguration {

    public static final String MUTABLE_SAMPLES_POSITION = "HBaseVariantConverterConfiguration.mutableSamplesPosition";
    @Deprecated
    public static final String STUDY_NAME_AS_STUDY_ID = "HBaseVariantConverterConfiguration.studyNameAsStudyId";
    public static final String SIMPLE_GENOTYPES = "HBaseVariantConverterConfiguration.simpleGenotypes";

    private final VariantQueryProjection projection;

    private final boolean failOnEmptyVariants;
    private final boolean studyNameAsStudyId;
    private final boolean simpleGenotypes;
    private final String unknownGenotype;
    private final boolean mutableSamplesPosition;
    private final List<String> sampleDataKeys;
    private final boolean includeSampleId;
    private final long searchIndexCreationTs;
    private final long searchIndexUpdateTs;
    private final boolean sparse;

    public HBaseVariantConverterConfiguration(VariantQueryProjection projection, boolean failOnEmptyVariants,
                                              boolean studyNameAsStudyId, boolean simpleGenotypes, String unknownGenotype,
                                              boolean mutableSamplesPosition, List<String> sampleDataKeys, boolean includeSampleId,
                                              long searchIndexCreationTs, long searchIndexUpdateTs, boolean sparse) {
        this.projection = projection;
        this.failOnEmptyVariants = failOnEmptyVariants;
        this.studyNameAsStudyId = studyNameAsStudyId;
        this.simpleGenotypes = simpleGenotypes;
        this.unknownGenotype = unknownGenotype;
        this.mutableSamplesPosition = mutableSamplesPosition;
        this.sampleDataKeys = sampleDataKeys;
        this.includeSampleId = includeSampleId;
        this.searchIndexCreationTs = searchIndexCreationTs;
        this.searchIndexUpdateTs = searchIndexUpdateTs;
        this.sparse = sparse;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(Configuration configuration) {
        Builder builder = builder();

        if (StringUtils.isNotEmpty(configuration.get(MUTABLE_SAMPLES_POSITION))) {
            builder.setMutableSamplesPosition(configuration.getBoolean(MUTABLE_SAMPLES_POSITION, true));
        }
        if (StringUtils.isNotEmpty(configuration.get(STUDY_NAME_AS_STUDY_ID))) {
            builder.setStudyNameAsStudyId(configuration.getBoolean(STUDY_NAME_AS_STUDY_ID, false));
        }
        if (StringUtils.isNotEmpty(configuration.get(SIMPLE_GENOTYPES))) {
            builder.setSimpleGenotypes(configuration.getBoolean(SIMPLE_GENOTYPES, false));
        }
        Query query = VariantMapReduceUtil.getQueryFromConfig(configuration);

        builder.setUnknownGenotype(query.getString(VariantQueryParam.UNKNOWN_GENOTYPE.key()));
        builder.setSampleDataKeys(getIncludeSampleData(query));
        if (isValidParam(query, VariantQueryUtils.SPARSE_SAMPLES)) {
            builder.setSparse(query.getBoolean(VariantQueryUtils.SPARSE_SAMPLES.key(), false));
        }
        if (isValidParam(query, VariantQueryParam.INCLUDE_SAMPLE_ID)) {
            builder.setIncludeSampleId(query.getBoolean(INCLUDE_SAMPLE_ID.key(), false));
        }

        return builder;
    }

    public static Builder builder(Query query, QueryOptions options) {
        Builder builder = builder();
        if (options == null) {
            return builder;
        }

        if (StringUtils.isNotEmpty(options.getString(MUTABLE_SAMPLES_POSITION))) {
            builder.setMutableSamplesPosition(options.getBoolean(MUTABLE_SAMPLES_POSITION, true));
        }
        if (StringUtils.isNotEmpty(options.getString(STUDY_NAME_AS_STUDY_ID))) {
            builder.setStudyNameAsStudyId(options.getBoolean(STUDY_NAME_AS_STUDY_ID, false));
        }
        if (StringUtils.isNotEmpty(options.getString(SIMPLE_GENOTYPES))) {
            builder.setSimpleGenotypes(options.getBoolean(SIMPLE_GENOTYPES, false));
        }
        builder.setUnknownGenotype(query.getString(VariantQueryParam.UNKNOWN_GENOTYPE.key()));
        builder.setSampleDataKeys(getIncludeSampleData(query));
        if (isValidParam(query, VariantQueryUtils.SPARSE_SAMPLES)) {
            builder.setSparse(query.getBoolean(VariantQueryUtils.SPARSE_SAMPLES.key(), false));
        }
        if (isValidParam(query, VariantQueryParam.INCLUDE_SAMPLE_ID)) {
            builder.setIncludeSampleId(query.getBoolean(INCLUDE_SAMPLE_ID.key(), false));
        }

        return builder;
    }

    public void configure(Configuration configuration) {
        configuration.setBoolean(MUTABLE_SAMPLES_POSITION, mutableSamplesPosition);
        configuration.setBoolean(STUDY_NAME_AS_STUDY_ID, studyNameAsStudyId);
        configuration.setBoolean(SIMPLE_GENOTYPES, simpleGenotypes);
        configuration.setBoolean(VariantQueryUtils.SPARSE_SAMPLES.key(), sparse);
        configuration.set(VariantQueryParam.UNKNOWN_GENOTYPE.key(), unknownGenotype);
    }

    public VariantQueryProjection getProjection() {
        return projection;
    }

    public boolean getStudyNameAsStudyId() {
        return studyNameAsStudyId;
    }

    public boolean getSimpleGenotypes() {
        return simpleGenotypes;
    }

    public boolean getFailOnEmptyVariants() {
        return failOnEmptyVariants;
    }

    public String getUnknownGenotype() {
        return unknownGenotype;
    }

    public boolean getMutableSamplesPosition() {
        return mutableSamplesPosition;
    }

    public boolean getSparse() {
        return sparse;
    }

    /**
     * Format of the converted variants. Discard other values.
     * @see org.opencb.opencga.storage.core.variant.query.VariantQueryUtils#getIncludeSampleData(Query)
     * @return expected format
     */
    public List<String> getSampleDataKeys() {
        return sampleDataKeys;
    }

    public boolean getIncludeSampleId() {
        return includeSampleId;
    }

    public long getSearchIndexCreationTs() {
        return searchIndexCreationTs;
    }

    public long getSearchIndexUpdateTs() {
        return searchIndexUpdateTs;
    }

    public static class Builder {
        private VariantQueryProjection projection;
        private boolean studyNameAsStudyId = false;
        private boolean simpleGenotypes = false;
        private boolean failOnEmptyVariants = false;
        private String unknownGenotype = UNKNOWN_GENOTYPE;
        private boolean mutableSamplesPosition = true;
        private List<String> sampleDataKeys;
        private boolean includeSampleId;
        private long searchIndexCreationTs = -1;
        private long searchIndexUpdateTs = -1;
        private boolean sparse = false;

        public Builder() {
        }

        public Builder setProjection(VariantQueryProjection projection) {
            this.projection = projection;
            return this;
        }

        public Builder setStudyNameAsStudyId(boolean studyNameAsStudyId) {
            this.studyNameAsStudyId = studyNameAsStudyId;
            return this;
        }

        public Builder setSimpleGenotypes(boolean simpleGenotypes) {
            this.simpleGenotypes = simpleGenotypes;
            return this;
        }

        public Builder setFailOnEmptyVariants(boolean failOnEmptyVariants) {
            this.failOnEmptyVariants = failOnEmptyVariants;
            return this;
        }

        public Builder setUnknownGenotype(String unknownGenotype) {
            if (StringUtils.isEmpty(unknownGenotype)) {
                this.unknownGenotype = UNKNOWN_GENOTYPE;
            } else {
                this.unknownGenotype = unknownGenotype;
            }
            return this;
        }

        public Builder setMutableSamplesPosition(boolean mutableSamplesPosition) {
            this.mutableSamplesPosition = mutableSamplesPosition;
            return this;
        }

        public Builder setSampleDataKeys(List<String> sampleDataKeys) {
            this.sampleDataKeys = sampleDataKeys;
            return this;
        }

        public Builder setIncludeSampleId(boolean includeSampleId) {
            this.includeSampleId = includeSampleId;
            return this;
        }

        public Builder setIncludeIndexStatus(SearchIndexMetadata indexMetadata) {
            this.searchIndexCreationTs = indexMetadata.getCreationDateTimestamp();
            this.searchIndexUpdateTs = indexMetadata.getLastUpdateDateTimestamp();
            return this;
        }

        public Builder setSparse(boolean sparse) {
            this.sparse = sparse;
            return this;
        }

        public HBaseVariantConverterConfiguration build() {
            return new HBaseVariantConverterConfiguration(
                    projection,
                    failOnEmptyVariants,
                    studyNameAsStudyId,
                    simpleGenotypes,
                    unknownGenotype,
                    mutableSamplesPosition,
                    sampleDataKeys,
                    includeSampleId,
                    searchIndexCreationTs,
                    searchIndexUpdateTs,
                    sparse);
        }
    }
}
