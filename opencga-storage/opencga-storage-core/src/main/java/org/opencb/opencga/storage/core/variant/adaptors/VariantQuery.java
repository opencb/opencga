package org.opencb.opencga.storage.core.variant.adaptors;

import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.core.api.ParamConstants;

import java.util.Arrays;
import java.util.List;

public class VariantQuery extends Query {

    public VariantQuery id(String value) {
        put(VariantQueryParam.ID.key(), value);
        return this;
    }

    public VariantQuery id(String... value) {
        put(VariantQueryParam.ID.key(), Arrays.asList(value));
        return this;
    }

    public VariantQuery id(Variant... value) {
        put(VariantQueryParam.ID.key(), Arrays.asList(value));
        return this;
    }

    public VariantQuery id(List<?> value) {
        put(VariantQueryParam.ID.key(), value);
        return this;
    }

    public String id() {
        return getString(VariantQueryParam.ID.key());
    }

    public VariantQuery region(String value) {
        put(VariantQueryParam.REGION.key(), value);
        return this;
    }
    public VariantQuery region(Region... value) {
        put(VariantQueryParam.REGION.key(), Arrays.asList(value));
        return this;
    }
    public String region() {
        return getString(VariantQueryParam.REGION.key());
    }

    public VariantQuery reference(String value) {
        put(VariantQueryParam.REFERENCE.key(), value);
        return this;
    }
    public String reference() {
        return getString(VariantQueryParam.REFERENCE.key());
    }

    public VariantQuery alternate(String value) {
        put(VariantQueryParam.ALTERNATE.key(), value);
        return this;
    }
    public String alternate() {
        return getString(VariantQueryParam.ALTERNATE.key());
    }

    public VariantQuery type(String value) {
        put(VariantQueryParam.TYPE.key(), value);
        return this;
    }
    public String type() {
        return getString(VariantQueryParam.TYPE.key());
    }

    public VariantQuery study(String value) {
        put(VariantQueryParam.STUDY.key(), value);
        return this;
    }
    public String study() {
        return getString(VariantQueryParam.STUDY.key());
    }

    public VariantQuery includeStudy(String value) {
        put(VariantQueryParam.INCLUDE_STUDY.key(), value);
        return this;
    }
    public String includeStudy() {
        return getString(VariantQueryParam.INCLUDE_STUDY.key());
    }

    public VariantQuery sample(String value) {
        put(VariantQueryParam.SAMPLE.key(), value);
        return this;
    }
    public String sample() {
        return getString(VariantQueryParam.SAMPLE.key());
    }

    public VariantQuery genotype(String value) {
        put(VariantQueryParam.GENOTYPE.key(), value);
        return this;
    }
    public String genotype() {
        return getString(VariantQueryParam.GENOTYPE.key());
    }

    public VariantQuery sampleData(String value) {
        put(VariantQueryParam.SAMPLE_DATA.key(), value);
        return this;
    }
    public String sampleData() {
        return getString(VariantQueryParam.SAMPLE_DATA.key());
    }

    public VariantQuery includeSample(List<String> value) {
        put(VariantQueryParam.INCLUDE_SAMPLE.key(), value);
        return this;
    }

    public VariantQuery includeSampleAll() {
        put(VariantQueryParam.INCLUDE_SAMPLE.key(), ParamConstants.ALL);
        return this;
    }

    public VariantQuery includeSample(String... value) {
        return includeSample(Arrays.asList(value));
    }

    public VariantQuery includeSample(String value) {
        put(VariantQueryParam.INCLUDE_SAMPLE.key(), value);
        return this;
    }

    public String includeSample() {
        return getString(VariantQueryParam.INCLUDE_SAMPLE.key());
    }

    public VariantQuery includeSampleId(String value) {
        put(VariantQueryParam.INCLUDE_SAMPLE_ID.key(), value);
        return this;
    }
    public VariantQuery includeSampleId(boolean value) {
        put(VariantQueryParam.INCLUDE_SAMPLE_ID.key(), value);
        return this;
    }
    public String includeSampleId() {
        return getString(VariantQueryParam.INCLUDE_SAMPLE_ID.key());
    }

    public VariantQuery sampleMetadata(String value) {
        put(VariantQueryParam.SAMPLE_METADATA.key(), value);
        return this;
    }
    public String sampleMetadata() {
        return getString(VariantQueryParam.SAMPLE_METADATA.key());
    }

    public VariantQuery includeSampleData(String value) {
        put(VariantQueryParam.INCLUDE_SAMPLE_DATA.key(), value);
        return this;
    }
    public String includeSampleData() {
        return getString(VariantQueryParam.INCLUDE_SAMPLE_DATA.key());
    }

    public VariantQuery includeGenotype(String value) {
        put(VariantQueryParam.INCLUDE_GENOTYPE.key(), value);
        return this;
    }
    public String includeGenotype() {
        return getString(VariantQueryParam.INCLUDE_GENOTYPE.key());
    }

    public VariantQuery sampleLimit(String value) {
        put(VariantQueryParam.SAMPLE_LIMIT.key(), value);
        return this;
    }
    public String sampleLimit() {
        return getString(VariantQueryParam.SAMPLE_LIMIT.key());
    }

    public VariantQuery sampleSkip(String value) {
        put(VariantQueryParam.SAMPLE_SKIP.key(), value);
        return this;
    }
    public String sampleSkip() {
        return getString(VariantQueryParam.SAMPLE_SKIP.key());
    }

    public VariantQuery file(String value) {
        put(VariantQueryParam.FILE.key(), value);
        return this;
    }
    public String file() {
        return getString(VariantQueryParam.FILE.key());
    }

    public VariantQuery fileData(String value) {
        put(VariantQueryParam.FILE_DATA.key(), value);
        return this;
    }
    public String fileData() {
        return getString(VariantQueryParam.FILE_DATA.key());
    }

    public VariantQuery filter(String value) {
        put(VariantQueryParam.FILTER.key(), value);
        return this;
    }
    public String filter() {
        return getString(VariantQueryParam.FILTER.key());
    }

    public VariantQuery qual(String value) {
        put(VariantQueryParam.QUAL.key(), value);
        return this;
    }
    public String qual() {
        return getString(VariantQueryParam.QUAL.key());
    }

    public VariantQuery includeFile(String value) {
        put(VariantQueryParam.INCLUDE_FILE.key(), value);
        return this;
    }
    public String includeFile() {
        return getString(VariantQueryParam.INCLUDE_FILE.key());
    }

    public VariantQuery cohort(String value) {
        put(VariantQueryParam.COHORT.key(), value);
        return this;
    }
    public String cohort() {
        return getString(VariantQueryParam.COHORT.key());
    }

    public VariantQuery cohortStatsRef(String value) {
        put(VariantQueryParam.STATS_REF.key(), value);
        return this;
    }
    public String cohortStatsRef() {
        return getString(VariantQueryParam.STATS_REF.key());
    }

    public VariantQuery cohortStatsAlt(String value) {
        put(VariantQueryParam.STATS_ALT.key(), value);
        return this;
    }
    public String cohortStatsAlt() {
        return getString(VariantQueryParam.STATS_ALT.key());
    }

    public VariantQuery cohortStatsMaf(String value) {
        put(VariantQueryParam.STATS_MAF.key(), value);
        return this;
    }
    public String cohortStatsMaf() {
        return getString(VariantQueryParam.STATS_MAF.key());
    }

    public VariantQuery cohortStatsMgf(String value) {
        put(VariantQueryParam.STATS_MGF.key(), value);
        return this;
    }
    public String cohortStatsMgf() {
        return getString(VariantQueryParam.STATS_MGF.key());
    }

    public VariantQuery cohortStatsPass(String value) {
        put(VariantQueryParam.STATS_PASS_FREQ.key(), value);
        return this;
    }
    public String cohortStatsPass() {
        return getString(VariantQueryParam.STATS_PASS_FREQ.key());
    }

    public VariantQuery missingAlleles(String value) {
        put(VariantQueryParam.MISSING_ALLELES.key(), value);
        return this;
    }
    public String missingAlleles() {
        return getString(VariantQueryParam.MISSING_ALLELES.key());
    }

    public VariantQuery missingGenotypes(String value) {
        put(VariantQueryParam.MISSING_GENOTYPES.key(), value);
        return this;
    }
    public String missingGenotypes() {
        return getString(VariantQueryParam.MISSING_GENOTYPES.key());
    }

    public VariantQuery score(String value) {
        put(VariantQueryParam.SCORE.key(), value);
        return this;
    }
    public String score() {
        return getString(VariantQueryParam.SCORE.key());
    }

    public VariantQuery annotationExists(String value) {
        put(VariantQueryParam.ANNOTATION_EXISTS.key(), value);
        return this;
    }
    public String annotationExists() {
        return getString(VariantQueryParam.ANNOTATION_EXISTS.key());
    }

    public VariantQuery xref(String value) {
        put(VariantQueryParam.ANNOT_XREF.key(), value);
        return this;
    }
    public String xref() {
        return getString(VariantQueryParam.ANNOT_XREF.key());
    }

    public VariantQuery gene(String value) {
        put(VariantQueryParam.GENE.key(), value);
        return this;
    }
    public String gene() {
        return getString(VariantQueryParam.GENE.key());
    }

    public VariantQuery biotype(String value) {
        put(VariantQueryParam.ANNOT_BIOTYPE.key(), value);
        return this;
    }
    public String biotype() {
        return getString(VariantQueryParam.ANNOT_BIOTYPE.key());
    }

    public VariantQuery ct(String value) {
        put(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), value);
        return this;
    }
    public String ct() {
        return getString(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key());
    }

    @Deprecated
    public VariantQuery polyphen(String value) {
        put(VariantQueryParam.ANNOT_POLYPHEN.key(), value);
        return this;
    }
    @Deprecated
    public String polyphen() {
        return getString(VariantQueryParam.ANNOT_POLYPHEN.key());
    }

    @Deprecated
    public VariantQuery sift(String value) {
        put(VariantQueryParam.ANNOT_SIFT.key(), value);
        return this;
    }
    @Deprecated
    public String sift() {
        return getString(VariantQueryParam.ANNOT_SIFT.key());
    }

    public VariantQuery proteinSubstitution(String value) {
        put(VariantQueryParam.ANNOT_PROTEIN_SUBSTITUTION.key(), value);
        return this;
    }
    public String proteinSubstitution() {
        return getString(VariantQueryParam.ANNOT_PROTEIN_SUBSTITUTION.key());
    }

    public VariantQuery conservation(String value) {
        put(VariantQueryParam.ANNOT_CONSERVATION.key(), value);
        return this;
    }
    public String conservation() {
        return getString(VariantQueryParam.ANNOT_CONSERVATION.key());
    }

    public VariantQuery populationFrequencyAlt(String value) {
        put(VariantQueryParam.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), value);
        return this;
    }
    public String populationFrequencyAlt() {
        return getString(VariantQueryParam.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key());
    }

    public VariantQuery populationFrequencyRef(String value) {
        put(VariantQueryParam.ANNOT_POPULATION_REFERENCE_FREQUENCY.key(), value);
        return this;
    }
    public String populationFrequencyRef() {
        return getString(VariantQueryParam.ANNOT_POPULATION_REFERENCE_FREQUENCY.key());
    }

    public VariantQuery populationFrequencyMaf(String value) {
        put(VariantQueryParam.ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(), value);
        return this;
    }
    public String populationFrequencyMaf() {
        return getString(VariantQueryParam.ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key());
    }

    public VariantQuery transcriptFlag(String value) {
        put(VariantQueryParam.ANNOT_TRANSCRIPT_FLAG.key(), value);
        return this;
    }
    public String transcriptFlag() {
        return getString(VariantQueryParam.ANNOT_TRANSCRIPT_FLAG.key());
    }

    public VariantQuery geneTraitId(String value) {
        put(VariantQueryParam.ANNOT_GENE_TRAIT_ID.key(), value);
        return this;
    }
    public String geneTraitId() {
        return getString(VariantQueryParam.ANNOT_GENE_TRAIT_ID.key());
    }

    @Deprecated
    public VariantQuery geneTraitName(String value) {
        put(VariantQueryParam.ANNOT_GENE_TRAIT_NAME.key(), value);
        return this;
    }
    @Deprecated
    public String geneTraitName() {
        return getString(VariantQueryParam.ANNOT_GENE_TRAIT_NAME.key());
    }

    public VariantQuery trait(String value) {
        put(VariantQueryParam.ANNOT_TRAIT.key(), value);
        return this;
    }
    public String trait() {
        return getString(VariantQueryParam.ANNOT_TRAIT.key());
    }

    public VariantQuery clinical(String value) {
        put(VariantQueryParam.ANNOT_CLINICAL.key(), value);
        return this;
    }
    public String clinical() {
        return getString(VariantQueryParam.ANNOT_CLINICAL.key());
    }

    public VariantQuery clinicalSignificance(String value) {
        put(VariantQueryParam.ANNOT_CLINICAL_SIGNIFICANCE.key(), value);
        return this;
    }
    public String clinicalSignificance() {
        return getString(VariantQueryParam.ANNOT_CLINICAL_SIGNIFICANCE.key());
    }

    public VariantQuery clinicalConfirmedStatus(String value) {
        put(VariantQueryParam.ANNOT_CLINICAL_CONFIRMED_STATUS.key(), value);
        return this;
    }
    public String clinicalConfirmedStatus() {
        return getString(VariantQueryParam.ANNOT_CLINICAL_CONFIRMED_STATUS.key());
    }

    @Deprecated
    public VariantQuery clinvar(String value) {
        put(VariantQueryParam.ANNOT_CLINVAR.key(), value);
        return this;
    }
    @Deprecated
    public String clinvar() {
        return getString(VariantQueryParam.ANNOT_CLINVAR.key());
    }

    @Deprecated
    public VariantQuery cosmic(String value) {
        put(VariantQueryParam.ANNOT_COSMIC.key(), value);
        return this;
    }
    @Deprecated
    public String cosmic() {
        return getString(VariantQueryParam.ANNOT_COSMIC.key());
    }

    @Deprecated
    public VariantQuery hpo(String value) {
        put(VariantQueryParam.ANNOT_HPO.key(), value);
        return this;
    }
    @Deprecated
    public String hpo() {
        return getString(VariantQueryParam.ANNOT_HPO.key());
    }

    public VariantQuery go(String value) {
        put(VariantQueryParam.ANNOT_GO.key(), value);
        return this;
    }
    public String go() {
        return getString(VariantQueryParam.ANNOT_GO.key());
    }

    public VariantQuery expression(String value) {
        put(VariantQueryParam.ANNOT_EXPRESSION.key(), value);
        return this;
    }
    public String expression() {
        return getString(VariantQueryParam.ANNOT_EXPRESSION.key());
    }

    public VariantQuery proteinKeyword(String value) {
        put(VariantQueryParam.ANNOT_PROTEIN_KEYWORD.key(), value);
        return this;
    }
    public String proteinKeyword() {
        return getString(VariantQueryParam.ANNOT_PROTEIN_KEYWORD.key());
    }

    public VariantQuery drug(String value) {
        put(VariantQueryParam.ANNOT_DRUG.key(), value);
        return this;
    }
    public String drug() {
        return getString(VariantQueryParam.ANNOT_DRUG.key());
    }

    public VariantQuery functionalScore(String value) {
        put(VariantQueryParam.ANNOT_FUNCTIONAL_SCORE.key(), value);
        return this;
    }
    public String functionalScore() {
        return getString(VariantQueryParam.ANNOT_FUNCTIONAL_SCORE.key());
    }

    public VariantQuery customAnnotation(String value) {
        put(VariantQueryParam.CUSTOM_ANNOTATION.key(), value);
        return this;
    }
    public String customAnnotation() {
        return getString(VariantQueryParam.CUSTOM_ANNOTATION.key());
    }

    public VariantQuery unknownGenotype(String value) {
        put(VariantQueryParam.UNKNOWN_GENOTYPE.key(), value);
        return this;
    }
    public String unknownGenotype() {
        return getString(VariantQueryParam.UNKNOWN_GENOTYPE.key());
    }

    public VariantQuery release(String value) {
        put(VariantQueryParam.RELEASE.key(), value);
        return this;
    }
    public String release() {
        return getString(VariantQueryParam.RELEASE.key());
    }


}
