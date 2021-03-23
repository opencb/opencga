package org.opencb.opencga.storage.core.variant.annotation.converters;


import org.opencb.biodata.models.variant.annotation.VariantTraitAssociation;
import org.opencb.biodata.models.variant.avro.ClinicalSignificance;
import org.opencb.biodata.models.variant.avro.EvidenceEntry;
import org.opencb.biodata.models.variant.avro.VariantClassification;
import org.opencb.biodata.tools.commons.Converter;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created on 11/09/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Deprecated
public abstract class VariantTraitAssociationToEvidenceEntryConverter implements Converter<VariantTraitAssociation, List<EvidenceEntry>> {

    protected static Logger logger = LoggerFactory.getLogger(VariantTraitAssociationToEvidenceEntryConverter.class);

    private static final String CLINVAR = "clinvar";
    private static final String COSMIC = "cosmic";
    private static final String CLINICAL_SIGNIFICANCE_IN_SOURCE_FILE = "ClinicalSignificance_in_source_file";
    private static final String REVIEW_STATUS_IN_SOURCE_FILE = "ReviewStatus_in_source_file";
    private static final String MUTATION_SOMATIC_STATUS_IN_SOURCE_FILE = "mutationSomaticStatus_in_source_file";
    private static final String SYMBOL = "symbol";
    private static final Map<String, ClinicalSignificance> CLINVAR_CLINSIG_TO_ACMG_WITH_ENUM;

    static {
        CLINVAR_CLINSIG_TO_ACMG_WITH_ENUM = new HashMap<>(VariantAnnotationConstants.CLINVAR_CLINSIG_TO_ACMG);
        for (ClinicalSignificance value : ClinicalSignificance.values()) {
            CLINVAR_CLINSIG_TO_ACMG_WITH_ENUM.put(value.name(), value);
            CLINVAR_CLINSIG_TO_ACMG_WITH_ENUM.put(value.name().toLowerCase(), value);
        }
    }


//    @Override
//    public List<EvidenceEntry> convert(VariantTraitAssociation variantTraitAssociation) {
//
//        if (variantTraitAssociation == null) {
//            return null;
//        } else {
//            List<EvidenceEntry> evidenceEntries = new ArrayList<>();
//            if (variantTraitAssociation.getClinvar() != null) {
//                for (ClinVar clinVar : variantTraitAssociation.getClinvar()) {
//                    EvidenceEntry evidenceEntry = fromClinVar(clinVar);
//                    evidenceEntries.add(evidenceEntry);
//                }
//            }
//
//            if (variantTraitAssociation.getCosmic() != null) {
//                for (Cosmic cosmic : variantTraitAssociation.getCosmic()) {
//                    EvidenceEntry evidenceEntry = fromCosmic(cosmic);
//                    evidenceEntries.add(evidenceEntry);
//                }
//            }
//
//            return evidenceEntries;
//        }
//    }
//
//    public EvidenceEntry fromClinVar(ClinVar clinVar) {
//        EvidenceSource evidenceSource = new EvidenceSource(CLINVAR, null, null);
//        String url = clinVar.getAccession().startsWith("RCV")
//                ? "https://www.ncbi.nlm.nih.gov/clinvar/" + clinVar.getAccession()
//                : null;
//        List<HeritableTrait> heritableTraits = null;
//        if (CollectionUtils.isNotEmpty(clinVar.getTraits())) {
//            heritableTraits = clinVar.getTraits()
//                    .stream()
//                    .map(trait -> {
//                        if (trait.startsWith("{")) {
//                            try {
//                                return JacksonUtils.getDefaultObjectMapper().readValue(trait, HeritableTrait.class);
//                            } catch (JsonProcessingException ignore) {
//                                logger.debug("Error parsing trait", ignore);
//                            }
//                        }
//                        return new HeritableTrait(trait, ModeOfInheritance.NA);
//                    })
//                    .collect(Collectors.toList());
//        }
//        List<GenomicFeature> genomicFeatures;
//        if (CollectionUtils.isNotEmpty(clinVar.getGeneNames())) {
//            genomicFeatures = clinVar.getGeneNames()
//                    .stream()
//                    .map(geneName -> new GenomicFeature(FeatureTypes.gene, null, Collections.singletonMap(SYMBOL, geneName)))
//                    .collect(Collectors.toList());
//        } else {
//            genomicFeatures = null;
//        }
//        List<Property> additionalProperties = new ArrayList<>(2);
//        if (StringUtils.isNotEmpty(clinVar.getReviewStatus())) {
//            additionalProperties.add(
//                    new Property(null, REVIEW_STATUS_IN_SOURCE_FILE, clinVar.getReviewStatus()));
//        }
//        if (StringUtils.isNotEmpty(clinVar.getClinicalSignificance())) {
//            additionalProperties.add(
//                    new Property(null, CLINICAL_SIGNIFICANCE_IN_SOURCE_FILE, clinVar.getClinicalSignificance()));
//        }
//        VariantClassification variantClassification = getVariantClassification(clinVar.getClinicalSignificance().toLowerCase());
//        ConsistencyStatus consistencyStatus = getConsistencyStatus(clinVar.getReviewStatus().toLowerCase());
//        return new EvidenceEntry(
//                evidenceSource, Collections.emptyList(), null, url,
//                clinVar.getAccession(), null, null,
//                heritableTraits, genomicFeatures,
//                variantClassification, null, null,
//                consistencyStatus,
//                EthnicCategory.Z, null, null, null,
//                additionalProperties, Collections.emptyList());
//    }
//
//    public EvidenceEntry fromCosmic(Cosmic cosmic) {
//        EvidenceSource evidenceSource = new EvidenceSource(COSMIC, null, null);
//        List<GenomicFeature> genomicFeatures;
//        if (cosmic.getGeneName() != null) {
//            genomicFeatures = Collections.singletonList(
//                    new GenomicFeature(FeatureTypes.gene, null, Collections.singletonMap(SYMBOL, cosmic.getGeneName())));
//        } else {
//            genomicFeatures = null;
//        }
//        SomaticInformation somaticInformation = new SomaticInformation(
//                cosmic.getPrimarySite(),
//                cosmic.getSiteSubtype(),
//                cosmic.getPrimaryHistology(),
//                cosmic.getHistologySubtype(),
//                cosmic.getTumourOrigin(),
//                cosmic.getSampleSource());
//        List<Property> additionalProperties = null;
//        if (StringUtils.isNotEmpty(cosmic.getMutationSomaticStatus())) {
//            additionalProperties = Collections.singletonList(
//                    new Property(null, MUTATION_SOMATIC_STATUS_IN_SOURCE_FILE, cosmic.getMutationSomaticStatus()));
//        }
//        return new EvidenceEntry(
//                evidenceSource, Collections.emptyList(),
//                somaticInformation, null,
//                cosmic.getMutationId(), null, null, Collections.emptyList(),
//                genomicFeatures, null, null, null, null, EthnicCategory.Z, null, null, null,
//                additionalProperties, Collections.emptyList());
//    }
//
//    private ConsistencyStatus getConsistencyStatus(String lineField) {
//        for (String value : lineField.split("[,/;]")) {
//            value = value.toLowerCase().trim();
//            if (VariantAnnotationUtils.CLINVAR_REVIEW_TO_CONSISTENCY_STATUS.containsKey(value)) {
//                return VariantAnnotationUtils.CLINVAR_REVIEW_TO_CONSISTENCY_STATUS.get(value);
//            }
//        }
//        return null;
//    }
//
    public static VariantClassification getVariantClassification(String lineField) {
        VariantClassification variantClassification = new VariantClassification();
        for (String value : lineField.split("[,/;]")) {
            value = value.toLowerCase().trim();
            if (CLINVAR_CLINSIG_TO_ACMG_WITH_ENUM.containsKey(value)) {
                // No value set
                if (variantClassification.getClinicalSignificance() == null) {
                    variantClassification.setClinicalSignificance(CLINVAR_CLINSIG_TO_ACMG_WITH_ENUM.get(value));
                    // Seen cases like Benign;Pathogenic;association;not provided;risk factor for the same record
                } else if (isBenign(CLINVAR_CLINSIG_TO_ACMG_WITH_ENUM.get(value))
                        && isPathogenic(variantClassification.getClinicalSignificance())) {
                    logger.warn("Benign and Pathogenic search significances found for the same record");
                    logger.warn("Will set uncertain_significance instead");
                    variantClassification.setClinicalSignificance(ClinicalSignificance.uncertain_significance);
                }
            } else if (VariantAnnotationConstants.CLINVAR_CLINSIG_TO_TRAIT_ASSOCIATION.containsKey(value)) {
                variantClassification.setTraitAssociation(VariantAnnotationConstants.CLINVAR_CLINSIG_TO_TRAIT_ASSOCIATION.get(value));
            } else if (VariantAnnotationConstants.CLINVAR_CLINSIG_TO_DRUG_RESPONSE.containsKey(value)) {
                variantClassification.setDrugResponseClassification(VariantAnnotationConstants.CLINVAR_CLINSIG_TO_DRUG_RESPONSE.get(value));
            } else if (!value.equals("not provided")) {
                logger.debug("No mapping found for referenceClinVarAssertion.clinicalSignificance '{}'", value);
                logger.debug("No value will be set at EvidenceEntry.variantClassification for this term");
            }
        }
        return variantClassification;
    }

    private static boolean isPathogenic(ClinicalSignificance clinicalSignificance) {
        return ClinicalSignificance.pathogenic.equals(clinicalSignificance)
                || ClinicalSignificance.likely_pathogenic.equals(clinicalSignificance);
    }

    private static boolean isBenign(ClinicalSignificance clinicalSignificance) {
        return ClinicalSignificance.benign.equals(clinicalSignificance)
                || ClinicalSignificance.likely_benign.equals(clinicalSignificance);
    }
}
