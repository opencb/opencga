package org.opencb.opencga.analysis.clinical.exomiser;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.ClinicalAcmg;
import org.opencb.biodata.models.clinical.ClinicalDiscussion;
import org.opencb.biodata.models.clinical.ClinicalProperty;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.biodata.models.clinical.interpretation.GenomicFeature;
import org.opencb.biodata.models.clinical.interpretation.VariantClassification;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.GeneCancerAssociation;
import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.opencb.biodata.models.clinical.interpretation.VariantClassification.calculateAcmgClassification;
import static org.opencb.biodata.models.clinical.interpretation.VariantClassification.computeClinicalSignificance;
import static org.opencb.opencga.analysis.clinical.ClinicalUtils.*;

public class ExomiserClinicalVariantCreator {

    // logger
    protected Logger logger = LoggerFactory.getLogger(this.getClass().toString());


    public ClinicalVariant create(Variant variant) {
        // Sanity check
        if (variant == null) {
            logger.warn("Input variant is null, so no clinical variant was created.");
            return null;
        }

        return new ClinicalVariant(variant.getImpl(), new ArrayList<>(), new ArrayList<>(), new HashMap<>(), new ClinicalDiscussion(),
                null, ClinicalVariant.Status.NOT_REVIEWED, new ArrayList<>(), new HashMap<>());
    }

    public void addClinicalVariantEvidences(ClinicalVariant clinicalVariant, List<ExomiserTranscriptAnnotation> exomiserTranscripts,
                                            ClinicalProperty.ModeOfInheritance moi, Map<String, Object> attributes) {
        // Sanity check
        if (clinicalVariant == null) {
            logger.warn("Input clinical variant is null, so no clinical variant evidences was added to it.");
            return;
        }

        List<ClinicalVariantEvidence> clinicalVariantEvidences = new ArrayList<>();
        if (clinicalVariant.getAnnotation() == null || CollectionUtils.isEmpty(clinicalVariant.getAnnotation().getConsequenceTypes())) {
            String varaintId = clinicalVariant.toStringSimple();
            logger.warn("Clinical variant annotation is null or no consequence types found for clinical variant {}, so no clinical variant"
                    + " was created.", varaintId);
            return;
        }

        for (ExomiserTranscriptAnnotation exomisertranscript : exomiserTranscripts) {
            boolean found = false;
            for (ConsequenceType ct : clinicalVariant.getAnnotation().getConsequenceTypes()) {
                if (StringUtils.isNotEmpty(exomisertranscript.getAccession()) && StringUtils.isNotEmpty(ct.getEnsemblTranscriptId())
                        && removeVersion(exomisertranscript.getAccession()).equals(removeVersion(ct.getEnsemblTranscriptId()))) {
                    clinicalVariantEvidences.add(createClinicalVariantEvidences(ct, moi, attributes, clinicalVariant));
                    found = true;
                    break;
                }
            }
            if (!found) {
                clinicalVariantEvidences.add(createClinicalVariantEvidences(exomisertranscript, moi, attributes, clinicalVariant));
            }
        }

        // Create a clinical variant only if we have evidences
        if (CollectionUtils.isEmpty(clinicalVariantEvidences)) {
            String varaintId = clinicalVariant.toStringSimple();
            logger.warn("No evidences found for clinical variant {}, so no clinical variant evidences were added.", varaintId);
            return;
        }

        clinicalVariant.getEvidences().addAll(clinicalVariantEvidences);
    }

    private ClinicalVariantEvidence createClinicalVariantEvidences(ExomiserTranscriptAnnotation exomiserTranscript,
                                                                   ClinicalProperty.ModeOfInheritance moi, Map<String, Object> attributes,
                                                                   Variant variant) {
        SequenceOntologyTerm soTerm = new SequenceOntologyTerm(" SO:0002220", "function_uncertain_variant");
        GenomicFeature genomicFeature = new GenomicFeature(String.valueOf(exomiserTranscript.getRank()), "GENE",
                exomiserTranscript.getAccession(), exomiserTranscript.getGeneSymbol(), Collections.singletonList(soTerm), null);

        return createClinicalVariantEvidence(genomicFeature, moi, attributes, variant);
    }

    private ClinicalVariantEvidence createClinicalVariantEvidences(ConsequenceType ct, ClinicalProperty.ModeOfInheritance moi,
                                                                   Map<String, Object> attributes, Variant variant) {
        List<SequenceOntologyTerm> soTerms = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(ct.getSequenceOntologyTerms())) {
            for (SequenceOntologyTerm soTerm : ct.getSequenceOntologyTerms()) {
                if (ModeOfInheritance.extendedLof.contains(soTerm.getName())) {
                    soTerms.add(soTerm);
                }
            }
        }
        GenomicFeature genomicFeature = new GenomicFeature(ct.getEnsemblGeneId(), "GENE", ct.getEnsemblTranscriptId(),
                ct.getGeneName(), soTerms, null);

        return createClinicalVariantEvidence(genomicFeature, moi, attributes, variant);
    }

    private ClinicalVariantEvidence createClinicalVariantEvidence(GenomicFeature genomicFeature, ClinicalProperty.ModeOfInheritance moi,
                                                                  Map<String, Object> attributes, Variant variant) {
        ClinicalVariantEvidence clinicalVariantEvidence = new ClinicalVariantEvidence();

        // Interpretation method name
        clinicalVariantEvidence.setInterpretationMethodName(ExomiserInterpretationAnalysis.ID);

        // Genomic feature
        if (genomicFeature != null) {
            clinicalVariantEvidence.setGenomicFeature(genomicFeature);
        }

        // Mode of inheritance
        if (moi != null) {
            clinicalVariantEvidence.setModeOfInheritances(Collections.singletonList(moi));
        }

        // Variant classification:
        clinicalVariantEvidence.setClassification(new VariantClassification());

        // Variant classification: ACMG
        List<ClinicalAcmg> acmgs = calculateAcmgClassification(getConsequenceType(genomicFeature.getTranscriptId(), variant),
                variant.getAnnotation(), Collections.singletonList(moi));
        clinicalVariantEvidence.getClassification().setAcmg(acmgs);

        // Variant classification: clinical significance
        clinicalVariantEvidence.getClassification().setClinicalSignificance(computeClinicalSignificance(acmgs));

        // Role in cancer
        if (variant.getAnnotation() != null && CollectionUtils.isNotEmpty(variant.getAnnotation().getGeneCancerAssociations())) {
            Set<ClinicalProperty.RoleInCancer> roles = new HashSet<>();
            for (GeneCancerAssociation geneCancerAssociation : variant.getAnnotation().getGeneCancerAssociations()) {
                if (CollectionUtils.isNotEmpty(geneCancerAssociation.getRoleInCancer())) {
                    for (String value : geneCancerAssociation.getRoleInCancer()) {
                        try {
                            roles.add(ClinicalProperty.RoleInCancer.valueOf(value.toUpperCase()));
                        } catch (Exception e) {
                            logger.info("Unknown role in cancer value: {}. It will be ignored.", value.toUpperCase());
                        }
                    }
                }
            }
            if (CollectionUtils.isNotEmpty(roles)) {
                List<ClinicalProperty.RoleInCancer> rolesInCancer = new ArrayList<>(roles);
                clinicalVariantEvidence.setRolesInCancer(rolesInCancer);
            }
        }

        // Attributes
        clinicalVariantEvidence.setAttributes(attributes);

        return clinicalVariantEvidence;
    }

    private ConsequenceType getConsequenceType(String transcriptId, Variant variant) {
        if (variant.getAnnotation() != null && CollectionUtils.isNotEmpty(variant.getAnnotation().getConsequenceTypes())) {
            for (ConsequenceType ct : variant.getAnnotation().getConsequenceTypes()) {
                if (StringUtils.isNotEmpty(transcriptId) &&
                        ((StringUtils.isNotEmpty(ct.getEnsemblTranscriptId()) && transcriptId.equals(ct.getEnsemblTranscriptId()))
                                || (StringUtils.isNotEmpty(ct.getTranscriptId()) && transcriptId.equals(ct.getTranscriptId())))) {
                    return ct;
                }
            }
        }
        return null;
    }
}