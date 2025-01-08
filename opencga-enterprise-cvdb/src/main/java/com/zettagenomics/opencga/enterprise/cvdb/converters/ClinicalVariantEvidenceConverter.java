package com.zettagenomics.opencga.enterprise.cvdb.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import com.zettagenomics.opencga.enterprise.cvdb.models.ClinicalVariantEvidenceSearch;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.biodata.models.clinical.interpretation.GenomicFeature;
import org.opencb.biodata.models.clinical.interpretation.VariantClassification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class ClinicalVariantEvidenceConverter extends SearchConverter<ClinicalVariantEvidence, ClinicalVariantEvidenceSearch> {

    private ObjectReader clinicalVariantEvidenceReader;

    private static Logger logger = LoggerFactory.getLogger(ClinicalVariantEvidenceConverter.class);

    public ClinicalVariantEvidenceConverter() {
        this.clinicalVariantEvidenceReader = mapper.readerFor(ClinicalVariantEvidence.class);
    }

    public ClinicalVariantEvidenceSearch toClinicalVariantEvidenceSearch(ClinicalVariantEvidence cve, int evidenceIndex, String variantId,
                                                                         String interpretationId, String clinicalAnalysisId, String studyId,
                                                                         List<String> viewers) throws CvdbException {
        ClinicalVariantEvidenceSearch cves = new ClinicalVariantEvidenceSearch()
                .setId((evidenceIndex) + "-" + variantId + "-" + interpretationId)
                .setVariantId(variantId)
                .setCvId(interpretationId + "-" + variantId)
                .setCiId(interpretationId)
                .setCaId(clinicalAnalysisId)
                .setStudyId(studyId)
                .setViewers(viewers);

        // Phenotypes (including IDs and names)
        if (CollectionUtils.isNotEmpty(cve.getPhenotypes())) {
            cves.setPhenotypeNames(cve.getPhenotypes().stream().map(p -> p.getName()).collect(Collectors.toList()));
            cves.getPhenotypeNames().addAll(cve.getPhenotypes().stream().map(p -> p.getId()).collect(Collectors.toList()));
        }

        // Genomic feature
        if (cve.getGenomicFeature() != null) {
            GenomicFeature genomicFeature = cve.getGenomicFeature();

            cves.setGeneName(genomicFeature.getGeneName());
            cves.setTranscriptId(genomicFeature.getTranscriptId());

            if (CollectionUtils.isNotEmpty(genomicFeature.getConsequenceTypes())) {
                cves.setSoTermNames(genomicFeature.getConsequenceTypes().stream().map(so -> so.getName()).collect(Collectors.toList()));
            }

            if (CollectionUtils.isNotEmpty(genomicFeature.getXrefs())) {
                cves.setXrefIds(genomicFeature.getXrefs().stream().map(x -> x.getId()).collect(Collectors.toList()));
            }
        }

        // Panel ID
        cves.setPanelId(cve.getPanelId());

        // Mode of inheritance (moi)
        if (CollectionUtils.isNotEmpty(cve.getModeOfInheritances())) {
            cves.setMois(cve.getModeOfInheritances().stream().map(moi -> moi.name()).collect(Collectors.toList()));
        }

        // Penetrance
        cves.setPenetrance(cve.getPenetrance().name());

        if (cve.getClassification() != null) {
            VariantClassification classification = cve.getClassification();
            cves.setTier(classification.getTier());
            if (CollectionUtils.isNotEmpty(classification.getAcmg())) {
                cves.setAcmgs(classification.getAcmg().stream().map(a -> a.getClassification()).collect(Collectors.toList()));
            }
            if (classification.getClinicalSignificance() != null) {
                cves.setClinicalSignificance(classification.getClinicalSignificance().name());
            }
            if (classification.getDrugResponse() != null) {
                cves.setDrugResponse(classification.getDrugResponse().name());
            }
            if (classification.getTraitAssociation() != null) {
                cves.setTraitAssociation(classification.getTraitAssociation().name());
            }
            if (classification.getFunctionalEffect() != null) {
                cves.setFunctionalEffect(classification.getFunctionalEffect().name());
            }
            if (classification.getTumorigenesis() != null) {
                cves.setTumorigenesis(classification.getTumorigenesis().name());
            }
            cves.setOtherClassifications(classification.getOther());
        }

        if (CollectionUtils.isNotEmpty(cve.getRolesInCancer())) {
            cves.setRolesInCancer(cve.getRolesInCancer().stream().map(r -> r.name()).collect(Collectors.toList()));
        }

        if (cve.getReview() != null) {
            if (CollectionUtils.isNotEmpty(cve.getReview().getAcmg())) {
                cves.setReviewAcmgs(cve.getReview().getAcmg().stream().map(a -> a.getClassification()).collect(Collectors.toList()));
            }
            if (StringUtils.isNotEmpty(cve.getReview().getTier())) {
                cves.setReviewTier(cve.getReview().getTier());
            }
            if (cve.getReview().getClinicalSignificance() != null) {
                cves.setReviewClinicalSignificance(cve.getReview().getClinicalSignificance().name());
            }
            if (cve.getReview().getDiscussion() != null  && StringUtils.isNotEmpty(cve.getReview().getDiscussion().getText())) {
                cves.setReviewText(cve.getReview().getDiscussion().getText());
            }
        }

//            private Map<String, Double> cveScores;

        // Clinical variant evidence stored in a JSON string
        try {
            cves.setJson(mapper.writeValueAsString(cve));
        } catch (JsonProcessingException e) {
            throw new CvdbException("Error when storing clinical varaint evidence JSON field", e);
        }

        return cves;
    }

    public ClinicalVariantEvidence toClinicalVariantEvidence(ClinicalVariantEvidenceSearch cves) throws CvdbException {
        return toClinicalVariantEvidence(Collections.singletonList(cves)).get(0);
    }

    public List<ClinicalVariantEvidence> toClinicalVariantEvidence(List<ClinicalVariantEvidenceSearch> cvesList) throws CvdbException {
        List<ClinicalVariantEvidence> cveList = new ArrayList<>();
        for (ClinicalVariantEvidenceSearch cves : cvesList) {
            try {
                ClinicalVariantEvidence cve = clinicalVariantEvidenceReader.readValue(cves.getJson());
                // Add clinical analysis, interpretation and study in attributes
                if (cve.getAttributes() == null) {
                    cve.setAttributes(new HashMap<>());
                }
                addStudyIdAsAttribute(cves.getStudyId(), cve.getAttributes());
                addClinicalAnalysisIdAsAttribute(cves.getCaId(), cve.getAttributes());
                addClinicalInterpretationIdAsAttribute(cves.getCiId(), cve.getAttributes());
                addClinicalVariantIdAsAttribute(cves.getVariantId(), cve.getAttributes());

                cveList.add(cve);
            } catch (JsonProcessingException e) {
                throw new CvdbException("Error when converting to clinical variant evidence", e);
            }
        }
        return cveList;
    }

    @Override
    public ClinicalVariantEvidence toModel(ClinicalVariantEvidenceSearch input) throws CvdbException {
        return toClinicalVariantEvidence(input);
    }
}
