package com.zettagenomics.opencga.enterprise.cva.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.zettagenomics.opencga.enterprise.cva.exceptions.CvaException;
import com.zettagenomics.opencga.enterprise.cva.models.ClinicalVariantEvidenceSearch;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.biodata.models.clinical.interpretation.GenomicFeature;
import org.opencb.biodata.models.clinical.interpretation.VariantClassification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ClinicalVariantEvidenceConverter extends SearchConverter {

    private ObjectReader clinicalVariantEvidenceReader;

    protected static Logger logger = LoggerFactory.getLogger(ClinicalVariantEvidenceConverter.class);

    public ClinicalVariantEvidenceConverter() {
        this.clinicalVariantEvidenceReader = mapper.readerFor(ClinicalVariantEvidence.class);
    }

    public ClinicalVariantEvidenceSearch toClinicalVariantEvidenceSearch(ClinicalVariantEvidence cve, String variantId,
                                                                         String interpretationId) throws CvaException {
        return toClinicalVariantEvidenceSearch(Collections.singletonList(cve), variantId, interpretationId).get(0);
    }

    public List<ClinicalVariantEvidenceSearch> toClinicalVariantEvidenceSearch(List<ClinicalVariantEvidence> cveList, String variantId,
                                                                               String interpreationId)
            throws CvaException {
        List<ClinicalVariantEvidenceSearch> cvesList = new ArrayList<>();

        int i = 0;
        for (ClinicalVariantEvidence cve : cveList) {
            ClinicalVariantEvidenceSearch cves = new ClinicalVariantEvidenceSearch();

            cves.setId((i++) + "-" + variantId + "-" + interpreationId);
            cves.setCveVariantId(variantId);
            cves.setCveInterpretationId(interpreationId);

            // Phenotypes (including IDs and names)
            if (CollectionUtils.isNotEmpty(cve.getPhenotypes())) {
                cves.setCvePhenotypeNames(cve.getPhenotypes().stream().map(p -> p.getName()).collect(Collectors.toList()));
                cves.getCvePhenotypeNames().addAll(cve.getPhenotypes().stream().map(p -> p.getId()).collect(Collectors.toList()));
            }

            // Genomic feature
            if (cve.getGenomicFeature() != null) {
                GenomicFeature genomicFeature = cve.getGenomicFeature();

                cves.setCveGeneName(genomicFeature.getGeneName());

                if (CollectionUtils.isNotEmpty(genomicFeature.getConsequenceTypes())) {
                    cves.setCveConsequenceTypeIds(genomicFeature.getConsequenceTypes().stream().map(so -> so.getAccession())
                            .collect(Collectors.toList()));
                    cves.getCveConsequenceTypeIds().addAll(genomicFeature.getConsequenceTypes().stream().map(so -> so.getName())
                            .collect(Collectors.toList()));
                }

                if (CollectionUtils.isNotEmpty(genomicFeature.getXrefs())) {
                    cves.setCveXrefIds(genomicFeature.getXrefs().stream().map(x -> x.getId()).collect(Collectors.toList()));
                }
            }

            // Panel ID
            cves.setCvePanelId(cve.getPanelId());

            if (cve.getClassification() != null) {
                VariantClassification classification = cve.getClassification();
                cves.setCveTier(classification.getTier());
                if (CollectionUtils.isNotEmpty(classification.getAcmg())) {
                    cves.setCveAcmgs(classification.getAcmg().stream().map(a -> a.getClassification()).collect(Collectors.toList()));
                }
                if (classification.getClinicalSignificance() != null) {
                    cves.setCveClinicalSignificance(classification.getClinicalSignificance().name());
                }
                if (classification.getDrugResponse() != null) {
                    cves.setCveDrugResponse(classification.getDrugResponse().name());
                }
                if (classification.getTraitAssociation() != null) {
                    cves.setCveTraitAssociation(classification.getTraitAssociation().name());
                }
                if (classification.getFunctionalEffect() != null) {
                    cves.setCveFunctionalEffect(classification.getFunctionalEffect().name());
                }
                if (classification.getTumorigenesis() != null) {
                    cves.setCveTumorigenesis(classification.getTumorigenesis().name());
                }
                cves.setCveOtherClassifications(classification.getOther());
            }

            if (CollectionUtils.isNotEmpty(cve.getRolesInCancer())) {
                cves.setCveRolesInCancer(cve.getRolesInCancer().stream().map(r -> r.name()).collect(Collectors.toList()));
            }

//            private Map<String, Double> cveScores;

            // Clinical variant evidence stored in a JSON string
            try {
                cves.setCveJson(mapper.writeValueAsString(cve));
            } catch (JsonProcessingException e) {
                throw new CvaException("Error when storing clinical varaint evidence JSON field", e);
            }

            // Add clinical variant evidence into the list
            cvesList.add(cves);
        }
        return cvesList;
    }

    public ClinicalVariantEvidence toClinicalVariantEvidence(ClinicalVariantEvidenceSearch cves) {
        return toClinicalVariantEvidence(Collections.singletonList(cves).get(0));
    }

    public List<ClinicalVariantEvidence> toClinicalVariantEvidence(List<ClinicalVariantEvidenceSearch> cvesList) throws CvaException {
        List<ClinicalVariantEvidence> cveList = new ArrayList<>();
        for (ClinicalVariantEvidenceSearch cves : cvesList) {
            try {
                cveList.add(clinicalVariantEvidenceReader.readValue(cves.getCveJson()));
            } catch (JsonProcessingException e) {
                throw new CvaException("Error when converting to clinical variant evidence", e);
            }
        }
        return cveList;
    }
}
