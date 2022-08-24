/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.clinical;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.models.clinical.ClinicalAcmg;
import org.opencb.biodata.models.clinical.ClinicalComment;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.biodata.models.clinical.interpretation.Software;
import org.opencb.biodata.models.core.Xref;
import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.storage.core.variant.search.VariantSearchToVariantConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class InterpretationConverter {

    private VariantSearchToVariantConverter variantSearchToVariantConverter;
    private ObjectMapper mapper;
    private ObjectReader mapReader;
    private ObjectReader interpretationReader;
    private ObjectReader clinicalAnalysisReader;

    private static final String FIELD_SEPARATOR = " -- ";
    private static final String KEY_VALUE_SEPARATOR = "=";

    private static final String DESCRIPTION_PREFIX = "DS";
    private static final String ANALYST_PREFIX = "AN";
    private static final String DEPENDENCY_PREFIX = "DP";
    private static final String FILTER_PREFIX = "FT";
    private static final String COMMENT_PREFIX = "CM";
    private static final String ATTRIBUTE_PREFIX = "AT";

    protected static Logger logger;

    public InterpretationConverter() {
        this.variantSearchToVariantConverter = new VariantSearchToVariantConverter();
        this.mapper = new ObjectMapper();
        this.mapReader = mapper.readerFor(HashMap.class);
        this.interpretationReader = mapper.readerFor(Interpretation.class);
        this.clinicalAnalysisReader = mapper.readerFor(ClinicalAnalysis.class);

        this.logger = LoggerFactory.getLogger(InterpretationConverter.class);
    }

    public Interpretation toInterpretation(List<ClinicalVariantSearchModel> clinicalVariantSearchModels) {
        if (ListUtils.isEmpty(clinicalVariantSearchModels)) {
            logger.error("Empty list of reported variant search models");
            return null;
        }

        // ------ Interpretation fields -------
        Interpretation interpretation = null;

        try {
            // We take the first reportedVariantSearchModel to initialize Interpretation
            interpretation = interpretationReader.readValue(clinicalVariantSearchModels.get(0).getIntBasicJson());
        } catch (IOException e) {
            logger.error("Unable to convert JSON string to Interpretation object. Error: " + e.getMessage());
            return null;
        }

        // Add clinical variants
        List<ClinicalVariant> primaryFindings = new ArrayList<>();
        List<ClinicalVariant> secondaryFindings = new ArrayList<>();
        for (ClinicalVariantSearchModel rvsm: clinicalVariantSearchModels) {
            if (rvsm.isCvPrimary()) {
                primaryFindings.add(toClinicalVariant(rvsm));
            } else {
                secondaryFindings.add(toClinicalVariant(rvsm));
            }
        }
        interpretation.setPrimaryFindings(primaryFindings);
        interpretation.setSecondaryFindings(secondaryFindings);

        return interpretation;
    }

    public List<ClinicalVariantSearchModel> toReportedVariantSearchList(org.opencb.biodata.models.clinical.interpretation.Interpretation
                                                                                interpretation) {
        List<ClinicalVariantSearchModel> output = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(interpretation.getPrimaryFindings())) {
            output.addAll(toReportedVariantSearchList(interpretation, true));
        }
        if (CollectionUtils.isNotEmpty(interpretation.getSecondaryFindings())) {
            output.addAll(toReportedVariantSearchList(interpretation, false));
        }

        // Update reported variant search model by setting the intBasicJson field
        interpretation.setPrimaryFindings(null);
        interpretation.setSecondaryFindings(null);
        String intBasicJson = null;
        try {
            intBasicJson = mapper.writeValueAsString(interpretation);
        } catch (JsonProcessingException e) {
            logger.error("Error converting from intrepretation to JSON string", e.getMessage());
            return null;
        }
        for (ClinicalVariantSearchModel rvsm: output) {
            rvsm.setIntBasicJson(intBasicJson);
        }

        return output;
    }

    public List<ClinicalVariantSearchModel> toReportedVariantSearchList(org.opencb.biodata.models.clinical.interpretation.Interpretation
                                                                                interpretation, boolean isPrimary) {
        ObjectMapper mapper = new ObjectMapper();

        List<ClinicalVariantSearchModel> output = new ArrayList<>();

        List<ClinicalVariant> clinicalVariants;
        if (isPrimary) {
             clinicalVariants = interpretation.getPrimaryFindings();
        } else {
            clinicalVariants = interpretation.getSecondaryFindings();
        }

        if (CollectionUtils.isEmpty(clinicalVariants)) {
            return output;
        }

        for (ClinicalVariant clinicalVariant: clinicalVariants) {
            // Set variant fields
            ClinicalVariantSearchModel cvsm = (ClinicalVariantSearchModel) variantSearchToVariantConverter
                    .convertToStorageType(clinicalVariant);

            // ------ ClinicalAnalisys fields -------

            ClinicalAnalysis ca = null;
            if (MapUtils.isNotEmpty(interpretation.getAttributes()) && interpretation.getAttributes()
                    .containsKey("OPENCGA_CLINICAL_ANALYSIS")) {
                String caJson = (String) interpretation.getAttributes().get("OPENCGA_CLINICAL_ANALYSIS");
                try {
                    ca = clinicalAnalysisReader.readValue(caJson);
                } catch (IOException e) {
                    logger.error("Unable to convert ClinicalAnalysis object to JSON string. Error: " + e.getMessage());
                    return null;
                }

                cvsm.setCaId(ca.getId());
                cvsm.setCaDescription(ca.getDescription());
                cvsm.setCaDisorder(ca.getDisorder().getId());
                if (CollectionUtils.isNotEmpty(ca.getFiles())) {
                    for (File file : ca.getFiles()) {
                        cvsm.getCaFiles().add(file.getName());
                    }
                }
                cvsm.setCaProbandId(ca.getProband().getId());
                if (ca.getFamily() != null) {
                    cvsm.setCaFamilyId(ca.getFamily().getId());

                    if (CollectionUtils.isNotEmpty(ca.getFamily().getMembers())) {
                        // phenotypes
                        List<String> list = new ArrayList<>(ca.getFamily().getPhenotypes().size());
                        for (Phenotype ot : ca.getFamily().getPhenotypes()) {
                            list.add(ot.getName());
                        }
                        cvsm.setCaFamilyPhenotypeNames(list);

                        // members
                        list = new ArrayList<>(ca.getFamily().getMembers().size());
                        for (Individual individual : ca.getFamily().getMembers()) {
                            list.add(individual.getId());
                        }
                        cvsm.setCaFamilyMemberIds(list);
                    }
                }
            }

            // ------ Interpretation fields -------

            // Interpretation ID and clinical analysis ID
            cvsm.setIntId(interpretation.getId());
            if (ca != null) {
                cvsm.setIntId(ca.getId());
            }

            // Interpretation software name and version
            if (interpretation.getMethod() != null) {
                cvsm.setIntMethodName(interpretation.getMethod().getName());
                cvsm.setIntMethodVersion(interpretation.getMethod().getVersion());
            }

            // Interpretation analyst
            if (interpretation.getAnalyst() != null) {
                cvsm.setIntAnalystName(interpretation.getAnalyst().getName());
            }

            // Interpretation panel names
//            if (CollectionUtils.isNotEmpty(interpretation.getPanels())) {
//                List<String> panelNames = new ArrayList<>();
//                for (DiseasePanel diseasePanel : interpretation.getPanels()) {
//                    panelNames.add(diseasePanel.getName());
//                }
//                cvsm.setIntPanelNames(panelNames);
//            }

            // Interpretation description, analysit, dependencies, versions, filters.... will be added to the list "info"
            StringBuilder line;
            List<String> info = new ArrayList<>();

            // Interpretation description
            if (StringUtils.isNotEmpty(interpretation.getDescription())) {
                line = new StringBuilder(DESCRIPTION_PREFIX).append(FIELD_SEPARATOR).append(interpretation.getDescription());
                info.add(line.toString());
            }

            // Interpretation analyst
            if (interpretation.getAnalyst() != null) {
                line = new StringBuilder(ANALYST_PREFIX).append(FIELD_SEPARATOR).append(interpretation.getAnalyst().getName())
                        .append(FIELD_SEPARATOR).append(interpretation.getAnalyst().getEmail());
                info.add(line.toString());
            }

            // Interpretation dependencies
            if (interpretation.getMethod() != null && CollectionUtils.isNotEmpty(interpretation.getMethod().getDependencies())) {
                for (Software dep : interpretation.getMethod().getDependencies()) {
                    info.add(DEPENDENCY_PREFIX + FIELD_SEPARATOR + dep.getName() + FIELD_SEPARATOR + dep.getVersion());
                }
            }

            // Interpretation filters
//            if (MapUtils.isNotEmpty(interpretation.getgetFilters())) {
//                for (String key: interpretation.getFilters().keySet()) {
//                    info.add(FILTER_PREFIX + FIELD_SEPARATOR + key + KEY_VALUE_SEPARATOR + interpretation.getFilters().get(key));
//                }
//            }

            // Interpretation comments
            if (CollectionUtils.isNotEmpty(interpretation.getComments())) {
                for (ClinicalComment comment: interpretation.getComments()) {
                    info.add(COMMENT_PREFIX + FIELD_SEPARATOR + comment.getMessage());
                }
            }

            // Interpretation attributes
            if (MapUtils.isNotEmpty(interpretation.getAttributes())) {
                for (String key: interpretation.getAttributes().keySet()) {
                    info.add(ATTRIBUTE_PREFIX + FIELD_SEPARATOR + key + KEY_VALUE_SEPARATOR + interpretation.getAttributes().get(key));
                }
            }

            // ...and finally, set Info
            cvsm.setIntInfo(info);

            // Interpretation creation date
            cvsm.setIntCreationDate(interpretation.getCreationDate());

            // Interpretation field intBasic is set later

            // ------ ClinicalVariant fields -------

            cvsm.setCvPrimary(isPrimary);

            if (CollectionUtils.isNotEmpty(clinicalVariant.getEvidences())) {
                try {
                    // Reported event list as a JSON string
                    cvsm.setCvClinicalVariantEvidencesJson(mapper.writeValueAsString(clinicalVariant.getEvidences()));
                } catch (JsonProcessingException e) {
                    logger.error("Unable to convert clinical variant evidence list to JSON string. Error: " + e.getMessage());
                    return null;
                }
            }

            // Reported variant comments
            if (CollectionUtils.isNotEmpty(clinicalVariant.getComments())) {
                List<String> comments = new ArrayList<>(clinicalVariant.getComments().size());
                for (ClinicalComment comment: clinicalVariant.getComments()) {
                    comments.add(encodeComent(comment));
                }
                cvsm.setCvComments(comments);
            }

            // Clinical variant attributes as a JSON string
            if (MapUtils.isNotEmpty(clinicalVariant.getAttributes())) {
                try {
                    cvsm.setCvAttributesJson(mapper.writeValueAsString(clinicalVariant.getAttributes()));
                } catch (JsonProcessingException e) {
                    logger.error("Unable to convert clinical variant attributes map to JSON string. Error: " + e.getMessage());
                    return null;
                }
            }

            // ------ ClinicalVariantEvidence fields -------

            if (CollectionUtils.isNotEmpty(clinicalVariant.getEvidences())) {
                // Create Set objects to avoid duplicated values
                Set<String> setPhenotypes = new HashSet<>();
                Set<String> setConsequenceTypeIds = new HashSet<>();
                Set<String> setXrefs = new HashSet<>();
                Set<String> setPanelNames = new HashSet<>();
                Set<String> setAcmg = new HashSet<>();
                Set<String> setClinicalSig = new HashSet<>();
                Set<String> setDrugResponse = new HashSet<>();
                Set<String> setTraitAssoc = new HashSet<>();
                Set<String> setFunctEffect = new HashSet<>();
                Set<String> setTumorigenesis = new HashSet<>();
                Set<String> setOtherClass = new HashSet<>();
                Set<String> setRolesInCancer = new HashSet<>();
                for (ClinicalVariantEvidence clinicalVariantEvidence: clinicalVariant.getEvidences()) {
                    // These structures will help us to manage the report event scores
                    List<String> list;
                    List<List<String>> lists = new ArrayList<>();

                    // Phenotypes
                    list = null;
                    if (CollectionUtils.isNotEmpty(clinicalVariantEvidence.getPhenotypes())) {
                        list = new ArrayList<>();
                        for (Phenotype phenotype : clinicalVariantEvidence.getPhenotypes()) {
                            list.add(phenotype.getId());
                        }
                        setPhenotypes.addAll(list);
                    }
                    lists.add(list);

                    // Consequence type IDs
                    list = null;
                    if (clinicalVariantEvidence.getGenomicFeature() != null) {
                        if (CollectionUtils.isNotEmpty(clinicalVariantEvidence.getGenomicFeature().getConsequenceTypes())) {
                            list = new ArrayList<>();
                            for (SequenceOntologyTerm so : clinicalVariantEvidence.getGenomicFeature().getConsequenceTypes()) {
                                list.add(so.getAccession());
                            }
                            setConsequenceTypeIds.addAll(list);
                        }
                    }
                    lists.add(list);

                    // Xrefs
                    list = null;
                    if (clinicalVariantEvidence.getGenomicFeature() != null) {
                        list = new ArrayList<>();
                        if (StringUtils.isNotEmpty(clinicalVariantEvidence.getGenomicFeature().getId())) {
                            list.add(clinicalVariantEvidence.getGenomicFeature().getId());
                        }
                        if (StringUtils.isNotEmpty(clinicalVariantEvidence.getGenomicFeature().getTranscriptId())) {
                            list.add(clinicalVariantEvidence.getGenomicFeature().getTranscriptId());
                        }
                        if (StringUtils.isNotEmpty(clinicalVariantEvidence.getGenomicFeature().getGeneName())) {
                            list.add(clinicalVariantEvidence.getGenomicFeature().getGeneName());
                        }
                        if (CollectionUtils.isNotEmpty(clinicalVariantEvidence.getGenomicFeature().getXrefs())) {
                            for (Xref xref : clinicalVariantEvidence.getGenomicFeature().getXrefs()) {
                                list.add(xref.getId());
                            }
                        }
                        setXrefs.addAll(list);
                    }
                    lists.add(list);

                    // Panel names
                    list = null;
                    if (StringUtils.isNotEmpty(clinicalVariantEvidence.getPanelId())) {
                        list = new ArrayList<>();
                        list.add(clinicalVariantEvidence.getPanelId());
                        setPanelNames.addAll(list);
                    }
                    lists.add(list);

                    // Roles in cancer
                    list = null;
                    if (clinicalVariantEvidence.getRoleInCancer() != null) {
                        list = new ArrayList<>();
                        list.add(clinicalVariantEvidence.getRoleInCancer().toString());
                        setRolesInCancer.addAll(list);
                    }
                    lists.add(list);

                    // Variant classification
                    if (clinicalVariantEvidence.getClassification() != null) {
                        // ACMG
                        list = null;
                        if (ListUtils.isNotEmpty(clinicalVariantEvidence.getClassification().getAcmg())) {
                            list = new ArrayList<>();
                            for (ClinicalAcmg acmg : clinicalVariantEvidence.getClassification().getAcmg()) {
                                list.add(acmg.getClassification());
                            }
                            setAcmg.addAll(list);
                        }
                        lists.add(list);

                        // Clinical significance
                        list = null;
                        if (clinicalVariantEvidence.getClassification().getClinicalSignificance() != null) {
                            list = new ArrayList<>();
                            list.add(clinicalVariantEvidence.getClassification().getClinicalSignificance().toString());
                            setClinicalSig.addAll(list);
                        }
                        lists.add(list);

                        // Drug response
                        list = null;
                        if (clinicalVariantEvidence.getClassification().getDrugResponse() != null) {
                            list = new ArrayList<>();
                            list.add(clinicalVariantEvidence.getClassification().getDrugResponse().toString());
                            setDrugResponse.addAll(list);
                        }
                        lists.add(list);

                        // Trait association
                        list = null;
                        if (clinicalVariantEvidence.getClassification().getTraitAssociation() != null) {
                            list = new ArrayList<>();
                            list.add(clinicalVariantEvidence.getClassification().getTraitAssociation().toString());
                            setTraitAssoc.addAll(list);
                        }
                        lists.add(list);

                        // Functional effect
                        list = null;
                        if (clinicalVariantEvidence.getClassification().getFunctionalEffect() != null) {
                            list = new ArrayList<>();
                            list.add(clinicalVariantEvidence.getClassification().getFunctionalEffect().toString());
                            setFunctEffect.addAll(list);
                        }
                        lists.add(list);

                        // Tumorigenesis
                        list = null;
                        if (clinicalVariantEvidence.getClassification().getTumorigenesis() != null) {
                            list = new ArrayList<>();
                            list.add(clinicalVariantEvidence.getClassification().getTumorigenesis().toString());
                            setTumorigenesis.addAll(list);
                        }
                        lists.add(list);

                        // Other classification
                        list = null;
                        if (ListUtils.isNotEmpty(clinicalVariantEvidence.getClassification().getOther())) {
                            list = new ArrayList<>();
                            for (String other: clinicalVariantEvidence.getClassification().getOther()) {
                                list.add(other);
                            }
                            setOtherClass.addAll(list);
                        }
                        lists.add(list);
                    }

                    // Clinical variant evidence
                    for (int i = 0; i < lists.size() - 1; i++) {
                        for (int j = i + 1; j < lists.size(); j++) {
                            for (String key1: lists.get(i)) {
                                for (String key2: lists.get(j)) {
                                    cvsm.getCveScores().put(key1 + FIELD_SEPARATOR + key2, clinicalVariantEvidence.getScore());
                                }
                            }
                        }
                    }
                }

                // Update reported event fields
                cvsm.getCvePhenotypes().addAll(setPhenotypes);
                cvsm.getCveConsequenceTypeIds().addAll(setConsequenceTypeIds);
                cvsm.getCveXrefs().addAll(setXrefs);
                cvsm.getCveRolesInCancer().addAll(setRolesInCancer);
                cvsm.getCveAcmg().addAll(setAcmg);
                cvsm.getCveClinicalSignificance().addAll(setClinicalSig);
                cvsm.getCveDrugResponse().addAll(setDrugResponse);
                cvsm.getCveTraitAssociation().addAll(setTraitAssoc);
                cvsm.getCveFunctionalEffect().addAll(setFunctEffect);
                cvsm.getCveTumorigenesis().addAll(setTumorigenesis);
                cvsm.getOther().addAll(setOtherClass);
            }

            // Finally, add the ReportedVariantSearchModel to the output list
            output.add(cvsm);
        }

        return output;
    }

    public ClinicalVariant toClinicalVariant(ClinicalVariantSearchModel cvsm) {
        ClinicalVariant clinicalVariant = (ClinicalVariant) variantSearchToVariantConverter
                .convertToDataModelType(cvsm);

        // ------ Clinical variant fields -------

        // Reported variant comments
        if (CollectionUtils.isNotEmpty(cvsm.getCvComments())) {
            clinicalVariant.setComments(new ArrayList<>());
            for (String text: cvsm.getCvComments()) {
                clinicalVariant.getComments().add(decodeComment(text));
            }
        }

        // Reported variant attributes
        if (StringUtils.isNotEmpty(cvsm.getCvAttributesJson())) {
            try {
                clinicalVariant.setAttributes(mapReader.readValue(cvsm.getCvAttributesJson()));
            } catch (IOException e) {
                logger.error("Error converting from JSON string to clinical variant attributes", e.getMessage());
                return null;
            }
        }

        // Reported events
        if (StringUtils.isNotEmpty(cvsm.getCvClinicalVariantEvidencesJson())) {
            try {
                // Just, convert the JSON into the reported event list and set the field
                clinicalVariant.setEvidences(mapper.readValue(cvsm.getCvClinicalVariantEvidencesJson(),
                        mapper.getTypeFactory().constructCollectionType(List.class, ClinicalVariantEvidence.class)));
            } catch (IOException e) {
                logger.error("Error converting from JSON string to clinical variant evidences. Error: " + e.getMessage());
                return null;
            }
        }

        // Reported variant attributes
        if (StringUtils.isNotEmpty(cvsm.getCvAttributesJson())) {
            try {
                clinicalVariant.setAttributes(mapReader.readValue(cvsm.getCvAttributesJson()));
            } catch (IOException e) {
                logger.error("Error converting from JSON string to clinical variant attributes. Error: " + e.getMessage());
                return null;
            }
        }

        return clinicalVariant;
    }

    public static String encodeComent(ClinicalComment comment) {
        StringBuilder sb = new StringBuilder();

        sb.append(comment.getAuthor() == null ? " " : comment.getAuthor()).append(FIELD_SEPARATOR);
        sb.append(CollectionUtils.isEmpty(comment.getTags()) ? " " : StringUtils.join(comment.getTags(), ",")).append(FIELD_SEPARATOR);
        sb.append(comment.getMessage() == null ? " " : comment.getMessage()).append(FIELD_SEPARATOR);
        sb.append(comment.getDate() == null ? " " : comment.getDate());

        return sb.toString();
    }

    public static ClinicalComment decodeComment(String str) {
        ClinicalComment comment = new ClinicalComment();

        String[] fields = str.split(FIELD_SEPARATOR);
        if (fields[0].trim().length() > 0) {
            comment.setAuthor(fields[0].trim());
        }
        if (fields[1].trim().length() > 0) {
            comment.setTags(Arrays.asList(fields[1].trim().split(",")));
        }
        if (fields[2].trim().length() > 0) {
            comment.setMessage(fields[2].trim());
        }
        if (fields[3].trim().length() > 0) {
            comment.setDate(fields[3].trim());
        }

        return comment;
    }
}

