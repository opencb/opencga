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
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.models.core.OntologyTerm;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.Interpretation;
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

    public Interpretation toInterpretation(List<ReportedVariantSearchModel> reportedVariantSearchModels) {
        if (ListUtils.isEmpty(reportedVariantSearchModels)) {
            logger.error("Empty list of reported variant search models");
            return null;
        }

        // ------ Interpretation fields -------
        Interpretation interpretation = null;

        try {
            // We take the first reportedVariantSearchModel to initialize Interpretation
            interpretation = interpretationReader.readValue(reportedVariantSearchModels.get(0).getIntBasicJson());
        } catch (IOException e) {
            logger.error("Unable to convert JSON string to Interpretation object. Error: " + e.getMessage());
            return null;
        }

        // Add reported variants
        interpretation.setReportedVariants(new ArrayList<>(reportedVariantSearchModels.size()));
        for (ReportedVariantSearchModel rvsm: reportedVariantSearchModels) {
            interpretation.getReportedVariants().add(toReportedVariant(rvsm));
        }

        return interpretation;
    }

    public List<ReportedVariantSearchModel> toReportedVariantSearchList(Interpretation interpretation) {
        ObjectMapper mapper = new ObjectMapper();

        // TODO: check reported variants is not null
        List<ReportedVariantSearchModel> output = new ArrayList<>(interpretation.getReportedVariants().size());

        for (ReportedVariant reportedVariant: interpretation.getReportedVariants()) {
            // Set variant fields
            ReportedVariantSearchModel rvsm = (ReportedVariantSearchModel) variantSearchToVariantConverter
                    .convertToStorageType(reportedVariant);

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

                rvsm.setCaName(ca.getName());
                rvsm.setCaDescription(ca.getDescription());
                rvsm.setCaDisease(ca.getDisease().getId());
                rvsm.getCaFiles().add(ca.getGermline().getName());
                rvsm.getCaFiles().add(ca.getSomatic().getName());
                rvsm.setCaProbandId(ca.getProband().getId());
                if (ca.getFamily() != null) {
                    rvsm.setCaFamilyId(ca.getFamily().getId());

                    if (ListUtils.isNotEmpty(ca.getFamily().getMembers())) {
                        // phenotypes
                        List<String> list = new ArrayList<>(ca.getFamily().getPhenotypes().size());
                        for (Phenotype ot: ca.getFamily().getPhenotypes()) {
                            list.add(ot.getName());
                        }
                        rvsm.setCaFamilyPhenotypeNames(list);

                        // members
                        list = new ArrayList<>(ca.getFamily().getMembers().size());
                        for (Individual individual : ca.getFamily().getMembers()) {
                            list.add(individual.getId());
                        }
                        rvsm.setCaFamilyMemberIds(list);
                    }
                }
            }

            // ------ Interpretation fields -------

            // Interpretation ID and clinical analysis ID
            rvsm.setIntId(interpretation.getId());
            if (ca != null) {
                rvsm.setIntId(ca.getId());
            }

            // Interpretation software name and version
            if (interpretation.getSoftware() != null) {
                rvsm.setIntSoftwareName(interpretation.getSoftware().getName());
            }
            if (interpretation.getSoftware() != null) {
                rvsm.setIntSoftwareVersion(interpretation.getSoftware().getVersion());
            }

            // Interpretation analyst
            if (interpretation.getAnalyst() != null) {
                rvsm.setIntAnalystName(interpretation.getAnalyst().getName());
            }

            // Interpretation panel names
            if (ListUtils.isNotEmpty(interpretation.getPanels())) {
                List<String> panelNames = new ArrayList<>();
                for (DiseasePanel diseasePanel: interpretation.getPanels()) {
                    panelNames.add(diseasePanel.getName());
                }
                rvsm.setIntPanelNames(panelNames);
            }

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
                        .append(FIELD_SEPARATOR).append(interpretation.getAnalyst().getEmail()).append(FIELD_SEPARATOR)
                        .append(interpretation.getAnalyst().getCompany());
                info.add(line.toString());
            }

            // Interpretation dependencies
            if (ListUtils.isNotEmpty(interpretation.getDependencies())) {
                for (Software dep: interpretation.getDependencies()) {
                    info.add(DEPENDENCY_PREFIX + FIELD_SEPARATOR + dep.getName() + FIELD_SEPARATOR + dep.getVersion());
                }
            }

            // Interpretation filters
            if (MapUtils.isNotEmpty(interpretation.getFilters())) {
                for (String key: interpretation.getFilters().keySet()) {
                    info.add(FILTER_PREFIX + FIELD_SEPARATOR + key + KEY_VALUE_SEPARATOR + interpretation.getFilters().get(key));
                }
            }

            // Interpretation comments
            if (ListUtils.isNotEmpty(interpretation.getComments())) {
                for (Comment comment: interpretation.getComments()) {
                    info.add(COMMENT_PREFIX + FIELD_SEPARATOR + comment.getText());
                }
            }

            // Interpretation attributes
            if (MapUtils.isNotEmpty(interpretation.getAttributes())) {
                for (String key: interpretation.getAttributes().keySet()) {
                    info.add(ATTRIBUTE_PREFIX + FIELD_SEPARATOR + key + KEY_VALUE_SEPARATOR + interpretation.getAttributes().get(key));
                }
            }

            // ...and finally, set Info
            rvsm.setIntInfo(info);

            // Interpretation creation date
            rvsm.setIntCreationDate(interpretation.getCreationDate());

            // Interpretation field intBasic is set later

            // ------ ReportedVariant fields -------

            if (ListUtils.isNotEmpty(reportedVariant.getReportedEvents())) {
                try {
                    // Reported event list as a JSON string
                    rvsm.setRvReportedEventsJson(mapper.writeValueAsString(reportedVariant.getReportedEvents()));
                } catch (JsonProcessingException e) {
                    logger.error("Unable to convert reported event list to JSON string. Error: " + e.getMessage());
                    return null;
                }
            }

            // Reported variant deNovoQualityScore
            rvsm.setRvDeNovoQualityScore(reportedVariant.getDeNovoQualityScore());

            // Reported variant comments
            if (ListUtils.isNotEmpty(reportedVariant.getComments())) {
                List<String> comments = new ArrayList<>(reportedVariant.getComments().size());
                for (Comment comment: reportedVariant.getComments()) {
                    comments.add(encodeComent(comment));
                }
                rvsm.setRvComments(comments);
            }

            // Reported variant attributes as a JSON string
            if (MapUtils.isNotEmpty(reportedVariant.getAttributes())) {
                try {
                    rvsm.setRvAttributesJson(mapper.writeValueAsString(reportedVariant.getAttributes()));
                } catch (JsonProcessingException e) {
                    logger.error("Unable to convert reported attributes map to JSON string. Error: " + e.getMessage());
                    return null;
                }
            }

            // ------ ReportedEvent fields -------

            if (ListUtils.isNotEmpty(reportedVariant.getReportedEvents())) {
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
                for (ReportedEvent reportedEvent: reportedVariant.getReportedEvents()) {
                    // These structures will help us to manage the report event scores
                    List<String> list;
                    List<List<String>> lists = new ArrayList<>();

                    // Phenotypes
                    list = null;
                    if (ListUtils.isNotEmpty(reportedEvent.getPhenotypes())) {
                        list = new ArrayList<>();
                        for (Phenotype phenotype : reportedEvent.getPhenotypes()) {
                            list.add(phenotype.getId());
                        }
                        setPhenotypes.addAll(list);
                    }
                    lists.add(list);

                    // Consequence type IDs
                    list = null;
                    if (ListUtils.isNotEmpty(reportedEvent.getConsequenceTypeIds())) {
                        list = new ArrayList<>();
                        for (String consequenceTypeId : reportedEvent.getConsequenceTypeIds()) {
                            list.add(consequenceTypeId);
                        }
                        setConsequenceTypeIds.addAll(list);
                    }
                    lists.add(list);

                    // Xrefs
                    list = null;
                    if (reportedEvent.getGenomicFeature() != null) {
                        list = new ArrayList<>();
                        if (StringUtils.isNotEmpty(reportedEvent.getGenomicFeature().getEnsemblGeneId())) {
                            list.add(reportedEvent.getGenomicFeature().getEnsemblGeneId());
                        }
                        if (StringUtils.isNotEmpty(reportedEvent.getGenomicFeature().getEnsemblTranscriptId())) {
                            list.add(reportedEvent.getGenomicFeature().getEnsemblTranscriptId());
                        }
                        if (StringUtils.isNotEmpty(reportedEvent.getGenomicFeature().getEnsemblRegulatoryId())) {
                            list.add(reportedEvent.getGenomicFeature().getEnsemblRegulatoryId());
                        }
                        if (StringUtils.isNotEmpty(reportedEvent.getGenomicFeature().getGeneName())) {
                            list.add(reportedEvent.getGenomicFeature().getGeneName());
                        }
                        if (MapUtils.isNotEmpty(reportedEvent.getGenomicFeature().getXrefs())) {
                            list.addAll(reportedEvent.getGenomicFeature().getXrefs().values());
                        }
                        setXrefs.addAll(list);
                    }
                    lists.add(list);

                    // Panel names
                    list = null;
                    if (StringUtils.isNotEmpty(reportedEvent.getPanelId())) {
                        list = new ArrayList<>();
                        list.add(reportedEvent.getPanelId());
                        setPanelNames.addAll(list);
                    }
                    lists.add(list);

                    // Roles in cancer
                    list = null;
                    if (reportedEvent.getRoleInCancer() != null) {
                        list = new ArrayList<>();
                        list.add(reportedEvent.getRoleInCancer().toString());
                        setRolesInCancer.addAll(list);
                    }
                    lists.add(list);

                    // Variant classification
                    if (reportedEvent.getClassification() != null) {
                        // ACMG
                        list = null;
                        if (ListUtils.isNotEmpty(reportedEvent.getClassification().getAcmg())) {
                            list = new ArrayList<>();
                            for (String acmg : reportedEvent.getClassification().getAcmg()) {
                                list.add(acmg);
                            }
                            setAcmg.addAll(list);
                        }
                        lists.add(list);

                        // Clinical significance
                        list = null;
                        if (reportedEvent.getClassification().getClinicalSignificance() != null) {
                            list = new ArrayList<>();
                            list.add(reportedEvent.getClassification().getClinicalSignificance().toString());
                            setClinicalSig.addAll(list);
                        }
                        lists.add(list);

                        // Drug response
                        list = null;
                        if (reportedEvent.getClassification().getDrugResponse() != null) {
                            list = new ArrayList<>();
                            list.add(reportedEvent.getClassification().getDrugResponse().toString());
                            setDrugResponse.addAll(list);
                        }
                        lists.add(list);

                        // Trait association
                        list = null;
                        if (reportedEvent.getClassification().getTraitAssociation() != null) {
                            list = new ArrayList<>();
                            list.add(reportedEvent.getClassification().getTraitAssociation().toString());
                            setTraitAssoc.addAll(list);
                        }
                        lists.add(list);

                        // Functional effect
                        list = null;
                        if (reportedEvent.getClassification().getFunctionalEffect() != null) {
                            list = new ArrayList<>();
                            list.add(reportedEvent.getClassification().getFunctionalEffect().toString());
                            setFunctEffect.addAll(list);
                        }
                        lists.add(list);

                        // Tumorigenesis
                        list = null;
                        if (reportedEvent.getClassification().getTumorigenesis() != null) {
                            list = new ArrayList<>();
                            list.add(reportedEvent.getClassification().getTumorigenesis().toString());
                            setTumorigenesis.addAll(list);
                        }
                        lists.add(list);

                        // Other classification
                        list = null;
                        if (ListUtils.isNotEmpty(reportedEvent.getClassification().getOther())) {
                            list = new ArrayList<>();
                            for (String other: reportedEvent.getClassification().getOther()) {
                                list.add(other);
                            }
                            setOtherClass.addAll(list);
                        }
                        lists.add(list);
                    }

                    // Reported event
                    for (int i = 0; i < lists.size() - 1; i++) {
                        for (int j = i + 1; j < lists.size(); j++) {
                            for (String key1: lists.get(i)) {
                                for (String key2: lists.get(j)) {
                                    rvsm.getReScores().put(key1 + FIELD_SEPARATOR + key2, reportedEvent.getScore());
                                }
                            }
                        }
                    }
                }

                // Update reported event fields
                rvsm.getRePhenotypes().addAll(setPhenotypes);
                rvsm.getReConsequenceTypeIds().addAll(setConsequenceTypeIds);
                rvsm.getReXrefs().addAll(setXrefs);
                rvsm.getReRolesInCancer().addAll(setRolesInCancer);
                rvsm.getReAcmg().addAll(setAcmg);
                rvsm.getReClinicalSignificance().addAll(setClinicalSig);
                rvsm.getReDrugResponse().addAll(setDrugResponse);
                rvsm.getReTraitAssociation().addAll(setTraitAssoc);
                rvsm.getReFunctionalEffect().addAll(setFunctEffect);
                rvsm.getReTumorigenesis().addAll(setTumorigenesis);
                rvsm.getOther().addAll(setOtherClass);
            }

            // Finally, add the ReportedVariantSearchModel to the output list
            output.add(rvsm);
        }

        // Update reported variant search model by setting the intBasicJson field
        interpretation.setReportedVariants(null);
        String intBasicJson = null;
        try {
            intBasicJson = mapper.writeValueAsString(interpretation);
        } catch (JsonProcessingException e) {
            logger.error("Error converting from intrepretation to JSON string", e.getMessage());
            return null;
        }
        for (ReportedVariantSearchModel rvsm: output) {
            rvsm.setIntBasicJson(intBasicJson);
        }

        return output;
    }

    public ReportedVariant toReportedVariant(ReportedVariantSearchModel rvsm) {
        ReportedVariant reportedVariant = (ReportedVariant) variantSearchToVariantConverter
                .convertToDataModelType(rvsm);

        // ------ Reported variant fields -------

        // Reported variant deNovoQualityScore
        reportedVariant.setDeNovoQualityScore(rvsm.getRvDeNovoQualityScore());

        // Reported variant comments
        if (ListUtils.isNotEmpty(rvsm.getRvComments())) {
            reportedVariant.setComments(new ArrayList<>());
            for (String text: rvsm.getRvComments()) {
                reportedVariant.getComments().add(decodeComment(text));
            }
        }

        // Reported variant attributes
        if (StringUtils.isNotEmpty(rvsm.getRvAttributesJson())) {
            try {
                reportedVariant.setAttributes(mapReader.readValue(rvsm.getRvAttributesJson()));
            } catch (IOException e) {
                logger.error("Error converting from JSON string to reported variant attributes", e.getMessage());
                return null;
            }
        }

        // Reported events
        if (StringUtils.isNotEmpty(rvsm.getRvReportedEventsJson())) {
            try {
                // Just, convert the JSON into the reported event list and set the field
                reportedVariant.setReportedEvents(mapper.readValue(rvsm.getRvReportedEventsJson(),
                        mapper.getTypeFactory().constructCollectionType(List.class, ReportedEvent.class)));
            } catch (IOException e) {
                logger.error("Error converting from JSON string to reported events. Error: " + e.getMessage());
                return null;
            }
        }

        // Reported variant attributes
        if (StringUtils.isNotEmpty(rvsm.getRvAttributesJson())) {
            try {
                reportedVariant.setAttributes(mapReader.readValue(rvsm.getRvAttributesJson()));
            } catch (IOException e) {
                logger.error("Error converting from JSON string to reported variant attributes. Error: " + e.getMessage());
                return null;
            }
        }

        return reportedVariant;
    }

    public static String encodeComent(Comment comment) {
        StringBuilder sb = new StringBuilder();

        sb.append(comment.getAuthor() == null ? " " : comment.getAuthor()).append(FIELD_SEPARATOR);
        sb.append(comment.getType() == null ? " " : comment.getType()).append(FIELD_SEPARATOR);
        sb.append(comment.getText() == null ? " " : comment.getText()).append(FIELD_SEPARATOR);
        sb.append(comment.getDate() == null ? " " : comment.getDate());

        return sb.toString();
    }

    public static Comment decodeComment(String str) {
        Comment comment = new Comment();

        String[] fields = str.split(FIELD_SEPARATOR);
        if (fields[0].trim().length() > 0) {
            comment.setAuthor(fields[0].trim());
        }
        if (fields[1].trim().length() > 0) {
            comment.setType(fields[1].trim());
        }
        if (fields[2].trim().length() > 0) {
            comment.setText(fields[2].trim());
        }
        if (fields[3].trim().length() > 0) {
            comment.setDate(fields[3].trim());
        }

        return comment;
    }
}

