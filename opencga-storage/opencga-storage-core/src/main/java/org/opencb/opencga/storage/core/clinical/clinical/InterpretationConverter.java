package org.opencb.opencga.storage.core.clinical.clinical;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.ClinicalComment;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.biodata.models.clinical.interpretation.Interpretation;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.storage.core.variant.search.VariantSearchToVariantConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

public class InterpretationConverter {

    private VariantSearchToVariantConverter variantSearchToVariantConverter;
    private ObjectMapper mapper;
    private ObjectReader mapReader;
    private ObjectReader interpretationReader;
    private ObjectReader clinicalAnalysisReader;

    private static final String KEY_VALUE_SEPARATOR = "=";

    // ------ ClinicalAnalisys fields -------
    private String caJson;
    private String caId;
    private String caDisorderId;
    private List<String> caFilenames;
    private String caProbandId;
    private List<String> caProbandDisorders;
    private List<String> caProbandPhenotypes;
    private String caFamilyId;
    private List<String> caFamilyMemberIds;
    private List<String> caInfo;

    // ------ Interpretation fields -------
    private String intId;
    private String intStatus;
    private String intSoftwareName;
    private String intSoftwareVersion;
    private String intAnalystName;
    private List<String> intPanels;
    private List<String> intInfo;
    private long intCreationDate = 0;
    private int intCreationYear = 0;
    private int intCreationMonth = 0;
    private int intCreationDay = 0;
    private String intCreationDayOfWeek;

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
        if (CollectionUtils.isEmpty(clinicalVariantSearchModels)) {
            logger.error("Empty list of reported variant search models");
            return null;
        }

        // ------ Interpretation fields -------
        Interpretation interpretation = null;

        try {
            // We take the first reportedVariantSearchModel to initialize Interpretation
            interpretation = interpretationReader.readValue(clinicalVariantSearchModels.get(0).getIntJson());
        } catch (IOException e) {
            logger.error("Unable to convert JSON string to Interpretation object. Error: " + e.getMessage());
            return null;
        }

        // Add reported variants (both primary and secondary findings)
        interpretation.setPrimaryFindings(new ArrayList<>(clinicalVariantSearchModels.size()));
        interpretation.setSecondaryFindings(new ArrayList<>(clinicalVariantSearchModels.size()));

        for (ClinicalVariantSearchModel rvsm: clinicalVariantSearchModels) {
            ClinicalVariant reportedVariant = toClinicalVariant(rvsm);
//            if (rvsm.isRvPrimaryFinding()) {
//                interpretation.getPrimaryFindings().add(reportedVariant);
//            } else {
//                interpretation.getSecondaryFindings().add(reportedVariant);
//            }
        }

        return interpretation;
    }

    public List<ClinicalVariantSearchModel> toReportedVariantSearchList(Interpretation interpretation) {
        ObjectMapper mapper = new ObjectMapper();

        List<ClinicalVariantSearchModel> output = new ArrayList<>();

        // ------ Init clinical analysis field variables -------

        if (MapUtils.isNotEmpty(interpretation.getAttributes())
                && interpretation.getAttributes().containsKey("OPENCGA_CLINICAL_ANALYSIS")) {
            caJson = (String) interpretation.getAttributes().get("OPENCGA_CLINICAL_ANALYSIS");

            try {
                ClinicalAnalysis ca = clinicalAnalysisReader.readValue(caJson);
                caInfo = getClinicalAnalysisInfo(ca);

                caId = ca.getId();
                if (ca.getDisorder() != null) {
                    caDisorderId = ca.getDisorder().getId();
                }
                caFilenames = getFiles(ca);
                if (ca.getProband() != null) {
                    caProbandId = ca.getProband().getId();

                    // Proband disorders
                    if (CollectionUtils.isNotEmpty(ca.getProband().getDisorders())) {
                        caProbandDisorders = new ArrayList<>();
                        for (Disorder disorder : ca.getProband().getDisorders()) {
                            if (StringUtils.isNotEmpty(disorder.getId())) {
                                caProbandDisorders.add(disorder.getId());
                            }
                            if (StringUtils.isNotEmpty(disorder.getName())) {
                                caProbandDisorders.add(disorder.getName());
                            }
                        }
                    }

                    // Proband phenotypes
                    if (CollectionUtils.isNotEmpty(ca.getProband().getPhenotypes())) {
                        caProbandPhenotypes = new ArrayList<>();
                        for (Phenotype phenotype : ca.getProband().getPhenotypes()) {
                            if (StringUtils.isNotEmpty(phenotype.getId())) {
                                caProbandPhenotypes.add(phenotype.getId());
                            }
                            if (StringUtils.isNotEmpty(phenotype.getName())) {
                                caProbandPhenotypes.add(phenotype.getName());
                            }
                        }
                    }
                }
                if (ca.getFamily() != null) {
                    caFamilyId = ca.getFamily().getId();

                    // Family members
                    caFamilyMemberIds = new ArrayList<>();
                    for (Individual individual : ca.getFamily().getMembers()) {
                        caFamilyMemberIds.add(individual.getId());
                    }
                }

                caInfo = getClinicalAnalysisInfo(ca);
            } catch (IOException e) {
                logger.error("Unable to convert ClinicalAnalysis object to JSON string. Error: " + e.getMessage());
                return null;
            }
        }

        // ------ Init interpretation field variables -------

        if (interpretation != null) {
            intId = interpretation.getId();

            if (interpretation.getStatus() != null) {
                intStatus = interpretation.getStatus().getId();
            }

            // Interpretation software name and version
//            if (interpretation.getSoftware() != null) {
//                intSoftwareName = interpretation.getSoftware().getName();
//                intSoftwareVersion = interpretation.getSoftware().getVersion();
//            }

            // Interpretation analyst
            if (interpretation.getAnalyst() != null) {
                intAnalystName = interpretation.getAnalyst().getName();
            }

            // Interpretation panel (ID and name)
//            if (ListUtils.isNotEmpty(interpretation.getPanels())) {
//                intPanels = new ArrayList<>();
//                for (DiseasePanel diseasePanel : interpretation.getPanels()) {
//                    if (StringUtils.isNotEmpty(diseasePanel.getId())) {
//                        intPanels.add(diseasePanel.getId());
//                    }
//                    if (StringUtils.isNotEmpty(diseasePanel.getName())) {
//                        intPanels.add(diseasePanel.getName());
//                    }
//                }
//            }

            // Store description, analysit, dependencies, versions, filters.... into the field 'info'
            intInfo = getInterpretationInfo(interpretation);

            // Interpretation creation date
            if (interpretation.getCreationDate() != null) {
                Date date = TimeUtils.toDate(interpretation.getCreationDate());
                LocalDate localDate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
                intCreationDate = date.getTime();
                intCreationYear = localDate.getYear();
                intCreationMonth = localDate.getMonth().getValue();
                intCreationDay = localDate.getDayOfMonth();
                intCreationDayOfWeek = localDate.getDayOfWeek().toString();
            }
        }

        // Primary findings
        output.addAll(createClinicalVariantSearchModels(interpretation.getPrimaryFindings(), true));

        // Secondary findings
        output.addAll(createClinicalVariantSearchModels(interpretation.getSecondaryFindings(), false));

        // Update reported variant search model by setting the intBasicJson field
        setInterpretationJson(interpretation, output);

        return output;
    }

    public ClinicalVariant toClinicalVariant(ClinicalVariantSearchModel rvsm) {
        ClinicalVariant clinicalVariant = (ClinicalVariant) variantSearchToVariantConverter
                .convertToDataModelType(rvsm);

        // ------ Reported variant fields -------

        // Reported variant status
//        if (StringUtils.isNotEmpty(rvsm.getRvStatus())) {
//            clinicalVariant.setStatus(ClinicalVariant.Status.valueOf(rvsm.getRvStatus()));
//        }

        // Reported variant deNovoQualityScore
        //clinicalVariant.setDeNovoQualityScore(rvsm.getRvDeNovoQualityScore());

        // Reported variant comments
//        if (ListUtils.isNotEmpty(rvsm.getRvComments())) {
//            clinicalVariant.setComments(new ArrayList<>());
//            for (String text: rvsm.getRvComments()) {
//                clinicalVariant.getComments().add(decodeComment(text));
//            }
//        }

        // Reported variant attributes
//        if (StringUtils.isNotEmpty(rvsm.getRvAttributesJson())) {
//            try {
//                clinicalVariant.setAttributes(mapReader.readValue(rvsm.getRvAttributesJson()));
//            } catch (IOException e) {
//                logger.error("Error converting from JSON string to reported variant attributes", e.getMessage());
//                return null;
//            }
//        }
//
//        // Reported events
//        if (StringUtils.isNotEmpty(rvsm.getRvReportedEventsJson())) {
//            try {
//                // Just, convert the JSON into the reported event list and set the field
//                clinicalVariant.setEvidences(mapper.readValue(rvsm.getRvReportedEventsJson(),
//                        mapper.getTypeFactory().constructCollectionType(List.class, ClinicalVariantEvidence.class)));
//            } catch (IOException e) {
//                logger.error("Error converting from JSON string to reported events. Error: " + e.getMessage());
//                return null;
//            }
//        }
//
//        // Reported variant attributes
//        if (StringUtils.isNotEmpty(rvsm.getRvAttributesJson())) {
//            try {
//                clinicalVariant.setAttributes(mapReader.readValue(rvsm.getRvAttributesJson()));
//            } catch (IOException e) {
//                logger.error("Error converting from JSON string to reported variant attributes. Error: " + e.getMessage());
//                return null;
//            }
//        }

        return clinicalVariant;
    }

    private List<ClinicalVariantSearchModel> createClinicalVariantSearchModels(List<ClinicalVariant> clinicalVariants,
                                                                               boolean arePrimaryFindings) {
        List<ClinicalVariantSearchModel> clinicalVariantSearchModels = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(clinicalVariants)) {
            for (ClinicalVariant clinicalVariant : clinicalVariants) {
                // Set variant fields
                ClinicalVariantSearchModel cvsm = (ClinicalVariantSearchModel) variantSearchToVariantConverter
                        .convertToStorageType(clinicalVariant);

                // ------ ClinicalAnalisys fields -------
                cvsm.setCaJson(caJson);
                cvsm.setCaId(caId);
                cvsm.setCaDisorderId(caDisorderId);
                cvsm.setCaFiles(caFilenames);
                cvsm.setCaProbandId(caProbandId);
                cvsm.setCaProbandDisorders(caProbandDisorders);
                cvsm.setCaProbandPhenotypes(caProbandPhenotypes);
                cvsm.setCaFamilyId(caFamilyId);
                cvsm.setCaFamilyMemberIds(caFamilyMemberIds);
                cvsm.setCaInfo(caInfo);

                // ------ Interpretaion fields -------
                cvsm.setIntId(intId);
                cvsm.setIntStatus(intStatus);
//                cvsm.setIntSoftwareName(intSoftwareName);
//                cvsm.setIntSoftwareVersion(intSoftwareVersion);
//                cvsm.setIntAnalystName(intAnalystName);
                cvsm.setIntPanels(intPanels);
                cvsm.setIntInfo(intInfo);
                cvsm.setIntCreationDate(intCreationDate);
                cvsm.setIntCreationYear(intCreationYear);
                cvsm.setIntCreationMonth(intCreationMonth);
                cvsm.setIntCreationDay(intCreationDay);
                cvsm.setIntCreationDayOfWeek(intCreationDayOfWeek);

                // Interpretation field intBasic is set later

                // ------ ReportedVariant fields -------
                if (CollectionUtils.isNotEmpty(clinicalVariant.getEvidences())) {
//                    try {
//                        // Reported event list as a JSON string
//                        cvsm.setRvReportedEventsJson(mapper.writeValueAsString(clinicalVariant.getEvidences()));
//                    } catch (JsonProcessingException e) {
//                        logger.error("Unable to convert reported event list to JSON string. Error: " + e.getMessage());
//                        return null;
//                    }
                }

//                // Reported variant primary finding
//                cvsm.setRvPrimaryFinding(arePrimaryFindings);
//
//                // Reported variant status
//                if (clinicalVariant.getStatus() != null) {
//                    cvsm.setRvStatus(clinicalVariant.getStatus().name());
//                }
//
//                // Reported variant deNovoQualityScore
//                //rvsm.setRvDeNovoQualityScore(reportedVariant.getDeNovoQualityScore());
//
//                // Reported variant comments
//                if (ListUtils.isNotEmpty(clinicalVariant.getComments())) {
//                    cvsm.setRvComments(getComments(clinicalVariant.getComments()));
//                }
//
//                // Reported variant attributes as a JSON string
//                if (MapUtils.isNotEmpty(clinicalVariant.getAttributes())) {
//                    try {
//                        cvsm.setRvAttributesJson(mapper.writeValueAsString(clinicalVariant.getAttributes()));
//                    } catch (JsonProcessingException e) {
//                        logger.error("Unable to convert reported attributes map to JSON string. Error: " + e.getMessage());
//                        return null;
//                    }
//                }

                // ------ ReportedEvent fields -------
                setReportedEvents(clinicalVariant.getEvidences(), cvsm);

                clinicalVariantSearchModels.add(cvsm);
            }
        }
        return clinicalVariantSearchModels;
    }


    private void setReportedEvents(List<ClinicalVariantEvidence> evidences, ClinicalVariantSearchModel rvsm) {
        if (CollectionUtils.isNotEmpty(evidences)) {
            Set<String> aux = new HashSet<>();
            Map<String, List<String>> justification = new HashMap<>();

            // Create Set objects to avoid duplicated values
            Set<String> setPhenotypes = new HashSet<>();
            Set<String> setConsequenceTypeIds = new HashSet<>();
            Set<String> setXrefs = new HashSet<>();
            Set<String> setPanelIds = new HashSet<>();
            Set<String> setAcmg = new HashSet<>();
            Set<String> setClinicalSig = new HashSet<>();
            Set<String> setDrugResponse = new HashSet<>();
            Set<String> setTraitAssoc = new HashSet<>();
            Set<String> setFunctEffect = new HashSet<>();
            Set<String> setTumorigenesis = new HashSet<>();
            Set<String> setOtherClass = new HashSet<>();
            Set<String> setRolesInCancer = new HashSet<>();
            Set<String> setTier = new HashSet<>();
            for (ClinicalVariantEvidence evidence: evidences) {
                // These structures will help us to manage the reported event justification
                List<String> list;
                List<List<String>> lists = new ArrayList<>();

                // Phenotypes
                if (CollectionUtils.isNotEmpty(evidence.getPhenotypes())) {
                    list = new ArrayList<>();
                    for (Phenotype phenotype : evidence.getPhenotypes()) {
                        list.add(phenotype.getId());
                    }
                    setPhenotypes.addAll(list);
                    lists.add(list);
                }

                // Consequence type IDs
//                if (CollectionUtils.isNotEmpty(evidence.getConsequenceTypes())) {
//                    list = new ArrayList<>();
//                    for (SequenceOntologyTerm soTerm : evidence.getConsequenceTypes()) {
//                        list.add(soTerm.getAccession());
//                    }
//                    setConsequenceTypeIds.addAll(list);
//                    lists.add(list);
//                }

                // Xrefs
//                if (evidence.getGenomicFeature() != null) {
//                    list = new ArrayList<>();
//                    if (StringUtils.isNotEmpty(evidence.getGenomicFeature().getEnsemblGeneId())) {
//                        list.add(evidence.getGenomicFeature().getEnsemblGeneId());
//                    }
//                    if (StringUtils.isNotEmpty(evidence.getGenomicFeature().getEnsemblTranscriptId())) {
//                        list.add(evidence.getGenomicFeature().getEnsemblTranscriptId());
//                    }
//                    if (StringUtils.isNotEmpty(evidence.getGenomicFeature().getEnsemblRegulatoryId())) {
//                        list.add(evidence.getGenomicFeature().getEnsemblRegulatoryId());
//                    }
//                    if (StringUtils.isNotEmpty(evidence.getGenomicFeature().getGeneName())) {
//                        list.add(evidence.getGenomicFeature().getGeneName());
//                    }
//                    if (MapUtils.isNotEmpty(evidence.getGenomicFeature().getXrefs())) {
//                        list.addAll(evidence.getGenomicFeature().getXrefs().values());
//                    }
//                    setXrefs.addAll(list);
//                    lists.add(list);
//                }

                // Panel IDs
                if (StringUtils.isNotEmpty(evidence.getPanelId())) {
                    list = new ArrayList<>();
                    list.add(evidence.getPanelId());
                    setPanelIds.addAll(list);
                    lists.add(list);
                }

                // Variant classification
                if (evidence.getClassification() != null) {
                    // ACMG
                    if (CollectionUtils.isNotEmpty(evidence.getClassification().getAcmg())) {
                        list = new ArrayList<>();
                        for (String acmg : evidence.getClassification().getAcmg()) {
                            list.add(acmg);
                        }
                        setAcmg.addAll(list);
                        lists.add(list);
                    }

                    // Clinical significance
                    if (evidence.getClassification().getClinicalSignificance() != null) {
                        list = new ArrayList<>();
                        list.add(evidence.getClassification().getClinicalSignificance().toString());
                        setClinicalSig.addAll(list);
                        lists.add(list);
                    }

                    // Drug response
                    if (evidence.getClassification().getDrugResponse() != null) {
                        list = new ArrayList<>();
                        list.add(evidence.getClassification().getDrugResponse().toString());
                        setDrugResponse.addAll(list);
                        lists.add(list);
                    }

                    // Trait association
                    if (evidence.getClassification().getTraitAssociation() != null) {
                        list = new ArrayList<>();
                        list.add(evidence.getClassification().getTraitAssociation().toString());
                        setTraitAssoc.addAll(list);
                        lists.add(list);
                    }

                    // Functional effect
                    if (evidence.getClassification().getFunctionalEffect() != null) {
                        list = new ArrayList<>();
                        list.add(evidence.getClassification().getFunctionalEffect().toString());
                        setFunctEffect.addAll(list);
                        lists.add(list);
                    }

                    // Tumorigenesis
                    if (evidence.getClassification().getTumorigenesis() != null) {
                        list = new ArrayList<>();
                        list.add(evidence.getClassification().getTumorigenesis().toString());
                        setTumorigenesis.addAll(list);
                        lists.add(list);
                    }

                    // Other classification
                    if (CollectionUtils.isNotEmpty(evidence.getClassification().getOther())) {
                        list = new ArrayList<>();
                        for (String other: evidence.getClassification().getOther()) {
                            list.add(other);
                        }
                        setOtherClass.addAll(list);
                        lists.add(list);
                    }
                }

                // Roles in cancer
                if (evidence.getRoleInCancer() != null) {
                    list = new ArrayList<>();
                    list.add(evidence.getRoleInCancer().toString());
                    setRolesInCancer.addAll(list);
                    lists.add(list);
                }

                // Tier
//                if (evidence.getTier() != null) {
//                    list = new ArrayList<>();
//                    list.add(evidence.getTier());
//                    setTier.addAll(list);
//                    lists.add(list);
//                }

                // Justification and auxiliar field
                String key;
                for (int i = 0; i < lists.size(); i++) {
                    for (String key1: lists.get(i)) {
                        if (!justification.containsKey(key1)) {
                            justification.put(key1, new ArrayList<>());
                        }
                        justification.get(key1).add(evidence.getJustification());
                    }
                }
                for (int i = 0; i < lists.size() - 1; i++) {
                    for (int j = i + 1; j < lists.size(); j++) {
                        for (String key1: lists.get(i)) {
                            for (String key2: lists.get(j)) {
                                key = key1 + ClinicalVariantUtils.FIELD_SEPARATOR + key2;
                                aux.add(key);
                                if (!justification.containsKey(key)) {
                                    justification.put(key, new ArrayList<>());
                                }
                                justification.get(key).add(evidence.getJustification());
                            }
                        }
                    }
                }
            }

            // Update reported event fields
//            rvsm.getRePhenotypes().addAll(setPhenotypes);
            rvsm.getReConsequenceTypeIds().addAll(setConsequenceTypeIds);
            rvsm.getReXrefs().addAll(setXrefs);
            rvsm.getReAcmg().addAll(setAcmg);
            rvsm.getReClinicalSignificance().addAll(setClinicalSig);
            rvsm.getReDrugResponse().addAll(setDrugResponse);
            rvsm.getReTraitAssociation().addAll(setTraitAssoc);
            rvsm.getReFunctionalEffect().addAll(setFunctEffect);
            rvsm.getReTumorigenesis().addAll(setTumorigenesis);
            rvsm.getOther().addAll(setOtherClass);
            rvsm.getReRolesInCancer().addAll(setRolesInCancer);
            rvsm.setReJustification(justification);
            rvsm.getReTier().addAll(setTier);
            rvsm.getReAux().addAll(aux);
        }
    }

    private void setInterpretationJson(Interpretation interpretation, List<ClinicalVariantSearchModel> clinicalVariantSearchModels) {
        try {
            // Set to null the reported variants (primary and secondary findings) in order to avoid save them into the json
            interpretation.setPrimaryFindings(null);
            interpretation.setSecondaryFindings(null);
            String intJson = mapper.writeValueAsString(interpretation);
            for (ClinicalVariantSearchModel rvsm: clinicalVariantSearchModels) {
                rvsm.setIntJson(intJson);
            }
        } catch (JsonProcessingException e) {
            logger.error("Error converting from intrepretation to JSON string", e.getMessage());
        }
    }

    private List<String> getClinicalAnalysisInfo(ClinicalAnalysis ca) {
        StringBuilder line;
        List<String> info = new ArrayList<>();

        // Interpretation description
        if (StringUtils.isNotEmpty(ca.getDescription())) {
            line = new StringBuilder(ClinicalVariantUtils.DESCRIPTION_PREFIX).append(ClinicalVariantUtils.FIELD_SEPARATOR)
                    .append(ca.getDescription());
            info.add(line.toString());
        }

        // Interpretation comments
        if (CollectionUtils.isNotEmpty(ca.getComments())) {
            for (ClinicalComment comment: ca.getComments()) {
                info.add(encodeComment(comment, ClinicalVariantUtils.COMMENT_PREFIX));
            }
        }

        // ...and finally, return this info
        return info;
    }

    private List<String> getInterpretationInfo(Interpretation interpretation) {
        StringBuilder line;
        List<String> info = new ArrayList<>();

        // Interpretation description
        if (StringUtils.isNotEmpty(interpretation.getDescription())) {
            line = new StringBuilder(ClinicalVariantUtils.DESCRIPTION_PREFIX).append(ClinicalVariantUtils.FIELD_SEPARATOR)
                    .append(interpretation.getDescription());
            info.add(line.toString());
        }

//        // Interpretation analyst
//        if (interpretation.getAnalyst() != null) {
//            line = new StringBuilder(ClinicalVariantUtils.ANALYST_PREFIX).append(ClinicalVariantUtils.FIELD_SEPARATOR)
//                    .append(interpretation.getAnalyst().getName())
//                    .append(ClinicalVariantUtils.FIELD_SEPARATOR).append(interpretation.getAnalyst().getEmail())
//                    .append(ClinicalVariantUtils.FIELD_SEPARATOR)
//                    .append(interpretation.getAnalyst().getCompany());
//            info.add(line.toString());
//        }

//        // Interpretation dependencies
//        if (CollectionUtils.isNotEmpty(interpretation.getDependencies())) {
//            for (Software dep: interpretation.getDependencies()) {
//                info.add(ClinicalVariantUtils.DEPENDENCY_PREFIX + ClinicalVariantUtils.FIELD_SEPARATOR + dep.getName()
//                        + ClinicalVariantUtils.FIELD_SEPARATOR + dep.getVersion());
//            }
//        }

//        // Interpretation filters
//        if (MapUtils.isNotEmpty(interpretation.getFilters())) {
//            for (String key: interpretation.getFilters().keySet()) {
//                info.add(ClinicalVariantUtils.FILTER_PREFIX + ClinicalVariantUtils.FIELD_SEPARATOR + key + KEY_VALUE_SEPARATOR
//                        + interpretation.getFilters().get(key));
//            }
//        }

        // Interpretation comments
        if (CollectionUtils.isNotEmpty(interpretation.getComments())) {
            for (ClinicalComment comment: interpretation.getComments()) {
                info.add(encodeComment(comment, ClinicalVariantUtils.COMMENT_PREFIX));
            }
        }

        // Interpretation attributes
        if (MapUtils.isNotEmpty(interpretation.getAttributes())) {
            for (String key: interpretation.getAttributes().keySet()) {
                info.add(ClinicalVariantUtils.ATTRIBUTE_PREFIX + ClinicalVariantUtils.FIELD_SEPARATOR + key + KEY_VALUE_SEPARATOR
                        + interpretation.getAttributes().get(key));
            }
        }

        // ...and finally, return this info
        return info;
    }

    private List<String> getComments(List<ClinicalComment> comments) {
        List<String> res = new ArrayList<>(comments.size());
        for (ClinicalComment comment: comments) {
            res.add(encodeComent(comment));
        }
        return res;
    }

    private List<String> getFiles(ClinicalAnalysis ca) {
        Set<String> files = new HashSet<>();
        if (CollectionUtils.isNotEmpty(ca.getFiles())) {
            for (File caFile : ca.getFiles()) {
                files.add(caFile.getName());
            }
        }
        return new ArrayList<>(files);
    }

    public static String encodeComent(ClinicalComment comment) {
        return encodeComment(comment, null);
    }

    public static String encodeComment(ClinicalComment comment, String prefix) {
        StringBuilder sb = new StringBuilder();

        if (StringUtils.isNotEmpty(prefix)) {
            sb.append(prefix).append(ClinicalVariantUtils.FIELD_SEPARATOR);
        }
        sb.append(comment.getAuthor() == null ? " " : comment.getAuthor()).append(ClinicalVariantUtils.FIELD_SEPARATOR);
//        sb.append(comment.getType() == null ? " " : comment.getType()).append(ClinicalVariantUtils.FIELD_SEPARATOR);
        sb.append(comment.getDate() == null ? " " : comment.getDate()).append(ClinicalVariantUtils.FIELD_SEPARATOR);
        sb.append(comment.getMessage() == null ? " " : comment.getMessage());

        return sb.toString();
    }

    public static ClinicalComment decodeComment(String str) {
        return decodeComment(str, false);
    }

    public static ClinicalComment decodeComment(String str, boolean prefix) {
        ClinicalComment comment = new ClinicalComment();
        String[] fields = str.split(ClinicalVariantUtils.FIELD_SEPARATOR);
        int start = 0;
        if (prefix) {
            start = 1;
        }
        for (int i = start; i < fields.length; i++) {
            String value = fields[i].trim();
            if (StringUtils.isNotEmpty(value)) {
                switch (i - start) {
                    case 0:
                        comment.setAuthor(value);
                        break;
//                    case 1:
//                        comment.setType(value);
//                    break;
                    case 2:
                        comment.setDate(value);
                        break;
                    case 3:
                        comment.setMessage(value);
                        break;
                    default:
                        break;
                }
            }
        }
        return comment;
    }
}

