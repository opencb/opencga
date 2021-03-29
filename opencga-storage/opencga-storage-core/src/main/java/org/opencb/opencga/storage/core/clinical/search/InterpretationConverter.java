package org.opencb.opencga.storage.core.clinical.search;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.ClinicalAudit;
import org.opencb.biodata.models.clinical.ClinicalComment;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.clinical.interpretation.*;
import org.opencb.biodata.models.core.Xref;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.common.FlagAnnotation;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.configuration.ClinicalConsentParam;
import org.opencb.opencga.storage.core.clinical.ClinicalVariantException;
import org.opencb.opencga.storage.core.variant.search.VariantSearchModel;
import org.opencb.opencga.storage.core.variant.search.VariantSearchToVariantConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

public class InterpretationConverter {

    private ObjectMapper mapper;

    private VariantSearchToVariantConverter variantSearchToVariantConverter;

    private static final String KEY_VALUE_SEPARATOR = "=";

    protected static Logger logger;

    public InterpretationConverter() {
        this.variantSearchToVariantConverter = new VariantSearchToVariantConverter();

        this.mapper = JacksonUtils.getDefaultObjectMapper();

        this.logger = LoggerFactory.getLogger(InterpretationConverter.class);
    }

    public Interpretation toInterpretation(List<ClinicalVariantSearchModel> clinicalVariantSearchModels) throws ClinicalVariantException {
        Interpretation interpretation = null;
        ObjectReader iReader = mapper.readerFor(Interpretation.class);

        if (CollectionUtils.isEmpty(clinicalVariantSearchModels)) {
            logger.warn("List of clinical variant search models is empty");
            return interpretation;
        }

        try {
            // We take the first reportedVariantSearchModel to initialize Interpretation
            interpretation = iReader.readValue(clinicalVariantSearchModels.get(0).getIntJson());
        } catch (IOException e) {
            throw new ClinicalVariantException("Unable to convert JSON string to Interpretation object.", e);
        }

        List<ClinicalVariant> primaryFindings = new ArrayList<>();
        List<ClinicalVariant> secondaryFindings = new ArrayList<>();

        // Add reported variants (both primary and secondary findings)
        for (ClinicalVariantSearchModel cvsm: clinicalVariantSearchModels) {
            ClinicalVariant clinicalVariant = toClinicalVariant(cvsm);
            if (cvsm.isCvSecondaryInterpretation()) {
                secondaryFindings.add(clinicalVariant);
            } else {
                primaryFindings.add(clinicalVariant);
            }
        }

        interpretation.setPrimaryFindings(primaryFindings);
        interpretation.setSecondaryFindings(secondaryFindings);

        return interpretation;
    }

    public List<ClinicalVariantSearchModel> toClinicalVariantSearchList(Interpretation interpretation) throws JsonProcessingException {
        // Sanity check
        if (interpretation == null) {
            return null;
        }

        ClinicalVariantSearchModel base = new ClinicalVariantSearchModel();
        List<ClinicalVariantSearchModel> clinicalVariantSearchList = new ArrayList<>();

        //
        // Clinical analysis
        //
        if (MapUtils.isNotEmpty(interpretation.getAttributes())
                && interpretation.getAttributes().containsKey("OPENCGA_CLINICAL_ANALYSIS")) {

            ObjectReader caReader = mapper.readerFor(ClinicalAnalysis.class);

            String caJson = (String) interpretation.getAttributes().get("OPENCGA_CLINICAL_ANALYSIS");
            ClinicalAnalysis clinicalAnalysis = caReader.readValue(caJson);

            base.setCaId(clinicalAnalysis.getId());

            if (clinicalAnalysis.getDisorder() != null) {
                base.setCaDisorderId(clinicalAnalysis.getDisorder().getId());
            }

            if (clinicalAnalysis.getType() != null) {
                base.setCaType(clinicalAnalysis.getType().name());
            }

            if (CollectionUtils.isNotEmpty(clinicalAnalysis.getFiles())) {
                base.setCaFiles(getFiles(clinicalAnalysis));
            }

            // Proband fields
            if (clinicalAnalysis.getProband() != null) {
                base.setCaProbandId(clinicalAnalysis.getProband().getId());

                // Proband phenotypes
                if (CollectionUtils.isNotEmpty(clinicalAnalysis.getProband().getPhenotypes())) {
                    for (Phenotype phenotype : clinicalAnalysis.getProband().getPhenotypes()) {
                        if (StringUtils.isNotEmpty(phenotype.getId())) {
                            base.getCaProbandPhenotypes().add(phenotype.getId());
                        }
                        if (StringUtils.isNotEmpty(phenotype.getName())) {
                            base.getCaProbandPhenotypes().add(phenotype.getName());
                        }
                    }
                }

                // Proband disorders
                if (CollectionUtils.isNotEmpty(clinicalAnalysis.getProband().getDisorders())) {
                    for (Disorder disorder : clinicalAnalysis.getProband().getDisorders()) {
                        if (StringUtils.isNotEmpty(disorder.getId())) {
                            base.getCaProbandDisorders().add(disorder.getId());
                        }
                        if (StringUtils.isNotEmpty(disorder.getName())) {
                            base.getCaProbandDisorders().add(disorder.getName());
                        }
                    }
                }

                // Proband sample IDs
                if (CollectionUtils.isNotEmpty(clinicalAnalysis.getProband().getSamples())) {
                    for (Sample sample : clinicalAnalysis.getProband().getSamples()) {
                        if (StringUtils.isNotEmpty(sample.getId())) {
                            base.getCaProbandSampleIds().add(sample.getId());
                        }
                    }
                }
            }

            // Family fields
            if (clinicalAnalysis.getFamily() != null) {
                base.setCaFamilyId(clinicalAnalysis.getFamily().getId());

                // Family members
                for (Individual individual : clinicalAnalysis.getFamily().getMembers()) {
                    base.getCaFamilyMemberIds().add(individual.getId());
                }
            }

            // Consent
            if (clinicalAnalysis.getConsent() != null && CollectionUtils.isNotEmpty(clinicalAnalysis.getConsent().getConsents())) {
                for (ClinicalConsentParam consent : clinicalAnalysis.getConsent().getConsents()) {
                    base.getCaConsent().add(consent.getName());
                }
            }

            // Priority
            if (clinicalAnalysis.getPriority() != null) {
                base.setCaPriority(clinicalAnalysis.getPriority().getId());
            }

            // Flags
            if (CollectionUtils.isNotEmpty(clinicalAnalysis.getFlags())) {
                for (FlagAnnotation flag : clinicalAnalysis.getFlags()) {
                    base.getCaFlags().add(flag.getId());
                }
            }

            // Creation date fields
            if (StringUtils.isNotEmpty(clinicalAnalysis.getCreationDate())) {
                Date date = TimeUtils.toDate(clinicalAnalysis.getCreationDate());
                LocalDate localDate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
                base.setCaCreationDate(date.getTime());
                base.setCaCreationYear(localDate.getYear());
                base.setCaCreationMonth(localDate.getMonth().getValue());
                base.setCaCreationDay(localDate.getDayOfMonth());
                base.setCaCreationDayOfWeek(localDate.getDayOfWeek().toString());
            }

            // Release
            base.setCaRelease(clinicalAnalysis.getRelease());

            // Quality control
            base.setCaQualityControl(clinicalAnalysis.getQualityControl().getComment());

            // Audit
            if (CollectionUtils.isNotEmpty(clinicalAnalysis.getAudit())) {
                for (ClinicalAudit clinicalAudit : clinicalAnalysis.getAudit()) {
                    base.getCaAudit().add(clinicalAudit.getMessage());
                }
            }

            // Internal status
            if (clinicalAnalysis.getInternal() != null && clinicalAnalysis.getInternal().getStatus() != null) {
                base.setCaInternalStatus(clinicalAnalysis.getInternal().getStatus().getName());
            }

            // Status
            if (clinicalAnalysis.getStatus() != null) {
                base.setCaStatus(clinicalAnalysis.getStatus().getId());
            }

            // Analyst name
            if (clinicalAnalysis.getAnalyst() != null) {
                base.setCaAnalystName(clinicalAnalysis.getAnalyst().getName());
            }

            // Info
            base.setCaInfo(getClinicalAnalysisInfo(clinicalAnalysis));

            // Clinical analysis JSON
            base.setCaJson(caJson);
        }

        //
        // Interpretation fields
        //

        base.setIntId(interpretation.getId());

        // Method names
        if (CollectionUtils.isNotEmpty(interpretation.getMethods())) {
            for (InterpretationMethod method : interpretation.getMethods()) {
                base.getIntMethodNames().add(method.getName());
            }
        }

        // Creation date fields
        if (interpretation.getCreationDate() != null) {
            Date date = TimeUtils.toDate(interpretation.getCreationDate());
            LocalDate localDate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
            base.setIntCreationDate(date.getTime());
            base.setIntCreationYear(localDate.getYear());
            base.setIntCreationMonth(localDate.getMonth().getValue());
            base.setIntCreationDay(localDate.getDayOfMonth());
            base.setIntCreationDayOfWeek(localDate.getDayOfWeek().toString());
        }

        // Version
        base.setIntVersion(interpretation.getVersion());

        // Status
        if (interpretation.getStatus() != null) {
            base.setIntStatus(interpretation.getStatus().getId());
        }

        // Analyst
        if (interpretation.getAnalyst() != null) {
            base.setIntAnalystName(interpretation.getAnalyst().getName());
        }

        // Store description, analysit, dependencies, versions, filters.... into the field 'info'
        base.setIntInfo(getInterpretationInfo(interpretation));

        // Save primary and secondary findings
        List<ClinicalVariant> primaryFindings = new ArrayList<>(interpretation.getPrimaryFindings());
        List<ClinicalVariant> secondaryFindings = new ArrayList<>(interpretation.getPrimaryFindings());

        // and now overwrite findings to save interpretation JSON without them
        interpretation.setPrimaryFindings(new ArrayList<>());
        interpretation.setSecondaryFindings(new ArrayList<>());
        base.setIntJson(mapper.writeValueAsString(interpretation));

        // Primary findings
        clinicalVariantSearchList.addAll(createClinicalVariantSearchList(primaryFindings, true, base));

        // Secondary findings
        clinicalVariantSearchList.addAll(createClinicalVariantSearchList(secondaryFindings, false, base));

        return clinicalVariantSearchList;
    }

    public ClinicalVariant toClinicalVariant(ClinicalVariantSearchModel cvsm) throws ClinicalVariantException {
        Variant variant = variantSearchToVariantConverter.convertToDataModelType(cvsm);
        ClinicalVariant clinicalVariant = null;
        try {
            clinicalVariant = mapper.readerFor(ClinicalVariant.class).readValue(variant.toJson());
        } catch (JsonProcessingException e) {
            throw new ClinicalVariantException("Error creating clinical variant from variant", e);
        }

        //
        // Clinical variant fields
        //

        if (CollectionUtils.isNotEmpty(cvsm.getCvInterpretationMethodNames())) {
            clinicalVariant.setInterpretationMethodNames(cvsm.getCvInterpretationMethodNames());
        }

        if (StringUtils.isNotEmpty(cvsm.getCvStatus())) {
            clinicalVariant.setStatus(ClinicalVariant.Status.valueOf(cvsm.getCvStatus()));
        }

        if (CollectionUtils.isNotEmpty(cvsm.getCvComments())) {
            clinicalVariant.setComments(new ArrayList<>());
            for (String text: cvsm.getCvComments()) {
                clinicalVariant.getComments().add(decodeComment(text));
            }
        }

        clinicalVariant.setDiscussion(cvsm.getCvDiscussion());

        if (StringUtils.isNotEmpty(cvsm.getCvClinicalVariantEvidencesJson())) {
            try {
                // Just, convert the JSON into the clinical variant evidences
                clinicalVariant.setEvidences(mapper.readerFor(List.class).readValue(cvsm.getCvClinicalVariantEvidencesJson()));
            } catch (IOException e) {
                throw new ClinicalVariantException("Error converting from JSON string to clinical variant evidences.", e);
            }
        }

        if (StringUtils.isNotEmpty(cvsm.getCvAttributesJson())) {
            try {
                clinicalVariant.setAttributes(mapper.readerFor(Map.class).readValue(cvsm.getCvAttributesJson()));
            } catch (IOException e) {
                throw new ClinicalVariantException("Error converting from JSON string to clinical variant attributes.", e);
            }
        }

        return clinicalVariant;
    }

    //-------------------------------------------------------------------------
    // P R I V A T E     M E T H O D S
    //-------------------------------------------------------------------------

    private List<ClinicalVariantSearchModel> createClinicalVariantSearchList(List<ClinicalVariant> clinicalVariants,
                                                                             boolean arePrimaryFindings, ClinicalVariantSearchModel base)
            throws JsonProcessingException {
        List<ClinicalVariantSearchModel> clinicalVariantSearchList = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(clinicalVariants)) {
            ObjectWriter vsmWriter = mapper.writerFor(VariantSearchModel.class);
            ObjectReader cvsmReader = mapper.readerFor(ClinicalVariantSearchModel.class);
            ObjectWriter listWriter = mapper.writerFor(List.class);
            ObjectWriter mapWriter = mapper.writerFor(Map.class);

            for (ClinicalVariant clinicalVariant : clinicalVariants) {

                // Set variant search fields
                VariantSearchModel vsm = variantSearchToVariantConverter.convertToStorageType(clinicalVariant);
                String vsmJson = vsmWriter.writeValueAsString(vsm);
                ClinicalVariantSearchModel cvsm = cvsmReader.readValue(vsmJson);

                // Set base fields: clinical analysis, interpretation and catalog fields
                updateClinicalVariantSearch(cvsm, base);

                // Set clinical variant fields
                cvsm.setCvSecondaryInterpretation(!arePrimaryFindings);
                if (CollectionUtils.isNotEmpty(clinicalVariant.getInterpretationMethodNames())) {
                    for (String interpretationMethodName : clinicalVariant.getInterpretationMethodNames()) {
                        cvsm.getCvInterpretationMethodNames().add(interpretationMethodName);
                    }
                }
                if (clinicalVariant.getStatus() != null) {
                    cvsm.setCvStatus(clinicalVariant.getStatus().name());
                }
                if (CollectionUtils.isNotEmpty(clinicalVariant.getComments())) {
                    cvsm.setCvComments(getComments(clinicalVariant.getComments()));
                }
                cvsm.setCvDiscussion(clinicalVariant.getDiscussion());
                if (CollectionUtils.isNotEmpty(clinicalVariant.getEvidences())) {
                    // Clinical variant evidences list as a JSON string
                    cvsm.setCvClinicalVariantEvidencesJson(listWriter.writeValueAsString(clinicalVariant.getEvidences()));
                }
                if (MapUtils.isNotEmpty(clinicalVariant.getAttributes())) {
                    cvsm.setCvAttributesJson(mapWriter.writeValueAsString(clinicalVariant.getAttributes()));
                }

                // Set clinical variant evidence fields
                setClinicalVariantEvidences(clinicalVariant.getEvidences(), cvsm);

                // Finally, add it to the list
                clinicalVariantSearchList.add(cvsm);
            }
        }
        return clinicalVariantSearchList;
    }

    private void updateClinicalVariantSearch(ClinicalVariantSearchModel input, ClinicalVariantSearchModel base) {

        // ClinicalAnalisys fields
        input.setCaId(base.getCaId());
        input.setCaDisorderId(base.getCaDisorderId());
        input.setCaType(base.getCaType());
        input.setCaFiles(base.getCaFiles());
        input.setCaProbandId(base.getCaProbandId());
        input.setCaProbandPhenotypes(base.getCaProbandPhenotypes());
        input.setCaProbandDisorders(base.getCaProbandDisorders());
        input.setCaProbandSampleIds(base.getCaProbandSampleIds());
        input.setCaFamilyId(base.getCaFamilyId());
        input.setCaFamilyMemberIds(base.getCaFamilyMemberIds());
        input.setCaConsent(base.getCaConsent());
        input.setCaPriority(base.getCaPriority());
        input.setCaFlags(base.getCaFlags());
        input.setCaCreationDate(base.getCaCreationDate());
        input.setCaCreationYear(base.getCaCreationYear());
        input.setCaCreationMonth(base.getCaCreationMonth());
        input.setCaCreationDay(base.getCaCreationDay());
        input.setCaCreationDayOfWeek(base.getCaCreationDayOfWeek());
        input.setCaRelease(base.getCaRelease());
        input.setCaQualityControl(base.getCaQualityControl());
        input.setCaAudit(base.getCaAudit());
        input.setCaInternalStatus(base.getCaInternalStatus());
        input.setCaStatus(base.getCaStatus());
        input.setCaAnalystName(base.getCaAnalystName());
        input.setCaInfo(base.getCaInfo());
        input.setCaJson(base.getCaJson());

        // Interpreation fields
        input.setIntId(base.getIntId());
        input.setIntMethodNames(base.getIntMethodNames());
        input.setIntCreationDate(base.getIntCreationDate());
        input.setIntCreationYear(base.getIntCreationYear());
        input.setIntCreationMonth(base.getIntCreationMonth());
        input.setIntCreationDay(base.getIntCreationDay());
        input.setIntCreationDayOfWeek(base.getIntCreationDayOfWeek());
        input.setIntVersion(base.getIntVersion());
        input.setIntStatus(base.getIntStatus());
        input.setIntAnalystName(base.getIntAnalystName());
        input.setIntInfo(base.getIntInfo());
        input.setIntJson(base.getIntJson());

        // Catalog fields
        input.setProjectId(base.getProjectId());
        input.setAssembly(base.getAssembly());
        input.setStudyId(base.getStudyId());
        input.setStudyJson(base.getStudyJson());
    }


    private void setClinicalVariantEvidences(List<ClinicalVariantEvidence> evidences, ClinicalVariantSearchModel cvsm) {
        if (CollectionUtils.isNotEmpty(evidences)) {
            Set<String> aux = new HashSet<>();
            Map<String, List<String>> justification = new HashMap<>();

            // Create Set objects to avoid duplicated values
            Set<String> phenotypes = new HashSet<>();
            Set<String> consequenceTypeIds = new HashSet<>();
            Set<String> xrefs = new HashSet<>();
            Set<String> modesOfInheritance = new HashSet<>();
            Set<String> penetrances = new HashSet<>();
            Set<String> panelIds = new HashSet<>();
            Set<String> tiers = new HashSet<>();
            Set<String> acmgs = new HashSet<>();
            Set<String> clinicalSignificances = new HashSet<>();
            Set<String> drugResponses = new HashSet<>();
            Set<String> traitAssociations = new HashSet<>();
            Set<String> functionalEffects = new HashSet<>();
            Set<String> tumorigenesis = new HashSet<>();
            Set<String> otherClassifications = new HashSet<>();
            Set<String> rolesInCancer = new HashSet<>();


            for (ClinicalVariantEvidence evidence: evidences) {
                // These structures will help us to manage the clinical variant evidence justification
                List<String> list;
                List<List<String>> lists = new ArrayList<>();

                // Phenotypes
                if (CollectionUtils.isNotEmpty(evidence.getPhenotypes())) {
                    list = new ArrayList<>();
                    for (Phenotype phenotype : evidence.getPhenotypes()) {
                        list.add(phenotype.getId());
                    }
                    phenotypes.addAll(list);
                    lists.add(list);
                }

                if (evidence.getGenomicFeature() != null) {
                    // Consequence types
                    list = new ArrayList<>();
                    for (SequenceOntologyTerm soTerm : evidence.getGenomicFeature().getConsequenceTypes()) {
                        list.add(soTerm.getName());
                        list.add(soTerm.getAccession());
                    }
                    consequenceTypeIds.addAll(list);
                    lists.add(list);


                    // Xrefs
                    list = new ArrayList<>();
                    if (StringUtils.isNotEmpty(evidence.getGenomicFeature().getGeneName())) {
                        list.add(evidence.getGenomicFeature().getGeneName());
                    }
                    if (StringUtils.isNotEmpty(evidence.getGenomicFeature().getTranscriptId())) {
                        list.add(evidence.getGenomicFeature().getTranscriptId());
                    }
                    if (StringUtils.isNotEmpty(evidence.getGenomicFeature().getId())) {
                        list.add(evidence.getGenomicFeature().getId());
                    }
                    if (CollectionUtils.isNotEmpty(evidence.getGenomicFeature().getXrefs())) {
                        for (Xref xref : evidence.getGenomicFeature().getXrefs()) {
                            list.add(xref.getId());
                        }

                    }
                    xrefs.addAll(list);
                    lists.add(list);
                }

                // Modes of inheritance
                if (evidence.getModeOfInheritance() != null) {
                    list = new ArrayList<>();
                    list.add(evidence.getModeOfInheritance().name());
                    modesOfInheritance.addAll(list);
                    lists.add(list);
                }

                // Penetrance
                if (evidence.getPenetrance() != null) {
                    list = new ArrayList<>();
                    list.add(evidence.getPenetrance().name());
                    penetrances.addAll(list);
                    lists.add(list);
                }

                // Panel IDs
                if (StringUtils.isNotEmpty(evidence.getPanelId())) {
                    list = new ArrayList<>();
                    list.add(evidence.getPanelId());
                    panelIds.addAll(list);
                    lists.add(list);
                }

                // Variant classification
                if (evidence.getClassification() != null) {
                    //  Tier
                    if (evidence.getClassification().getTier() != null) {
                        list = new ArrayList<>();
                        list.add(evidence.getClassification().getTier());
                        tiers.addAll(list);
                        lists.add(list);
                    }

                    // ACMG
                    if (CollectionUtils.isNotEmpty(evidence.getClassification().getAcmg())) {
                        list = new ArrayList<>();
                        for (String acmg : evidence.getClassification().getAcmg()) {
                            list.add(acmg);
                        }
                        acmgs.addAll(list);
                        lists.add(list);
                    }

                    // Clinical significance
                    if (evidence.getClassification().getClinicalSignificance() != null) {
                        list = new ArrayList<>();
                        list.add(evidence.getClassification().getClinicalSignificance().toString());
                        clinicalSignificances.addAll(list);
                        lists.add(list);
                    }

                    // Drug response
                    if (evidence.getClassification().getDrugResponse() != null) {
                        list = new ArrayList<>();
                        list.add(evidence.getClassification().getDrugResponse().toString());
                        drugResponses.addAll(list);
                        lists.add(list);
                    }

                    // Trait association
                    if (evidence.getClassification().getTraitAssociation() != null) {
                        list = new ArrayList<>();
                        list.add(evidence.getClassification().getTraitAssociation().toString());
                        traitAssociations.addAll(list);
                        lists.add(list);
                    }

                    // Functional effect
                    if (evidence.getClassification().getFunctionalEffect() != null) {
                        list = new ArrayList<>();
                        list.add(evidence.getClassification().getFunctionalEffect().toString());
                        functionalEffects.addAll(list);
                        lists.add(list);
                    }

                    // Tumorigenesis
                    if (evidence.getClassification().getTumorigenesis() != null) {
                        list = new ArrayList<>();
                        list.add(evidence.getClassification().getTumorigenesis().toString());
                        tumorigenesis.addAll(list);
                        lists.add(list);
                    }

                    // Other classification
                    if (CollectionUtils.isNotEmpty(evidence.getClassification().getOther())) {
                        list = new ArrayList<>();
                        for (String other: evidence.getClassification().getOther()) {
                            list.add(other);
                        }
                        otherClassifications.addAll(list);
                        lists.add(list);
                    }
                }

                // Roles in cancer
                if (evidence.getRoleInCancer() != null) {
                    list = new ArrayList<>();
                    list.add(evidence.getRoleInCancer().toString());
                    rolesInCancer.addAll(list);
                    lists.add(list);
                }

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
            cvsm.getCvePhenotypeNames().addAll(phenotypes);
            cvsm.getCveConsequenceTypes().addAll(consequenceTypeIds);
            cvsm.getCveXrefs().addAll(xrefs);
            cvsm.getCveTiers().addAll(tiers);
            cvsm.getCveAcmgs().addAll(acmgs);
            cvsm.getCveClinicalSignificances().addAll(clinicalSignificances);
            cvsm.getCveDrugResponses().addAll(drugResponses);
            cvsm.getCveTraitAssociations().addAll(traitAssociations);
            cvsm.getCveFunctionalEffects().addAll(functionalEffects);
            cvsm.getCveTumorigenesis().addAll(tumorigenesis);
            cvsm.getOther().addAll(otherClassifications);
            cvsm.getCveRolesInCancer().addAll(rolesInCancer);
            cvsm.getCveAux().addAll(aux);
            cvsm.getCveJustification().putAll(justification);
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
        sb.append(CollectionUtils.isEmpty(comment.getTags()) ? " " : StringUtils.join(comment.getTags(), ";"))
                .append(ClinicalVariantUtils.FIELD_SEPARATOR);
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
                    case 1:
                        if (StringUtils.isNotEmpty(value)) {
                            comment.setTags(Arrays.asList(value.split(";")));
                        }
                        break;
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

