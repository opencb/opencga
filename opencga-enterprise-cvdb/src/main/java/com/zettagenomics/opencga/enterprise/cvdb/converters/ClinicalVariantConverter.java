package com.zettagenomics.opencga.enterprise.cvdb.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import com.zettagenomics.opencga.enterprise.cvdb.models.ClinicalVariantSearch;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.ClinicalDiscussion;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantConfidence;
import org.opencb.opencga.storage.core.variant.search.VariantSearchModel;
import org.opencb.opencga.storage.core.variant.search.VariantSearchToVariantConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class ClinicalVariantConverter extends SearchConverter<ClinicalVariant, ClinicalVariantSearch> {

    private VariantSearchToVariantConverter variantSearchToVariantConverter;
    private ObjectReader clinicalVariantReader;

    protected Logger logger = LoggerFactory.getLogger(ClinicalVariantConverter.class);

    public ClinicalVariantConverter() {
        this.variantSearchToVariantConverter = new VariantSearchToVariantConverter();
        this.clinicalVariantReader = mapper.readerFor(ClinicalVariant.class);
    }

    public ClinicalVariantSearch toClinicalVariantSearch(ClinicalVariant cv, boolean primary, String interpretationId,
                                                         String clinicalAnalysisId, String studyId, List<String> viewers)
            throws CvdbException {
        return toClinicalVariantSearch(Collections.singletonList(cv), primary, interpretationId, clinicalAnalysisId, studyId, viewers)
                .get(0);
    }

    public List<ClinicalVariantSearch> toClinicalVariantSearch(List<ClinicalVariant> cvList, boolean primary, String interpretationId,
                                                               String clinicalAnalysisId, String studyId, List<String> viewers)
            throws CvdbException {
        List<ClinicalVariantSearch> cvsList = new ArrayList<>();

        for (ClinicalVariant cv : cvList) {
            VariantSearchModel variantSearchModel = variantSearchToVariantConverter.convertToStorageType(cv);
            ClinicalVariantSearch cvs = new ClinicalVariantSearch(variantSearchModel);

            cvs.setId(interpretationId + "-" + variantSearchModel.getVariantId());

            cvs.setCiId(interpretationId)
                    .setCaId(clinicalAnalysisId)
                    .setPrimary(primary)
                    .setStudyId(studyId)
                    .setViewers(viewers);

            // Comments are stores: author -- message -- tag1:tag2:.. -- date
            if (CollectionUtils.isNotEmpty(cv.getComments())) {
                cvs.setComments(cv.getComments().stream().map(c -> ConverterUtils.encodeComent(c)).collect(Collectors.toList()));
            }

            // Filters are stored in two dynamic fields: one for string values, the other one for numeric ones
//            private Map<String, String> cvAnnotations;
//            private Map<String, Float> cvAnnotationScores;

            // Discussion
            if (cv.getDiscussion() != null) {
                ClinicalDiscussion discussion = cv.getDiscussion();
                cvs.setDiscussionAuthor(discussion.getAuthor())
                        .setDiscussionText(discussion.getText());
                if (StringUtils.isNotEmpty(discussion.getDate())) {
                    try {
                        String solrDate = solrDateFormat.format(simpleDateFormat.parse(discussion.getDate()));
                        cvs.setDiscussionDate(solrDateFormat.parse(solrDate));
                    } catch (ParseException e) {
                        logger.warn("Impossible to process clinical variant discussion date {}: {}", discussion.getDate(), e.getMessage());
                    }
                }
            }

            // Confidence
            if (cv.getConfidence() != null) {
                ClinicalVariantConfidence confidence = cv.getConfidence();
                cvs.setConfidenceAuthor(confidence.getAuthor());
                if (StringUtils.isNotEmpty(confidence.getDate())) {
                    try {
                        String solrDate = solrDateFormat.format(simpleDateFormat.parse(confidence.getDate()));
                        cvs.setConfidenceDate(solrDateFormat.parse(solrDate));
                    } catch (ParseException e) {
                        logger.warn("Impossible to process clinical variant confidence date {}: {}", confidence.getDate(), e.getMessage());
                    }
                }
                if (confidence.getValue() != null) {
                    cvs.setConfidenceValue(confidence.getValue().name());
                }
            }

            // Tags
            cvs.setTags(cv.getTags());

            // Status
            if (cv.getStatus() != null) {
                cvs.setStatus(cv.getStatus().name());
            }

            // Clinical variant stored in a JSON string
            try {
                cvs.setJson(mapper.writeValueAsString(cv));
            } catch (JsonProcessingException e) {
                throw new CvdbException("Error when storing clinical variant JSON field", e);
            }

            // Add clinical variant search into the list
            cvsList.add(cvs);
        }

        return cvsList;
    }

    public ClinicalVariant toClinicalVariant(ClinicalVariantSearch cvs) throws CvdbException {
        return toClinicalVariant(Collections.singletonList(cvs)).get(0);
    }

    public List<ClinicalVariant> toClinicalVariant(List<ClinicalVariantSearch> cvsList) throws CvdbException {
        List<ClinicalVariant> cvList = new ArrayList<>();
        for (ClinicalVariantSearch cvs : cvsList) {
            try {
                ClinicalVariant cv;
                if (StringUtils.isNotEmpty(cvs.getJson())) {
                    //logger.info("Convert to clinical variant from JSON");
                    cv = clinicalVariantReader.readValue(cvs.getJson());
                } else {
                    //logger.info("Convert to clinical variant from indexed fields");
                    cv = new ClinicalVariant();

                    // Status
                    if (StringUtils.isNotEmpty(cvs.getStatus())) {
                        cv.setStatus(ClinicalVariant.Status.valueOf(cvs.getStatus()));
                    }

                    // Confidence
                    ClinicalVariantConfidence confidence = new ClinicalVariantConfidence();
                    if (StringUtils.isNotEmpty(cvs.getConfidenceValue())) {
                        confidence.setValue(ClinicalVariantConfidence.Confidence.valueOf(cvs.getConfidenceValue()));
                    }
                    if (StringUtils.isNotEmpty(cvs.getConfidenceAuthor())) {
                        confidence.setAuthor(cvs.getConfidenceAuthor());
                    }
                    if (cvs.getConfidenceDate() != null) {
                        confidence.setAuthor(simpleDateFormat.format(cvs.getConfidenceDate()));
                    }
                    cv.setConfidence(confidence);
                }

                // Add clinical analysis, interpretation and study in attributes
                if (cv.getAttributes() == null) {
                    cv.setAttributes(new HashMap<>());
                }
                addStudyIdAsAttribute(cvs.getStudyId(), cv.getAttributes());
                addClinicalAnalysisIdAsAttribute(cvs.getCaId(), cv.getAttributes());
                addClinicalInterpretationIdAsAttribute(cvs.getCiId(), cv.getAttributes());

                // Add to the list
                cvList.add(cv);
            } catch (JsonProcessingException e) {
                throw new CvdbException("Error when converting to clinical variant " + cvs.getVariantId(), e);
            }
        }
        return cvList;
    }

    @Override
    public ClinicalVariant toModel(ClinicalVariantSearch input) throws CvdbException {
        return toClinicalVariant(input);
    }
}
