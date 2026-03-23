package org.opencb.opencga.clinical.cvdb.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import org.opencb.opencga.clinical.cvdb.exceptions.CvdbException;
import org.opencb.opencga.clinical.cvdb.models.ClinicalVariantSearch;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.ClinicalDiscussion;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantConfidence;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.project.SearchIndexMetadata;
import org.opencb.opencga.storage.core.variant.search.VariantSearchModel;
import org.opencb.opencga.storage.core.variant.search.VariantSearchToVariantConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ClinicalVariantConverter extends SearchConverter<ClinicalVariant, ClinicalVariantSearch> {

    private VariantSearchToVariantConverter variantSearchToVariantConverter;
    private ObjectReader clinicalVariantReader;

    protected Logger logger = LoggerFactory.getLogger(ClinicalVariantConverter.class);

    public ClinicalVariantConverter() {
        this.clinicalVariantReader = mapper.readerFor(ClinicalVariant.class);
    }

    public ClinicalVariantConverter(SearchIndexMetadata searchIndexMetadata) {
        this.variantSearchToVariantConverter = VariantSearchToVariantConverter.converterSimpleStats(searchIndexMetadata);
        this.clinicalVariantReader = mapper.readerFor(ClinicalVariant.class);
    }

    public ClinicalVariantSearch toClinicalVariantSearch(ClinicalVariant cv, boolean isPrimaryFinding, String interpretationId,
                                                         boolean isPrimaryInterpretation, String clinicalAnalysisId, String studyId)
            throws CvdbException {
        return toClinicalVariantSearch(Collections.singletonList(cv), isPrimaryFinding, interpretationId, isPrimaryInterpretation,
                clinicalAnalysisId, studyId)
                .get(0);
    }

    public List<ClinicalVariantSearch> toClinicalVariantSearch(List<ClinicalVariant> cvList, boolean isPrimaryFinding,
                                                               String interpretationId, boolean isPrimaryInterpretation,
                                                               String clinicalAnalysisId, String studyId)
            throws CvdbException {
        List<ClinicalVariantSearch> cvsList = new ArrayList<>();

        for (ClinicalVariant cv : cvList) {
            VariantSearchModel variantSearchModel = variantSearchToVariantConverter.convertToStorageType(cv);
            ClinicalVariantSearch cvs = new ClinicalVariantSearch(variantSearchModel);

            cvs.setId(interpretationId + "-" + variantSearchModel.getId());
            cvs.setVariantId(variantSearchModel.getId());

            cvs.setPrimaryFinding(isPrimaryFinding)
                    .setCiId(interpretationId)
                    .setPrimaryInterpretation(isPrimaryInterpretation)
                    .setCaId(clinicalAnalysisId)
                    .setStudyId(studyId);

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

                // Add to the list
                cvList.add(cv);
            } catch (JsonProcessingException e) {
                throw new CvdbException("Error when converting to clinical variant " + cvs.getFullId(), e);
            }
        }
        return cvList;
    }

    @Override
    public ClinicalVariant toModel(ClinicalVariantSearch input) throws CvdbException {
        return toClinicalVariant(input);
    }

    public ClinicalVariantConverter setVariantSearchToVariantConverter(VariantSearchToVariantConverter variantSearchToVariantConverter) {
        this.variantSearchToVariantConverter = variantSearchToVariantConverter;
        return this;
    }
}
