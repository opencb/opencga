package com.zettagenomics.opencga.enterprise.cva.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.zettagenomics.opencga.enterprise.cva.exceptions.CvaException;
import com.zettagenomics.opencga.enterprise.cva.models.ClinicalVariantSearch;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.beans.Field;
import org.opencb.biodata.models.clinical.ClinicalDiscussion;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantConfidence;
import org.opencb.biodata.models.clinical.interpretation.InterpretationMethod;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.variant.search.VariantSearchModel;
import org.opencb.opencga.storage.core.variant.search.VariantSearchToVariantConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ClinicalVariantConverter extends SearchConverter {

    private VariantSearchToVariantConverter variantSearchToVariantConverter;
    private ObjectReader clinicalVariantReader;

    protected Logger logger = LoggerFactory.getLogger(ClinicalVariantConverter.class);

    public ClinicalVariantConverter() {
        this.variantSearchToVariantConverter = new VariantSearchToVariantConverter();
        this.clinicalVariantReader = mapper.readerFor(ClinicalVariant.class);
    }

    public ClinicalVariantSearch toClinicalVariantSearch(ClinicalVariant cv, boolean isPrimary, String interpretationId)
            throws CvaException {
        return toClinicalVariantSearch(Collections.singletonList(cv), isPrimary, interpretationId).get(0);
    }

    public List<ClinicalVariantSearch> toClinicalVariantSearch(List<ClinicalVariant> cvList, boolean isPrimary, String interpretationId)
            throws CvaException {
        List<ClinicalVariantSearch> cvsList = new ArrayList<>();

        for (ClinicalVariant cv : cvList) {
            VariantSearchModel variantSearchModel = variantSearchToVariantConverter.convertToStorageType(cv);
            ClinicalVariantSearch cvs = new ClinicalVariantSearch(variantSearchModel);

            cvs.setId(cv.toStringSimple() + "-" + interpretationId);
            cvs.setCvInterpretationId(interpretationId);

            cvs.setCvPrimary(isPrimary);

            // Comments are stores: author -- message -- tag1:tag2:.. -- date
            if (CollectionUtils.isNotEmpty(cv.getComments())) {
                cvs.setCvComments(cv.getComments().stream().map(c -> ConverterUtils.encodeComent(c)).collect(Collectors.toList()));
            }

            // Filters are stored in two dynamic fields: one for string values, the other one for numeric ones
//            private Map<String, String> cvAnnotations;
//            private Map<String, Float> cvAnnotationScores;

            // Discussion
            if (cv.getDiscussion() != null) {
                ClinicalDiscussion discussion = cv.getDiscussion();
                cvs.setCvDiscussionAuthor(discussion.getAuthor())
                        .setCvDiscussionDate(discussion.getDate())
                        .setCvDiscussionText(discussion.getText());
            }

            // Confidence
            if (cv.getConfidence() != null) {
                ClinicalVariantConfidence confidence = cv.getConfidence();
                cvs.setCvConfidenceAuthor(confidence.getAuthor())
                        .setCvConfidenceDate(confidence.getDate());
                if (confidence.getValue() != null) {
                    cvs.setCvConfidenceValue(confidence.getValue().name());
                }
            }

            // Tags
            cvs.setCvTags(cv.getTags());

            // Status
            if (cv.getStatus() != null) {
                cvs.setCvStatus(cv.getStatus().name());
            }

            // Clinical variant stored in a JSON string
            // First, we have to clone and remove the clinical variants to do not store them!
            ClinicalVariant clone = new ClinicalVariant ();
            clone.setEvidences(cv.getEvidences());

            cv.setEvidences(null);
            try {
                cvs.setCvJson(mapper.writeValueAsString(cv));
            } catch (JsonProcessingException e) {
                throw new CvaException("Error when storing clinical variant JSON field", e);
            }
            cv.setEvidences(clone.getEvidences());

            // Add clinical variant search into the list
            cvsList.add(cvs);
        }

        return cvsList;
    }

    public ClinicalVariant toClinicalVariant(ClinicalVariantSearch cvs) throws CvaException {
        return toClinicalVariant(Collections.singletonList(cvs)).get(0);
    }

    public List<ClinicalVariant> toClinicalVariant(List<ClinicalVariantSearch> cvsList) throws CvaException {
        List<ClinicalVariant> cvList = new ArrayList<>();
        for (ClinicalVariantSearch cvs : cvsList) {
            try {
                cvList.add(clinicalVariantReader.readValue(cvs.getCvJson()));
            } catch (JsonProcessingException e) {
                throw new CvaException("Error when converting to clinical variant " + cvs.getVariantId(), e);
            }
        }
        return cvList;
    }
}
