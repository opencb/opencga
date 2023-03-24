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

package com.zettagenomics.opencga.enterprise.cva.models;

import org.apache.solr.client.solrj.beans.Field;
import org.opencb.opencga.storage.core.variant.search.VariantSearchModel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClinicalVariantSearch extends VariantSearchModel {

    @Field("cvInterpretationId")
    private String cvInterpretationId;

    @Field("cvPrimary")
    private boolean cvPrimary;

	// Comments are stores: author -- message -- tag1:tag2:.. -- date
    @Field("cvComments")
    private List<String> cvComments;

	// Filters are stored in two dynamic fields: one for string values, the other one for numeric ones
    @Field("cvAnnotations")
    private Map<String, String> cvAnnotations;

    @Field("cvAnnotationScores")
    private Map<String, Float> cvAnnotationScores;

    @Field("cvDiscussionAuthor")
    private String cvDiscussionAuthor;

    @Field("cvDiscussionDate")
    private String cvDiscussionDate;

    @Field("cvDiscussionText")
    private String cvDiscussionText;

    @Field("cvConfidenceValue")
    private String cvConfidenceValue;

    @Field("cvConfidenceAuthor")
    private String cvConfidenceAuthor;

    @Field("cvConfidenceDate")
    private String cvConfidenceDate;

    @Field("cvTags")
    private List<String> cvTags;

    @Field("cvStatus")
    private String cvStatus;

    // Clinical variant stored in a JSON string
    @Field("cvJson")
    private String cvJson;

    public ClinicalVariantSearch() {
        init();
    }

    public ClinicalVariantSearch(VariantSearchModel variantSearchModel) {
        super(variantSearchModel);
        init();
    }

    private void init() {
        cvComments = new ArrayList<>();
        cvAnnotations = new HashMap<>();
        cvAnnotationScores = new HashMap<>();
        cvTags = new ArrayList<>();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalVariantSearch{");
        sb.append("cvInterpretationId='").append(cvInterpretationId).append('\'');
        sb.append(", cvPrimary=").append(cvPrimary);
        sb.append(", cvComments=").append(cvComments);
        sb.append(", cvAnnotations=").append(cvAnnotations);
        sb.append(", cvAnnotationScores=").append(cvAnnotationScores);
        sb.append(", cvDiscussionAuthor='").append(cvDiscussionAuthor).append('\'');
        sb.append(", cvDiscussionDate='").append(cvDiscussionDate).append('\'');
        sb.append(", cvDiscussionText='").append(cvDiscussionText).append('\'');
        sb.append(", cvConfidenceValue='").append(cvConfidenceValue).append('\'');
        sb.append(", cvConfidenceAuthor='").append(cvConfidenceAuthor).append('\'');
        sb.append(", cvConfidenceDate='").append(cvConfidenceDate).append('\'');
        sb.append(", cvTags=").append(cvTags);
        sb.append(", cvStatus='").append(cvStatus).append('\'');
        sb.append(", cvJson='").append(cvJson).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getCvInterpretationId() {
        return cvInterpretationId;
    }

    public ClinicalVariantSearch setCvInterpretationId(String cvInterpretationId) {
        this.cvInterpretationId = cvInterpretationId;
        return this;
    }

    public boolean isCvPrimary() {
        return cvPrimary;
    }

    public ClinicalVariantSearch setCvPrimary(boolean cvPrimary) {
        this.cvPrimary = cvPrimary;
        return this;
    }

    public List<String> getCvComments() {
        return cvComments;
    }

    public ClinicalVariantSearch setCvComments(List<String> cvComments) {
        this.cvComments = cvComments;
        return this;
    }

    public Map<String, String> getCvAnnotations() {
        return cvAnnotations;
    }

    public ClinicalVariantSearch setCvAnnotations(Map<String, String> cvAnnotations) {
        this.cvAnnotations = cvAnnotations;
        return this;
    }

    public Map<String, Float> getCvAnnotationScores() {
        return cvAnnotationScores;
    }

    public ClinicalVariantSearch setCvAnnotationScores(Map<String, Float> cvAnnotationScores) {
        this.cvAnnotationScores = cvAnnotationScores;
        return this;
    }

    public String getCvDiscussionAuthor() {
        return cvDiscussionAuthor;
    }

    public ClinicalVariantSearch setCvDiscussionAuthor(String cvDiscussionAuthor) {
        this.cvDiscussionAuthor = cvDiscussionAuthor;
        return this;
    }

    public String getCvDiscussionDate() {
        return cvDiscussionDate;
    }

    public ClinicalVariantSearch setCvDiscussionDate(String cvDiscussionDate) {
        this.cvDiscussionDate = cvDiscussionDate;
        return this;
    }

    public String getCvDiscussionText() {
        return cvDiscussionText;
    }

    public ClinicalVariantSearch setCvDiscussionText(String cvDiscussionText) {
        this.cvDiscussionText = cvDiscussionText;
        return this;
    }

    public String getCvConfidenceValue() {
        return cvConfidenceValue;
    }

    public ClinicalVariantSearch setCvConfidenceValue(String cvConfidenceValue) {
        this.cvConfidenceValue = cvConfidenceValue;
        return this;
    }

    public String getCvConfidenceAuthor() {
        return cvConfidenceAuthor;
    }

    public ClinicalVariantSearch setCvConfidenceAuthor(String cvConfidenceAuthor) {
        this.cvConfidenceAuthor = cvConfidenceAuthor;
        return this;
    }

    public String getCvConfidenceDate() {
        return cvConfidenceDate;
    }

    public ClinicalVariantSearch setCvConfidenceDate(String cvConfidenceDate) {
        this.cvConfidenceDate = cvConfidenceDate;
        return this;
    }

    public List<String> getCvTags() {
        return cvTags;
    }

    public ClinicalVariantSearch setCvTags(List<String> cvTags) {
        this.cvTags = cvTags;
        return this;
    }

    public String getCvStatus() {
        return cvStatus;
    }

    public ClinicalVariantSearch setCvStatus(String cvStatus) {
        this.cvStatus = cvStatus;
        return this;
    }

    public String getCvJson() {
        return cvJson;
    }

    public ClinicalVariantSearch setCvJson(String cvJson) {
        this.cvJson = cvJson;
        return this;
    }
}


