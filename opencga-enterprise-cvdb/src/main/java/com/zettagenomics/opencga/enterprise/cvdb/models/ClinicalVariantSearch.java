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

package com.zettagenomics.opencga.enterprise.cvdb.models;

import org.apache.solr.client.solrj.beans.Field;
import org.opencb.opencga.storage.core.variant.search.VariantSearchModel;

import java.util.*;

public class ClinicalVariantSearch extends VariantSearchModel {

    // Catalog fields

    @Field("studyId")
    private String studyId;

    @Field("viewers")
    private List<String> viewers;

    // "Primary" and "foreign" keys

    @Field("caId")
    private String caId;

    @Field("ciId")
    private String ciId;

    // Clinical variant fields

    @Field("primaryFinding")
    private boolean primaryFinding;

    @Field("primaryInterpretation")
    private boolean primaryInterpretation;

	// Comments are stores: author -- message -- tag1:tag2:.. -- date
    @Field("comments")
    private List<String> comments;

	// Filters are stored in two dynamic fields: one for string values, the other one for numeric ones
    @Field("annotations")
    private Map<String, String> annotations;

    @Field("annotationScores")
    private Map<String, Float> annotationScores;

    @Field("discussionAuthor")
    private String discussionAuthor;

    @Field("discussionDate")
    private Date discussionDate;

    @Field("discussionText")
    private String discussionText;

    @Field("confidenceValue")
    private String confidenceValue;

    @Field("confidenceAuthor")
    private String confidenceAuthor;

    @Field("confidenceDate")
    private Date confidenceDate;

    @Field("tags")
    private List<String> tags;

    @Field("status")
    private String status;

    // Clinical variant stored in a JSON string
    @Field("json")
    private String json;

    public ClinicalVariantSearch() {
        init();
    }

    public ClinicalVariantSearch(VariantSearchModel variantSearchModel) {
        super(variantSearchModel);
        init();
    }

    private void init() {
        viewers = new ArrayList<>();
        comments = new ArrayList<>();
        annotations = new HashMap<>();
        annotationScores = new HashMap<>();
        tags = new ArrayList<>();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalVariantSearch{");
        sb.append("studyId='").append(studyId).append('\'');
        sb.append(", viewers=").append(viewers);
        sb.append(", caId='").append(caId).append('\'');
        sb.append(", ciId='").append(ciId).append('\'');
        sb.append(", primaryFinding=").append(primaryFinding);
        sb.append(", primaryInterpretation=").append(primaryInterpretation);
        sb.append(", comments=").append(comments);
        sb.append(", annotations=").append(annotations);
        sb.append(", annotationScores=").append(annotationScores);
        sb.append(", discussionAuthor='").append(discussionAuthor).append('\'');
        sb.append(", discussionDate=").append(discussionDate);
        sb.append(", discussionText='").append(discussionText).append('\'');
        sb.append(", confidenceValue='").append(confidenceValue).append('\'');
        sb.append(", confidenceAuthor='").append(confidenceAuthor).append('\'');
        sb.append(", confidenceDate=").append(confidenceDate);
        sb.append(", tags=").append(tags);
        sb.append(", status='").append(status).append('\'');
        sb.append(", json='").append(json).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getStudyId() {
        return studyId;
    }

    public ClinicalVariantSearch setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public List<String> getViewers() {
        return viewers;
    }

    public ClinicalVariantSearch setViewers(List<String> viewers) {
        this.viewers = viewers;
        return this;
    }

    public String getCaId() {
        return caId;
    }

    public ClinicalVariantSearch setCaId(String caId) {
        this.caId = caId;
        return this;
    }

    public String getCiId() {
        return ciId;
    }

    public ClinicalVariantSearch setCiId(String ciId) {
        this.ciId = ciId;
        return this;
    }

    public boolean isPrimaryFinding() {
        return primaryFinding;
    }

    public ClinicalVariantSearch setPrimaryFinding(boolean primaryFinding) {
        this.primaryFinding = primaryFinding;
        return this;
    }

    public boolean isPrimaryInterpretation() {
        return primaryInterpretation;
    }

    public ClinicalVariantSearch setPrimaryInterpretation(boolean primaryInterpretation) {
        this.primaryInterpretation = primaryInterpretation;
        return this;
    }

    public List<String> getComments() {
        return comments;
    }

    public ClinicalVariantSearch setComments(List<String> comments) {
        this.comments = comments;
        return this;
    }

    public Map<String, String> getAnnotations() {
        return annotations;
    }

    public ClinicalVariantSearch setAnnotations(Map<String, String> annotations) {
        this.annotations = annotations;
        return this;
    }

    public Map<String, Float> getAnnotationScores() {
        return annotationScores;
    }

    public ClinicalVariantSearch setAnnotationScores(Map<String, Float> annotationScores) {
        this.annotationScores = annotationScores;
        return this;
    }

    public String getDiscussionAuthor() {
        return discussionAuthor;
    }

    public ClinicalVariantSearch setDiscussionAuthor(String discussionAuthor) {
        this.discussionAuthor = discussionAuthor;
        return this;
    }

    public Date getDiscussionDate() {
        return discussionDate;
    }

    public ClinicalVariantSearch setDiscussionDate(Date discussionDate) {
        this.discussionDate = discussionDate;
        return this;
    }

    public String getDiscussionText() {
        return discussionText;
    }

    public ClinicalVariantSearch setDiscussionText(String discussionText) {
        this.discussionText = discussionText;
        return this;
    }

    public String getConfidenceValue() {
        return confidenceValue;
    }

    public ClinicalVariantSearch setConfidenceValue(String confidenceValue) {
        this.confidenceValue = confidenceValue;
        return this;
    }

    public String getConfidenceAuthor() {
        return confidenceAuthor;
    }

    public ClinicalVariantSearch setConfidenceAuthor(String confidenceAuthor) {
        this.confidenceAuthor = confidenceAuthor;
        return this;
    }

    public Date getConfidenceDate() {
        return confidenceDate;
    }

    public ClinicalVariantSearch setConfidenceDate(Date confidenceDate) {
        this.confidenceDate = confidenceDate;
        return this;
    }

    public List<String> getTags() {
        return tags;
    }

    public ClinicalVariantSearch setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public String getStatus() {
        return status;
    }

    public ClinicalVariantSearch setStatus(String status) {
        this.status = status;
        return this;
    }

    public String getJson() {
        return json;
    }

    public ClinicalVariantSearch setJson(String json) {
        this.json = json;
        return this;
    }
}


