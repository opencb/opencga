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

    // Catalog fields

    @Field("studyId")
    private String studyId;

    @Field("studyJson")
    private String studyJson;

    // "Primary" and "foreign" keys

//    @Field("id")
//    private String id;

    @Field("caId")
    private String caId;

    @Field("ciId")
    private String ciId;

    // Clinical variant fields

    @Field("primary")
    private boolean primary;

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
    private String discussionDate;

    @Field("discussionText")
    private String discussionText;

    @Field("confidenceValue")
    private String confidenceValue;

    @Field("confidenceAuthor")
    private String confidenceAuthor;

    @Field("confidenceDate")
    private String confidenceDate;

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
        comments = new ArrayList<>();
        annotations = new HashMap<>();
        annotationScores = new HashMap<>();
        tags = new ArrayList<>();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalVariantSearch{");
        sb.append("studyId='").append(studyId).append('\'');
        sb.append(", studyJson='").append(studyJson).append('\'');
        sb.append(", caId='").append(caId).append('\'');
        sb.append(", ciId='").append(ciId).append('\'');
        sb.append(", primary=").append(primary);
        sb.append(", comments=").append(comments);
        sb.append(", annotations=").append(annotations);
        sb.append(", annotationScores=").append(annotationScores);
        sb.append(", discussionAuthor='").append(discussionAuthor).append('\'');
        sb.append(", discussionDate='").append(discussionDate).append('\'');
        sb.append(", discussionText='").append(discussionText).append('\'');
        sb.append(", confidenceValue='").append(confidenceValue).append('\'');
        sb.append(", confidenceAuthor='").append(confidenceAuthor).append('\'');
        sb.append(", confidenceDate='").append(confidenceDate).append('\'');
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

    public String getStudyJson() {
        return studyJson;
    }

    public ClinicalVariantSearch setStudyJson(String studyJson) {
        this.studyJson = studyJson;
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

    public boolean isPrimary() {
        return primary;
    }

    public ClinicalVariantSearch setPrimary(boolean primary) {
        this.primary = primary;
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

    public String getDiscussionDate() {
        return discussionDate;
    }

    public ClinicalVariantSearch setDiscussionDate(String discussionDate) {
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

    public String getConfidenceDate() {
        return confidenceDate;
    }

    public ClinicalVariantSearch setConfidenceDate(String confidenceDate) {
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


