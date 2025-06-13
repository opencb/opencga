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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClinicalVariantEvidenceSearch {

    // Catalog fields

    @Field("studyId")
    private String studyId;

    // "Primary" and "foreign" keys

    @Field("id")
    private String id;

    @Field("caId")
    private String caId;

    @Field("ciId")
    private String ciId;

    @Field("cvId")
    private String cvId;

    @Field("variantId")
    private String variantId;

    // Clinical variant evidence fields

    @Field("primaryFinding")
    private boolean primaryFinding;

    @Field("primaryInterpretation")
    private boolean primaryInterpretation;

    @Field("phenotypeNames")
    private List<String> phenotypeNames;

    @Field("geneName")
    private String geneName;

    @Field("transcriptId")
    private String transcriptId;

    @Field("soTermNames")
    private List<String> soTermNames;

    @Field("xrefIds")
    private List<String> xrefIds;

    @Field("panelId")
    private String panelId;

    @Field("mois")
    private List<String> mois;

    @Field("penetrance")
    private String penetrance;

    @Field("acmgs")
    private List<String> acmgs;

    @Field("tier")
    private String tier;

    @Field("clinicalSignificance")
    private String clinicalSignificance;

    @Field("drugResponse")
    private String drugResponse;

    @Field("traitAssociation")
    private String traitAssociation;

    @Field("functionalEffect")
    private String functionalEffect;

    @Field("tumorigenesis")
    private String tumorigenesis;

    @Field("otherClassifications")
    private List<String> otherClassifications;

    @Field("rolesInCancer")
    private List<String> rolesInCancer;

    @Field("reviewAcmgs")
    private List<String> reviewAcmgs;

    @Field("reviewTier")
    private String reviewTier;

    @Field("reviewClinicalSignificance")
    private String reviewClinicalSignificance;

    @Field("reviewText")
    private String reviewText;

    @Field("score_*")
    private Map<String, Double> scores;

    // Clinical variant evidence stored in a JSON string
    @Field("json")
    private String json;

    public ClinicalVariantEvidenceSearch() {
        phenotypeNames = new ArrayList<>();
        soTermNames = new ArrayList<>();
        xrefIds = new ArrayList<>();
        mois = new ArrayList<>();
        acmgs = new ArrayList<>();
        otherClassifications = new ArrayList<>();
        rolesInCancer = new ArrayList<>();
        reviewAcmgs = new ArrayList<>();
        scores = new HashMap<>();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalVariantEvidenceSearch{");
        sb.append("studyId='").append(studyId).append('\'');
        sb.append(", id='").append(id).append('\'');
        sb.append(", caId='").append(caId).append('\'');
        sb.append(", ciId='").append(ciId).append('\'');
        sb.append(", cvId='").append(cvId).append('\'');
        sb.append(", variantId='").append(variantId).append('\'');
        sb.append(", primaryFinding=").append(primaryFinding);
        sb.append(", primaryInterpretation=").append(primaryInterpretation);
        sb.append(", phenotypeNames=").append(phenotypeNames);
        sb.append(", geneName='").append(geneName).append('\'');
        sb.append(", transcriptId='").append(transcriptId).append('\'');
        sb.append(", soTermNames=").append(soTermNames);
        sb.append(", xrefIds=").append(xrefIds);
        sb.append(", panelId='").append(panelId).append('\'');
        sb.append(", mois=").append(mois);
        sb.append(", penetrance='").append(penetrance).append('\'');
        sb.append(", acmgs=").append(acmgs);
        sb.append(", tier='").append(tier).append('\'');
        sb.append(", clinicalSignificance='").append(clinicalSignificance).append('\'');
        sb.append(", drugResponse='").append(drugResponse).append('\'');
        sb.append(", traitAssociation='").append(traitAssociation).append('\'');
        sb.append(", functionalEffect='").append(functionalEffect).append('\'');
        sb.append(", tumorigenesis='").append(tumorigenesis).append('\'');
        sb.append(", otherClassifications=").append(otherClassifications);
        sb.append(", rolesInCancer=").append(rolesInCancer);
        sb.append(", reviewAcmgs=").append(reviewAcmgs);
        sb.append(", reviewTier='").append(reviewTier).append('\'');
        sb.append(", reviewClinicalSignificance='").append(reviewClinicalSignificance).append('\'');
        sb.append(", reviewText='").append(reviewText).append('\'');
        sb.append(", scores=").append(scores);
        sb.append(", json='").append(json).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getStudyId() {
        return studyId;
    }

    public ClinicalVariantEvidenceSearch setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public String getId() {
        return id;
    }

    public ClinicalVariantEvidenceSearch setId(String id) {
        this.id = id;
        return this;
    }

    public String getCaId() {
        return caId;
    }

    public ClinicalVariantEvidenceSearch setCaId(String caId) {
        this.caId = caId;
        return this;
    }

    public String getCiId() {
        return ciId;
    }

    public ClinicalVariantEvidenceSearch setCiId(String ciId) {
        this.ciId = ciId;
        return this;
    }

    public String getCvId() {
        return cvId;
    }

    public ClinicalVariantEvidenceSearch setCvId(String cvId) {
        this.cvId = cvId;
        return this;
    }

    public String getVariantId() {
        return variantId;
    }

    public ClinicalVariantEvidenceSearch setVariantId(String variantId) {
        this.variantId = variantId;
        return this;
    }

    public boolean isPrimaryFinding() {
        return primaryFinding;
    }

    public ClinicalVariantEvidenceSearch setPrimaryFinding(boolean primaryFinding) {
        this.primaryFinding = primaryFinding;
        return this;
    }

    public boolean isPrimaryInterpretation() {
        return primaryInterpretation;
    }

    public ClinicalVariantEvidenceSearch setPrimaryInterpretation(boolean primaryInterpretation) {
        this.primaryInterpretation = primaryInterpretation;
        return this;
    }

    public List<String> getPhenotypeNames() {
        return phenotypeNames;
    }

    public ClinicalVariantEvidenceSearch setPhenotypeNames(List<String> phenotypeNames) {
        this.phenotypeNames = phenotypeNames;
        return this;
    }

    public String getGeneName() {
        return geneName;
    }

    public ClinicalVariantEvidenceSearch setGeneName(String geneName) {
        this.geneName = geneName;
        return this;
    }

    public String getTranscriptId() {
        return transcriptId;
    }

    public ClinicalVariantEvidenceSearch setTranscriptId(String transcriptId) {
        this.transcriptId = transcriptId;
        return this;
    }

    public List<String> getSoTermNames() {
        return soTermNames;
    }

    public ClinicalVariantEvidenceSearch setSoTermNames(List<String> soTermNames) {
        this.soTermNames = soTermNames;
        return this;
    }

    public List<String> getXrefIds() {
        return xrefIds;
    }

    public ClinicalVariantEvidenceSearch setXrefIds(List<String> xrefIds) {
        this.xrefIds = xrefIds;
        return this;
    }

    public String getPanelId() {
        return panelId;
    }

    public ClinicalVariantEvidenceSearch setPanelId(String panelId) {
        this.panelId = panelId;
        return this;
    }

    public List<String> getMois() {
        return mois;
    }

    public ClinicalVariantEvidenceSearch setMois(List<String> mois) {
        this.mois = mois;
        return this;
    }

    public String getPenetrance() {
        return penetrance;
    }

    public ClinicalVariantEvidenceSearch setPenetrance(String penetrance) {
        this.penetrance = penetrance;
        return this;
    }

    public List<String> getAcmgs() {
        return acmgs;
    }

    public ClinicalVariantEvidenceSearch setAcmgs(List<String> acmgs) {
        this.acmgs = acmgs;
        return this;
    }

    public String getTier() {
        return tier;
    }

    public ClinicalVariantEvidenceSearch setTier(String tier) {
        this.tier = tier;
        return this;
    }

    public String getClinicalSignificance() {
        return clinicalSignificance;
    }

    public ClinicalVariantEvidenceSearch setClinicalSignificance(String clinicalSignificance) {
        this.clinicalSignificance = clinicalSignificance;
        return this;
    }

    public String getDrugResponse() {
        return drugResponse;
    }

    public ClinicalVariantEvidenceSearch setDrugResponse(String drugResponse) {
        this.drugResponse = drugResponse;
        return this;
    }

    public String getTraitAssociation() {
        return traitAssociation;
    }

    public ClinicalVariantEvidenceSearch setTraitAssociation(String traitAssociation) {
        this.traitAssociation = traitAssociation;
        return this;
    }

    public String getFunctionalEffect() {
        return functionalEffect;
    }

    public ClinicalVariantEvidenceSearch setFunctionalEffect(String functionalEffect) {
        this.functionalEffect = functionalEffect;
        return this;
    }

    public String getTumorigenesis() {
        return tumorigenesis;
    }

    public ClinicalVariantEvidenceSearch setTumorigenesis(String tumorigenesis) {
        this.tumorigenesis = tumorigenesis;
        return this;
    }

    public List<String> getOtherClassifications() {
        return otherClassifications;
    }

    public ClinicalVariantEvidenceSearch setOtherClassifications(List<String> otherClassifications) {
        this.otherClassifications = otherClassifications;
        return this;
    }

    public List<String> getRolesInCancer() {
        return rolesInCancer;
    }

    public ClinicalVariantEvidenceSearch setRolesInCancer(List<String> rolesInCancer) {
        this.rolesInCancer = rolesInCancer;
        return this;
    }

    public List<String> getReviewAcmgs() {
        return reviewAcmgs;
    }

    public ClinicalVariantEvidenceSearch setReviewAcmgs(List<String> reviewAcmgs) {
        this.reviewAcmgs = reviewAcmgs;
        return this;
    }

    public String getReviewTier() {
        return reviewTier;
    }

    public ClinicalVariantEvidenceSearch setReviewTier(String reviewTier) {
        this.reviewTier = reviewTier;
        return this;
    }

    public String getReviewClinicalSignificance() {
        return reviewClinicalSignificance;
    }

    public ClinicalVariantEvidenceSearch setReviewClinicalSignificance(String reviewClinicalSignificance) {
        this.reviewClinicalSignificance = reviewClinicalSignificance;
        return this;
    }

    public String getReviewText() {
        return reviewText;
    }

    public ClinicalVariantEvidenceSearch setReviewText(String reviewText) {
        this.reviewText = reviewText;
        return this;
    }

    public Map<String, Double> getScores() {
        return scores;
    }

    public ClinicalVariantEvidenceSearch setScores(Map<String, Double> scores) {
        this.scores = scores;
        return this;
    }

    public String getJson() {
        return json;
    }

    public ClinicalVariantEvidenceSearch setJson(String json) {
        this.json = json;
        return this;
    }
}


