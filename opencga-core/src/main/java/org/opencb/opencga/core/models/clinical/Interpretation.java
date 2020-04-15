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

package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.clinical.interpretation.*;
import org.opencb.opencga.core.models.IPrivateStudyUid;

import java.util.List;
import java.util.Map;

public class Interpretation extends org.opencb.biodata.models.clinical.interpretation.Interpretation implements IPrivateStudyUid {

    // Private fields
    private long studyUid;
    private long uid;

    private InterpretationInternal internal;

    public Interpretation() {
        super();
    }

    public Interpretation(org.opencb.biodata.models.clinical.interpretation.Interpretation interpretation) {
        this(interpretation.getId(), interpretation.getDescription(), interpretation.getClinicalAnalysisId(), interpretation.getPanels(),
                interpretation.getSoftware(), interpretation.getAnalyst(), interpretation.getDependencies(), interpretation.getFilters(),
                interpretation.getCreationDate(), interpretation.getPrimaryFindings(), interpretation.getSecondaryFindings(),
                interpretation.getLowCoverageRegions(), interpretation.getComments(), interpretation.getAttributes());
    }

    public Interpretation(String id, String description, String clinicalAnalysisId, List<DiseasePanel> panels, Software software,
                          Analyst analyst, List<Software> dependencies, Map<String, Object> filters, String creationDate,
                          List<ClinicalVariant> primaryFindinds, List<ClinicalVariant> secondaryFindings,
                          List<ReportedLowCoverage> reportedLowCoverages, List<Comment> comments, Map<String, Object> attributes) {
        super(id, "", description, clinicalAnalysisId, software, analyst, dependencies, filters, panels, primaryFindinds, secondaryFindings,
                reportedLowCoverages, comments, InterpretationStatus.NOT_REVIEWED, creationDate, 1, attributes);
    }

    public Interpretation(String uuid, String id, String description, String clinicalAnalysisId, List<DiseasePanel> panels, Software software,
                          Analyst analyst, List<Software> dependencies, Map<String, Object> filters, String creationDate,
                          List<ClinicalVariant> primaryFindinds, List<ClinicalVariant> secondaryFindings,
                          List<ReportedLowCoverage> reportedLowCoverages, List<Comment> comments, Map<String, Object> attributes,
                          InterpretationInternal internal) {
        super(id, uuid, description, clinicalAnalysisId, software, analyst, dependencies, filters, panels, primaryFindinds,
                secondaryFindings, reportedLowCoverages, comments, InterpretationStatus.NOT_REVIEWED, creationDate, 1, attributes);
        this.internal = internal;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Interpretation{");
        sb.append("id='").append(this.getId()).append('\'');
        sb.append(", uuid='").append(this.getUuid()).append('\'');
        sb.append(", description='").append(this.getDescription()).append('\'');
        sb.append(", clinicalAnalysisId='").append(this.getClinicalAnalysisId()).append('\'');
        sb.append(", software=").append(this.getSoftware()).append('\'');
        sb.append(", analyst=").append(this.getAnalyst()).append('\'');
        sb.append(", dependencies=").append(this.getDependencies()).append('\'');
        sb.append(", filters=").append(this.getFilters()).append('\'');
        sb.append(", panels=").append(this.getPanels()).append('\'');
        sb.append(", primaryFindings=").append(this.getPrimaryFindings()).append('\'');
        sb.append(", secondaryFindings=").append(this.getSecondaryFindings()).append('\'');
        sb.append(", lowCoverageRegions=").append(this.getLowCoverageRegions()).append('\'');
        sb.append(", comments=").append(this.getComments()).append('\'');
        sb.append(", status=").append(this.getStatus()).append('\'');
        sb.append(", creationDate='").append(this.getCreationDate()).append('\'');
        sb.append(", version=").append(this.getVersion()).append('\'');
        sb.append(", attributes=").append(this.getAttributes()).append('\'');
        sb.append(", internal=").append(internal);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public Interpretation setUid(long uid) {
        this.uid = uid;
        return this;
    }

    @Override
    public long getStudyUid() {
        return studyUid;
    }

    @Override
    public Interpretation setStudyUid(long studyUid) {
        this.studyUid = studyUid;
        return this;
    }


    // Biodata interpretation setters

    @Override
    public Interpretation setId(String id) {
        super.setId(id);
        return this;
    }

    @Override
    public Interpretation setDescription(String description) {
        super.setDescription(description);
        return this;
    }

    @Override
    public Interpretation setClinicalAnalysisId(String clinicalAnalysisId) {
        super.setClinicalAnalysisId(clinicalAnalysisId);
        return this;
    }

    @Override
    public Interpretation setSoftware(Software software) {
        super.setSoftware(software);
        return this;
    }

    @Override
    public Interpretation setAnalyst(Analyst analyst) {
        super.setAnalyst(analyst);
        return this;
    }

    @Override
    public Interpretation setDependencies(List<Software> dependencies) {
        super.setDependencies(dependencies);
        return this;
    }

    @Override
    public Interpretation setFilters(Map<String, Object> filters) {
        super.setFilters(filters);
        return this;
    }

    @Override
    public Interpretation setPanels(List<DiseasePanel> panels) {
        super.setPanels(panels);
        return this;
    }

    @Override
    public Interpretation setPrimaryFindings(List<ClinicalVariant> primaryFindings) {
        super.setPrimaryFindings(primaryFindings);
        return this;
    }

    @Override
    public Interpretation setSecondaryFindings(List<ClinicalVariant> secondaryFindings) {
        super.setSecondaryFindings(secondaryFindings);
        return this;
    }

    @Override
    public Interpretation setLowCoverageRegions(List<ReportedLowCoverage> lowCoverageRegions) {
        super.setLowCoverageRegions(lowCoverageRegions);
        return this;
    }

    @Override
    public Interpretation setComments(List<Comment> comments) {
        super.setComments(comments);
        return this;
    }

    @Override
    public Interpretation setStatus(String status) {
        super.setStatus(status);
        return this;
    }

    @Override
    public Interpretation setCreationDate(String creationDate) {
        super.setCreationDate(creationDate);
        return this;
    }

    @Override
    public Interpretation setVersion(int version) {
        super.setVersion(version);
        return this;
    }

    @Override
    public Interpretation setAttributes(Map<String, Object> attributes) {
        super.setAttributes(attributes);
        return this;
    }

    public InterpretationInternal getInternal() {
        return internal;
    }

    public Interpretation setInternal(InterpretationInternal internal) {
        this.internal = internal;
        return this;
    }
}
