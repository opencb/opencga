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

import org.opencb.biodata.models.clinical.ClinicalAnalyst;
import org.opencb.biodata.models.clinical.ClinicalComment;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.InterpretationMethod;
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.models.IPrivateStudyUid;
import org.opencb.opencga.core.models.panel.Panel;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Interpretation extends org.opencb.biodata.models.clinical.interpretation.Interpretation implements IPrivateStudyUid {

    // Private fields

    @DataField(id = "studyUid", indexed = true,
            description = FieldConstants.INTERPRETATION_STUDY_UID)
    private long studyUid;

    @DataField(id = "uid", indexed = true,
            description = FieldConstants.INTERPRETATION_UID)
    private long uid;

    @DataField(id = "panels", indexed = true,
            description = FieldConstants.INTERPRETATION_PANELS)
    private List<Panel> panels;

    @DataField(id = "internal", indexed = true,
            description = FieldConstants.GENERIC_INTERNAL)
    private InterpretationInternal internal;

    @DataField(id = "release", indexed = true,
            description = FieldConstants.GENERIC_RELEASE_DESCRIPTION)
    private int release;

    @DataField(id = "status", indexed = true,
            description = FieldConstants.GENERIC_STATUS_DESCRIPTION)
    private ClinicalStatus status;

    public Interpretation() {
        super();
    }

    public Interpretation(String id, String name, String description, String clinicalAnalysisId, ClinicalAnalyst analyst,
                          InterpretationMethod method, String creationDate, String modificationDate, boolean locked,
                          List<ClinicalVariant> primaryFindings, List<ClinicalVariant> secondaryFindings, List<Panel> panels,
                          List<ClinicalComment> comments, ClinicalStatus status, Map<String, Object> attributes) {
        super(id, "", name, description, clinicalAnalysisId, analyst, method, primaryFindings, secondaryFindings, comments, null, locked,
                creationDate, modificationDate, 0, attributes);
        this.status = status;
        this.panels = panels;
    }

    public Interpretation(org.opencb.biodata.models.clinical.interpretation.Interpretation interpretation) {
        this(interpretation.getId(), interpretation.getName(), interpretation.getDescription(), interpretation.getClinicalAnalysisId(),
                interpretation.getAnalyst(), interpretation.getMethod(), interpretation.getCreationDate(),
                interpretation.getModificationDate(), interpretation.isLocked(), interpretation.getPrimaryFindings(),
                interpretation.getSecondaryFindings(), Collections.emptyList(), interpretation.getComments(), null,
                interpretation.getAttributes());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Interpretation{");
        sb.append("id='").append(id).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", studyUid=").append(studyUid);
        sb.append(", uid=").append(uid);
        sb.append(", panels=").append(panels);
        sb.append(", internal=").append(internal);
        sb.append(", release=").append(release);
        sb.append(", status=").append(status);
        sb.append(", clinicalAnalysisId='").append(clinicalAnalysisId).append('\'');
        sb.append(", analyst=").append(analyst);
        sb.append(", method=").append(method);
        sb.append(", primaryFindings=").append(primaryFindings);
        sb.append(", secondaryFindings=").append(secondaryFindings);
        sb.append(", comments=").append(comments);
        sb.append(", stats=").append(stats);
        sb.append(", locked=").append(locked);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", version=").append(version);
        sb.append('}');
        return sb.toString();
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

    @Override
    public Interpretation setId(String id) {
        super.setId(id);
        return this;
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

    public List<Panel> getPanels() {
        return panels;
    }

    public Interpretation setPanels(List<Panel> panels) {
        this.panels = panels;
        return this;
    }

    public InterpretationInternal getInternal() {
        return internal;
    }

    public Interpretation setInternal(InterpretationInternal internal) {
        this.internal = internal;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public Interpretation setRelease(int release) {
        this.release = release;
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
    public Interpretation setAnalyst(ClinicalAnalyst analyst) {
        super.setAnalyst(analyst);
        return this;
    }

    @Override
    public Interpretation setMethod(InterpretationMethod method) {
        super.setMethod(method);
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
    public Interpretation setComments(List<ClinicalComment> comments) {
        super.setComments(comments);
        return this;
    }

    @Override
    public Interpretation setLocked(boolean locked) {
        super.setLocked(locked);
        return this;
    }

    public ClinicalStatus getStatus() {
        return status;
    }

    public Interpretation setStatus(ClinicalStatus status) {
        this.status = status;
        return this;
    }
}
