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
import org.opencb.opencga.core.models.IPrivateStudyUid;
import org.opencb.opencga.core.models.panel.Panel;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Interpretation extends org.opencb.biodata.models.clinical.interpretation.Interpretation implements IPrivateStudyUid {

    // Private fields
    private long studyUid;
    private long uid;

    private List<Panel> panels;

    private InterpretationStats stats;
    private InterpretationInternal internal;
    private int release;

    public Interpretation() {
        super();
    }

    public Interpretation(String id, String description, String clinicalAnalysisId, ClinicalAnalyst analyst,
                          List<InterpretationMethod> methods, String creationDate, String modificationDate,
                          List<ClinicalVariant> primaryFindings, List<ClinicalVariant> secondaryFindings, List<Panel> panels,
                          List<ClinicalComment> comments, Map<String, Object> attributes) {
        super(id, "", description, clinicalAnalysisId, analyst, methods, primaryFindings, secondaryFindings, comments, null,
                creationDate, modificationDate, 0, attributes);
        this.panels = panels;
    }

    public Interpretation(org.opencb.biodata.models.clinical.interpretation.Interpretation interpretation) {
        this(interpretation.getId(), interpretation.getDescription(), interpretation.getClinicalAnalysisId(), interpretation.getAnalyst(),
                interpretation.getMethods(), interpretation.getCreationDate(), interpretation.getModificationDate(),
                interpretation.getPrimaryFindings(), interpretation.getSecondaryFindings(), Collections.emptyList(),
                interpretation.getComments(), interpretation.getAttributes());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Interpretation{");
        sb.append("studyUid=").append(studyUid);
        sb.append(", uid=").append(uid);
        sb.append(", panels=").append(panels);
        sb.append(", stats=").append(stats);
        sb.append(", internal=").append(internal);
        sb.append(", release=").append(release);
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

    public InterpretationStats getStats() {
        return stats;
    }

    public Interpretation setStats(InterpretationStats stats) {
        this.stats = stats;
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
    public Interpretation setMethods(List<InterpretationMethod> methods) {
        super.setMethods(methods);
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

}
