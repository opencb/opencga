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

import org.opencb.biodata.models.clinical.Comment;
import org.opencb.biodata.models.clinical.interpretation.Analyst;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.InterpretationMethod;
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

    public Interpretation(String id, String description, String clinicalAnalysisId, Analyst analyst, List<InterpretationMethod> methods,
                          String creationDate, List<ClinicalVariant> primaryFindings, List<ClinicalVariant> secondaryFindings,
                          List<Comment> comments, Map<String, Object> attributes) {
        super(id, "", description, clinicalAnalysisId, analyst, methods, primaryFindings, secondaryFindings, comments, null, creationDate,
                0, attributes);
    }

    public Interpretation(org.opencb.biodata.models.clinical.interpretation.Interpretation interpretation) {
        this(interpretation.getId(), interpretation.getDescription(), interpretation.getClinicalAnalysisId(), interpretation.getAnalyst(),
                interpretation.getMethods(), interpretation.getCreationDate(), interpretation.getPrimaryFindings(),
                interpretation.getSecondaryFindings(), interpretation.getComments(), interpretation.getAttributes());
    }

//    public Interpretation(org.opencb.biodata.models.clinical.interpretation.Interpretation interpretation) {
//        this(interpretation.getId(), "", interpretation.getDescription(), interpretation.getClinicalAnalysisId(),
//                interpretation.getAnalyst(), interpretation.getMethod(),interpretation.getPrimaryFindings(),
//                interpretation.getSecondaryFindings(), interpretation.getComments(), interpretation.getStatus(),
//                interpretation.getCreationDate(), interpretation.getVersion(), interpretation.getAttributes());
//    }
//
//    public Interpretation(String id, String description, String clinicalAnalysisId, List<DiseasePanel> panels, Software software,
//                          Analyst analyst, List<Software> dependencies, Map<String, Object> filters, String creationDate,
//                          List<ClinicalVariant> primaryFindinds, List<ClinicalVariant> secondaryFindings,
//                          List<ReportedLowCoverage> reportedLowCoverages, List<Comment> comments, Map<String, Object> attributes) {
//        super(id, "", description, clinicalAnalysisId, software, analyst, dependencies, filters, panels, primaryFindinds, secondaryFindings,
//                reportedLowCoverages, comments, InterpretationStatus.NOT_REVIEWED, creationDate, 1, attributes);
//    }
//
//    public Interpretation(String uuid, String id, String description, String clinicalAnalysisId, List<DiseasePanel> panels, Software software,
//                          Analyst analyst, List<Software> dependencies, Map<String, Object> filters, String creationDate,
//                          List<ClinicalVariant> primaryFindinds, List<ClinicalVariant> secondaryFindings,
//                          List<ReportedLowCoverage> reportedLowCoverages, List<Comment> comments, Map<String, Object> attributes,
//                          InterpretationInternal internal) {
//        super(id, uuid, description, clinicalAnalysisId, software, analyst, dependencies, filters, panels, primaryFindinds,
//                secondaryFindings, reportedLowCoverages, comments, InterpretationStatus.NOT_REVIEWED, creationDate, 1, attributes);
//        this.internal = internal;
//    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Interpretation{");
        sb.append("studyUid=").append(studyUid);
        sb.append(", uid=").append(uid);
        sb.append(", internal=").append(internal);
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

    public InterpretationInternal getInternal() {
        return internal;
    }

    public Interpretation setInternal(InterpretationInternal internal) {
        this.internal = internal;
        return this;
    }
}
