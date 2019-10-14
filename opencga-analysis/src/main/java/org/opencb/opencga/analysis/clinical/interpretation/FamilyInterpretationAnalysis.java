/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.analysis.clinical.interpretation;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.List;

public abstract class FamilyInterpretationAnalysis extends InterpretationAnalysis {

    protected List<String> diseasePanelIds;

    @Deprecated
    protected final static String SEPARATOR = "__";

    public FamilyInterpretationAnalysis(String clinicalAnalysisId, String studyId, List<String> diseasePanelIds, ObjectMap options,
                                        String opencgaHome, String sessionId) {
        super(clinicalAnalysisId, studyId, options, opencgaHome, sessionId);
        this.diseasePanelIds = diseasePanelIds;
    }
}
