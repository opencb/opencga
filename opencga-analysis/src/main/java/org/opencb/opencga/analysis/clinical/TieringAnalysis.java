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

package org.opencb.opencga.analysis.clinical;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.AnalysisResult;
import org.opencb.opencga.analysis.OpenCgaAnalysis;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.clinical.ReportedEvent;
import org.opencb.opencga.core.models.clinical.ReportedVariant;

import java.util.List;

public class TieringAnalysis extends OpenCgaAnalysis<Interpretation> {

    public TieringAnalysis(String opencgaHome) {
        super(opencgaHome);
    }

    public TieringAnalysis(String opencgaHome, String id, ObjectMap config) {
        super(opencgaHome);
    }

    @Override
    public AnalysisResult<Interpretation> execute() throws Exception {
        // checks

        // set defaults

        // createInterpretation()

        // dominant() + recessive() ...

        // BAM coverage

        return null;
    }

    private List<ReportedVariant> dominant() {
        return null;
    }

    private List<ReportedVariant> recessive() {
        // MoI -> genotypes
        // Variant Query query -> (biotype, gene, genoptype)
        // Iterator for (Var) -> getReportedEvents(rv)
        // create RV
        return null;
    }


    private List<ReportedEvent> getReportedEvents(Variant variant) {
        return null;
    }

    private Interpretation createInterpretation() {
        return null;
    }
}
