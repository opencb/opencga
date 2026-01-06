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

package org.opencb.opencga.analysis.clinical.interpreter;

import org.opencb.opencga.analysis.clinical.interpreter.config.InterpreterConfiguration;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.Interpretation;

public class Interpreter {

    private InterpreterConfiguration configuration;

    public InterpreterConfiguration getConfiguration() {
        return configuration;
    }

    public Interpreter(InterpreterConfiguration configuration) {
        this.configuration = configuration;
    }



    public Interpretation interpret(ClinicalAnalysis clinicalAnalysis) {
        Interpretation interpretation = new Interpretation();
        // TODO Implement interpretation logic
        return new Interpretation();
    }
}
