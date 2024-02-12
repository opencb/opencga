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

package org.opencb.opencga.catalog.models;

import java.util.HashMap;
import java.util.Map;

public class ClinicalAnalysisLoadResult {
    private int numLoaded;
    private Map<String, String> failures;
    private String filename;
    private int time;

    public ClinicalAnalysisLoadResult() {
        this.numLoaded = 0;
        failures = new HashMap<>();
    }

    public ClinicalAnalysisLoadResult(int numLoaded, Map<String, String> failures, String filename, int time) {
        this.numLoaded = numLoaded;
        this.failures = failures;
        this.filename = filename;
        this.time = time;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalAnalysisLoadResult{");
        sb.append("numLoaded=").append(numLoaded);
        sb.append(", failures=").append(failures);
        sb.append(", filename='").append(filename).append('\'');
        sb.append(", time='").append(time).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public int getNumLoaded() {
        return numLoaded;
    }

    public ClinicalAnalysisLoadResult setNumLoaded(int numLoaded) {
        this.numLoaded = numLoaded;
        return this;
    }

    public Map<String, String> getFailures() {
        return failures;
    }

    public ClinicalAnalysisLoadResult setFailures(Map<String, String> failures) {
        this.failures = failures;
        return this;
    }

    public String getFilename() {
        return filename;
    }

    public ClinicalAnalysisLoadResult setFilename(String filename) {
        this.filename = filename;
        return this;
    }

    public int getTime() {
        return time;
    }

    public ClinicalAnalysisLoadResult setTime(int time) {
        this.time = time;
        return this;
    }
}
