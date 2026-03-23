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

package org.opencb.opencga.clinical.cvdb.models;

import java.util.HashMap;
import java.util.Map;

public class CvdbIndexResult {
    private int numIndexed;
    private Map<String, String> failures;
    private int time;

    public CvdbIndexResult() {
        this.numIndexed = 0;
        failures = new HashMap<>();
    }

    public CvdbIndexResult(int numIndexed, Map<String, String> failures, int time) {
        this.numIndexed = numIndexed;
        this.failures = failures;
        this.time = time;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CvdbIndexResult{");
        sb.append("numIndexed=").append(numIndexed);
        sb.append(", failures=").append(failures);
        sb.append(", time='").append(time).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public int getNumIndexed() {
        return numIndexed;
    }

    public CvdbIndexResult setNumIndexed(int numIndexed) {
        this.numIndexed = numIndexed;
        return this;
    }

    public Map<String, String> getFailures() {
        return failures;
    }

    public CvdbIndexResult setFailures(Map<String, String> failures) {
        this.failures = failures;
        return this;
    }

    public int getTime() {
        return time;
    }

    public CvdbIndexResult setTime(int time) {
        this.time = time;
        return this;
    }
}
