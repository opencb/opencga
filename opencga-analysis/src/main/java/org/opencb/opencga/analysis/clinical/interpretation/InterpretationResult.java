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

import org.opencb.biodata.models.clinical.interpretation.Interpretation;
import org.opencb.opencga.analysis.AnalysisResult;

import java.util.Map;

public class InterpretationResult extends AnalysisResult<Interpretation> {

    private int dbTime;
    private int numResults;
    private long numTotalResults;
    private String warningMsg;
    private String errorMsg;

    public InterpretationResult(Interpretation result) {
        super(result);
    }

    public InterpretationResult(Interpretation result, int time, Map<String, Object> attributes, int dbTime, int numResults, long numTotalResults,
                                String warningMsg, String errorMsg) {
        super(result, time, attributes);

        this.dbTime = dbTime;
        this.numResults = numResults;
        this.numTotalResults = numTotalResults;
        this.warningMsg = warningMsg;
        this.errorMsg = errorMsg;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InterpretationResult{");
        sb.append("dbTime=").append(dbTime);
        sb.append(", numResults=").append(numResults);
        sb.append(", numTotalResults=").append(numTotalResults);
        sb.append(", warningMsg='").append(warningMsg).append('\'');
        sb.append(", errorMsg='").append(errorMsg).append('\'');
        sb.append(", result=").append(result);
        sb.append(", time=").append(time);
        sb.append(", status='").append(status).append('\'');
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public int getDbTime() {
        return dbTime;
    }

    public InterpretationResult setDbTime(int dbTime) {
        this.dbTime = dbTime;
        return this;
    }

    public int getNumResults() {
        return numResults;
    }

    public InterpretationResult setNumResults(int numResults) {
        this.numResults = numResults;
        return this;
    }

    public long getNumTotalResults() {
        return numTotalResults;
    }

    public InterpretationResult setNumTotalResults(long numTotalResults) {
        this.numTotalResults = numTotalResults;
        return this;
    }

    public String getWarningMsg() {
        return warningMsg;
    }

    public InterpretationResult setWarningMsg(String warningMsg) {
        this.warningMsg = warningMsg;
        return this;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public InterpretationResult setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
        return this;
    }
}