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

package org.opencb.opencga.analysis;

import java.util.Map;

public class AnalysisResult<T> {

    protected T result;

    protected int time;
    protected String status;
    protected Map<String, Object> attributes;

    public AnalysisResult(T result) {
        this.result = result;
    }

    public AnalysisResult(T result, int time, Map<String, Object> attributes) {
        this.result = result;
        this.time = time;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AnalysisResult{");
        sb.append("result=").append(result);
        sb.append(", time=").append(time);
        sb.append(", status='").append(status).append('\'');
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public int getTime() {
        return time;
    }

    public AnalysisResult<T> setTime(int time) {
        this.time = time;
        return this;
    }

    public String getStatus() {
        return status;
    }

    public AnalysisResult<T> setStatus(String status) {
        this.status = status;
        return this;
    }

    public T getResult() {
        return result;
    }

    public AnalysisResult<T> setResult(T result) {
        this.result = result;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public AnalysisResult<T> setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
