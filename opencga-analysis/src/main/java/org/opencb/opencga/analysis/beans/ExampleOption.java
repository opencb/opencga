/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.analysis.beans;


public class ExampleOption {
    private String paramName, value;

    public ExampleOption() {

    }

    public ExampleOption(String executionId, String value) {
        this.paramName = executionId;
        this.value = value;
    }

    @Override
    public String toString() {
        return "ExampleOption{" +
                "paramName='" + paramName + '\'' +
                ", defaultValue='" + value + '\'' +
                '}';
    }

    public String getParamName() {
        return paramName;
    }

    public void setParamName(String paramName) {
        this.paramName = paramName;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
