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

package org.opencb.opencga.core.models.variant;

/**
 * Basic set of VariantQueryParams, containing only the most used ones.
 */
public class BasicVariantQueryParams extends AbstractBasicVariantQueryParams {

    private String project;
    private String study;

    public String getProject() {
        return project;
    }

    public BasicVariantQueryParams setProject(String project) {
        this.project = project;
        return this;
    }

    public String getStudy() {
        return study;
    }

    public BasicVariantQueryParams setStudy(String study) {
        this.study = study;
        return this;
    }
}

