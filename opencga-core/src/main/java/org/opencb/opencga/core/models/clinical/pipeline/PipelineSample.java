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

package org.opencb.opencga.core.models.clinical.pipeline;

import org.opencb.commons.annotations.DataField;

import java.util.List;

public class PipelineSample {

    @DataField(id = "id", description = "Sample identifier")
    private String id;

    @DataField(id = "role", description = "Sample role (father, mother, child, etc.)")
    private String role;

    @DataField(id = "somatic", description = "Whether this is a somatic sample")
    private boolean somatic;

    @DataField(id = "files", description = "List of input files for this sample")
    private List<String> files;

    public PipelineSample() {
    }

    public PipelineSample(String id, String role, boolean somatic, List<String> files) {
        this.id = id;
        this.role = role;
        this.somatic = somatic;
        this.files = files;
    }

    public String getId() { return id; }
    public PipelineSample setId(String id) { this.id = id; return this; }

    public String getRole() { return role; }
    public PipelineSample setRole(String role) { this.role = role; return this; }

    public boolean isSomatic() { return somatic; }
    public PipelineSample setSomatic(boolean somatic) { this.somatic = somatic; return this; }

    public List<String> getFiles() { return files; }
    public PipelineSample setFiles(List<String> files) { this.files = files; return this; }

    @Override
    public String toString() {
        return "PipelineSample{" +
                "id='" + id + '\'' +
                ", role='" + role + '\'' +
                ", somatic=" + somatic +
                ", files=" + files +
                '}';
    }
}