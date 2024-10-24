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

package org.opencb.opencga.core.models.operations.variant;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class VariantFileDeleteParams extends ToolParams {

    public static final String DESCRIPTION = "Variant delete file params";

    public VariantFileDeleteParams() {
    }

    public VariantFileDeleteParams(List<String> file, boolean resume) {
        this.file = file;
        this.resume = resume;
    }

    @DataField(description = "List of file ids to delete. Use 'all' to remove the whole study", required = true)
    private List<String> file;

    @DataField(description = "Resume failed delete operation.", defaultValue = "false")
    private boolean resume;

    @DataField(description = "Force delete operation. This would allow deleting partially loaded files.", defaultValue = "false")
    private boolean force;

    public List<String> getFile() {
        return file;
    }

    public VariantFileDeleteParams setFile(List<String> file) {
        this.file = file;
        return this;
    }

    public boolean isResume() {
        return resume;
    }

    public VariantFileDeleteParams setResume(boolean resume) {
        this.resume = resume;
        return this;
    }

    public boolean isForce() {
        return force;
    }

    public VariantFileDeleteParams setForce(boolean force) {
        this.force = force;
        return this;
    }
}
