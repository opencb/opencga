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

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class VariantFileDeleteParams extends ToolParams {

    public VariantFileDeleteParams() {
    }

    public VariantFileDeleteParams(List<String> file, boolean resume) {
        this.file = file;
        this.resume = resume;
    }

    private List<String> file;
    private boolean resume;

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
}
