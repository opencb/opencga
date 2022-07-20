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

package org.opencb.opencga.core.models.file;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class FileLinkToolParams extends ToolParams {
    @DataField(description = ParamConstants.FILE_LINK_TOOL_PARAMS_URI_DESCRIPTION)
    private List<String> uri;
    @DataField(description = ParamConstants.FILE_LINK_TOOL_PARAMS_PATH_DESCRIPTION)
    private String path;
    @DataField(description = ParamConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;
    @DataField(description = ParamConstants.FILE_LINK_TOOL_PARAMS_PARENTS_DESCRIPTION)
    private boolean parents;

    public FileLinkToolParams() {
    }

    public FileLinkToolParams(List<String> uri, String path, String description, boolean parents) {
        this.uri = uri;
        this.path = path;
        this.description = description;
        this.parents = parents;
    }

    public List<String> getUri() {
        return uri;
    }

    public FileLinkToolParams setUri(List<String> uri) {
        this.uri = uri;
        return this;
    }

    public String getPath() {
        return path;
    }

    public FileLinkToolParams setPath(String path) {
        this.path = path;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public FileLinkToolParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public boolean isParents() {
        return parents;
    }

    public FileLinkToolParams setParents(boolean parents) {
        this.parents = parents;
        return this;
    }
}
