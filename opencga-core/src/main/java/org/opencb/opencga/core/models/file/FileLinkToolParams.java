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

public class FileLinkToolParams extends ToolParams {
    private List<String> uri;
    private String path;
    private String description;
    private String virtualFile;
    private boolean parents;
    private boolean skipPostLink;

    public FileLinkToolParams() {
    }

    public FileLinkToolParams(List<String> uri, String path, String description, String virtualFile, boolean parents,
                              boolean skipPostLink) {
        this.uri = uri;
        this.path = path;
        this.description = description;
        this.virtualFile = virtualFile;
        this.parents = parents;
        this.skipPostLink = skipPostLink;
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

    public String getVirtualFile() {
        return virtualFile;
    }

    public FileLinkToolParams setVirtualFile(String virtualFile) {
        this.virtualFile = virtualFile;
        return this;
    }

    public boolean isParents() {
        return parents;
    }

    public FileLinkToolParams setParents(boolean parents) {
        this.parents = parents;
        return this;
    }

    public boolean isSkipPostLink() {
        return skipPostLink;
    }

    public FileLinkToolParams setSkipPostLink(boolean skipPostLink) {
        this.skipPostLink = skipPostLink;
        return this;
    }
}
