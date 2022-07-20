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

import java.util.Collections;
import java.util.List;

/**
 * Created by pfurio on 28/07/16.
 */
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class FileTree {

    @DataField(description = ParamConstants.FILE_TREE_FILE_DESCRIPTION)
    private File file;
    @DataField(description = ParamConstants.FILE_TREE_CHILDREN_DESCRIPTION)
    private List<FileTree> children;

    public FileTree() {
    }

    public FileTree(File file) {
        this.file = file;
        this.children = Collections.emptyList();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileTree{");
        sb.append("file=").append(file);
        sb.append(", children=").append(children);
        sb.append('}');
        return sb.toString();
    }

    public File getFile() {
        return file;
    }

    public FileTree setFile(File file) {
        this.file = file;
        return this;
    }

    public List<FileTree> getChildren() {
        return children;
    }

    public FileTree setChildren(List<FileTree> children) {
        this.children = children;
        return this;
    }
}
