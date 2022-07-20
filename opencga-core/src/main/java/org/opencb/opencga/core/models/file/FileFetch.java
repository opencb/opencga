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

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class FileFetch extends ToolParams {

    /**
     * External url where the file to be registered can be downloaded from.
     */
    @DataField(description = ParamConstants.FILE_FETCH_URL_DESCRIPTION)
    private String url;

    /**
     * Folder path where the file will be downloaded.
     */
    @DataField(description = ParamConstants.FILE_FETCH_PATH_DESCRIPTION)
    private String path;

    public FileFetch() {
    }

    public FileFetch(String url, String path) {
        this.path = path;
        this.url = url;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileFetch{");
        sb.append("url='").append(url).append('\'');
        sb.append(", path='").append(path).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getUrl() {
        return url;
    }

    public FileFetch setUrl(String url) {
        this.url = url;
        return this;
    }

    public String getPath() {
        return path;
    }

    public FileFetch setPath(String path) {
        this.path = path;
        return this;
    }
}
