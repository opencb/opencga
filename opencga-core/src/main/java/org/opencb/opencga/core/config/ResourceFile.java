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

package org.opencb.opencga.core.config;

public class ResourceFile {

    private String id;
    private String url;
    private String md5;
    private String path;

    public ResourceFile() {
    }

    public ResourceFile(String id, String url, String md5, String path) {
        this.id = id;
        this.url = url;
        this.md5 = md5;
        this.path = path;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ResourceFile{");
        sb.append("id='").append(id).append('\'');
        sb.append(", url='").append(url).append('\'');
        sb.append(", md5='").append(md5).append('\'');
        sb.append(", path='").append(path).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public ResourceFile setId(String id) {
        this.id = id;
        return this;
    }

    public String getUrl() {
        return url;
    }

    public ResourceFile setUrl(String url) {
        this.url = url;
        return this;
    }

    public String getMd5() {
        return md5;
    }

    public ResourceFile setMd5(String md5) {
        this.md5 = md5;
        return this;
    }

    public String getPath() {
        return path;
    }

    public ResourceFile setPath(String path) {
        this.path = path;
        return this;
    }
}
