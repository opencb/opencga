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

package org.opencb.opencga.core.models;

/**
 * Created by jacobo on 11/09/14.
 */
@Deprecated
public class Tool {

    private long id;
    private String alias;
    private String name;
    private String description;
    private Object manifest;
    private Object result;
    private String path;

    public Tool() {
    }

    public Tool(String alias, String name, String description, Object manifest, Object result, String path) {
        this(-1, alias, name, description, manifest, result, path);
    }

    public Tool(long id, String alias, String name, String description, Object manifest, Object result, String path) {
        this.id = id;
        this.alias = alias;
        this.name = name;
        this.description = description;
        this.manifest = manifest;
        this.result = result;
        this.path = path;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Tool{");
        sb.append("id=").append(id);
        sb.append(", alias='").append(alias).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", manifest=").append(manifest);
        sb.append(", result=").append(result);
        sb.append(", path='").append(path).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public long getId() {
        return id;
    }

    public Tool setId(long id) {
        this.id = id;
        return this;
    }

    public String getAlias() {
        return alias;
    }

    public Tool setAlias(String alias) {
        this.alias = alias;
        return this;
    }

    public String getName() {
        return name;
    }

    public Tool setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Tool setDescription(String description) {
        this.description = description;
        return this;
    }

    public Object getManifest() {
        return manifest;
    }

    public Tool setManifest(Object manifest) {
        this.manifest = manifest;
        return this;
    }

    public Object getResult() {
        return result;
    }

    public Tool setResult(Object result) {
        this.result = result;
        return this;
    }

    public String getPath() {
        return path;
    }

    public Tool setPath(String path) {
        this.path = path;
        return this;
    }

}
