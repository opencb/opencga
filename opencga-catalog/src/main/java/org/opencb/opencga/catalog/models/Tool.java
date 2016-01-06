/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.catalog.models;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by jacobo on 11/09/14.
 */
public class Tool {

    private int id;
    private String alias;
    private String name;
    private String description;
    private Object manifest;
    private Object result;
    private String path;
    private List<AclEntry> acl;

    public Tool() {
        this("", "", "", null, null, "");
    }

    public Tool(String alias, String name, String description, Object manifest, Object result, String path) {
        this(-1, alias, name, description, manifest, result, path, new LinkedList());
    }

    public Tool(int id, String alias, String name, String description, Object manifest, Object result, String path, List<AclEntry> acl) {
        this.id = id;
        this.alias = alias;
        this.name = name;
        this.description = description;
        this.manifest = manifest;
        this.result = result;
        this.path = path;
        this.acl = acl;
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
        sb.append(", acl=").append(acl);
        sb.append('}');
        return sb.toString();
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Object getManifest() {
        return manifest;
    }

    public void setManifest(Object manifest) {
        this.manifest = manifest;
    }

    public Object getResult() {
        return result;
    }

    public void setResult(Object result) {
        this.result = result;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }


    public List<AclEntry> getAcl() {
        return acl;
    }

    public void setAcl(List<AclEntry> acl) {
        this.acl = acl;
    }

}
