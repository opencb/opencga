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

package org.opencb.opencga.core.models.resource;

import java.util.ArrayList;
import java.util.List;

public class AnalysisResource {

    private String url;
    private String md5;
    private String localRelativePath;
    private List<String> target;
    private List<AnalysisResourceAction> action;

    public enum AnalysisResourceAction {
        UNZIP
    }

    public AnalysisResource() {
        this("", "", "", new ArrayList<>(), new ArrayList<>());
    }

    public AnalysisResource(String localRelativePath, String url, String md5, List<String> target, List<AnalysisResourceAction> action) {
        this.localRelativePath = localRelativePath;
        this.url = url;
        this.md5 = md5;
        this.target = target;
        this.action = action;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AnalysisResource{");
        sb.append("localRelativePath='").append(localRelativePath).append('\'');
        sb.append(", url='").append(url).append('\'');
        sb.append(", md5='").append(md5).append('\'');
        sb.append(", target=").append(target);
        sb.append(", action=").append(action);
        sb.append('}');
        return sb.toString();
    }

    public String getLocalRelativePath() {
        return localRelativePath;
    }

    public AnalysisResource setLocalRelativePath(String localRelativePath) {
        this.localRelativePath = localRelativePath;
        return this;
    }

    public String getUrl() {
        return url;
    }

    public AnalysisResource setUrl(String url) {
        this.url = url;
        return this;
    }

    public String getMd5() {
        return md5;
    }

    public AnalysisResource setMd5(String md5) {
        this.md5 = md5;
        return this;
    }

    public List<String> getTarget() {
        return target;
    }

    public AnalysisResource setTarget(List<String> target) {
        this.target = target;
        return this;
    }

    public List<AnalysisResourceAction> getAction() {
        return action;
    }

    public AnalysisResource setAction(List<AnalysisResourceAction> action) {
        this.action = action;
        return this;
    }
}
