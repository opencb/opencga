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

package org.opencb.opencga.server.generator.models;

import org.opencb.opencga.core.common.GitRepositoryState;

import java.util.ArrayList;
import java.util.List;

public class RestApi {

    private String version;
    private String commit;
    private List<RestCategory> categories;

    public RestApi() {
        this(GitRepositoryState.get().getBuildVersion(), GitRepositoryState.get().getCommitId(), new ArrayList<>());
    }

    public RestApi(String version, String commit, List<RestCategory> categories) {
        this.version = version;
        this.commit = commit;
        this.categories = categories;
    }

    @Override
    public String toString() {
        return "RestApi{" +
                "version='" + version + '\'' +
                ", commit='" + commit + '\'' +
                ", categories=" + categories +
                '}';
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getCommit() {
        return commit;
    }

    public void setCommit(String commit) {
        this.commit = commit;
    }

    public List<RestCategory> getCategories() {
        return categories;
    }

    public void setCategories(List<RestCategory> categories) {
        this.categories = categories;
    }
}
