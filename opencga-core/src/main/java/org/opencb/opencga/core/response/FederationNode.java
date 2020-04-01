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

package org.opencb.opencga.core.response;

public class FederationNode {

    private String id;
    private String commit;
    private String version;

    public FederationNode() {
    }

    public FederationNode(String id, String commit, String version) {
        this.id = id;
        this.commit = commit;
        this.version = version;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FederationNode{");
        sb.append("id='").append(id).append('\'');
        sb.append(", commit='").append(commit).append('\'');
        sb.append(", version='").append(version).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public FederationNode setId(String id) {
        this.id = id;
        return this;
    }

    public String getCommit() {
        return commit;
    }

    public FederationNode setCommit(String commit) {
        this.commit = commit;
        return this;
    }

    public String getVersion() {
        return version;
    }

    public FederationNode setVersion(String version) {
        this.version = version;
        return this;
    }
}
