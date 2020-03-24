/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.core.models.study;

import java.util.List;

/**
 * Created on 21/08/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class Group {

    /**
     * Group id, unique in the belonging study.
     */
    private String id;

    /**
     * Set of users belonging to this group.
     */
    private List<String> userIds;

    /**
     * Group has been synchronised from an external authorization.
     */
    private Sync syncedFrom;

    public Group() {
    }

    public Group(String id, List<String> userIds) {
        this(id, userIds, null);
    }

    public Group(String id, List<String> userIds, Sync syncedFrom) {
        this.id = id;
        this.userIds = userIds;
        this.syncedFrom = syncedFrom;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Group{");
        sb.append("id='").append(id).append('\'');
        sb.append(", userIds=").append(userIds);
        sb.append(", syncedFrom=").append(syncedFrom);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public Group setId(String id) {
        this.id = id;
        return this;
    }

    public List<String> getUserIds() {
        return userIds;
    }

    public Group setUserIds(List<String> userIds) {
        this.userIds = userIds;
        return this;
    }

    public Sync getSyncedFrom() {
        return syncedFrom;
    }

    public Group setSyncedFrom(Sync syncedFrom) {
        this.syncedFrom = syncedFrom;
        return this;
    }

    public static class Sync {

        private String authOrigin;
        private String remoteGroup;

        public Sync() {
        }

        public Sync(String authOrigin, String remoteGroup) {
            this.authOrigin = authOrigin;
            this.remoteGroup = remoteGroup;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Sync{");
            sb.append("authOrigin='").append(authOrigin).append('\'');
            sb.append(", remoteGroup='").append(remoteGroup).append('\'');
            sb.append('}');
            return sb.toString();
        }

        public String getAuthOrigin() {
            return authOrigin;
        }

        public Sync setAuthOrigin(String authOrigin) {
            this.authOrigin = authOrigin;
            return this;
        }

        public String getRemoteGroup() {
            return remoteGroup;
        }

        public Sync setRemoteGroup(String remoteGroup) {
            this.remoteGroup = remoteGroup;
            return this;
        }
    }

}

