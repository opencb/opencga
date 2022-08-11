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

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

/**
 * Created by pfurio on 04/07/16.
 */
public class AclEntry<E extends Enum<E>> {

    protected String member;
    protected EnumSet<E> permissions;
    protected List<GroupAclEntry<E>> groups;

    public AclEntry() {
    }

    public AclEntry(String member, EnumSet<E> permissions) {
        this.member = member;
        this.permissions = permissions;
        this.groups = Collections.emptyList();
    }

    public AclEntry(String member, EnumSet<E> permissions, List<GroupAclEntry<E>> groups) {
        this.member = member;
        this.permissions = permissions;
        this.groups = groups;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AclEntry{");
        sb.append("member='").append(member).append('\'');
        sb.append(", permissions=").append(permissions);
        sb.append(", groups=").append(groups);
        sb.append('}');
        return sb.toString();
    }

    public String getMember() {
        return member;
    }

    public AclEntry setMember(String member) {
        this.member = member;
        return this;
    }

    public EnumSet<E> getPermissions() {
        return permissions;
    }

    public AclEntry setPermissions(EnumSet<E> permissions) {
        this.permissions = permissions;
        return this;
    }

    public List<GroupAclEntry<E>> getGroups() {
        return groups;
    }

    public AclEntry<E> setGroups(List<GroupAclEntry<E>> groups) {
        this.groups = groups;
        return this;
    }

    public static class GroupAclEntry<E extends Enum<E>> {
        private String id;
        private EnumSet<E> permissions;

        public GroupAclEntry() {
        }

        public GroupAclEntry(String id, EnumSet<E> permissions) {
            this.id = id;
            this.permissions = permissions;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("GroupAclEntry{");
            sb.append("id='").append(id).append('\'');
            sb.append(", permissions=").append(permissions);
            sb.append('}');
            return sb.toString();
        }

        public String getId() {
            return id;
        }

        public GroupAclEntry<E> setId(String id) {
            this.id = id;
            return this;
        }

        public EnumSet<E> getPermissions() {
            return permissions;
        }

        public GroupAclEntry<E> setPermissions(EnumSet<E> permissions) {
            this.permissions = permissions;
            return this;
        }
    }
}
