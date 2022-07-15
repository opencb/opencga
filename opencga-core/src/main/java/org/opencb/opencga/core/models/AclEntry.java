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

import java.util.EnumSet;

/**
 * Created by pfurio on 04/07/16.
 */
public class AclEntry<E extends Enum<E>> {

    protected String member;
    protected EnumSet<E> permissions;

    public AclEntry() {
        this("", null);
    }

    public AclEntry(String member, EnumSet<E> permissions) {
        this.member = member;
        this.permissions = permissions;
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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AclEntry{");
        sb.append("member='").append(member).append('\'');
        sb.append(", permissions=").append(permissions);
        sb.append('}');
        return sb.toString();
    }
}
