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

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by sgallego on 6/30/16.
 */
@Deprecated
public class ToolAclEntry extends AbstractAclEntry<ToolAclEntry.ToolPermissions> {

    public enum ToolPermissions {
        EXECUTE,
        UPDATE,
        DELETE
    }

    public ToolAclEntry() {
        this("", Collections.emptyList());
    }

    public ToolAclEntry(String member, EnumSet<ToolPermissions> permissions) {
        super(member, permissions);
    }

    public ToolAclEntry(String member, ObjectMap permissions) {
        super(member, EnumSet.noneOf(ToolPermissions.class));

        EnumSet<ToolPermissions> aux = EnumSet.allOf(ToolPermissions.class);
        for (ToolPermissions permission : aux) {
            if (permissions.containsKey(permission.name()) && permissions.getBoolean(permission.name())) {
                this.permissions.add(permission);
            }
        }
    }

    public ToolAclEntry(String member, List<String> permissions) {
        super(member, EnumSet.noneOf(ToolPermissions.class));
        if (permissions.size() > 0) {
            this.permissions.addAll(permissions.stream().map(ToolPermissions::valueOf).collect(Collectors.toList()));
        }
    }

}

