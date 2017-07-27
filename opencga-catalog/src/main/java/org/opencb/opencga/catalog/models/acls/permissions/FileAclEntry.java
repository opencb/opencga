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

package org.opencb.opencga.catalog.models.acls.permissions;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 11/05/16.
 */
public class FileAclEntry extends AbstractAclEntry<FileAclEntry.FilePermissions> {

    public enum FilePermissions {
        VIEW_HEADER,  // Includes permission to view the sample ids from a VCF file.
        VIEW_CONTENT,
        VIEW,
        WRITE,       // If a folder contains this permission for a user, the user will be able to create files under that folder.
        DELETE,
        DOWNLOAD,
        UPLOAD,
        SHARE
    }

    public FileAclEntry() {
        this("", Collections.emptyList());
    }

    public FileAclEntry(String member, EnumSet<FilePermissions> permissions) {
        super(member, permissions);
    }

    public FileAclEntry(String member, ObjectMap permissions) {
        super(member, EnumSet.noneOf(FilePermissions.class));

        EnumSet<FilePermissions> aux = EnumSet.allOf(FilePermissions.class);
        for (FilePermissions permission : aux) {
            if (permissions.containsKey(permission.name()) && permissions.getBoolean(permission.name())) {
                this.permissions.add(permission);
            }
        }
    }

    public FileAclEntry(String member, List<String> permissions) {
        super(member, EnumSet.noneOf(FilePermissions.class));
        if (permissions.size() > 0) {
            this.permissions.addAll(permissions.stream().map(FilePermissions::valueOf).collect(Collectors.toList()));
        }
    }
}
