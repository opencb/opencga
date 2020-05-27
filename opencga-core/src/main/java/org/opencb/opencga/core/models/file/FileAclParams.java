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

package org.opencb.opencga.core.models.file;

import org.opencb.opencga.core.models.AclParams;

// Acl params to communicate the WS and the sample manager
public class FileAclParams extends AclParams {

    private String sample;

    public FileAclParams() {
    }

    public FileAclParams(String sample, String permissions) {
        super(permissions);
        this.sample = sample;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileAclParams{");
        sb.append("permissions='").append(permissions).append('\'');
        sb.append(", sample='").append(sample).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getSample() {
        return sample;
    }

    public FileAclParams setSample(String sample) {
        this.sample = sample;
        return this;
    }

    public FileAclParams setPermissions(String permissions) {
        super.setPermissions(permissions);
        return this;
    }

}
