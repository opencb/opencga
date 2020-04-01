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

package org.opencb.opencga.catalog.old.models.tool;

@Deprecated
public class Acl {

    private String userId;
    private boolean read, write, execute;

    public Acl() {
    }

    public Acl(String userId, boolean read, boolean write, boolean execute) {
        this.userId = userId;
        this.read = read;
        this.write = write;
        this.execute = execute;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Acl{");
        sb.append("userId='").append(userId).append('\'');
        sb.append(", read=").append(read);
        sb.append(", write=").append(write);
        sb.append(", execute=").append(execute);
        sb.append('}');
        return sb.toString();
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public boolean isRead() {
        return read;
    }

    public void setRead(boolean read) {
        this.read = read;
    }

    public boolean isWrite() {
        return write;
    }

    public void setWrite(boolean write) {
        this.write = write;
    }

    public boolean isExecute() {
        return execute;
    }

    public void setExecute(boolean execute) {
        this.execute = execute;
    }
}
