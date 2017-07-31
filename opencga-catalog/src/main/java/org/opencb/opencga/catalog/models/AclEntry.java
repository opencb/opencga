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

package org.opencb.opencga.catalog.models;

/**
 * Created by jacobo on 11/09/14.
 */
@Deprecated
public class AclEntry {

    public static final String USER_OTHERS_ID = "*";
    private String userId;

    private boolean read;
    private boolean write;
    private boolean execute;
    private boolean delete;


    public AclEntry() {
    }

    public AclEntry(String userId, boolean read, boolean write, boolean execute, boolean delete) {
        this.userId = userId;
        this.read = read;
        this.write = write;
        this.execute = execute;
        this.delete = delete;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleAcl{");
        sb.append("userId='").append(userId).append('\'');
        sb.append(", read=").append(read);
        sb.append(", write=").append(write);
        sb.append(", execute=").append(execute);
        sb.append(", delete=").append(delete);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public int hashCode() {
        int result = userId.hashCode();
        result = 31 * result + (read ? 1 : 0);
        result = 31 * result + (write ? 1 : 0);
        result = 31 * result + (execute ? 1 : 0);
        result = 31 * result + (delete ? 1 : 0);
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AclEntry)) {
            return false;
        }

        AclEntry acl = (AclEntry) o;

        if (delete != acl.delete) {
            return false;
        }
        if (execute != acl.execute) {
            return false;
        }
        if (read != acl.read) {
            return false;
        }
        if (write != acl.write) {
            return false;
        }
        if (!userId.equals(acl.userId)) {
            return false;
        }

        return true;
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

    public boolean isDelete() {
        return delete;
    }

    public void setDelete(boolean delete) {
        this.delete = delete;
    }
}
