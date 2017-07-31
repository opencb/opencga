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

import org.opencb.opencga.core.common.TimeUtils;

import java.util.Objects;

/**
 * Created by pfurio on 11/03/16.
 */
public class Status {

    /**
     * READY name means that the object is being used.
     */
    public static final String READY = "READY";

    /**
     * TRASHED name means that the object is marked as deleted although is still available in the database.
     */
    public static final String TRASHED = "TRASHED";

    /**
     * DELETED name means that the object is marked as removed, so it will get completely removed from the database ASAP.
     */
    public static final String DELETED = "DELETED";

    private String name;
    private String date;
    private String message;

    public Status() {
        this(READY, "");
    }

    public Status(String name) {
        this(name, "");
    }

    public Status(String name, String message) {
        if (isValid(name)) {
            init(name, message);
        } else {
            throw new IllegalArgumentException("Unknown name '" + name + "'");
        }
    }

    protected void init(String status, String message) {
        this.name = status;
        this.date = TimeUtils.getTime();
        this.message = message;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public void setCurrentDate() {
        this.date = TimeUtils.getTime();
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public static boolean isValid(String status) {
        if (status != null && (status.equals(READY) || status.equals(TRASHED) || status.equals(DELETED))) {
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Status{");
        sb.append("name='").append(name).append('\'');
        sb.append(", date='").append(date).append('\'');
        sb.append(", message='").append(message).append('\'');
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Status)) {
            return false;
        }
        Status status = (Status) o;
        return Objects.equals(name, status.name)
                && Objects.equals(date, status.date)
                && Objects.equals(message, status.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, date, message);
    }
}
