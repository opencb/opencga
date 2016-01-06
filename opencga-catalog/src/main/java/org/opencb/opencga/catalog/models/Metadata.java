/*
 * Copyright 2015 OpenCB
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

/**
 * Created by imedina on 13/09/14.
 */
public class Metadata {

    private String version;
    private String date;
    private String open;

    private int idCounter;


    public Metadata() {
        this("v2", TimeUtils.getTime(), "public");
    }

    public Metadata(String version, String date, String open) {
        this(version, date, open, 0);
    }

    public Metadata(String version, String date, String open, int idCounter) {
        this.version = version;
        this.date = date;
        this.open = open;
        this.idCounter = idCounter;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Metadata{");
        sb.append("version='").append(version).append('\'');
        sb.append(", date='").append(date).append('\'');
        sb.append(", open='").append(open).append('\'');
        sb.append(", idCounter=").append(idCounter);
        sb.append('}');
        return sb.toString();
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getOpen() {
        return open;
    }

    public void setOpen(String open) {
        this.open = open;
    }

    public int getIdCounter() {
        return idCounter;
    }

    public void setIdCounter(int idCounter) {
        this.idCounter = idCounter;
    }
}
