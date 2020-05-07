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

package org.opencb.opencga.catalog.stats.solr;

import org.apache.solr.client.solrj.beans.Field;

import java.util.List;

public abstract class CatalogSolrModel {

    @Field
    protected String id;

    @Field
    protected long uid;

    @Field
    protected String studyId;

    @Field
    protected int creationYear;

    @Field
    protected String creationMonth;

    @Field
    protected int creationDay;

    @Field
    protected String creationDayOfWeek;

    @Field
    protected String status;

    @Field
    protected int release;

    @Field
    protected List<String> acl;

    public CatalogSolrModel() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CatalogSolrModel{");
        sb.append("id='").append(id).append('\'');
        sb.append(", uid=").append(uid);
        sb.append(", studyId='").append(studyId).append('\'');
        sb.append(", creationYear=").append(creationYear);
        sb.append(", creationMonth='").append(creationMonth).append('\'');
        sb.append(", creationDay=").append(creationDay);
        sb.append(", creationDayOfWeek='").append(creationDayOfWeek).append('\'');
        sb.append(", status='").append(status).append('\'');
        sb.append(", release=").append(release);
        sb.append(", acl=").append(acl);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public CatalogSolrModel setId(String id) {
        this.id = id;
        return this;
    }

    public long getUid() {
        return uid;
    }

    public CatalogSolrModel setUid(long uid) {
        this.uid = uid;
        return this;
    }

    public String getStudyId() {
        return studyId;
    }

    public CatalogSolrModel setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public int getCreationYear() {
        return creationYear;
    }

    public CatalogSolrModel setCreationYear(int creationYear) {
        this.creationYear = creationYear;
        return this;
    }

    public String getCreationMonth() {
        return creationMonth;
    }

    public CatalogSolrModel setCreationMonth(String creationMonth) {
        this.creationMonth = creationMonth;
        return this;
    }

    public int getCreationDay() {
        return creationDay;
    }

    public CatalogSolrModel setCreationDay(int creationDay) {
        this.creationDay = creationDay;
        return this;
    }

    public String getCreationDayOfWeek() {
        return creationDayOfWeek;
    }

    public CatalogSolrModel setCreationDayOfWeek(String creationDayOfWeek) {
        this.creationDayOfWeek = creationDayOfWeek;
        return this;
    }

    public String getStatus() {
        return status;
    }

    public CatalogSolrModel setStatus(String status) {
        this.status = status;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public CatalogSolrModel setRelease(int release) {
        this.release = release;
        return this;
    }

    public List<String> getAcl() {
        return acl;
    }

    public CatalogSolrModel setAcl(List<String> acl) {
        this.acl = acl;
        return this;
    }
}
