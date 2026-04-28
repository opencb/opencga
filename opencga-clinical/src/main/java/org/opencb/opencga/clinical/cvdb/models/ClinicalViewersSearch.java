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

package org.opencb.opencga.clinical.cvdb.models;

import org.apache.solr.client.solrj.beans.Field;

import java.util.ArrayList;
import java.util.List;

public class ClinicalViewersSearch {

    // ID is the clinical analysis ID
    @Field("id")
    private String id;

    // Study ID
    @Field("studyId")
    private String studyId;

    // List of viewers (i.e., user IDs) that have access to the clinical analysis
    @Field("viewers")
    private List<String> viewers;


    public ClinicalViewersSearch() {
        viewers = new ArrayList<>();
    }

    public ClinicalViewersSearch(String id, String studyId, List<String> viewers) {
        this.id = id;
        this.studyId = studyId;
        this.viewers = viewers;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalViewersSearch{");
        sb.append("id='").append(id).append('\'');
        sb.append(", studyId=").append(studyId);
        sb.append(", viewers=").append(viewers);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public ClinicalViewersSearch setId(String id) {
        this.id = id;
        return this;
    }

    public String getStudyId() {
        return studyId;
    }

    public ClinicalViewersSearch setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public List<String> getViewers() {
        return viewers;
    }

    public ClinicalViewersSearch setViewers(List<String> viewers) {
        this.viewers = viewers;
        return this;
    }
}


