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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wasim on 27/06/18.
 */
public class FamilySolrModel extends CatalogSolrModel {

    @Field
    private List<String> phenotypes;

    @Field
    private List<String> disorders;

    @Field
    private int numMembers;

    @Field
    private int expectedSize;

    @Field
    private int version;

    @Field
    private List<String> annotationSets;

    @Field("annotations__*")
    private Map<String, Object> annotations;

    public FamilySolrModel() {
        this.annotationSets = new ArrayList<>();
        this.annotations = new HashMap<>();
        this.phenotypes = new ArrayList<>();
        this.disorders = new ArrayList<>();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FamilySolrModel{");
        sb.append("id='").append(id).append('\'');
        sb.append(", uid=").append(uid);
        sb.append(", studyId='").append(studyId).append('\'');
        sb.append(", creationYear=").append(creationYear);
        sb.append(", creationMonth='").append(creationMonth).append('\'');
        sb.append(", creationDay=").append(creationDay);
        sb.append(", creationDayOfWeek='").append(creationDayOfWeek).append('\'');
        sb.append(", status='").append(status).append('\'');
        sb.append(", phenotypes=").append(phenotypes);
        sb.append(", disorders=").append(disorders);
        sb.append(", numMembers=").append(numMembers);
        sb.append(", expectedSize=").append(expectedSize);
        sb.append(", release=").append(release);
        sb.append(", version=").append(version);
        sb.append(", acl=").append(acl);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append(", annotations=").append(annotations);
        sb.append('}');
        return sb.toString();
    }

    public List<String> getPhenotypes() {
        return phenotypes;
    }

    public FamilySolrModel setPhenotypes(List<String> phenotypes) {
        this.phenotypes = phenotypes;
        return this;
    }

    public List<String> getDisorders() {
        return disorders;
    }

    public FamilySolrModel setDisorders(List<String> disorders) {
        this.disorders = disorders;
        return this;
    }

    public int getNumMembers() {
        return numMembers;
    }

    public FamilySolrModel setNumMembers(int numMembers) {
        this.numMembers = numMembers;
        return this;
    }

    public int getExpectedSize() {
        return expectedSize;
    }

    public FamilySolrModel setExpectedSize(int expectedSize) {
        this.expectedSize = expectedSize;
        return this;
    }

    public int getVersion() {
        return version;
    }

    public FamilySolrModel setVersion(int version) {
        this.version = version;
        return this;
    }

    public List<String> getAnnotationSets() {
        return annotationSets;
    }

    public FamilySolrModel setAnnotationSets(List<String> annotationSets) {
        this.annotationSets = annotationSets;
        return this;
    }

    public Map<String, Object> getAnnotations() {
        return annotations;
    }

    public FamilySolrModel setAnnotations(Map<String, Object> annotations) {
        this.annotations = annotations;
        return this;
    }
}
