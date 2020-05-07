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
import java.util.List;

public class JobSolrModel extends CatalogSolrModel {

    @Field
    private String toolId;

    @Field
    private String toolScope;

    @Field
    private String toolType;

    @Field
    private String toolResource;

    @Field
    private String userId;

    @Field
    private String priority;

    @Field
    private List<String> tags;

    @Field
    private String executorId;

    @Field
    private String executorFramework;

    public JobSolrModel() {
        this.tags = new ArrayList<>();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JobSolrModel{");
        sb.append("toolId='").append(toolId).append('\'');
        sb.append(", toolScope='").append(toolScope).append('\'');
        sb.append(", toolType='").append(toolType).append('\'');
        sb.append(", toolResource='").append(toolResource).append('\'');
        sb.append(", userId='").append(userId).append('\'');
        sb.append(", priority='").append(priority).append('\'');
        sb.append(", tags=").append(tags);
        sb.append(", executorId='").append(executorId).append('\'');
        sb.append(", executorFramework='").append(executorFramework).append('\'');
        sb.append(", id='").append(id).append('\'');
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

    public String getToolId() {
        return toolId;
    }

    public JobSolrModel setToolId(String toolId) {
        this.toolId = toolId;
        return this;
    }

    public String getToolScope() {
        return toolScope;
    }

    public JobSolrModel setToolScope(String toolScope) {
        this.toolScope = toolScope;
        return this;
    }

    public String getToolType() {
        return toolType;
    }

    public JobSolrModel setToolType(String toolType) {
        this.toolType = toolType;
        return this;
    }

    public String getToolResource() {
        return toolResource;
    }

    public JobSolrModel setToolResource(String toolResource) {
        this.toolResource = toolResource;
        return this;
    }

    public String getUserId() {
        return userId;
    }

    public JobSolrModel setUserId(String userId) {
        this.userId = userId;
        return this;
    }

    public String getPriority() {
        return priority;
    }

    public JobSolrModel setPriority(String priority) {
        this.priority = priority;
        return this;
    }

    public List<String> getTags() {
        return tags;
    }

    public JobSolrModel setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public String getExecutorId() {
        return executorId;
    }

    public JobSolrModel setExecutorId(String executorId) {
        this.executorId = executorId;
        return this;
    }

    public String getExecutorFramework() {
        return executorFramework;
    }

    public JobSolrModel setExecutorFramework(String executorFramework) {
        this.executorFramework = executorFramework;
        return this;
    }
}
