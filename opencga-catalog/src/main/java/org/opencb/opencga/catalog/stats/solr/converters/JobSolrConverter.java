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

package org.opencb.opencga.catalog.stats.solr.converters;

import org.apache.commons.lang3.NotImplementedException;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.catalog.stats.solr.JobSolrModel;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.study.Study;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JobSolrConverter implements ComplexTypeConverter<Job, JobSolrModel> {

    private Study study;

    protected static Logger logger= LoggerFactory.getLogger(JobSolrConverter.class);

    public JobSolrConverter(Study study) {
        this.study=study;
    }

    @Override
    public Job convertToDataModelType(JobSolrModel jobSolrModel) {
        throw new NotImplementedException("Operation not supported");
    }

    @Override
    public JobSolrModel convertToStorageType(Job job) {
        JobSolrModel jobSolrModel =new JobSolrModel();

        jobSolrModel.setId(job.getUuid());
        jobSolrModel.setUid(job.getUid());
        jobSolrModel.setStudyId(study.getFqn().replace(":", "__"));

        jobSolrModel.setRelease(job.getRelease());

        Date date= TimeUtils.toDate(job.getCreationDate());
        LocalDate localDate=date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();

        jobSolrModel.setCreationYear(localDate.getYear());
        jobSolrModel.setCreationMonth(localDate.getMonth().toString());
        jobSolrModel.setCreationDay(localDate.getDayOfMonth());
        jobSolrModel.setCreationDayOfWeek(localDate.getDayOfWeek().toString());
        jobSolrModel.setStatus(job.getInternal().getStatus().getId());

        if (job.getTool() != null) {
            jobSolrModel.setToolId(job.getTool().getId());
            jobSolrModel.setToolScope(job.getTool().getScope() != null ? job.getTool().getScope().name() : null);
            jobSolrModel.setToolType(job.getTool().getType() != null ? job.getTool().getType().name() : null);
            jobSolrModel.setToolResource(job.getTool().getResource() != null ? job.getTool().getResource().name() : null);
        }

        jobSolrModel.setUserId(job.getUserId());
        jobSolrModel.setPriority(job.getPriority() != null ? job.getPriority().name() : null);
        jobSolrModel.setTags(job.getTags());

        if (job.getResult() != null && job.getResult().getExecutor() != null) {
            jobSolrModel.setExecutorId(job.getResult().getExecutor().getId());
            jobSolrModel.setExecutorFramework(job.getResult().getExecutor().getFramework() != null
                    ? job.getResult().getExecutor().getFramework().name() : null);
        }

        // Extract the permissions
        Map<String, Set<String>> jobAcl =
                SolrConverterUtil.parseInternalOpenCGAAcls((List<Map<String, Object>>)job.getAttributes().get("OPENCGA_ACL"));
        List<String> effectivePermissions =
                SolrConverterUtil.getEffectivePermissions((Map<String, Set<String>>)study.getAttributes().get("OPENCGA_ACL"), jobAcl,
                        Enums.Resource.JOB.name());
        jobSolrModel.setAcl(effectivePermissions);

        return jobSolrModel;
    }
}
