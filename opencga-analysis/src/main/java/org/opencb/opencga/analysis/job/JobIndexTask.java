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

package org.opencb.opencga.analysis.job;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.db.api.DBAdaptor;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.stats.solr.CatalogSolrManager;
import org.opencb.opencga.catalog.stats.solr.converters.JobSolrConverter;
import org.opencb.opencga.catalog.stats.solr.converters.SolrConverterUtil;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Tool(id = JobIndexTask.ID, resource = Enums.Resource.JOB, type = Tool.Type.OPERATION, description = "Index Job entries in Solr.")
public class JobIndexTask extends OpenCgaTool {

    public final static String ID = "job-secondary-index";

    private CatalogSolrManager catalogSolrManager;

    @Override
    protected void check() throws Exception {
        catalogSolrManager = new CatalogSolrManager(this.catalogManager);
    }

    @Override
    protected void run() throws Exception {
        // Get all the studies
        Query query = new Query();
        QueryOptions options = new QueryOptions()
                .append(QueryOptions.INCLUDE, Arrays.asList(StudyDBAdaptor.QueryParams.UID.key(),
                        StudyDBAdaptor.QueryParams.ID.key(), StudyDBAdaptor.QueryParams.FQN.key(),
                        StudyDBAdaptor.QueryParams.VARIABLE_SET.key()))
                .append(DBAdaptor.INCLUDE_ACLS, true);
        OpenCGAResult<Study> studyDataResult = catalogManager.getStudyManager().get(query, options, token);
        if (studyDataResult.getNumResults() == 0) {
            throw new CatalogException("Could not index catalog into solr. No studies found");
        }

        // Create solr collections if they don't exist
        catalogSolrManager.createSolrCollections(CatalogSolrManager.JOB_SOLR_COLLECTION);

        for (Study study : studyDataResult.getResults()) {
            Map<String, Set<String>> studyAcls = SolrConverterUtil
                    .parseInternalOpenCGAAcls((List<Map<String, Object>>) study.getAttributes().get("OPENCGA_ACL"));
            // We replace the current studyAcls for the parsed one
            study.getAttributes().put("OPENCGA_ACL", studyAcls);

            indexJob(catalogSolrManager, study);
        }

    }

    private void indexJob(CatalogSolrManager catalogSolrManager, Study study) throws CatalogException {
        logger.info("Indexing jobs of study {}", study.getFqn());

        Query query = new Query(JobDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), Arrays.asList(
                Enums.ExecutionStatus.ERROR, Enums.ExecutionStatus.DONE, Enums.ExecutionStatus.ABORTED));
        QueryOptions jobQueryOptions = new QueryOptions()
                .append(QueryOptions.INCLUDE, Arrays.asList(JobDBAdaptor.QueryParams.UID.key(), JobDBAdaptor.QueryParams.UUID.key(),
                        JobDBAdaptor.QueryParams.STUDY_UID.key(), JobDBAdaptor.QueryParams.CREATION_DATE.key(),
                        JobDBAdaptor.QueryParams.RELEASE.key(), JobDBAdaptor.QueryParams.INTERNAL_STATUS.key(), JobDBAdaptor.QueryParams.TOOL.key(),
                        JobDBAdaptor.QueryParams.USER_ID.key(), JobDBAdaptor.QueryParams.PRIORITY.key(),
                        JobDBAdaptor.QueryParams.TAGS.key(), JobDBAdaptor.QueryParams.EXECUTION.key()))
                .append(DBAdaptor.INCLUDE_ACLS, true);

        catalogSolrManager.insertCatalogCollection(catalogManager.getJobManager().iterator(study.getFqn(), query, jobQueryOptions, token),
                new JobSolrConverter(study), CatalogSolrManager.JOB_SOLR_COLLECTION);
    }
}
