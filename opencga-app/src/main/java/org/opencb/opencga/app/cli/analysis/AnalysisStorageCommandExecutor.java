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

package org.opencb.opencga.app.cli.analysis;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.storage.core.StorageManagerFactory;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created on 10/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class AnalysisStorageCommandExecutor extends AnalysisCommandExecutor {

    protected CatalogManager catalogManager;
    protected StorageManagerFactory storageManagerFactory;


    public AnalysisStorageCommandExecutor(GeneralCliOptions.CommonCommandOptions options) {
        super(options);
    }


    protected void configure()
            throws IllegalAccessException, ClassNotFoundException, InstantiationException, CatalogException {

        //  Creating CatalogManager
        catalogManager = new CatalogManager(catalogConfiguration);

        // Creating StorageManagerFactory
        storageManagerFactory = StorageManagerFactory.get(storageConfiguration);

    }


    protected Job getJob(long studyId, String jobId, String sessionId) throws CatalogException {
        Query query = new Query(JobDBAdaptor.QueryParams.RESOURCE_MANAGER_ATTRIBUTES.key() + "." + Job.JOB_SCHEDULER_NAME, jobId);
        QueryResult<Job> result = catalogManager.getAllJobs(studyId, query, null, sessionId);
        if (result.getResult().isEmpty()) {
            throw new IllegalArgumentException("Unknown job. Can't find job " + jobId + " in study " + studyId);
        }
        return result.first();
    }

    protected Map<Long, String> getStudyIds(String sessionId) throws CatalogException {
        return catalogManager.getAllStudies(new Query(), new QueryOptions("include", "projects.studies.id,projects.studies.alias"), sessionId)
                .getResult()
                .stream()
                .collect(Collectors.toMap(Study::getId, Study::getAlias));
    }
}
