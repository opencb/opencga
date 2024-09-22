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

package org.opencb.opencga.analysis.tools;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.StudyManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.job.ToolInfoExecutor;

import java.util.List;

public abstract class OpenCgaToolScopeStudy extends OpenCgaTool {

    protected String study;

    @Override
    protected void check() throws Exception {
        super.check();
        study = getStudyFqn();
    }

    public OpenCgaToolScopeStudy setStudy(String study) {
        this.study = study;
        getParams().put(ParamConstants.STUDY_PARAM, study);
        return this;
    }

    /**
     * Get the study Fully Qualified Name.
     * The study is validated during {@link #check()}
     * @return study FQN
     */
    public String getStudy() {
        return study;
    }

    protected final String getStudyFqn() throws CatalogException {
        return getCatalogManager().getStudyManager().get(getParams().getString(ParamConstants.STUDY_PARAM), StudyManager.INCLUDE_STUDY_IDS,
                getToken()).first().getFqn();
    }

    protected void updateJobInformation(List<String> tags, ToolInfoExecutor executor) throws CatalogException {
        ObjectMap params = new ObjectMap()
                .append(JobDBAdaptor.QueryParams.TAGS.key(), tags)
                .append(JobDBAdaptor.QueryParams.TOOL_EXTERNAL_EXECUTOR.key(), executor);
        catalogManager.getJobManager().update(getStudyFqn(), getJobId(), params, QueryOptions.empty(), token);
    }

}
