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

import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.api.ParamConstants;

public abstract class OpenCgaToolScopeStudy extends OpenCgaTool {

    protected String study;

    @Override
    protected void check() throws Exception {
        super.check();
        study = getStudyFqn();
    }

    public void setStudy(String study) {
        getParams().put(ParamConstants.STUDY_PARAM, study);
    }

    public String getStudy() {
        return study;
    }

    protected final String getStudyFqn() throws CatalogException {
        String userId = getCatalogManager().getUserManager().getUserId(getToken());
        return getCatalogManager().getStudyManager().resolveId(getParams().getString(ParamConstants.STUDY_PARAM), userId).getFqn();
    }

}
