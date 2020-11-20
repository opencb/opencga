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

package org.opencb.opencga.analysis.variant.operations;

import org.apache.commons.lang.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.StudyManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;

import java.nio.file.Path;

public abstract class OperationTool extends OpenCgaTool {

    public static final String KEEP_INTERMEDIATE_FILES = "keepIntermediateFiles";

    protected Boolean keepIntermediateFiles;

    @Override
    protected void check() throws Exception {
        super.check();
        if (keepIntermediateFiles == null) {
            keepIntermediateFiles = params.getBoolean(KEEP_INTERMEDIATE_FILES);
        }
        if (StringUtils.isNotBlank(getParams().getString(ParamConstants.PROJECT_PARAM))) {
            getParams().put(ParamConstants.PROJECT_PARAM, getProjectFqn());
        }
        if (StringUtils.isNotBlank(getParams().getString(ParamConstants.STUDY_PARAM))) {
            String study = getParams().getString(ParamConstants.STUDY_PARAM);
            if (VariantQueryUtils.isNegated(study) || study.contains(VariantQueryUtils.AND) || study.contains(VariantQueryUtils.OR)) {
                // Ignore query-like study
            } else {
                getParams().put(ParamConstants.STUDY_PARAM, getStudyFqn());
            }
        }
    }

    public OperationTool setKeepIntermediateFiles(Boolean keepIntermediateFiles) {
        this.keepIntermediateFiles = keepIntermediateFiles;
        return this;
    }

    protected Path getOutDir(boolean keepIntermediateFiles) {
        if (keepIntermediateFiles) {
            return getOutDir();
        } else {
            return getScratchDir();
        }
    }

    protected final String getProjectFqn() throws CatalogException {
        return catalogManager.getProjectManager().get(params.getString(ParamConstants.PROJECT_PARAM),
                new QueryOptions(QueryOptions.INCLUDE, ProjectDBAdaptor.QueryParams.FQN.key()), token).first().getFqn();
    }

    protected final String getStudyFqn() throws CatalogException {
        String study = getParams().getString(ParamConstants.STUDY_PARAM);
        try {
            return getStudyFqn(study);
        } catch (CatalogException e) {
            String project = params.getString(ParamConstants.PROJECT_PARAM);
            if (StringUtils.isNotEmpty(project) && !study.contains(":")) {
                study = project + ":" + study;
                try {
                    return getStudyFqn(study);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                    throw e;
                }
            } else {
                throw e;
            }
        }
    }

    protected String getStudyFqn(String study) throws CatalogException {
        return getCatalogManager().getStudyManager().get(study, StudyManager.INCLUDE_STUDY_ID, getToken()).first().getFqn();
    }

    private static boolean isVcfFormat(File.Format format) {
        return format.equals(File.Format.VCF) || format.equals(File.Format.GVCF) || format.equals(File.Format.BCF);
    }

}