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

package org.opencb.opencga.analysis.family;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.exec.Command;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.PedigreeGraphUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.tools.annotations.Tool;

import java.util.Arrays;
import java.util.List;

@Tool(id = PedigreeGraphInitAnalysis.ID, resource = Enums.Resource.FAMILY)
public class PedigreeGraphInitAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "pedigree-graph-init";
    public static final String DESCRIPTION = "Compute the family pedigree graph image for all families in the study.";

    @Override
    protected void check() throws Exception {
        super.check();
        setUpStorageEngineExecutor(study);

        if (StringUtils.isEmpty(getStudy())) {
            throw new ToolException("Missing study");
        }
    }

    @Override
    public List<String> getSteps() {
        return Arrays.asList("pull-docker", getId());
    }

    @Override
    protected void run() throws ToolException {
        step("pull-docker", () -> {
            // Be sure docker is alive and that opencga-ext-tools is downloaded
            logger.info("Checking if docker daemon is alive");
            DockerUtils.checkDockerDaemonAlive();
            logger.info("Pulling docker '{}'", PedigreeGraphUtils.R_DOCKER_IMAGE);
            new Command("docker pull " + PedigreeGraphUtils.R_DOCKER_IMAGE).run();
        });

        step(getId(), () -> {
            // Get all families from that study
            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList("id", "members", "pedigreeGraph"));
            try (DBIterator<Family> iterator = catalogManager.getFamilyManager().iterator(study, new Query(), queryOptions, token)) {
                while (iterator.hasNext()) {
                    Family family = iterator.next();
                    if (PedigreeGraphUtils.hasMinTwoGenerations(family)
                            && (family.getPedigreeGraph() == null || StringUtils.isEmpty(family.getPedigreeGraph().getBase64()))) {
                        try {
                            logger.info("Updating pedigree graph for family '{}'", family.getId());
                            catalogManager.getFamilyManager().update(study, family.getId(), null,
                                    new QueryOptions(ParamConstants.FAMILY_UPDATE_PEDIGREEE_GRAPH_PARAM, true), token);
                            String msg = "Updated pedigree graph for family '" + family.getId() + "'";
                            logger.info(msg);
                            addInfo(msg);
                        } catch (CatalogException e) {
                            String msg = "Something wrong happened when updating pedigree graph for family '" + family.getId() + "'.";
                            logger.warn(msg, e);
                            addWarning(msg + " Error: " + e.getMessage());
                        }
                    }
                }
            }

            logger.info("Finished updating pedigree graph for families of the study '{}'", study);
        });
    }
}

