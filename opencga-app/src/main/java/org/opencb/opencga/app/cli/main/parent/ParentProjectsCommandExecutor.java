/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this project except in compliance with the License.
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
package org.opencb.opencga.app.cli.main.parent;

import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.ProjectsCommandOptions;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.project.ProjectCreateParams;
import org.opencb.opencga.core.models.project.ProjectOrganism;
import org.opencb.opencga.core.response.RestResponse;

/**
 * Created by imedina on 02/03/15.
 */
public abstract class ParentProjectsCommandExecutor extends OpencgaCommandExecutor {

    private ProjectsCommandOptions projectsCommandOptions;

    public ParentProjectsCommandExecutor(GeneralCliOptions.CommonCommandOptions options, boolean command,
                                         ProjectsCommandOptions projectsCommandOptions) {

        super(options, command);
        this.projectsCommandOptions = projectsCommandOptions;
    }

    protected RestResponse<Project> create() throws Exception {
        ProjectsCommandOptions.CreateCommandOptions createCommandOptions = projectsCommandOptions.createCommandOptions;

        ProjectCreateParams createParams = new ProjectCreateParams()
                .setId(createCommandOptions.id)
                .setName(createCommandOptions.name)
                .setDescription(createCommandOptions.description)
                .setOrganism(new ProjectOrganism(createCommandOptions.scientificName, createCommandOptions.commonName,
                        createCommandOptions.assembly));
        return openCGAClient.getProjectClient().create(createParams);
    }
}
