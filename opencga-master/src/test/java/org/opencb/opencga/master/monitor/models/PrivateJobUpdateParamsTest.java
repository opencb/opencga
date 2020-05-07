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

package org.opencb.opencga.master.monitor.models;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManagerTest;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.job.JobStudyParam;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.Arrays;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PrivateJobUpdateParamsTest extends AbstractManagerTest {

    @Test
    public void test() throws JsonProcessingException {
        PrivateJobUpdateParams params = new PrivateJobUpdateParams();

        params.setOutDir(new File()
                .setUid(1)
                .setPath("/tmp/path")
                .setId("myJobId"));

        ObjectMap updateMap = params.getUpdateMap();

        Object path = updateMap.getMap("outDir").get("path");
        assertEquals("/tmp/path", path);

        Object id = updateMap.getMap("outDir").get("id");
        assertEquals("myJobId", id);

        Object uid = updateMap.getMap("outDir").get("uid");
        assertEquals(1, uid);

        assertNull(updateMap.getMap("outDir").get("uri"));
        assertNull(updateMap.get("id"));
        assertNull(updateMap.get("path"));
    }

    @Test
    public void updateJobInformation() throws CatalogException {
        OpenCGAResult<Job> jobResult = catalogManager.getJobManager().submit(studyFqn, "variant-index", Enums.Priority.HIGH,
                new HashMap<>(), token);

        PrivateJobUpdateParams updateParams = new PrivateJobUpdateParams().setCommandLine("myCommandLine");
        catalogManager.getJobManager().update(studyFqn, jobResult.first().getId(), updateParams, QueryOptions.empty(), token);
        jobResult = catalogManager.getJobManager().get(studyFqn, jobResult.first().getId(), QueryOptions.empty(), token);

        assertEquals("myCommandLine", jobResult.first().getCommandLine());

        updateParams.setOutDir(new File()
                .setUid(1)
                .setPath("/tmp/path")
                .setId("myJobId"))
                .setStudy(new JobStudyParam(studyFqn, Arrays.asList(studyFqn2, studyFqn3)));
        catalogManager.getJobManager().update(studyFqn, jobResult.first().getId(), updateParams, QueryOptions.empty(), token);
        jobResult = catalogManager.getJobManager().get(studyFqn, jobResult.first().getId(), QueryOptions.empty(), token);
        assertEquals(2, jobResult.first().getStudy().getOthers().size());
        System.out.println(jobResult);

    }

}