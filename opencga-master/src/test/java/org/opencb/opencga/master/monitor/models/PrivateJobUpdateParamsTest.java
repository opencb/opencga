package org.opencb.opencga.master.monitor.models;

import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManagerTest;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.Job;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PrivateJobUpdateParamsTest extends AbstractManagerTest {

    @Test
    public void test() throws CatalogException {
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
                new HashMap<>(), sessionIdUser);

        PrivateJobUpdateParams updateParams = new PrivateJobUpdateParams().setCommandLine("myCommandLine");
        catalogManager.getJobManager().update(studyFqn, jobResult.first().getId(), updateParams, QueryOptions.empty(), sessionIdUser);
        jobResult = catalogManager.getJobManager().get(studyFqn, jobResult.first().getId(), QueryOptions.empty(), sessionIdUser);

        assertEquals("myCommandLine", jobResult.first().getCommandLine());

        updateParams.setOutDir(new File()
                .setUid(1)
                .setPath("/tmp/path")
                .setId("myJobId"));
        catalogManager.getJobManager().update(studyFqn, jobResult.first().getId(), updateParams, QueryOptions.empty(), sessionIdUser);
        jobResult = catalogManager.getJobManager().get(studyFqn, jobResult.first().getId(), QueryOptions.empty(), sessionIdUser);
        System.out.println(jobResult);
    }

}