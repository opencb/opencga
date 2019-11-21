package org.opencb.opencga.catalog.models.update;

import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class JobUpdateParamsTest {

    @Test
    public void test() throws CatalogException {
        JobUpdateParams params = new JobUpdateParams();

        params.setTmpDir(new File()
                .setUid(1)
                .setPath("/tmp/path")
                .setId("myJobId"));

        ObjectMap updateMap = params.getUpdateMap();

        Object path = updateMap.getMap("tmpDir").get("path");
        assertEquals("/tmp/path", path);

        Object id = updateMap.getMap("tmpDir").get("id");
        assertEquals("myJobId", id);

        Object uid = updateMap.getMap("tmpDir").get("uid");
        assertEquals(1, uid);

        assertNull(updateMap.getMap("tmpDir").get("uri"));
        assertNull(updateMap.get("id"));
        assertNull(updateMap.get("path"));
    }

}