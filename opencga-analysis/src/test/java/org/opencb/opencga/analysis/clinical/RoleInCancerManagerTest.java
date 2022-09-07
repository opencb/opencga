package org.opencb.opencga.analysis.clinical;

import org.junit.Test;
import org.opencb.biodata.models.clinical.ClinicalProperty;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class RoleInCancerManagerTest {

    @Test
    public void load() throws IOException {
        Map<String, List<ClinicalProperty.RoleInCancer>> roleInCancer = new RoleInCancerManager(null).getRoleInCancer();
        assertNotNull(roleInCancer);
    }
}