package org.opencb.opencga.analysis.clinical;

import org.junit.Test;
import org.opencb.biodata.models.clinical.ClinicalProperty;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.*;

public class RoleInCancerManagerTest {

    @Test
    public void load() throws IOException {
        Map<String, ClinicalProperty.RoleInCancer> roleInCancer = new RoleInCancerManager().getRoleInCancer();
        assertNotNull(roleInCancer);
    }
}