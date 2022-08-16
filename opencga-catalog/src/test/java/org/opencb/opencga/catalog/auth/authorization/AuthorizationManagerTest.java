package org.opencb.opencga.catalog.auth.authorization;

import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.opencb.opencga.core.models.study.StudyPermissions;

import static org.junit.Assert.*;

public class AuthorizationManagerTest {

    @Test
    public void validateTemplates() {
        assertThat(AuthorizationManager.getAnalystAcls(),
                CoreMatchers.hasItems(AuthorizationManager.getViewOnlyAcls().toArray(new StudyPermissions.Permissions[0])));
        assertThat(AuthorizationManager.getAdminAcls(),
                CoreMatchers.hasItems(AuthorizationManager.getAnalystAcls().toArray(new StudyPermissions.Permissions[0])));
    }


}