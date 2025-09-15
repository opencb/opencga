package org.opencb.opencga.catalog.managers;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.study.Group;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

@Category(MediumTests.class)
public class AdminManagerTest extends AbstractManagerTest {

    @Test
    public void syncUsersInGroups() throws CatalogException {
        Group group1 = new Group("@group1", Collections.emptyList(), new Group.Sync("origin", "r_group1"));
        Group group2 = new Group("@group2", Collections.emptyList(), new Group.Sync("origin", "r_group2"));
        Group group3 = new Group("@group3", Collections.emptyList(), new Group.Sync("origin", "r_group3"));

        for (long studyUid : Arrays.asList(studyUid, studyUid2)) {
            catalogManager.getStudyManager().getStudyDBAdaptor(organizationId).createGroup(studyUid, group1);
            catalogManager.getStudyManager().getStudyDBAdaptor(organizationId).createGroup(studyUid, group2);
            catalogManager.getStudyManager().getStudyDBAdaptor(organizationId).createGroup(studyUid, group3);
        }

        catalogManager.getAdminManager().syncRemoteGroups(organizationId, normalUserId1, Arrays.asList("r_group1", "r_group3"), "origin",
                opencgaToken);

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.GROUPS.key());
        Study study = catalogManager.getStudyManager().get(studyFqn, options, ownerToken).first();
        for (Group group : study.getGroups()) {
            if (group.getId().equals("@group1")) {
                assertEquals(Collections.singletonList(normalUserId1), group.getUserIds());
            } else if (group.getId().equals("@group2")) {
                assertEquals(Collections.emptyList(), group.getUserIds());
            } else if (group.getId().equals("@group3")) {
                assertEquals(Collections.singletonList(normalUserId1), group.getUserIds());
            }
        }

        study = catalogManager.getStudyManager().get(studyFqn2, options, ownerToken).first();
        for (Group group : study.getGroups()) {
            if (group.getId().equals("@group1")) {
                assertEquals(Collections.singletonList(normalUserId1), group.getUserIds());
            } else if (group.getId().equals("@group2")) {
                assertEquals(Collections.emptyList(), group.getUserIds());
            } else if (group.getId().equals("@group3")) {
                assertEquals(Collections.singletonList(normalUserId1), group.getUserIds());
            }
        }

    }

}