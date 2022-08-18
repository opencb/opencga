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

package org.opencb.opencga.catalog.db.mongodb;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.biodata.models.common.Status;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationDBAdaptor;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.AclEntry;
import org.opencb.opencga.core.models.AclEntryList;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleInternal;
import org.opencb.opencga.core.models.sample.SamplePermissions;
import org.opencb.opencga.core.models.study.PermissionRule;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Created by pfurio on 21/04/17.
 */
public class AuthorizationMongoDBAdaptorTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private AuthorizationDBAdaptor aclDBAdaptor;
    private DBAdaptorFactory dbAdaptorFactory;
    private User user1;
    private User user2;
    private User user3;
    private long studyId;
    private Sample s1;
    AclEntryList<SamplePermissions> acls;

    @After
    public void after() {
        dbAdaptorFactory.close();
    }

    @Before
    public void before() throws IOException, CatalogException {
        MongoDBAdaptorTest dbAdaptorTest = new MongoDBAdaptorTest();
        dbAdaptorTest.before();

        user1 = MongoDBAdaptorTest.user1;
        user2 = MongoDBAdaptorTest.user2;
        user3 = MongoDBAdaptorTest.user3;
        dbAdaptorFactory = dbAdaptorTest.catalogDBAdaptor;
        aclDBAdaptor = new AuthorizationMongoDBAdaptor(dbAdaptorFactory, dbAdaptorTest.getConfiguration());

        studyId = user3.getProjects().get(0).getStudies().get(0).getUid();
        dbAdaptorFactory.getCatalogSampleDBAdaptor().insert(studyId, new Sample("s1", TimeUtils.getTime(), TimeUtils.getTime(), null, null,
                null, null, 1, 1, "", false, Collections.emptyList(), new ArrayList<>(), new Status(), SampleInternal.init(),
                Collections.emptyMap()), Collections.emptyList(), QueryOptions.empty());
        s1 = getSample(studyId, "s1");

        acls = new AclEntryList<>();
        acls.add(new AclEntry<>(user1.getId(), EnumSet.noneOf(SamplePermissions.class)));
        acls.add(new AclEntry<>(user2.getId(), EnumSet.of(SamplePermissions.VIEW, SamplePermissions.VIEW_ANNOTATIONS, SamplePermissions.WRITE)));
        aclDBAdaptor.setAcls(Collections.singletonList(s1.getUid()), acls, Enums.Resource.SAMPLE);
    }

    Sample getSample(long studyUid, String sampleId) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(SampleDBAdaptor.QueryParams.ID.key(), sampleId);
        return dbAdaptorFactory.getCatalogSampleDBAdaptor().get(query, QueryOptions.empty()).first();
    }

    @Test
    public void addSetGetAndRemoveAcls() throws Exception {
        aclDBAdaptor.resetMembersFromAllEntries(studyId, Arrays.asList(user1.getId(), user2.getId()));

        aclDBAdaptor.addToMembers(studyId, Arrays.asList("user1", "user2", "user3"), Collections.singletonList(
                new AuthorizationManager.CatalogAclParams(Arrays.asList(s1.getUid()),
                        Arrays.asList(SamplePermissions.VIEW.name(), SamplePermissions.WRITE.name()),
                        Enums.Resource.SAMPLE)));
        aclDBAdaptor.addToMembers(studyId, Arrays.asList("user4"), Collections.singletonList(
                new AuthorizationManager.CatalogAclParams(Arrays.asList(s1.getUid()), Collections.emptyList(), Enums.Resource.SAMPLE)));
        // We attempt to store the same permissions
        aclDBAdaptor.addToMembers(studyId, Arrays.asList("user1", "user2", "user3"), Collections.singletonList(
                new AuthorizationManager.CatalogAclParams(Arrays.asList(s1.getUid()),
                        Arrays.asList(SamplePermissions.VIEW.name(), SamplePermissions.WRITE.name()),
                        Enums.Resource.SAMPLE)));

        OpenCGAResult<AclEntryList<SamplePermissions>> sampleAcl =
                aclDBAdaptor.get(s1.getUid(), null, null, Enums.Resource.SAMPLE, SamplePermissions.class);
        assertEquals(1, sampleAcl.getNumResults());
        assertEquals(4, sampleAcl.first().size());

        sampleAcl = aclDBAdaptor.get(s1.getUid(), Arrays.asList("user1", "user2"), null, Enums.Resource.SAMPLE, SamplePermissions.class);
        assertEquals(1, sampleAcl.getNumResults());
        assertEquals(2, sampleAcl.first().size());
        assertEquals("user1", sampleAcl.first().get(0).getMember());
        assertEquals("user2", sampleAcl.first().get(1).getMember());
        assertEquals(2, sampleAcl.first().get(0).getPermissions().size());
        assertTrue(sampleAcl.first().get(0).getPermissions()
                .containsAll(Arrays.asList(SamplePermissions.VIEW, SamplePermissions.WRITE)));
        assertEquals(2, sampleAcl.first().get(1).getPermissions().size());
        assertTrue(sampleAcl.first().get(1).getPermissions()
                .containsAll(Arrays.asList(SamplePermissions.VIEW, SamplePermissions.WRITE)));

        aclDBAdaptor.setToMembers(studyId, Collections.singletonList("user1"), Collections.singletonList(
                new AuthorizationManager.CatalogAclParams(Collections.singletonList(s1.getUid()), Collections.singletonList("DELETE"), Enums.Resource.SAMPLE)));
        sampleAcl = aclDBAdaptor.get(s1.getUid(), Arrays.asList("user1", "user2"), null, Enums.Resource.SAMPLE, SamplePermissions.class);
        assertEquals(1, sampleAcl.getNumResults());
        assertEquals(2, sampleAcl.first().size());

        assertTrue(sampleAcl.first().get(0).getPermissions().contains(SamplePermissions.DELETE));
        assertEquals(1, sampleAcl.first().get(0).getPermissions().size());
        assertTrue(sampleAcl.first().get(1).getPermissions()
                .containsAll(Arrays.asList(SamplePermissions.VIEW, SamplePermissions.WRITE)));
        assertEquals(2, sampleAcl.first().get(1).getPermissions().size());

        // Remove one permission from one user
        aclDBAdaptor.removeFromMembers(Arrays.asList("user1"), Collections.singletonList(new AuthorizationManager.CatalogAclParams(Arrays.asList(s1.getUid()),
                Arrays.asList("DELETE"), Enums.Resource.SAMPLE)));
        sampleAcl = aclDBAdaptor.get(s1.getUid(), Arrays.asList("user1"), null, Enums.Resource.SAMPLE, SamplePermissions.class);
        assertEquals(1, sampleAcl.getNumResults());
        assertEquals(1, sampleAcl.first().get(0).getPermissions().size());
        assertTrue(sampleAcl.first().get(0).getPermissions().contains(SamplePermissions.NONE));

        // Reset user
        aclDBAdaptor.removeFromMembers(Arrays.asList("user1"), Collections.singletonList(new AuthorizationManager.CatalogAclParams(Arrays.asList(s1.getUid()),
                null, Enums.Resource.SAMPLE)));
        sampleAcl = aclDBAdaptor.get(s1.getUid(), Arrays.asList("user1"), null, Enums.Resource.SAMPLE, SamplePermissions.class);
        assertEquals(1, sampleAcl.getNumResults());
        assertEquals(1, sampleAcl.first().size());
        assertEquals("user1", sampleAcl.first().get(0).getMember());
        assertNull(sampleAcl.first().get(0).getPermissions());

        // Remove from all samples (there is only one) in study
        aclDBAdaptor.removeFromStudy(studyId, "user3", Enums.Resource.SAMPLE);
        sampleAcl = aclDBAdaptor.get(s1.getUid(), Arrays.asList("user3"), null, Enums.Resource.SAMPLE, SamplePermissions.class);
        assertEquals(1, sampleAcl.getNumResults());
        assertEquals(1, sampleAcl.first().size());
        assertEquals("user3", sampleAcl.first().get(0).getMember());
        assertNull(sampleAcl.first().get(0).getPermissions());

        sampleAcl = aclDBAdaptor.get(s1.getUid(), null, null, Enums.Resource.SAMPLE, SamplePermissions.class);
        assertEquals(1, sampleAcl.getNumResults());
        assertEquals(2, sampleAcl.first().size());

        assertTrue(sampleAcl.first().stream().map(AclEntry::getMember).collect(Collectors.toSet()).containsAll(Arrays.asList("user2", "user4")));
        for (AclEntry<SamplePermissions> acl : sampleAcl.first()) {
            switch (acl.getMember()) {
                case "user2":
                    assertTrue(acl.getPermissions().containsAll(Arrays.asList(SamplePermissions.VIEW, SamplePermissions.WRITE)));
                    assertEquals(2, acl.getPermissions().size());
                    break;
                case "user4":
                    assertEquals(1, acl.getPermissions().size());
                    assertTrue(acl.getPermissions().contains(SamplePermissions.NONE));
                    break;
                default:
                    break;
            }
        }

        // Reset user4
        aclDBAdaptor.removeFromMembers(Arrays.asList("user4"), Collections.singletonList(
                new AuthorizationManager.CatalogAclParams(Arrays.asList(s1.getUid()), null, Enums.Resource.SAMPLE)));
        sampleAcl = aclDBAdaptor.get(s1.getUid(), null, null, Enums.Resource.SAMPLE, SamplePermissions.class);
        assertEquals(1, sampleAcl.getNumResults());
        assertEquals(1, sampleAcl.first().size());
        for (AclEntry<SamplePermissions> acl : sampleAcl.first()) {
            if ("user2".equals(acl.getMember())) {
                assertTrue(acl.getPermissions().containsAll(Arrays.asList(SamplePermissions.VIEW, SamplePermissions.WRITE)));
                assertEquals(2, acl.getPermissions().size());
            }
        }
    }

    @Test
    public void getSampleAcl() throws Exception {
        OpenCGAResult<AclEntryList<SamplePermissions>> sampleAcl = aclDBAdaptor.get(s1.getUid(), Arrays.asList(user1.getId(), user2.getId()),
                null, Enums.Resource.SAMPLE, SamplePermissions.class);
        assertEquals(1, sampleAcl.getNumResults());
        assertEquals(2, sampleAcl.first().size());
        assertEquals(user1.getId(), sampleAcl.first().get(0).getMember());
        assertEquals(1, sampleAcl.first().get(0).getPermissions().size());
        assertTrue(sampleAcl.first().get(0).getPermissions().contains(SamplePermissions.NONE));

        assertEquals(user1.getId(), sampleAcl.first().get(0).getMember());
        assertEquals(1, sampleAcl.first().get(0).getPermissions().size());
        assertTrue(acls.get(1).getPermissions().containsAll(sampleAcl.first().get(1).getPermissions()));
    }

    @Test
    public void getSampleAclWrongUser() throws Exception {
        OpenCGAResult<AclEntryList<SamplePermissions>> wrongUser = aclDBAdaptor.get(s1.getUid(), Collections.singletonList("wrongUser"), null, Enums.Resource.SAMPLE, SamplePermissions.class);
        assertEquals(1, wrongUser.getNumResults());
        assertEquals(1, wrongUser.first().size());
        assertEquals("wrongUser", wrongUser.first().get(0).getMember());
        assertNull(wrongUser.first().get(0).getPermissions());
        assertTrue(wrongUser.first().get(0).getGroups().isEmpty());
    }

    @Test
    public void getSampleAclFromUserWithoutAcl() throws Exception {
        OpenCGAResult<AclEntryList<SamplePermissions>> sampleAcl = aclDBAdaptor.get(s1.getUid(), Collections.singletonList(user3.getId()),
                null, Enums.Resource.SAMPLE, SamplePermissions.class);
        assertEquals(1, sampleAcl.getNumResults());
        assertEquals(1, sampleAcl.first().size());
        assertEquals(user3.getId(), sampleAcl.first().get(0).getMember());
        assertNull(sampleAcl.first().get(0).getPermissions());
        assertTrue(sampleAcl.first().get(0).getGroups().isEmpty());
    }

    // Remove some concrete permissions
    @Test
    public void unsetSampleAcl2() throws Exception {
        // Unset permissions
        OpenCGAResult<AclEntryList<SamplePermissions>> sampleAcl = aclDBAdaptor.get(s1.getUid(),
                Collections.singletonList(user2.getId()), null, Enums.Resource.SAMPLE, SamplePermissions.class);
        assertEquals(1, sampleAcl.getNumResults());
        assertEquals(1, sampleAcl.first().size());
        assertEquals(3, sampleAcl.first().get(0).getPermissions().size());
        aclDBAdaptor.removeFromMembers(Collections.singletonList(user2.getId()), Collections.singletonList(
                new AuthorizationManager.CatalogAclParams(Collections.singletonList(s1.getUid()),
                        Arrays.asList("VIEW_ANNOTATIONS", "DELETE", "VIEW"), Enums.Resource.SAMPLE)));
//        sampleDBAdaptor.unsetSampleAcl(s1.getId(), Arrays.asList(user2.getId()),
//                Arrays.asList("VIEW_ANNOTATIONS", "DELETE", "VIEW"));
        sampleAcl = aclDBAdaptor.get(s1.getUid(), Collections.singletonList(user2.getId()), null, Enums.Resource.SAMPLE,
                SamplePermissions.class);
        assertEquals(1, sampleAcl.getNumResults());
        assertEquals(1, sampleAcl.first().get(0).getPermissions().size());
        assertTrue(sampleAcl.first().get(0).getPermissions().contains(SamplePermissions.WRITE));
    }

    @Test
    public void setSampleAclOverride() throws Exception {
        // user2 permissions check
        assertEquals(acls.get(1).getPermissions(),
                aclDBAdaptor.get(s1.getUid(), Collections.singletonList(user2.getId()), null, Enums.Resource.SAMPLE, SamplePermissions.class).first().get(0).getPermissions());

        aclDBAdaptor.setToMembers(studyId, Collections.singletonList(user2.getId()), Collections.singletonList(
                new AuthorizationManager.CatalogAclParams(Arrays.asList(s1.getUid()), Arrays.asList(SamplePermissions.DELETE.name()),
                        Enums.Resource.SAMPLE)));

        assertEquals(EnumSet.of(SamplePermissions.DELETE),
                aclDBAdaptor.get(s1.getUid(), Collections.singletonList(user2.getId()), null, Enums.Resource.SAMPLE, SamplePermissions.class).first().get(0).getPermissions());
    }

    @Test
    public void testPermissionRulesPlusManualPermissions() throws CatalogException {
        // We create a new sample s2
        dbAdaptorFactory.getCatalogSampleDBAdaptor().insert(studyId, new Sample("s2", TimeUtils.getTime(), TimeUtils.getTime(), null, null,
                null, null, 1, 1, "", false, Collections.emptyList(), new ArrayList<>(), new Status(), SampleInternal.init(),
                Collections.emptyMap()), Collections.emptyList(), QueryOptions.empty());
        Sample s2 = getSample(studyId, "s2");

        // We create a new permission rule
        PermissionRule pr = new PermissionRule("myPermissionRule", new Query(), Arrays.asList(user3.getId()),
                Arrays.asList(SamplePermissions.VIEW.name()));
        dbAdaptorFactory.getCatalogStudyDBAdaptor().createPermissionRule(studyId, Enums.Entity.SAMPLES, pr);

        // Apply the permission rule
        aclDBAdaptor.applyPermissionRules(studyId, pr, Enums.Entity.SAMPLES);

        // All the samples should have view permissions for user user2
        OpenCGAResult<AclEntryList<SamplePermissions>> dataResult = aclDBAdaptor.get(Arrays.asList(s1.getUid(), s2.getUid()),
                Arrays.asList(user3.getId()), null, Enums.Resource.SAMPLE, SamplePermissions.class);
        assertEquals(2, dataResult.getNumResults());
        for (AclEntryList<SamplePermissions> result : dataResult.getResults()) {
            assertTrue(result.get(0).getPermissions().contains(SamplePermissions.VIEW));
        }

        // Assign a manual permission to s2
        aclDBAdaptor.addToMembers(studyId, Arrays.asList(user3.getId()), Collections.singletonList(
                new AuthorizationManager.CatalogAclParams(Arrays.asList(s2.getUid()),
                        Arrays.asList(SamplePermissions.DELETE.name()), Enums.Resource.SAMPLE)));
    }

}