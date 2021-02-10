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

import org.junit.*;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.DataResult;
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
import org.opencb.opencga.core.models.common.CustomStatus;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleAclEntry;
import org.opencb.opencga.core.models.sample.SampleInternal;
import org.opencb.opencga.core.models.study.PermissionRule;
import org.opencb.opencga.core.models.user.User;

import java.io.IOException;
import java.util.*;

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
    Map<String, List<String>> acls;

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
        dbAdaptorFactory.getCatalogSampleDBAdaptor().insert(studyId, new Sample("s1", null, null, null, 1, 1, "", false,
                Collections.emptyList(), new ArrayList<>(), new CustomStatus(), new SampleInternal(new Status()), Collections.emptyMap()),
                Collections.emptyList(), QueryOptions.empty());
        s1 = getSample(studyId, "s1");
        acls = new HashMap<>();
        acls.put(user1.getId(), Arrays.asList());
        acls.put(user2.getId(), Arrays.asList(
                SampleAclEntry.SamplePermissions.VIEW.name(),
                SampleAclEntry.SamplePermissions.VIEW_ANNOTATIONS.name(),
                SampleAclEntry.SamplePermissions.UPDATE.name()
        ));
        aclDBAdaptor.setAcls(Arrays.asList(s1.getUid()), acls, Enums.Resource.SAMPLE);
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
                new AuthorizationManager.CatalogAclParams(Arrays.asList(s1.getUid()), Arrays.asList("VIEW", "UPDATE"), Enums.Resource.SAMPLE)));
        aclDBAdaptor.addToMembers(studyId, Arrays.asList("user4"), Collections.singletonList(
                new AuthorizationManager.CatalogAclParams(Arrays.asList(s1.getUid()), Collections.emptyList(), Enums.Resource.SAMPLE)));
        // We attempt to store the same permissions
        aclDBAdaptor.addToMembers(studyId, Arrays.asList("user1", "user2", "user3"), Collections.singletonList(
                new AuthorizationManager.CatalogAclParams(Arrays.asList(s1.getUid()), Arrays.asList("VIEW", "UPDATE"), Enums.Resource.SAMPLE)));

        DataResult<Map<String, List<String>>> sampleAcl = aclDBAdaptor.get(s1.getUid(), null, Enums.Resource.SAMPLE);
        assertEquals(1, sampleAcl.getNumResults());
        assertEquals(4, sampleAcl.first().size());

        sampleAcl = aclDBAdaptor.get(s1.getUid(), Arrays.asList("user1", "user2"), Enums.Resource.SAMPLE);
        assertEquals(1, sampleAcl.getNumResults());
        assertEquals(2, sampleAcl.first().size());
        assertTrue(sampleAcl.first().get("user1").containsAll(Arrays.asList("VIEW", "UPDATE")));
        assertEquals(2, sampleAcl.first().get("user1").size());
        assertTrue(sampleAcl.first().get("user2").containsAll(Arrays.asList("VIEW", "UPDATE")));
        assertEquals(2, sampleAcl.first().get("user2").size());

        aclDBAdaptor.setToMembers(studyId, Arrays.asList("user1"), Collections.singletonList(
                new AuthorizationManager.CatalogAclParams(Arrays.asList(s1.getUid()), Arrays.asList("DELETE"), Enums.Resource.SAMPLE)));
        sampleAcl = aclDBAdaptor.get(s1.getUid(), Arrays.asList("user1", "user2"), Enums.Resource.SAMPLE);
        assertEquals(1, sampleAcl.getNumResults());
        assertEquals(2, sampleAcl.first().size());

        assertTrue(sampleAcl.first().get("user1").contains("DELETE"));
        assertEquals(1, sampleAcl.first().get("user1").size());
        assertTrue(sampleAcl.first().get("user2").containsAll(Arrays.asList("VIEW", "UPDATE")));
        assertEquals(2, sampleAcl.first().get("user2").size());

        // Remove one permission from one user
        aclDBAdaptor.removeFromMembers(Arrays.asList("user1"), Collections.singletonList(new AuthorizationManager.CatalogAclParams(Arrays.asList(s1.getUid()),
                Arrays.asList("DELETE"), Enums.Resource.SAMPLE)));
        sampleAcl = aclDBAdaptor.get(s1.getUid(), Arrays.asList("user1"), Enums.Resource.SAMPLE);
        assertEquals(1, sampleAcl.getNumResults());
        assertEquals(0, sampleAcl.first().get("user1").size());

        // Reset user
        aclDBAdaptor.removeFromMembers(Arrays.asList("user1"), Collections.singletonList(new AuthorizationManager.CatalogAclParams(Arrays.asList(s1.getUid()),
                null, Enums.Resource.SAMPLE)));
        sampleAcl = aclDBAdaptor.get(s1.getUid(), Arrays.asList("user1"), Enums.Resource.SAMPLE);
        assertEquals(0, sampleAcl.getNumResults());

        // Remove from all samples (there is only one) in study
        aclDBAdaptor.removeFromStudy(studyId, "user3", Enums.Resource.SAMPLE);
        sampleAcl = aclDBAdaptor.get(s1.getUid(), Arrays.asList("user3"), Enums.Resource.SAMPLE);
        assertEquals(0, sampleAcl.getNumResults());

        sampleAcl = aclDBAdaptor.get(s1.getUid(), null, Enums.Resource.SAMPLE);
        assertEquals(1, sampleAcl.getNumResults());
        assertEquals(2, sampleAcl.first().size());

        assertTrue(sampleAcl.first().get("user2").containsAll(Arrays.asList("VIEW", "UPDATE")));
        assertEquals(2, sampleAcl.first().get("user2").size());
        assertEquals(0, sampleAcl.first().get("user4").size());

        // Reset user4
        aclDBAdaptor.removeFromMembers(Arrays.asList("user4"), Collections.singletonList(
                new AuthorizationManager.CatalogAclParams(Arrays.asList(s1.getUid()), null, Enums.Resource.SAMPLE)));
        sampleAcl = aclDBAdaptor.get(s1.getUid(), null, Enums.Resource.SAMPLE);
        assertEquals(1, sampleAcl.getNumResults());
        assertEquals(1, sampleAcl.first().size());
        assertTrue(sampleAcl.first().get("user2").containsAll(Arrays.asList("VIEW", "UPDATE")));
        assertEquals(2, sampleAcl.first().get("user2").size());
    }

    @Test
    public void getSampleAcl() throws Exception {
        DataResult<Map<String, List<String>>> sampleAcl = aclDBAdaptor.get(s1.getUid(), Arrays.asList(user1.getId()), Enums.Resource.SAMPLE);
        Map<String, List<String>> acl = sampleAcl.first();
        assertNotNull(acl);
        assertEquals(acl.get(user1.getId()), acl.get(user1.getId()));

        acl = aclDBAdaptor.get(s1.getUid(), Arrays.asList(user2.getId()), Enums.Resource.SAMPLE).first();
        assertNotNull(acl);
        assertEquals(acl.get(user2.getId()), acl.get(user2.getId()));
    }

    @Test
    public void getSampleAclWrongUser() throws Exception {
        DataResult<Map<String, List<String>>> wrongUser = aclDBAdaptor.get(s1.getUid(), Arrays.asList("wrongUser"), Enums.Resource.SAMPLE);
        assertEquals(0, wrongUser.getNumResults());
    }

    @Test
    public void getSampleAclFromUserWithoutAcl() throws Exception {
        DataResult<Map<String, List<String>>> sampleAcl = aclDBAdaptor.get(s1.getUid(), Arrays.asList(user3.getId()), Enums.Resource.SAMPLE);
        assertTrue(sampleAcl.getResults().isEmpty());
    }

    // Remove some concrete permissions
    @Test
    public void unsetSampleAcl2() throws Exception {
        // Unset permissions
        DataResult<Map<String, List<String>>> sampleAcl = aclDBAdaptor.get(s1.getUid(), Arrays.asList(user2.getId()), Enums.Resource.SAMPLE);
        assertEquals(1, sampleAcl.getNumResults());
        assertEquals(1, sampleAcl.first().size());
        assertEquals(3, sampleAcl.first().get(user2.getId()).size());
        aclDBAdaptor.removeFromMembers(Arrays.asList(user2.getId()), Collections.singletonList(
                new AuthorizationManager.CatalogAclParams(Arrays.asList(s1.getUid()),
                Arrays.asList("VIEW_ANNOTATIONS", "DELETE", "VIEW"), Enums.Resource.SAMPLE)));
//        sampleDBAdaptor.unsetSampleAcl(s1.getId(), Arrays.asList(user2.getId()),
//                Arrays.asList("VIEW_ANNOTATIONS", "DELETE", "VIEW"));
        sampleAcl = aclDBAdaptor.get(s1.getUid(), Arrays.asList(user2.getId()), Enums.Resource.SAMPLE);
        assertEquals(1, sampleAcl.getNumResults());
        assertEquals(1, sampleAcl.first().get(user2.getId()).size());
        assertTrue(sampleAcl.first().get(user2.getId()).containsAll(Arrays.asList(SampleAclEntry.SamplePermissions.UPDATE.name())));
    }

    @Test
    public void setSampleAclOverride() throws Exception {
        assertEquals(acls.get(user2.getId()),
                aclDBAdaptor.get(s1.getUid(), Arrays.asList(user2.getId()), Enums.Resource.SAMPLE).first().get(user2.getId()));

        List<String> newPermissions = Collections.singletonList(SampleAclEntry.SamplePermissions.DELETE.name());
        assertTrue(!acls.get(user2.getId()).equals(newPermissions));

        aclDBAdaptor.setToMembers(studyId, Arrays.asList(user2.getId()), Collections.singletonList(
                new AuthorizationManager.CatalogAclParams(Arrays.asList(s1.getUid()), Arrays.asList(SampleAclEntry.SamplePermissions.DELETE.name()),
                        Enums.Resource.SAMPLE)));
//        sampleDBAdaptor.setSampleAcl(s1.getId(), newAcl, true);

        assertEquals(newPermissions, aclDBAdaptor.get(s1.getUid(), Arrays.asList(user2.getId()), Enums.Resource.SAMPLE).first().get(user2.getId()));
    }

    @Test
    public void testPermissionRulesPlusManualPermissions() throws CatalogException {
        // We create a new sample s2
        dbAdaptorFactory.getCatalogSampleDBAdaptor().insert(studyId, new Sample("s2", null, null, null, 1, 1, "", false,
                Collections.emptyList(), new ArrayList<>(), new CustomStatus(), new SampleInternal(new Status()), Collections.emptyMap()),
                Collections.emptyList(), QueryOptions.empty());
        Sample s2 = getSample(studyId, "s2");

        // We create a new permission rule
        PermissionRule pr = new PermissionRule("myPermissionRule", new Query(), Arrays.asList(user3.getId()),
                Arrays.asList(SampleAclEntry.SamplePermissions.VIEW.name()));
        dbAdaptorFactory.getCatalogStudyDBAdaptor().createPermissionRule(studyId, Enums.Entity.SAMPLES, pr);

        // Apply the permission rule
        aclDBAdaptor.applyPermissionRules(studyId, pr, Enums.Entity.SAMPLES);

        // All the samples should have view permissions for user user2
        DataResult<Map<String, List<String>>> dataResult = aclDBAdaptor.get(Arrays.asList(s1.getUid(), s2.getUid()),
                Arrays.asList(user3.getId()), Enums.Resource.SAMPLE);
        assertEquals(2, dataResult.getNumResults());
        for (Map<String, List<String>> result : dataResult.getResults()) {
            assertTrue(result.get(user3.getId()).contains(SampleAclEntry.SamplePermissions.VIEW.name()));
        }

        // Assign a manual permission to s2
        aclDBAdaptor.addToMembers(studyId, Arrays.asList(user3.getId()), Collections.singletonList(
                new AuthorizationManager.CatalogAclParams(Arrays.asList(s2.getUid()),
                Arrays.asList(SampleAclEntry.SamplePermissions.DELETE.name()), Enums.Resource.SAMPLE)));
    }

}