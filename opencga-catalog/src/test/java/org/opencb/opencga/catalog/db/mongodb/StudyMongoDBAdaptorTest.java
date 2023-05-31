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

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.models.study.*;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by pfurio on 19/01/16.
 */
@Category(MediumTests.class)
public class StudyMongoDBAdaptorTest extends MongoDBAdaptorTest {

    Study getStudy(long projectUid, String studyId) throws CatalogDBException {
        Query query = new Query()
                .append(StudyDBAdaptor.QueryParams.PROJECT_UID.key(), projectUid)
                .append(StudyDBAdaptor.QueryParams.ID.key(), studyId);
        return catalogStudyDBAdaptor.get(query, QueryOptions.empty()).first();
    }

    @Test
    public void updateDiskUsage() throws Exception {
        catalogDBAdaptor.getCatalogStudyDBAdaptor().updateDiskUsage(null, 5, 100);
        assertEquals(2100, catalogStudyDBAdaptor.get(5, null).getResults().get(0).getSize());
        catalogDBAdaptor.getCatalogStudyDBAdaptor().updateDiskUsage(null, 5, -200);
        assertEquals(1900, catalogStudyDBAdaptor.get(5, null).getResults().get(0).getSize());
    }

    /***
     * The test will check whether it is possible to create a new study using an alias that is already being used, but on a different
     * project.
     */
    @Test
    public void createStudySameAliasDifferentProject() throws CatalogException {
        catalogStudyDBAdaptor.insert(user1.getProjects().get(0), new Study("Phase 1", "ph1", "", null, StudyInternal.init(), null, 1), null);
        Study ph1 = getStudy(user1.getProjects().get(0).getUid(), "ph1");
        assertTrue("It is impossible creating an study with an existing alias on a different project.", ph1.getUid() > 0);
    }

    private DataResult<VariableSet> createExampleVariableSet(String name, boolean confidential) throws CatalogDBException {
        Set<Variable> variables = new HashSet<>();
        variables.addAll(Arrays.asList(
                new Variable("NAME", "", Variable.VariableType.STRING, "", true, false, Collections.emptyList(), null, 0, "", "", null,
                        Collections.emptyMap()),
                new Variable("AGE", "", Variable.VariableType.DOUBLE, null, true, false, Collections.singletonList("0:99"), null, 1, "", "",
                        null, Collections.emptyMap()),
                new Variable("HEIGHT", "", Variable.VariableType.DOUBLE, "1.5", false, false, Collections.singletonList("0:"), null, 2, "",
                        "", null, Collections.emptyMap()),
                new Variable("ALIVE", "", Variable.VariableType.BOOLEAN, "", true, false, Collections.emptyList(), null, 3, "", "",
                        null, Collections.emptyMap()),
                new Variable("PHEN", "", Variable.VariableType.CATEGORICAL, "", true, false, Arrays.asList("CASE", "CONTROL"), null, 4, "", "",
                        null, Collections.emptyMap())
        ));
        VariableSet variableSet = new VariableSet(name, name, false, confidential, false, "My description",
                variables, Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), 1, Collections.emptyMap());
        catalogStudyDBAdaptor.createVariableSet(5L, variableSet);

        return catalogStudyDBAdaptor.getVariableSet(5L, name, QueryOptions.empty());
    }

    @Test
    public void createVariableSetTest() throws CatalogDBException {
        DataResult<VariableSet> queryResult = createExampleVariableSet("VARSET_1", false);
        assertEquals("VARSET_1", queryResult.first().getId());
        assertTrue("The id of the variableSet is wrong.", queryResult.first().getUid() > -1);
    }

    @Test
    public void testRemoveFieldFromVariableSet() throws CatalogDBException, CatalogAuthorizationException {
        DataResult<VariableSet> variableSetDataResult = createExampleVariableSet("VARSET_1", false);
        DataResult result =
                catalogStudyDBAdaptor.removeFieldFromVariableSet(variableSetDataResult.first().getUid(), "NAME", user3.getId());
        assertEquals(1, result.getNumUpdated());

        VariableSet variableSet = catalogStudyDBAdaptor.getVariableSet(variableSetDataResult.first().getUid(), QueryOptions.empty()).first();

        assertTrue(variableSet.getVariables()
                .stream()
                .filter(v -> "NAME".equals(v.getId()))
                .collect(Collectors.toList()).isEmpty());
    }

    // TODO: Uncomment when renames are working again.
//    @Test
//    public void testRenameFieldInVariableSet() throws CatalogDBException, CatalogAuthorizationException {
//        DataResult<VariableSet> variableSetDataResult = createExampleVariableSet("VARSET_1", false);
//        catalogStudyDBAdaptor.renameFieldVariableSet(variableSetDataResult.first().getId(), "NAME", "NEW_NAME",
//                user3.getId());
//    }
//
//    @Test
//    public void testRenameFieldInVariableSetOldFieldNotExist() throws CatalogDBException, CatalogAuthorizationException {
//        DataResult<VariableSet> variableSetDataResult = createExampleVariableSet("VARSET_1", false);
//        thrown.expect(CatalogDBException.class);
//        thrown.expectMessage("NAM} does not exist.");
//        catalogStudyDBAdaptor.renameFieldVariableSet(variableSetDataResult.first().getId(), "NAM", "NEW_NAME",
//                user3.getId());
//    }
//
//    @Test
//    public void testRenameFieldInVariableSetNewFieldExist() throws CatalogDBException, CatalogAuthorizationException {
//        DataResult<VariableSet> variableSetDataResult = createExampleVariableSet("VARSET_1", false);
//        thrown.expect(CatalogDBException.class);
//        thrown.expectMessage("The variable {id: AGE} already exists.");
//        catalogStudyDBAdaptor.renameFieldVariableSet(variableSetDataResult.first().getId(), "NAME", "AGE", user3.getId());
//    }
//
//    @Test
//    public void testRenameFieldInVariableSetVariableSetNotExist() throws CatalogDBException, CatalogAuthorizationException {
//        createExampleVariableSet("VARSET_1", false);
//        thrown.expect(CatalogDBException.class);
//        thrown.expectMessage("not found");
//        catalogStudyDBAdaptor.renameFieldVariableSet(-1, "NAME", "NEW_NAME", user3.getId());
//    }

    /**
     * Creates a new variable once and attempts to create the same one again.
     *
     * @throws CatalogDBException
     */
    @Test
    public void addFieldToVariableSetTest1() throws CatalogDBException, CatalogAuthorizationException {
        createExampleVariableSet("VARSET_1", false);
        createExampleVariableSet("VARSET_2", true);
        Variable variable = new Variable("NAM", "", Variable.VariableType.STRING, "", true, false, Collections.emptyList(), null, 0, "", "", null,
                Collections.emptyMap());
        DataResult result = catalogStudyDBAdaptor.addFieldToVariableSet(18, variable, user3.getId());
        assertEquals(1, result.getNumUpdated());

        DataResult<VariableSet> queryResult = catalogStudyDBAdaptor.getVariableSet(18L, QueryOptions.empty());

        // Check that the new variable has been inserted in the variableSet
        assertTrue(queryResult.first().getVariables().stream().filter(variable1 -> variable.getId().equals(variable1.getId())).findAny()
                .isPresent());

        // We try to insert the same one again.
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("already exist");
        catalogStudyDBAdaptor.addFieldToVariableSet(18, variable, user3.getId());
    }

    /**
     * Tries to add a new variable to a non existent variableSet.
     *
     * @throws CatalogDBException
     */
    @Test
    public void addFieldToVariableSetTest2() throws CatalogDBException, CatalogAuthorizationException {
        Variable variable = new Variable("NAM", "", Variable.VariableType.STRING, "", true, false, Collections.emptyList(), null, 0, "", "", null,
                Collections.emptyMap());
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("not found");
        catalogStudyDBAdaptor.addFieldToVariableSet(18, variable, user3.getId());
    }

    @Test
    public void createGroup() throws CatalogDBException {
        catalogStudyDBAdaptor.createGroup(5L, new Group("name", Arrays.asList("user1", "user2")));
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("Group already existed");
        catalogStudyDBAdaptor.createGroup(5L, new Group("name", Arrays.asList("user1", "user2")));
    }

    @Test
    public void removeUsersFromAllGroups() throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        catalogStudyDBAdaptor.createGroup(5L, new Group("name1", Arrays.asList("user1", "user2")));
        catalogStudyDBAdaptor.createGroup(5L, new Group("name2", Arrays.asList("user1", "user2", "user3")));
        catalogStudyDBAdaptor.createGroup(5L, new Group("name3", Arrays.asList("user1", "user3")));

        DataResult<Group> group = catalogStudyDBAdaptor.getGroup(5L, null, Arrays.asList("user1", "user3"));
        assertEquals(3, group.getNumResults());
        catalogStudyDBAdaptor.removeUsersFromAllGroups(5L, Arrays.asList("user1", "user3"));
        group = catalogStudyDBAdaptor.getGroup(5L, null, Arrays.asList("user1", "user3"));
        assertEquals(0, group.getNumResults());
    }

    @Test
    public void resyncUserWithSyncedGroups() throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        // We create synced groups and not synced groups in study 5
        Group group = new Group("@notSyncedGroup", Arrays.asList("user1", "user2", "user3"));
        catalogStudyDBAdaptor.createGroup(5L, group);
        group.setId("@syncedGroup1");
        group.setSyncedFrom(new Group.Sync("origin1", "@syncedGroup1"));
        catalogStudyDBAdaptor.createGroup(5L, group);
        group.setId("@syncedGroup2");
        group.setSyncedFrom(new Group.Sync("origin1", "@syncedGroup2"));
        catalogStudyDBAdaptor.createGroup(5L, group);
        group.setId("@syncedGroup3");
        group.setSyncedFrom(new Group.Sync("otherOrigin", "@syncedGroup3"));
        catalogStudyDBAdaptor.createGroup(5L, group);
        group = new Group("@otherNotSyncedGroup", Arrays.asList("user1", "user3"));
        catalogStudyDBAdaptor.createGroup(5L, group);

        // We create the same synced groups and not synced groups in study 9
        group = new Group("@notSyncedGroup", Arrays.asList("user1", "user2", "user3"));
        catalogStudyDBAdaptor.createGroup(9L, group);
        group.setId("@syncedGroup1");
        group.setSyncedFrom(new Group.Sync("origin1", "@syncedGroup1"));
        catalogStudyDBAdaptor.createGroup(9L, group);
        group.setId("@syncedGroup2");
        group.setSyncedFrom(new Group.Sync("origin1", "@syncedGroup2"));
        catalogStudyDBAdaptor.createGroup(9L, group);
        group.setId("@syncedGroup3");
        group.setSyncedFrom(new Group.Sync("otherOrigin", "@syncedGroup3"));
        catalogStudyDBAdaptor.createGroup(9L, group);
        group = new Group("@otherNotSyncedGroup", Arrays.asList("user1", "user3"));
        catalogStudyDBAdaptor.createGroup(9L, group);

        catalogStudyDBAdaptor.resyncUserWithSyncedGroups("user2", Collections.emptyList(), "origin1");
        DataResult<Group> groupsStudy1 = catalogStudyDBAdaptor.getGroup(5L, null, Arrays.asList("user2"));
        DataResult<Group> groupsStudy2 = catalogStudyDBAdaptor.getGroup(9L, null, Arrays.asList("user2"));
        assertEquals(groupsStudy1.getNumResults(), groupsStudy2.getNumResults());
        assertEquals(2, groupsStudy1.getNumResults());
        assertTrue(groupsStudy1.getResults().stream().map(Group::getId).collect(Collectors.toList())
                .containsAll(Arrays.asList("@notSyncedGroup", "@syncedGroup3")));

        // Nothing should change with this resync. Group1 doesn't exist and syncedGroup3 is not from origin1.
        // But because this time it will try to insert users to groups, user2 will be automatically added to group @members
        catalogStudyDBAdaptor.resyncUserWithSyncedGroups("user2", Arrays.asList("@group1", "@syncedGroup3"), "origin1");
        groupsStudy1 = catalogStudyDBAdaptor.getGroup(5L, null, Arrays.asList("user2"));
        groupsStudy2 = catalogStudyDBAdaptor.getGroup(9L, null, Arrays.asList("user2"));
        assertEquals(groupsStudy1.getNumResults(), groupsStudy2.getNumResults());
        assertEquals(3, groupsStudy1.getNumResults());
        assertTrue(groupsStudy1.getResults().stream().map(Group::getId).collect(Collectors.toList())
                .containsAll(Arrays.asList("@notSyncedGroup", "@syncedGroup3", "@members")));

        // Now we add one new user that will have to be added to @syncedGroup3 only. It didn't still exist there
        catalogStudyDBAdaptor.resyncUserWithSyncedGroups("user5", Arrays.asList("@group1", "@syncedGroup3"), "otherOrigin");
        groupsStudy1 = catalogStudyDBAdaptor.getGroup(5L, null, Arrays.asList("user5"));
        groupsStudy2 = catalogStudyDBAdaptor.getGroup(9L, null, Arrays.asList("user5"));
        assertEquals(groupsStudy1.getNumResults(), groupsStudy2.getNumResults());
        assertEquals(2, groupsStudy1.getNumResults());
        assertTrue(groupsStudy1.getResults().stream().map(Group::getId).collect(Collectors.toList())
                .containsAll(Arrays.asList("@syncedGroup3", "@members")));

        catalogStudyDBAdaptor.resyncUserWithSyncedGroups("user2", Arrays.asList("@group1", "@syncedGroup2", "@syncedGroup3"), "origin1");
        groupsStudy1 = catalogStudyDBAdaptor.getGroup(5L, null, Arrays.asList("user2"));
        groupsStudy2 = catalogStudyDBAdaptor.getGroup(9L, null, Arrays.asList("user2"));
        assertEquals(groupsStudy1.getNumResults(), groupsStudy2.getNumResults());
        assertEquals(4, groupsStudy1.getNumResults());
        assertTrue(groupsStudy1.getResults().stream().map(Group::getId).collect(Collectors.toList())
                .containsAll(Arrays.asList("@notSyncedGroup", "@syncedGroup2", "@syncedGroup3", "@members")));
    }

    @Test
    public void updateUserToGroups() throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        // We create synced groups and not synced groups in study 5
        Group group = new Group("@notSyncedGroup", Collections.emptyList());
        catalogStudyDBAdaptor.createGroup(5L, group);
        group.setId("@syncedGroup1");
        group.setSyncedFrom(new Group.Sync("origin1", "@syncedGroup1"));
        catalogStudyDBAdaptor.createGroup(5L, group);
        group.setId("@syncedGroup2");
        group.setSyncedFrom(new Group.Sync("origin1", "@syncedGroup2"));
        catalogStudyDBAdaptor.createGroup(5L, group);
        group.setId("@syncedGroup3");
        group.setSyncedFrom(new Group.Sync("otherOrigin", "@syncedGroup3"));
        catalogStudyDBAdaptor.createGroup(5L, group);

        // We create the same synced groups and not synced groups in study 9
        group = new Group("@notSyncedGroup", Collections.emptyList());
        catalogStudyDBAdaptor.createGroup(9L, group);
        group.setId("@syncedGroup1");
        group.setSyncedFrom(new Group.Sync("origin1", "@syncedGroup1"));
        catalogStudyDBAdaptor.createGroup(9L, group);
        group.setId("@syncedGroup2");
        group.setSyncedFrom(new Group.Sync("origin1", "@syncedGroup2"));
        catalogStudyDBAdaptor.createGroup(9L, group);
        group.setId("@syncedGroup3");
        group.setSyncedFrom(new Group.Sync("otherOrigin", "@syncedGroup3"));
        catalogStudyDBAdaptor.createGroup(9L, group);
        group = new Group("@otherNotSyncedGroup", Collections.emptyList());
        catalogStudyDBAdaptor.createGroup(9L, group);

        catalogStudyDBAdaptor.updateUserFromGroups("user2", null, Arrays.asList("syncedGroup1", "notSyncedGroup"), ParamUtils.AddRemoveAction.ADD);
        DataResult<Group> groupsStudy1 = catalogStudyDBAdaptor.getGroup(5L, null, Arrays.asList("user2"));
        DataResult<Group> groupsStudy2 = catalogStudyDBAdaptor.getGroup(9L, null, Arrays.asList("user2"));
        assertEquals(groupsStudy1.getNumResults(), groupsStudy2.getNumResults());
        assertEquals(3, groupsStudy1.getNumResults());
        assertTrue(groupsStudy1.getResults().stream().map(Group::getId).collect(Collectors.toList())
                .containsAll(Arrays.asList("@notSyncedGroup", "@syncedGroup1", "@members")));
        assertEquals(3, groupsStudy2.getNumResults());
        assertTrue(groupsStudy2.getResults().stream().map(Group::getId).collect(Collectors.toList())
                .containsAll(Arrays.asList("@notSyncedGroup", "@syncedGroup1", "@members")));

        catalogStudyDBAdaptor.updateUserFromGroups("user2", null, Arrays.asList("syncedGroup1", "notSyncedGroup"), ParamUtils.AddRemoveAction.REMOVE);
        groupsStudy1 = catalogStudyDBAdaptor.getGroup(5L, null, Arrays.asList("user2"));
        groupsStudy2 = catalogStudyDBAdaptor.getGroup(9L, null, Arrays.asList("user2"));
        assertEquals(1, groupsStudy1.getNumResults());
        assertEquals(1, groupsStudy2.getNumResults());
        assertEquals("@members", groupsStudy1.first().getId());
        assertEquals("@members", groupsStudy2.first().getId());

        catalogStudyDBAdaptor.updateUserFromGroups("user2", Arrays.asList(5L, 9L), Arrays.asList("syncedGroup1", "notSyncedGroup"), ParamUtils.AddRemoveAction.ADD);
        groupsStudy1 = catalogStudyDBAdaptor.getGroup(5L, null, Arrays.asList("user2"));
        groupsStudy2 = catalogStudyDBAdaptor.getGroup(9L, null, Arrays.asList("user2"));
        assertEquals(groupsStudy1.getNumResults(), groupsStudy2.getNumResults());
        assertEquals(3, groupsStudy1.getNumResults());
        assertTrue(groupsStudy1.getResults().stream().map(Group::getId).collect(Collectors.toList())
                .containsAll(Arrays.asList("@notSyncedGroup", "@syncedGroup1", "@members")));
        assertEquals(3, groupsStudy2.getNumResults());
        assertTrue(groupsStudy2.getResults().stream().map(Group::getId).collect(Collectors.toList())
                .containsAll(Arrays.asList("@notSyncedGroup", "@syncedGroup1", "@members")));

        catalogStudyDBAdaptor.updateUserFromGroups("user2", Collections.singletonList(5L), Arrays.asList("syncedGroup1", "notSyncedGroup"), ParamUtils.AddRemoveAction.REMOVE);
        groupsStudy1 = catalogStudyDBAdaptor.getGroup(5L, null, Arrays.asList("user2"));
        groupsStudy2 = catalogStudyDBAdaptor.getGroup(9L, null, Arrays.asList("user2"));
        assertEquals(1, groupsStudy1.getNumResults());
        assertEquals("@members", groupsStudy1.first().getId());
        assertEquals(3, groupsStudy2.getNumResults());
        assertTrue(groupsStudy2.getResults().stream().map(Group::getId).collect(Collectors.toList())
                .containsAll(Arrays.asList("@notSyncedGroup", "@syncedGroup1", "@members")));
    }

}
