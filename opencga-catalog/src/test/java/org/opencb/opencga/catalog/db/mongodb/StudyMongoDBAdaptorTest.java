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
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.FqnUtils;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Group;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.Variable;
import org.opencb.opencga.core.models.study.VariableSet;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Created by pfurio on 19/01/16.
 */
@Category(MediumTests.class)
public class StudyMongoDBAdaptorTest extends AbstractMongoDBAdaptorTest {

    @Test
    public void updateDiskUsage() throws Exception {
        long size = catalogStudyDBAdaptor.get(studyUid, null).getResults().get(0).getSize();
        catalogStudyDBAdaptor.updateDiskUsage(null, studyUid, 100);
        assertEquals(size + 100, catalogStudyDBAdaptor.get(studyUid, null).getResults().get(0).getSize());
        catalogStudyDBAdaptor.updateDiskUsage(null, studyUid, -200);
        assertEquals(size - 100, catalogStudyDBAdaptor.get(studyUid, null).getResults().get(0).getSize());
    }

    private Study getMinimalStudyInstance(String projectId, String id) {
        return new Study()
                .setId(id)
                .setFqn(FqnUtils.buildFqn(organizationId, projectId, id));
    }
    
    /***
     * The test will check whether it is possible to create a new study using an id that is already being used, but on a different
     * project.
     */
    @Test
    public void createStudyWithSameIdInDifferentProject() throws CatalogException {
        Project project = getProject(project1);
        OpenCGAResult<Study> create = catalogStudyDBAdaptor.insert(project, getMinimalStudyInstance(project1, "studyId"), null);
        assertEquals(1, create.getNumInserted());
        Study ph1 = getStudy(project.getUid(), "studyId");
        assertNotNull(ph1);

        project = getProject(project2);
        create = catalogStudyDBAdaptor.insert(project, getMinimalStudyInstance(project2, "studyId"), null);
        assertEquals(1, create.getNumInserted());
        ph1 = getStudy(project.getUid(), "studyId");
        assertNotNull(ph1);
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
        catalogStudyDBAdaptor.createVariableSet(studyUid, variableSet);

        return catalogStudyDBAdaptor.getVariableSet(studyUid, name, QueryOptions.empty());
    }

    @Test
    public void createVariableSetTest() throws CatalogDBException {
        DataResult<VariableSet> queryResult = createExampleVariableSet("VARSET_1", false);
        assertEquals("VARSET_1", queryResult.first().getId());
        assertTrue("The id of the variableSet is wrong.", queryResult.first().getUid() > -1);
    }

    @Test
    public void testRemoveFieldFromVariableSet() throws CatalogException {
        DataResult<VariableSet> variableSetDataResult = createExampleVariableSet("VARSET_1", false);
        DataResult result = catalogStudyDBAdaptor.removeFieldFromVariableSet(5L, variableSetDataResult.first().getUid(), "NAME",
                orgAdminUserId1);
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
//                normalUserId1);
//    }
//
//    @Test
//    public void testRenameFieldInVariableSetOldFieldNotExist() throws CatalogDBException, CatalogAuthorizationException {
//        DataResult<VariableSet> variableSetDataResult = createExampleVariableSet("VARSET_1", false);
//        thrown.expect(CatalogDBException.class);
//        thrown.expectMessage("NAM} does not exist.");
//        catalogStudyDBAdaptor.renameFieldVariableSet(variableSetDataResult.first().getId(), "NAM", "NEW_NAME",
//                normalUserId1);
//    }
//
//    @Test
//    public void testRenameFieldInVariableSetNewFieldExist() throws CatalogDBException, CatalogAuthorizationException {
//        DataResult<VariableSet> variableSetDataResult = createExampleVariableSet("VARSET_1", false);
//        thrown.expect(CatalogDBException.class);
//        thrown.expectMessage("The variable {id: AGE} already exists.");
//        catalogStudyDBAdaptor.renameFieldVariableSet(variableSetDataResult.first().getId(), "NAME", "AGE", normalUserId1);
//    }
//
//    @Test
//    public void testRenameFieldInVariableSetVariableSetNotExist() throws CatalogDBException, CatalogAuthorizationException {
//        createExampleVariableSet("VARSET_1", false);
//        thrown.expect(CatalogDBException.class);
//        thrown.expectMessage("not found");
//        catalogStudyDBAdaptor.renameFieldVariableSet(-1, "NAME", "NEW_NAME", normalUserId1);
//    }

    /**
     * Creates a new variable once and attempts to create the same one again.
     *
     * @throws CatalogDBException
     */
    @Test
    public void addFieldToVariableSetTest1() throws CatalogException {
        DataResult<VariableSet> varset1 = createExampleVariableSet("VARSET_1", false);
        createExampleVariableSet("VARSET_2", true);
        Variable variable = new Variable("NAM", "", Variable.VariableType.STRING, "", true, false, Collections.emptyList(), null, 0, "", "", null,
                Collections.emptyMap());
        DataResult result = catalogStudyDBAdaptor.addFieldToVariableSet(5L, varset1.first().getUid(), variable, orgAdminUserId1);
        assertEquals(1, result.getNumUpdated());

        DataResult<VariableSet> queryResult = catalogStudyDBAdaptor.getVariableSet(varset1.first().getUid(), QueryOptions.empty());

        // Check that the new variable has been inserted in the variableSet
        assertTrue(queryResult.first().getVariables().stream().filter(variable1 -> variable.getId().equals(variable1.getId())).findAny()
                .isPresent());

        // We try to insert the same one again.
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("already exist");
        catalogStudyDBAdaptor.addFieldToVariableSet(5L, varset1.first().getUid(), variable, orgAdminUserId1);
    }

    /**
     * Tries to add a new variable to a non existent variableSet.
     *
     * @throws CatalogDBException
     */
    @Test
    public void addFieldToVariableSetTest2() throws CatalogException {
        Variable variable = new Variable("NAM", "", Variable.VariableType.STRING, "", true, false, Collections.emptyList(), null, 0, "", "",
                null, Collections.emptyMap());
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("not found");
        catalogStudyDBAdaptor.addFieldToVariableSet(5L, 2L, variable, orgAdminUserId1);
    }

    @Test
    public void createGroup() throws CatalogDBException {
        catalogStudyDBAdaptor.createGroup(studyUid, new Group("name", Arrays.asList(normalUserId1, normalUserId2)));
        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("Group already existed");
        catalogStudyDBAdaptor.createGroup(studyUid, new Group("name", Arrays.asList(normalUserId1, normalUserId2)));
    }

    @Test
    public void removeUsersFromAllGroups() throws CatalogException {
        catalogStudyDBAdaptor.createGroup(studyUid, new Group("name1", Arrays.asList(normalUserId1, normalUserId2)));
        catalogStudyDBAdaptor.createGroup(studyUid, new Group("name2", Arrays.asList(normalUserId1, normalUserId2, normalUserId3)));
        catalogStudyDBAdaptor.createGroup(studyUid, new Group("name3", Arrays.asList(normalUserId1, normalUserId3)));

        DataResult<Group> group = catalogStudyDBAdaptor.getGroup(studyUid, null, Arrays.asList(normalUserId1, normalUserId3));
        assertEquals(5, group.getNumResults());
        catalogStudyDBAdaptor.removeUsersFromAllGroups(studyUid, Arrays.asList(normalUserId1, normalUserId3));
        group = catalogStudyDBAdaptor.getGroup(studyUid, null, Arrays.asList(normalUserId1, normalUserId3));
        assertEquals(0, group.getNumResults());
    }

    @Test
    public void resyncUserWithSyncedGroups() throws CatalogException {
        // We create synced groups and not synced groups in study studyUid
        Group group = new Group("@notSyncedGroup", Arrays.asList(normalUserId1, normalUserId2, normalUserId3));
        catalogStudyDBAdaptor.createGroup(studyUid, group);
        group.setId("@syncedGroup1");
        group.setSyncedFrom(new Group.Sync("origin1", "@syncedGroup1"));
        catalogStudyDBAdaptor.createGroup(studyUid, group);
        group.setId("@syncedGroup2");
        group.setSyncedFrom(new Group.Sync("origin1", "@syncedGroup2"));
        catalogStudyDBAdaptor.createGroup(studyUid, group);
        group.setId("@syncedGroup3");
        group.setSyncedFrom(new Group.Sync("otherOrigin", "@syncedGroup3"));
        catalogStudyDBAdaptor.createGroup(studyUid, group);
        group = new Group("@otherNotSyncedGroup", Arrays.asList(normalUserId1, normalUserId3));
        catalogStudyDBAdaptor.createGroup(studyUid, group);

        // We create the same synced groups and not synced groups in study studyUid2
        group = new Group("@notSyncedGroup", Arrays.asList(normalUserId1, normalUserId2, normalUserId3));
        catalogStudyDBAdaptor.createGroup(studyUid2, group);
        group.setId("@syncedGroup1");
        group.setSyncedFrom(new Group.Sync("origin1", "@syncedGroup1"));
        catalogStudyDBAdaptor.createGroup(studyUid2, group);
        group.setId("@syncedGroup2");
        group.setSyncedFrom(new Group.Sync("origin1", "@syncedGroup2"));
        catalogStudyDBAdaptor.createGroup(studyUid2, group);
        group.setId("@syncedGroup3");
        group.setSyncedFrom(new Group.Sync("otherOrigin", "@syncedGroup3"));
        catalogStudyDBAdaptor.createGroup(studyUid2, group);
        group = new Group("@otherNotSyncedGroup", Arrays.asList(normalUserId1, normalUserId3));
        catalogStudyDBAdaptor.createGroup(studyUid2, group);

        catalogStudyDBAdaptor.resyncUserWithSyncedGroups(normalUserId2, Collections.emptyList(), "origin1");
        DataResult<Group> groupsStudy1 = catalogStudyDBAdaptor.getGroup(studyUid, null, Arrays.asList(normalUserId2));
        DataResult<Group> groupsStudy2 = catalogStudyDBAdaptor.getGroup(studyUid2, null, Arrays.asList(normalUserId2));
        assertEquals(4, groupsStudy1.getNumResults());
        assertEquals(2, groupsStudy2.getNumResults());
        assertTrue(groupsStudy1.getResults().stream().map(Group::getId).collect(Collectors.toList())
                .containsAll(Arrays.asList("@notSyncedGroup", "@syncedGroup3")));
        assertTrue(groupsStudy2.getResults().stream().map(Group::getId).collect(Collectors.toList())
                .containsAll(Arrays.asList("@notSyncedGroup", "@syncedGroup3")));

        // Nothing should change with this resync. Group1 doesn't exist and syncedGroup3 is not from origin1.
        // But because this time it will try to insert users to groups, user2 will be automatically added to group @members
        catalogStudyDBAdaptor.resyncUserWithSyncedGroups(normalUserId2, Arrays.asList("@group1", "@syncedGroup3"), "origin1");
//        groupsStudy1 = catalogStudyDBAdaptor.getGroup(studyUid, null, Arrays.asList(normalUserId2));
        groupsStudy2 = catalogStudyDBAdaptor.getGroup(studyUid2, null, Arrays.asList(normalUserId2));
//        assertEquals(groupsStudy1.getNumResults(), groupsStudy2.getNumResults());
        assertEquals(3, groupsStudy2.getNumResults());
        assertTrue(groupsStudy2.getResults().stream().map(Group::getId).collect(Collectors.toList())
                .containsAll(Arrays.asList("@notSyncedGroup", "@syncedGroup3", ParamConstants.MEMBERS_GROUP)));

        // Now we add one new user that will have to be added to @syncedGroup3 only. It didn't still exist there
        catalogStudyDBAdaptor.resyncUserWithSyncedGroups("user5", Arrays.asList("@group1", "@syncedGroup3"), "otherOrigin");
        groupsStudy1 = catalogStudyDBAdaptor.getGroup(studyUid, null, Arrays.asList("user5"));
        groupsStudy2 = catalogStudyDBAdaptor.getGroup(studyUid2, null, Arrays.asList("user5"));
        assertEquals(groupsStudy1.getNumResults(), groupsStudy2.getNumResults());
        assertEquals(2, groupsStudy1.getNumResults());
        assertTrue(groupsStudy1.getResults().stream().map(Group::getId).collect(Collectors.toList())
                .containsAll(Arrays.asList("@syncedGroup3", ParamConstants.MEMBERS_GROUP)));

        catalogStudyDBAdaptor.resyncUserWithSyncedGroups(normalUserId2, Arrays.asList("@group1", "@syncedGroup2", "@syncedGroup3"), "origin1");
        groupsStudy1 = catalogStudyDBAdaptor.getGroup(studyUid, null, Arrays.asList(normalUserId2));
        groupsStudy2 = catalogStudyDBAdaptor.getGroup(studyUid2, null, Arrays.asList(normalUserId2));
        assertEquals(5, groupsStudy1.getNumResults());
        assertEquals(4, groupsStudy2.getNumResults());
        assertTrue(groupsStudy1.getResults().stream().map(Group::getId).collect(Collectors.toList())
                .containsAll(Arrays.asList("@notSyncedGroup", "@syncedGroup2", "@syncedGroup3", ParamConstants.MEMBERS_GROUP)));
        assertTrue(groupsStudy2.getResults().stream().map(Group::getId).collect(Collectors.toList())
                .containsAll(Arrays.asList("@notSyncedGroup", "@syncedGroup2", "@syncedGroup3", ParamConstants.MEMBERS_GROUP)));
    }

    @Test
    public void updateUserToGroups() throws CatalogException {
        // We create synced groups and not synced groups in study studyUid
        Group group = new Group("@notSyncedGroup", Collections.emptyList());
        catalogStudyDBAdaptor.createGroup(studyUid, group);
        group.setId("@syncedGroup1");
        group.setSyncedFrom(new Group.Sync("origin1", "@syncedGroup1"));
        catalogStudyDBAdaptor.createGroup(studyUid, group);
        group.setId("@syncedGroup2");
        group.setSyncedFrom(new Group.Sync("origin1", "@syncedGroup2"));
        catalogStudyDBAdaptor.createGroup(studyUid, group);
        group.setId("@syncedGroup3");
        group.setSyncedFrom(new Group.Sync("otherOrigin", "@syncedGroup3"));
        catalogStudyDBAdaptor.createGroup(studyUid, group);

        // We create the same synced groups and not synced groups in study studyUid2
        group = new Group("@notSyncedGroup", Collections.emptyList());
        catalogStudyDBAdaptor.createGroup(studyUid2, group);
        group.setId("@syncedGroup1");
        group.setSyncedFrom(new Group.Sync("origin1", "@syncedGroup1"));
        catalogStudyDBAdaptor.createGroup(studyUid2, group);
        group.setId("@syncedGroup2");
        group.setSyncedFrom(new Group.Sync("origin1", "@syncedGroup2"));
        catalogStudyDBAdaptor.createGroup(studyUid2, group);
        group.setId("@syncedGroup3");
        group.setSyncedFrom(new Group.Sync("otherOrigin", "@syncedGroup3"));
        catalogStudyDBAdaptor.createGroup(studyUid2, group);
        group = new Group("@otherNotSyncedGroup", Collections.emptyList());
        catalogStudyDBAdaptor.createGroup(studyUid2, group);

        catalogStudyDBAdaptor.updateUserFromGroups(normalUserId2, null, Arrays.asList("syncedGroup1", "notSyncedGroup"), ParamUtils.AddRemoveAction.ADD);
        DataResult<Group> groupsStudy1 = catalogStudyDBAdaptor.getGroup(studyUid, null, Arrays.asList(normalUserId2));
        DataResult<Group> groupsStudy2 = catalogStudyDBAdaptor.getGroup(studyUid2, null, Arrays.asList(normalUserId2));
        assertEquals(4, groupsStudy1.getNumResults());
        assertTrue(groupsStudy1.getResults().stream().map(Group::getId).collect(Collectors.toList())
                .containsAll(Arrays.asList(restrictedGroup, "@notSyncedGroup", "@syncedGroup1", ParamConstants.MEMBERS_GROUP)));
        assertEquals(3, groupsStudy2.getNumResults());
        assertTrue(groupsStudy2.getResults().stream().map(Group::getId).collect(Collectors.toList())
                .containsAll(Arrays.asList("@notSyncedGroup", "@syncedGroup1", ParamConstants.MEMBERS_GROUP)));

        catalogStudyDBAdaptor.updateUserFromGroups(normalUserId2, null, Arrays.asList("syncedGroup1", "notSyncedGroup"), ParamUtils.AddRemoveAction.REMOVE);
        groupsStudy1 = catalogStudyDBAdaptor.getGroup(studyUid, null, Arrays.asList(normalUserId2));
        groupsStudy2 = catalogStudyDBAdaptor.getGroup(studyUid2, null, Arrays.asList(normalUserId2));
        assertEquals(2, groupsStudy1.getNumResults());
        assertEquals(1, groupsStudy2.getNumResults());
        assertTrue(groupsStudy1.getResults().stream().map(Group::getId).collect(Collectors.toList())
                .containsAll(Arrays.asList(restrictedGroup, ParamConstants.MEMBERS_GROUP)));
        assertEquals(ParamConstants.MEMBERS_GROUP, groupsStudy2.first().getId());

        catalogStudyDBAdaptor.updateUserFromGroups(normalUserId2, Arrays.asList(studyUid, studyUid2), Arrays.asList("syncedGroup1", "notSyncedGroup"), ParamUtils.AddRemoveAction.ADD);
        groupsStudy1 = catalogStudyDBAdaptor.getGroup(studyUid, null, Arrays.asList(normalUserId2));
        groupsStudy2 = catalogStudyDBAdaptor.getGroup(studyUid2, null, Arrays.asList(normalUserId2));
        assertEquals(4, groupsStudy1.getNumResults());
        assertTrue(groupsStudy1.getResults().stream().map(Group::getId).collect(Collectors.toList())
                .containsAll(Arrays.asList(restrictedGroup, "@notSyncedGroup", "@syncedGroup1", ParamConstants.MEMBERS_GROUP)));
        assertEquals(3, groupsStudy2.getNumResults());
        assertTrue(groupsStudy2.getResults().stream().map(Group::getId).collect(Collectors.toList())
                .containsAll(Arrays.asList("@notSyncedGroup", "@syncedGroup1", ParamConstants.MEMBERS_GROUP)));

        catalogStudyDBAdaptor.updateUserFromGroups(normalUserId2, Collections.singletonList(studyUid), Arrays.asList("syncedGroup1", "notSyncedGroup"), ParamUtils.AddRemoveAction.REMOVE);
        groupsStudy1 = catalogStudyDBAdaptor.getGroup(studyUid, null, Arrays.asList(normalUserId2));
        groupsStudy2 = catalogStudyDBAdaptor.getGroup(studyUid2, null, Arrays.asList(normalUserId2));
        assertEquals(2, groupsStudy1.getNumResults());
        assertTrue(groupsStudy1.getResults().stream().map(Group::getId).collect(Collectors.toList())
                .containsAll(Arrays.asList(restrictedGroup, ParamConstants.MEMBERS_GROUP)));
        assertEquals(3, groupsStudy2.getNumResults());
        assertTrue(groupsStudy2.getResults().stream().map(Group::getId).collect(Collectors.toList())
                .containsAll(Arrays.asList("@notSyncedGroup", "@syncedGroup1", ParamConstants.MEMBERS_GROUP)));
    }

}
