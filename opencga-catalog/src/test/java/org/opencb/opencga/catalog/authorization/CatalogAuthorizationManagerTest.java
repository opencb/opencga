package org.opencb.opencga.catalog.authorization;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.CatalogManagerTest;
import org.opencb.opencga.catalog.db.api.CatalogFileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Acl;
import org.opencb.opencga.catalog.models.Group;
import org.opencb.opencga.catalog.models.Study;

import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Created on 19/08/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogAuthorizationManagerTest {

    private final String ownerUser = "user1";
    private final String memberUser = "user2";
    private final String externalUser = "user3";
    private final String password = "1234";
    private CatalogManager catalogManager;
    private String ownerSessionId;
    private String memberSessionId;
    private String externalSessionId;
    private int p1;
    private int s1;

    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private int data;
    private int data_d1;
    private int data_d1_d2;
    private int data_d1_d2_d3;
    private int data_d1_d2_d3_d4;

    @Before
    public void before () throws Exception {
        InputStream is = CatalogManagerTest.class.getClassLoader().getResourceAsStream("catalog.properties");
        Properties properties = new Properties();
        properties.load(is);

        CatalogManagerTest.clearCatalog(properties);

        catalogManager = new CatalogManager(properties);

        catalogManager.createUser(ownerUser, ownerUser, "email@ccc.ccc", password, "ASDF", null);
        catalogManager.createUser(memberUser, memberUser, "email@ccc.ccc", password, "ASDF", null);
        catalogManager.createUser(externalUser, externalUser, "email@ccc.ccc", password, "ASDF", null);

        ownerSessionId = catalogManager.login(ownerUser, password, "localhost").first().get("sessionId").toString();
        memberSessionId = catalogManager.login(memberUser, password, "localhost").first().get("sessionId").toString();
        externalSessionId = catalogManager.login(externalUser, password, "localhost").first().get("sessionId").toString();

        p1 = catalogManager.createProject(ownerUser, "p1", "p1", null, null, null, ownerSessionId).first().getId();
        s1 = catalogManager.createStudy(p1, "s1", "s1", Study.Type.CASE_CONTROL, null, ownerSessionId).first().getId();
        data = catalogManager.searchFile(s1, new QueryOptions(CatalogFileDBAdaptor.FileFilterOption.path.toString(), "data/"), ownerSessionId).first().getId();
        data_d1 = catalogManager.createFolder(s1, Paths.get("data/d1/"), false, null, ownerSessionId).first().getId();
        data_d1_d2 = catalogManager.createFolder(s1, Paths.get("data/d1/d2/"), false, null, ownerSessionId).first().getId();
        data_d1_d2_d3 = catalogManager.createFolder(s1, Paths.get("data/d1/d2/d3/"), false, null, ownerSessionId).first().getId();
        data_d1_d2_d3_d4 = catalogManager.createFolder(s1, Paths.get("data/d1/d2/d3/d4/"), false, null, ownerSessionId).first().getId();

        catalogManager.shareProject(p1, new Acl(memberUser, true, true, true, true), ownerSessionId);
        catalogManager.addMemberToGroup(s1, AuthorizationManager.MEMBERS_GROUP, memberUser, ownerSessionId);

        catalogManager.shareFile(data_d1, new Acl(memberUser, true, true, true, true), ownerSessionId);
        catalogManager.shareFile(data_d1_d2_d3, new Acl(memberUser, false, false, false, false), ownerSessionId);


    }

    @After
    public void after() throws Exception {
        catalogManager.close();
    }

    /*--------------------------*/
    // Add group members
    /*--------------------------*/

    @Test
    public void addMemberToGroup() throws CatalogException {
        catalogManager.addMemberToGroup(s1, AuthorizationManager.ADMINS_GROUP, externalUser, ownerSessionId);
        Map<String, Group> groups = getGroupMap();
        assertTrue(groups.get(AuthorizationManager.ADMINS_GROUP).getUserIds().contains(externalUser));
    }

    @Test
    public void addMemberToGroupExistingNoPermission() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.addMemberToGroup(s1, AuthorizationManager.ADMINS_GROUP, externalUser, externalSessionId);
    }

    @Test
    public void addMemberToGroupInOtherGroup() throws CatalogException {
        catalogManager.addMemberToGroup(s1, AuthorizationManager.ADMINS_GROUP, externalUser, ownerSessionId);
        thrown.expect(CatalogException.class);
        catalogManager.addMemberToGroup(s1, AuthorizationManager.MEMBERS_GROUP, externalUser, ownerSessionId);
    }

    @Test
    public void addMemberToTheBelongingGroup() throws CatalogException {
        catalogManager.addMemberToGroup(s1, AuthorizationManager.ADMINS_GROUP, externalUser, ownerSessionId);
        thrown.expect(CatalogException.class);
        catalogManager.addMemberToGroup(s1, AuthorizationManager.ADMINS_GROUP, externalUser, ownerSessionId);
    }

    @Test
    public void addMemberToNonExistingGroup() throws CatalogException {
        thrown.expect(CatalogDBException.class);
        catalogManager.addMemberToGroup(s1, "NO_GROUP", externalUser, ownerSessionId);
    }

    /*--------------------------*/
    // Remove group members
    /*--------------------------*/

    @Test
    public void removeMemberFromGroup() throws CatalogException {
        catalogManager.removeMemberFromGroup(s1, AuthorizationManager.ADMINS_GROUP, ownerUser, ownerSessionId);
        assertFalse(getGroupMap().get(AuthorizationManager.ADMINS_GROUP).getUserIds().contains(ownerUser));
    }

    @Test
    public void removeMemberFromGroupNoPermission() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.removeMemberFromGroup(s1, AuthorizationManager.ADMINS_GROUP, ownerUser, externalSessionId);
    }

    @Test
    public void removeMemberFromNonExistingGroup() throws CatalogException {
        thrown.expect(CatalogException.class);
        catalogManager.removeMemberFromGroup(s1, "NO_GROUP", ownerUser, ownerSessionId);
    }

    @Test
    public void removeMemberFromNonBelongingGroup() throws CatalogException {
        thrown.expect(CatalogException.class);
        catalogManager.removeMemberFromGroup(s1, AuthorizationManager.MEMBERS_GROUP, ownerUser, ownerSessionId);
    }

    /*--------------------------*/
    // Read Project
    /*--------------------------*/

    @Test
    public void readProject() throws CatalogException {
        catalogManager.getProject(p1, null, memberSessionId);
    }

    @Test
    public void readProjectDenny() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getProject(p1, null, externalSessionId);
    }

    /*--------------------------*/
    // Read Study
    /*--------------------------*/

    @Test
    public void readStudy() throws CatalogException {
        catalogManager.getStudy(s1, memberSessionId);
    }

    @Test
    public void readStudyDenny() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getStudy(s1, externalSessionId);
    }

    /*--------------------------*/
    // Read file
    /*--------------------------*/

    @Test
    public void readFileByOwner() throws CatalogException {
        catalogManager.getFile(data_d1, ownerSessionId);
    }

    @Test
    public void readFileByOwnerNoGroups() throws CatalogException {
        catalogManager.removeMemberFromGroup(s1, AuthorizationManager.ADMINS_GROUP, ownerUser, ownerSessionId);
        catalogManager.getFile(data_d1, ownerSessionId);
    }

    @Test
    public void readExplicitlySharedFile() throws CatalogException {
        catalogManager.getFile(data_d1, memberSessionId);
    }

    @Test
    public void readInheritedSharedFile() throws CatalogException {
        catalogManager.getFile(data_d1_d2, memberSessionId);
    }

    @Test
    public void readExplicitlyForbiddenFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFile(data_d1_d2_d3, memberSessionId);
    }

    @Test
    public void readInheritedForbiddenFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFile(data_d1_d2_d3_d4, memberSessionId);
    }

    @Test
    public void readNonSharedFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFile(data, memberSessionId);
    }

    @Test
    public void readFileNoStudyMember() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.getFile(data, externalSessionId);
    }

    /*--------------------------*/
    // Create file
    /*--------------------------*/

    @Test
    public void createFileByOwnerNoGroups() throws CatalogException {
        catalogManager.removeMemberFromGroup(s1, AuthorizationManager.ADMINS_GROUP, ownerUser, ownerSessionId);
        catalogManager.createFolder(s1, Paths.get("data/newFolder"), true, null, ownerSessionId);
    }

    @Test
    public void createExplicitlySharedFile() throws CatalogException {
        catalogManager.createFolder(s1, Paths.get("data/d1/folder/"), false, null, memberSessionId);
    }

    @Test
    public void createInheritedSharedFile() throws CatalogException {
        catalogManager.createFolder(s1, Paths.get("data/d1/d2/folder/"), false, null, memberSessionId);
    }

    @Test
    public void createExplicitlyForbiddenFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.createFolder(s1, Paths.get("data/d1/d2/d3/folder/"), false, null, memberSessionId);
    }

    @Test
    public void createInheritedForbiddenFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.createFolder(s1, Paths.get("data/d1/d2/d3/d4/folder/"), false, null, memberSessionId);
    }

    @Test
    public void createNonSharedFile() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.createFolder(s1, Paths.get("folder/"), false, null, memberSessionId);
    }

    @Test
    public void createFileNoStudyMember() throws CatalogException {
        thrown.expect(CatalogAuthorizationException.class);
        catalogManager.createFolder(s1, Paths.get("data/my_folder/"), false, null, externalSessionId);
    }


    /////////// Aux methods
    private Map<String, Group> getGroupMap() throws CatalogException {
        return catalogManager.getStudy(s1, ownerSessionId).first().getGroups().stream().collect(Collectors.toMap(Group::getId, f -> f));
    }

}