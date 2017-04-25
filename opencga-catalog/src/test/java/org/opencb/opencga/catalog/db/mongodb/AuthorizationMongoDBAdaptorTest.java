package org.opencb.opencga.catalog.db.mongodb;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationDBAdaptor;
import org.opencb.opencga.catalog.config.Configuration;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Individual;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.catalog.models.User;
import org.opencb.opencga.catalog.models.acls.permissions.SampleAclEntry;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * Created by pfurio on 21/04/17.
 */
public class AuthorizationMongoDBAdaptorTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private AuthorizationDBAdaptor aclDBAdaptor;
    private User user1;
    private User user2;
    private User user3;
    private long studyId;
    private Sample s1;
    private SampleAclEntry acl_s1_user1;
    private SampleAclEntry acl_s1_user2;
    private SampleAclEntry acl_s2_user1;
    private SampleAclEntry acl_s2_user2;

    @AfterClass
    public static void afterClass() {
        MongoDBAdaptorTest.afterClass();
    }

    @Before
    public void before() throws IOException, CatalogException {
        MongoDBAdaptorTest dbAdaptorTest = new MongoDBAdaptorTest();
        dbAdaptorTest.before();

        Configuration configuration = Configuration.load(getClass().getResource("/configuration-test.yml").openStream());

        user1 = MongoDBAdaptorTest.user1;
        user2 = MongoDBAdaptorTest.user2;
        user3 = MongoDBAdaptorTest.user3;
        DBAdaptorFactory dbAdaptorFactory = MongoDBAdaptorTest.catalogDBAdaptor;
        aclDBAdaptor = new AuthorizationMongoDBAdaptor(configuration);

        studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        acl_s1_user1 = new SampleAclEntry(user1.getId(), Arrays.asList());
        acl_s1_user2 = new SampleAclEntry(user2.getId(), Arrays.asList(
                SampleAclEntry.SamplePermissions.VIEW.name(),
                SampleAclEntry.SamplePermissions.VIEW_ANNOTATIONS.name(),
                SampleAclEntry.SamplePermissions.SHARE.name(),
                SampleAclEntry.SamplePermissions.UPDATE.name()
        ));
        s1 = dbAdaptorFactory.getCatalogSampleDBAdaptor().insert(new Sample(0, "s1", "", new Individual(), "", Arrays.asList(acl_s1_user1, acl_s1_user2), null,
                null), studyId, null).first();
        acl_s2_user1 = new SampleAclEntry(user1.getId(), Arrays.asList());
        acl_s2_user2 = new SampleAclEntry(user2.getId(), Arrays.asList(
                SampleAclEntry.SamplePermissions.VIEW.name(),
                SampleAclEntry.SamplePermissions.VIEW_ANNOTATIONS.name(),
                SampleAclEntry.SamplePermissions.SHARE.name(),
                SampleAclEntry.SamplePermissions.UPDATE.name()
        ));
    }

    @Test
    public void getSampleAcl() throws Exception {
        QueryResult<SampleAclEntry> sampleAcl = aclDBAdaptor.get(s1.getId(), Arrays.asList(user1.getId()),
                MongoDBAdaptorFactory.SAMPLE_COLLECTION);
        SampleAclEntry acl = sampleAcl.first();
        assertNotNull(acl);
        assertEquals(acl_s1_user1.getPermissions(), acl.getPermissions());

        acl = (SampleAclEntry) aclDBAdaptor.get(s1.getId(), Arrays.asList(user2.getId()), MongoDBAdaptorFactory.SAMPLE_COLLECTION).first();
        assertNotNull(acl);
        assertEquals(acl_s1_user2.getPermissions(), acl.getPermissions());
    }

    @Test
    public void getSampleAclWrongUser() throws Exception {
        QueryResult<SampleAclEntry> wrongUser = aclDBAdaptor.get(s1.getId(), Arrays.asList("wrongUser"),
                MongoDBAdaptorFactory.SAMPLE_COLLECTION);
        assertEquals(0, wrongUser.getNumResults());
    }

    @Test
    public void getSampleAclFromUserWithoutAcl() throws Exception {
        QueryResult<SampleAclEntry> sampleAcl = aclDBAdaptor.get(s1.getId(), Arrays.asList(user3.getId()),
                MongoDBAdaptorFactory.SAMPLE_COLLECTION);
        assertTrue(sampleAcl.getResult().isEmpty());
    }

    // Remove some concrete permissions
    @Test
    public void unsetSampleAcl2() throws Exception {
        // Unset permissions
        QueryResult<SampleAclEntry> sampleAcl = aclDBAdaptor.get(s1.getId(), Arrays.asList(user2.getId()),
                MongoDBAdaptorFactory.SAMPLE_COLLECTION);
        assertEquals(1, sampleAcl.getNumResults());
        assertEquals(4, sampleAcl.first().getPermissions().size());
        aclDBAdaptor.removeFromMembers(Arrays.asList(s1.getId()), Arrays.asList(user2.getId()),
                Arrays.asList("VIEW_ANNOTATIONS", "DELETE", "VIEW"), MongoDBAdaptorFactory.SAMPLE_COLLECTION);
//        sampleDBAdaptor.unsetSampleAcl(s1.getId(), Arrays.asList(user2.getId()),
//                Arrays.asList("VIEW_ANNOTATIONS", "DELETE", "VIEW"));
        sampleAcl = aclDBAdaptor.get(s1.getId(), Arrays.asList(user2.getId()), MongoDBAdaptorFactory.SAMPLE_COLLECTION);
        assertEquals(1, sampleAcl.getNumResults());
        assertEquals(2, sampleAcl.first().getPermissions().size());
        assertTrue(sampleAcl.first().getPermissions().containsAll(Arrays.asList(SampleAclEntry.SamplePermissions.UPDATE,
                SampleAclEntry.SamplePermissions.SHARE)));

    }

    @Test
    public void setSampleAclOverride() throws Exception {
        assertEquals(acl_s1_user2.getPermissions(),
                aclDBAdaptor.get(s1.getId(), Arrays.asList(user2.getId()), MongoDBAdaptorFactory.SAMPLE_COLLECTION).first().getPermissions());

        SampleAclEntry newAcl = new SampleAclEntry(user2.getId(), Arrays.asList(SampleAclEntry.SamplePermissions.DELETE.name()));
        assertTrue(!acl_s1_user2.getPermissions().equals(newAcl.getPermissions()));
        aclDBAdaptor.setToMembers(Arrays.asList(s1.getId()), Arrays.asList(user2.getId()),
                Arrays.asList(SampleAclEntry.SamplePermissions.DELETE.name()), MongoDBAdaptorFactory.SAMPLE_COLLECTION);
//        sampleDBAdaptor.setSampleAcl(s1.getId(), newAcl, true);

        assertEquals(newAcl.getPermissions(), aclDBAdaptor.get(s1.getId(), Arrays.asList(user2.getId()),
                MongoDBAdaptorFactory.SAMPLE_COLLECTION).first().getPermissions());
    }

}