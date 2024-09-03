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

package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoBackupUtils;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.FqnUtils;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.file.*;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualAclParams;
import org.opencb.opencga.core.models.individual.IndividualPermissions;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleAclParams;
import org.opencb.opencga.core.models.sample.SamplePermissions;
import org.opencb.opencga.core.models.study.*;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.commons.lang3.StringUtils.join;
import static org.opencb.opencga.catalog.utils.ParamUtils.AclAction.ADD;
import static org.opencb.opencga.catalog.utils.ParamUtils.AclAction.SET;

@Category(MediumTests.class)
public class  AbstractManagerTest extends GenericTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TestName testName = new TestName();

    @ClassRule
    public static CatalogManagerExternalResource catalogManagerResource = new CatalogManagerExternalResource();
    public static Path mongoDumpFolder;

    protected CatalogManager catalogManager;
    protected final static String organizationId = "test";
    protected String opencgaToken;
    protected final static String orgOwnerUserId = "owner";
    protected String ownerToken;
    protected final static String orgAdminUserId1 = "orgAdminUser1";
    protected String orgAdminToken1;
    protected final static String orgAdminUserId2 = "orgAdminUser2";
    protected String orgAdminToken2;
    protected final static String studyAdminUserId1 = "studyAdminUser1";
    protected String studyAdminToken1;
    protected final static String normalUserId1 = "normalUser1";
    protected String normalToken1;
    protected final static String normalUserId2 = "normalUser2";
    protected String normalToken2;
    protected final static String normalUserId3 = "normalUser3";
    protected String normalToken3;
    protected final static String noAccessUserId1 = "noAccessUserId1";
    protected String noAccessToken1;

    protected String restrictedGroup = "@restrictedGroup"; // normalUserId2, normalUserId3

    protected final String project1 = "1000G";
    protected final String projectFqn1 = organizationId + "@1000G";
    protected final String project2 = "pmp";
    protected final String projectFqn2 = organizationId + "@pmp";
    protected final String project3 = "p1";
    protected final String projectFqn3 = organizationId + "@p1";

    protected final String studyId = "phase1";
    protected long studyUid;
    protected String studyFqn;
    protected final String studyId2 = "phase3";
    protected long studyUid2;
    protected String studyFqn2;
    protected final String studyId3 = "s1";
    protected String studyFqn3;
    protected final String s_1Id = "s_1"; // shared with @restrictedGroup
    protected long s_1Uid;
    protected final String s_2Id = "s_2"; // shared with *
    protected long s_2Uid;
    protected final String s_3Id = "s_3"; // shared with @restrictedGroup
    protected long s_3Uid;
    protected final String s_4Id = "s_4";
    protected long s_4Uid;
    protected final String s_5Id = "s_5"; // shared with @restrictedGroup
    protected long s_5Uid;
    protected final String s_6Id = "s_6"; // shared with @members
    protected long s_6Uid;
    protected final String s_7Id = "s_7";
    protected long s_7Uid;
    protected final String s_8Id = "s_8";
    protected long s_8Uid;
    protected final String s_9Id = "s_9";
    protected long s_9Uid;

    protected final String ind1 = "ind1";
    protected final String ind2 = "ind2";

    protected final String testFile1 = "data/test/folder/test_1K.txt.gz";;
    protected final String testFile2 = "data/test/folder/test_0.5K.txt";;

    protected String data = "data/";                                   //
    protected String data_d1 = "data/d1/";                             // Shared with normalUser1
    protected String data_d1_d2 = "data/d1/d2/";                       //
    protected String data_d1_d2_d3 = "data/d1/d2/d3/";                 // Forbidden for normalUser1
    protected String data_d1_d2_d3_d4 = "data/d1/d2/d3/d4/";           //
    protected String data_d1_d2_d3_d4_txt = "data/d1/d2/d3/d4/my.txt"; // Shared for normalUser1

    protected final String ALL_FILE_PERMISSIONS = join(
            EnumSet.allOf(FilePermissions.class)
                    .stream()
                    .map(FilePermissions::name)
                    .collect(Collectors.toList()),
            ",");
    protected final String DENY_FILE_PERMISSIONS = "";

    protected final String ALL_SAMPLE_PERMISSIONS = join(
            EnumSet.allOf(SamplePermissions.class)
                    .stream()
                    .map(SamplePermissions::name)
                    .collect(Collectors.toList()),
            ",");
    protected final String DENY_SAMPLE_PERMISSIONS = "";

    protected final String ALL_INDIVIDUAL_PERMISSIONS = join(
            EnumSet.allOf(IndividualPermissions.class)
                    .stream()
                    .map(IndividualPermissions::name)
                    .collect(Collectors.toList()),
            ",");

    protected final SampleAclParams allSamplePermissions = new SampleAclParams(null, null, null, null, ALL_SAMPLE_PERMISSIONS);
    protected final SampleAclParams noSamplePermissions = new SampleAclParams(null, null, null, null, DENY_SAMPLE_PERMISSIONS);

    protected final IndividualAclParams allIndividualPermissions = new IndividualAclParams(null, ALL_INDIVIDUAL_PERMISSIONS);

    private static boolean firstExecutionFinished = false;

    protected static final QueryOptions INCLUDE_RESULT = new QueryOptions(ParamConstants.INCLUDE_RESULT_PARAM, true);

    @Before
    public void setUp() throws Exception {
        catalogManager = catalogManagerResource.getCatalogManager();
        setUpCatalogManager(catalogManager);
    }

    public void setUpCatalogManager(CatalogManager catalogManager) throws Exception {
        if (!firstExecutionFinished) {
            createDummyData(catalogManager);
            MongoBackupUtils.dump(catalogManager, catalogManagerResource.getOpencgaHome());
            firstExecutionFinished = true;
        } else {
            // Clear sessions folder
            catalogManagerResource.clearOpenCGAHome(testName.getMethodName());
            MongoBackupUtils.restore(catalogManager, catalogManagerResource.getOpencgaHome());
            initVariables();
        }
    }

    private void initVariables() throws CatalogException {
        catalogManager = catalogManagerResource.resetCatalogManager();
        opencgaToken = catalogManager.getUserManager().loginAsAdmin(TestParamConstants.ADMIN_PASSWORD).getToken();
        ownerToken = catalogManager.getUserManager().login(organizationId, orgOwnerUserId, TestParamConstants.PASSWORD).getToken();
        orgAdminToken1 = catalogManager.getUserManager().login(organizationId, orgAdminUserId1, TestParamConstants.PASSWORD).getToken();
        orgAdminToken2 = catalogManager.getUserManager().login(organizationId, orgAdminUserId2, TestParamConstants.PASSWORD).getToken();
        studyAdminToken1 = catalogManager.getUserManager().login(organizationId, studyAdminUserId1, TestParamConstants.PASSWORD).getToken();
        normalToken1 = catalogManager.getUserManager().login(organizationId, normalUserId1, TestParamConstants.PASSWORD).getToken();
        normalToken2 = catalogManager.getUserManager().login(organizationId, normalUserId2, TestParamConstants.PASSWORD).getToken();
        normalToken3 = catalogManager.getUserManager().login(organizationId, normalUserId3, TestParamConstants.PASSWORD).getToken();
        noAccessToken1 = catalogManager.getUserManager().login(organizationId, noAccessUserId1, TestParamConstants.PASSWORD).getToken();

        Study study = catalogManager.getStudyManager().get(studyId, StudyManager.INCLUDE_STUDY_IDS, ownerToken).first();
        studyUid = study.getUid();
        studyFqn = study.getFqn();

        study = catalogManager.getStudyManager().get(studyId2, StudyManager.INCLUDE_STUDY_IDS, ownerToken).first();
        studyUid2 = study.getUid();
        studyFqn2 = study.getFqn();

        studyFqn3 = FqnUtils.buildFqn(organizationId, project2, studyId3);

        OpenCGAResult<Sample> result = catalogManager.getSampleManager().get(studyFqn,
                Arrays.asList(s_1Id, s_2Id, s_3Id, s_4Id, s_5Id, s_6Id, s_7Id, s_8Id, s_9Id), SampleManager.INCLUDE_SAMPLE_IDS, ownerToken);
        s_1Uid = result.getResults().get(0).getUid();
        s_2Uid = result.getResults().get(1).getUid();
        s_3Uid = result.getResults().get(2).getUid();
        s_4Uid = result.getResults().get(3).getUid();
        s_5Uid = result.getResults().get(4).getUid();
        s_6Uid = result.getResults().get(5).getUid();
        s_7Uid = result.getResults().get(6).getUid();
        s_8Uid = result.getResults().get(7).getUid();
        s_9Uid = result.getResults().get(8).getUid();
    }

    private void createDummyData(CatalogManager catalogManager) throws CatalogException {
        // Create new organization, owner and admins
        opencgaToken = catalogManager.getUserManager().loginAsAdmin(TestParamConstants.ADMIN_PASSWORD).getToken();
        catalogManager.getOrganizationManager().create(new OrganizationCreateParams().setId(organizationId).setName("Test"), QueryOptions.empty(), opencgaToken);
        catalogManager.getUserManager().create(new User().setId(orgOwnerUserId).setName(orgOwnerUserId).setOrganization(organizationId), TestParamConstants.PASSWORD, opencgaToken);
        catalogManager.getUserManager().create(orgAdminUserId1, "User2 Name", "mail2@ebi.ac.uk", TestParamConstants.PASSWORD, organizationId, null, opencgaToken);
        catalogManager.getUserManager().create(orgAdminUserId2, "User3 Name", "user.3@e.mail", TestParamConstants.PASSWORD, organizationId, null, opencgaToken);
        catalogManager.getUserManager().create(studyAdminUserId1, "User3 Name", "user.3@e.mail", TestParamConstants.PASSWORD, organizationId, null, opencgaToken);
        catalogManager.getUserManager().create(normalUserId1, "User4 Name", "user.4@e.mail", TestParamConstants.PASSWORD, organizationId, null, opencgaToken);
        catalogManager.getUserManager().create(normalUserId2, "User5 Name", "user.5@e.mail", TestParamConstants.PASSWORD, organizationId, null, opencgaToken);
        catalogManager.getUserManager().create(normalUserId3, "User6 Name", "user.6@e.mail", TestParamConstants.PASSWORD, organizationId, null, opencgaToken);
        catalogManager.getUserManager().create(noAccessUserId1, "Name", "user.6@e.mail", TestParamConstants.PASSWORD, organizationId, null, opencgaToken);

        catalogManager.getOrganizationManager().update(organizationId, 
                new OrganizationUpdateParams()
                        .setOwner(orgOwnerUserId)
                        .setAdmins(Arrays.asList(orgAdminUserId1, orgAdminUserId2)),
                null, opencgaToken);

        ownerToken = catalogManager.getUserManager().login(organizationId, orgOwnerUserId, TestParamConstants.PASSWORD).getToken();
        orgAdminToken1 = catalogManager.getUserManager().login(organizationId, orgAdminUserId1, TestParamConstants.PASSWORD).getToken();
        orgAdminToken2 = catalogManager.getUserManager().login(organizationId, orgAdminUserId2, TestParamConstants.PASSWORD).getToken();
        studyAdminToken1 = catalogManager.getUserManager().login(organizationId, studyAdminUserId1, TestParamConstants.PASSWORD).getToken();
        normalToken1 = catalogManager.getUserManager().login(organizationId, normalUserId1, TestParamConstants.PASSWORD).getToken();
        normalToken2 = catalogManager.getUserManager().login(organizationId, normalUserId2, TestParamConstants.PASSWORD).getToken();
        normalToken3 = catalogManager.getUserManager().login(organizationId, normalUserId3, TestParamConstants.PASSWORD).getToken();
        noAccessToken1 = catalogManager.getUserManager().login(organizationId, noAccessUserId1, TestParamConstants.PASSWORD).getToken();

        catalogManager.getProjectManager().create(project1, "Project about some genomes", "", "Homo sapiens",
                null, "GRCh38", INCLUDE_RESULT, ownerToken);
        catalogManager.getProjectManager().create(project2, "Project Management Project", "life art intelligent system",
                "Homo sapiens", null, "GRCh38", INCLUDE_RESULT, orgAdminToken1);
        catalogManager.getProjectManager().create(project3, "project 1", "", "Homo sapiens", null, "GRCh38", INCLUDE_RESULT,
                orgAdminToken2);

        Study study = catalogManager.getStudyManager().create(project1, studyId, null, "Phase 1", "Done", null, null, null, null,
                INCLUDE_RESULT, ownerToken).first();
        studyUid = study.getUid();
        studyFqn = study.getFqn();
        catalogManager.getStudyManager().updateGroup(studyFqn, ParamConstants.ADMINS_GROUP, ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList(studyAdminUserId1)), ownerToken);

        study = catalogManager.getStudyManager().create(project1, studyId2, null, "Phase 3", "d", null, null, null, null, INCLUDE_RESULT,
                ownerToken).first();
        studyUid2 = study.getUid();
        studyFqn2 = study.getFqn();
        catalogManager.getStudyManager().updateGroup(studyFqn2, ParamConstants.ADMINS_GROUP, ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList(studyAdminUserId1)), ownerToken);

        study = catalogManager.getStudyManager().create(project2, studyId3, null, "Study 1", "", null, null, null, null, INCLUDE_RESULT,
                orgAdminToken1).first();
        studyFqn3 = study.getFqn();
        catalogManager.getStudyManager().updateGroup(studyFqn3, ParamConstants.ADMINS_GROUP, ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList(studyAdminUserId1)), ownerToken);

        catalogManager.getFileManager().createFolder(studyFqn2, Paths.get("data/test/folder/").toString(), true,
                null, QueryOptions.empty(), ownerToken);

        File testFolder = catalogManager.getFileManager().createFolder(studyFqn, Paths.get("data/test/folder/").toString(),
                true, null, INCLUDE_RESULT, ownerToken).first();
        ObjectMap attributes = new ObjectMap();
        attributes.put("field", "value");
        attributes.put("numValue", 5);
        catalogManager.getFileManager().update(studyFqn, testFolder.getPath(),
                new FileUpdateParams().setAttributes(attributes), new QueryOptions(), ownerToken);

        DataResult<File> queryResult2 = catalogManager.getFileManager().create(studyFqn,
                new FileCreateParams()
                        .setContent(RandomStringUtils.randomAlphanumeric(1000))
                        .setPath(testFile1)
                        .setType(File.Type.FILE),
                false, ownerToken);

        File fileTest1k = catalogManager.getFileManager().get(studyFqn, queryResult2.first().getPath(), INCLUDE_RESULT, ownerToken).first();
        attributes = new ObjectMap();
        attributes.put("field", "value");
        attributes.put("name", "fileTest1k");
        attributes.put("numValue", "10");
        attributes.put("boolean", false);
        catalogManager.getFileManager().update(studyFqn, fileTest1k.getPath(),
                new FileUpdateParams().setAttributes(attributes), new QueryOptions(), ownerToken);

        DataResult<File> queryResult1 = catalogManager.getFileManager().create(studyFqn,
                new FileCreateParams()
                        .setContent(RandomStringUtils.randomAlphanumeric(500))
                        .setPath(testFile2)
                        .setBioformat(File.Bioformat.DATAMATRIX_EXPRESSION)
                        .setType(File.Type.FILE),
                false, ownerToken);

        File fileTest05k = catalogManager.getFileManager().get(studyFqn, queryResult1.first().getPath(), INCLUDE_RESULT, ownerToken).first();
        attributes = new ObjectMap();
        attributes.put("field", "valuable");
        attributes.put("name", "fileTest05k");
        attributes.put("numValue", 5);
        attributes.put("boolean", true);
        catalogManager.getFileManager().update(studyFqn, fileTest05k.getPath(),
                new FileUpdateParams().setAttributes(attributes), new QueryOptions(), ownerToken);

        DataResult<File> queryResult = catalogManager.getFileManager().create(studyFqn,
                new FileCreateParams()
                        .setContent("iVBORw0KGgoAAAANSUhEUgAAABgAAAAYCAYAAADgdz34AAAABHNCSVQICAgIfAhkiAAAAAlwSFlzAAAApgAAAKYB3X3/OAAAABl0RVh0U29mdHdhcmUAd3d3Lmlua3NjYXBlLm9yZ5vuPBoAAANCSURBVEiJtZZPbBtFFMZ/M7ubXdtdb1xSFyeilBapySVU8h8OoFaooFSqiihIVIpQBKci6KEg9Q6H9kovIHoCIVQJJCKE1ENFjnAgcaSGC6rEnxBwA04Tx43t2FnvDAfjkNibxgHxnWb2e/u992bee7tCa00YFsffekFY+nUzFtjW0LrvjRXrCDIAaPLlW0nHL0SsZtVoaF98mLrx3pdhOqLtYPHChahZcYYO7KvPFxvRl5XPp1sN3adWiD1ZAqD6XYK1b/dvE5IWryTt2udLFedwc1+9kLp+vbbpoDh+6TklxBeAi9TL0taeWpdmZzQDry0AcO+jQ12RyohqqoYoo8RDwJrU+qXkjWtfi8Xxt58BdQuwQs9qC/afLwCw8tnQbqYAPsgxE1S6F3EAIXux2oQFKm0ihMsOF71dHYx+f3NND68ghCu1YIoePPQN1pGRABkJ6Bus96CutRZMydTl+TvuiRW1m3n0eDl0vRPcEysqdXn+jsQPsrHMquGeXEaY4Yk4wxWcY5V/9scqOMOVUFthatyTy8QyqwZ+kDURKoMWxNKr2EeqVKcTNOajqKoBgOE28U4tdQl5p5bwCw7BWquaZSzAPlwjlithJtp3pTImSqQRrb2Z8PHGigD4RZuNX6JYj6wj7O4TFLbCO/Mn/m8R+h6rYSUb3ekokRY6f/YukArN979jcW+V/S8g0eT/N3VN3kTqWbQ428m9/8k0P/1aIhF36PccEl6EhOcAUCrXKZXXWS3XKd2vc/TRBG9O5ELC17MmWubD2nKhUKZa26Ba2+D3P+4/MNCFwg59oWVeYhkzgN/JDR8deKBoD7Y+ljEjGZ0sosXVTvbc6RHirr2reNy1OXd6pJsQ+gqjk8VWFYmHrwBzW/n+uMPFiRwHB2I7ih8ciHFxIkd/3Omk5tCDV1t+2nNu5sxxpDFNx+huNhVT3/zMDz8usXC3ddaHBj1GHj/As08fwTS7Kt1HBTmyN29vdwAw+/wbwLVOJ3uAD1wi/dUH7Qei66PfyuRj4Ik9is+hglfbkbfR3cnZm7chlUWLdwmprtCohX4HUtlOcQjLYCu+fzGJH2QRKvP3UNz8bWk1qMxjGTOMThZ3kvgLI5AzFfo379UAAAAASUVORK5CYII=")
                        .setPath(testFolder.getPath() + "test_0.1K.png")
                        .setFormat(File.Format.IMAGE)
                        .setType(File.Type.FILE),
                false, ownerToken);

        File test01k = catalogManager.getFileManager().get(studyFqn, queryResult.first().getPath(), INCLUDE_RESULT, ownerToken).first();
        attributes = new ObjectMap();
        attributes.put("field", "other");
        attributes.put("name", "test01k");
        attributes.put("numValue", 50);
        attributes.put("nested", new ObjectMap("num1", 45).append("num2", 33).append("text", "HelloWorld"));
        catalogManager.getFileManager().update(studyFqn, test01k.getPath(),
                new FileUpdateParams().setAttributes(attributes), new QueryOptions(), ownerToken);

        List<Variable> variables = new ArrayList<>();
        variables.addAll(Arrays.asList(
                new Variable("NAME", "", "", Variable.VariableType.STRING, "", true, false, Collections.emptyList(), null, 0, "", "", null,
                        Collections.emptyMap()),
                new Variable("AGE", "", "", Variable.VariableType.INTEGER, null, true, false, Collections.singletonList("0:130"), null, 1, "", "",
                        null, Collections.emptyMap()),
                new Variable("HEIGHT", "", "", Variable.VariableType.DOUBLE, "1.5", false, false, Collections.singletonList("0:"), null, 2, "",
                        "", null, Collections.emptyMap()),
                new Variable("ALIVE", "", "", Variable.VariableType.BOOLEAN, "", true, false, Collections.emptyList(), null, 3, "", "",
                        null, Collections.emptyMap()),
                new Variable("PHEN", "", "", Variable.VariableType.CATEGORICAL, "CASE", true, false, Arrays.asList("CASE", "CONTROL"), null, 4,
                        "", "", null, Collections.emptyMap()),
                new Variable("EXTRA", "", "", Variable.VariableType.STRING, "", false, false, Collections.emptyList(), null, 5, "", "", null,
                        Collections.emptyMap())
        ));
        VariableSet vs = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs", "vs", true, false, "", null, variables,
                null, ownerToken).first();

        Sample sample = new Sample().setId(s_1Id);
        sample.setAnnotationSets(Collections.singletonList(new AnnotationSet("annot1", vs.getId(),
                new ObjectMap("NAME", s_1Id).append("AGE", 6).append("ALIVE", true).append("PHEN", "CONTROL"))));
        Sample tmpSample = catalogManager.getSampleManager().create(studyFqn, sample, INCLUDE_RESULT, ownerToken).first();
        s_1Uid = tmpSample.getUid();

        sample.setId(s_2Id);
        sample.setAnnotationSets(Collections.singletonList(new AnnotationSet("annot1", vs.getId(),
                new ObjectMap("NAME", s_2Id).append("AGE", 10).append("ALIVE", false).append("PHEN", "CASE"))));
        tmpSample = catalogManager.getSampleManager().create(studyFqn, sample, INCLUDE_RESULT, ownerToken).first();
        s_2Uid = tmpSample.getUid();

        sample.setId(s_3Id);
        sample.setAnnotationSets(Collections.singletonList(new AnnotationSet("annot1", vs.getId(),
                new ObjectMap("NAME", s_3Id).append("AGE", 15).append("ALIVE", true).append("PHEN", "CONTROL"))));
        tmpSample = catalogManager.getSampleManager().create(studyFqn, sample, INCLUDE_RESULT, ownerToken).first();
        s_3Uid = tmpSample.getUid();

                sample.setId(s_4Id);
        sample.setAnnotationSets(Collections.singletonList(new AnnotationSet("annot1", vs.getId(),
                new ObjectMap("NAME", s_4Id).append("AGE", 22).append("ALIVE", false).append("PHEN", "CONTROL"))));
        tmpSample = catalogManager.getSampleManager().create(studyFqn, sample, INCLUDE_RESULT, ownerToken).first();
        s_4Uid = tmpSample.getUid();

        sample.setId(s_5Id);
        sample.setAnnotationSets(Collections.singletonList(new AnnotationSet("annot1", vs.getId(),
                new ObjectMap("NAME", s_5Id).append("AGE", 29).append("ALIVE", true).append("PHEN", "CASE"))));
        tmpSample = catalogManager.getSampleManager().create(studyFqn, sample, INCLUDE_RESULT, ownerToken).first();
        s_5Uid = tmpSample.getUid();

        sample.setId(s_6Id);
        sample.setAnnotationSets(Collections.singletonList(new AnnotationSet("annot2", vs.getId(),
                new ObjectMap("NAME", s_6Id).append("AGE", 38).append("ALIVE", true).append("PHEN", "CONTROL"))));
        tmpSample = catalogManager.getSampleManager().create(studyFqn, sample, INCLUDE_RESULT, ownerToken).first();
        s_6Uid = tmpSample.getUid();

        sample.setId(s_7Id);
        sample.setAnnotationSets(Collections.singletonList(new AnnotationSet("annot2", vs.getId(),
                new ObjectMap("NAME", s_7Id).append("AGE", 46).append("ALIVE", false).append("PHEN", "CASE"))));
        tmpSample = catalogManager.getSampleManager().create(studyFqn, sample, INCLUDE_RESULT, ownerToken).first();
        s_7Uid = tmpSample.getUid();

        sample.setId(s_8Id);
        sample.setAnnotationSets(Collections.singletonList(new AnnotationSet("annot2", vs.getId(),
                new ObjectMap("NAME", s_8Id).append("AGE", 72).append("ALIVE", true).append("PHEN", "CONTROL"))));
        tmpSample = catalogManager.getSampleManager().create(studyFqn, sample, INCLUDE_RESULT, ownerToken).first();
        s_8Uid = tmpSample.getUid();

        sample.setId(s_9Id);
        sample.setAnnotationSets(Collections.emptyList());
        tmpSample = catalogManager.getSampleManager().create(studyFqn, sample, INCLUDE_RESULT, ownerToken).first();
        s_9Uid = tmpSample.getUid();

        catalogManager.getFileManager().update(studyFqn, test01k.getPath(), new FileUpdateParams()
                .setSampleIds(Arrays.asList(s_1Id, s_2Id, s_3Id, s_4Id, s_5Id)), INCLUDE_RESULT, ownerToken);

        // ================ PERMISSIONS =================

        catalogManager.getFileManager().createFolder(studyFqn, data_d1, true, null, QueryOptions.empty(), ownerToken);
        catalogManager.getFileManager().createFolder(studyFqn, data_d1_d2, false, null, QueryOptions.empty(), ownerToken);
        catalogManager.getFileManager().createFolder(studyFqn, data_d1_d2_d3, false, null, QueryOptions.empty(), ownerToken);
        catalogManager.getFileManager().createFolder(studyFqn, data_d1_d2_d3_d4, false, null, QueryOptions.empty(), ownerToken);

        catalogManager.getFileManager().create(studyFqn,
                new FileCreateParams()
                        .setContent("file content")
                        .setPath(data_d1_d2_d3_d4_txt)
                        .setType(File.Type.FILE),
                false, ownerToken);

        StudyAclParams aclParams = new StudyAclParams("", AuthorizationManager.ROLE_ANALYST);
        catalogManager.getStudyManager().updateAcl(studyFqn, normalUserId1, aclParams, ADD, orgAdminToken1);
        catalogManager.getStudyManager().createGroup(studyFqn, new Group(restrictedGroup, Arrays.asList(normalUserId2, normalUserId3)),
                orgAdminToken2);

        catalogManager.getFileManager().updateAcl(studyFqn, Collections.singletonList(data_d1), restrictedGroup,
                new FileAclParams(null, ALL_FILE_PERMISSIONS), SET, ownerToken);
        catalogManager.getFileManager().updateAcl(studyFqn, Collections.singletonList(data_d1_d2_d3), restrictedGroup,
                new FileAclParams(null, DENY_FILE_PERMISSIONS), SET, ownerToken);
        catalogManager.getFileManager().updateAcl(studyFqn, Collections.singletonList(data_d1_d2_d3_d4_txt), restrictedGroup,
                new FileAclParams(null, ALL_FILE_PERMISSIONS), SET, ownerToken);

        catalogManager.getCohortManager().create(studyFqn, new Cohort()
                        .setId("all")
                        .setSamples(Stream.of(s_1Id, s_2Id, s_3Id).map(s -> new Sample().setId(s)).collect(Collectors.toList())),
                QueryOptions.empty(), ownerToken);

        catalogManager.getIndividualManager().create(studyFqn, new Individual().setId(ind1), Collections.singletonList(s_1Id),
                QueryOptions.empty(), ownerToken);
        catalogManager.getIndividualManager().create(studyFqn, new Individual().setId(ind2), Collections.singletonList(s_2Id),
                QueryOptions.empty(), ownerToken);

        catalogManager.getIndividualManager().updateAcl(studyFqn, Collections.singletonList(ind1), restrictedGroup,
                allIndividualPermissions, SET, true, ownerToken);

        catalogManager.getSampleManager().updateAcl(studyFqn, Collections.singletonList(s_1Id), restrictedGroup, allSamplePermissions,
                SET, ownerToken);
        catalogManager.getSampleManager().updateAcl(studyFqn, Collections.singletonList(s_2Id), ParamConstants.ANONYMOUS_USER_ID,
                noSamplePermissions, SET, ownerToken);
        catalogManager.getSampleManager().updateAcl(studyFqn, Collections.singletonList(s_3Id), restrictedGroup, noSamplePermissions,
                SET, ownerToken);
        catalogManager.getSampleManager().updateAcl(studyFqn, Collections.singletonList(s_5Id), restrictedGroup, noSamplePermissions,
                SET, ownerToken);
        catalogManager.getSampleManager().updateAcl(studyFqn, Collections.singletonList(s_6Id), ParamConstants.MEMBERS_GROUP,
                allSamplePermissions, SET, ownerToken);

        // ================ END PERMISSIONS =================

    }

    protected CatalogManager mockCatalogManager() throws CatalogDBException {
        CatalogManager spy = Mockito.spy(catalogManager);
        UserManager userManager = spy.getUserManager();
        UserManager userManagerSpy = Mockito.spy(userManager);
        Mockito.doReturn(userManagerSpy).when(spy).getUserManager();
        MongoDBAdaptorFactory mongoDBAdaptorFactory = mockMongoDBAdaptorFactory();
        Mockito.doReturn(mongoDBAdaptorFactory).when(userManagerSpy).getCatalogDBAdaptorFactory();
        return spy;
    }

    protected MongoDBAdaptorFactory mockMongoDBAdaptorFactory() throws CatalogDBException {
        MongoDBAdaptorFactory catalogDBAdaptorFactory = (MongoDBAdaptorFactory) catalogManager.getUserManager().getCatalogDBAdaptorFactory();
        MongoDBAdaptorFactory dbAdaptorFactorySpy = Mockito.spy(catalogDBAdaptorFactory);
        UserDBAdaptor userDBAdaptor = dbAdaptorFactorySpy.getCatalogUserDBAdaptor(organizationId);
        UserDBAdaptor userDBAdaptorSpy = Mockito.spy(userDBAdaptor);
        Mockito.doReturn(userDBAdaptorSpy).when(dbAdaptorFactorySpy).getCatalogUserDBAdaptor(organizationId);
        return dbAdaptorFactorySpy;
    }

}
