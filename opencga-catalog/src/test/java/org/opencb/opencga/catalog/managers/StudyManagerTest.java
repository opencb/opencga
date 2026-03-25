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

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.TestParamConstants;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.UserOrganizationConfiguration;
import org.opencb.opencga.core.config.storage.SampleIndexConfiguration;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.organizations.OrganizationConfiguration;
import org.opencb.opencga.core.models.study.*;
import org.opencb.opencga.core.models.study.configuration.ClinicalAnalysisStudyConfiguration;
import org.opencb.opencga.core.models.study.configuration.ClinicalPriorityValue;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

@Category(MediumTests.class)
public class StudyManagerTest extends AbstractManagerTest {

    @Test
    public void testDefaultVariableSets() throws Exception {
        String fqn = catalogManager.getStudyManager().create(project1, "newStudy", "newStudy", "newStudy", null, null, null, null, null,
                INCLUDE_RESULT, ownerToken).first().getFqn();

        Study study = catalogManager.getStudyManager().get(fqn, null, ownerToken).first();

        Set<String> s = new Reflections(new ResourcesScanner(), "variablesets/").getResources(Pattern.compile(".*\\.json"));

        // This variable set is internal so it will not be returned from study
        s.remove("variablesets/sample-variant-stats-variableset.json");
        assertEquals(s.size(), study.getVariableSets().size());
        assertEquals(s, study.getVariableSets().stream().map(v->v.getAttributes().get("resource")).collect(Collectors.toSet()));

//        for (VariableSet variableSet : study.getVariableSets()) {
//            Object avroClassStr = variableSet.getAttributes().get("avroClass");
//            System.out.println("variableSet.getAttributes().get(\"avroClass\") = " + avroClassStr);
//            if (avroClassStr != null) {
//                Class<?> avroClass = Class.forName(avroClassStr.toString().split(" ")[1]);
//                Schema schema = (Schema) avroClass.getMethod("getClassSchema").invoke(null);
//                Map<String, Variable> expectedVariables = AvroToAnnotationConverter.convertToVariableSet(schema).stream().collect(Collectors.toMap(Variable::getId, v -> v));
//                Map<String, Variable> actualVariables = variableSet.getVariables().stream().collect(Collectors.toMap(Variable::getId, v->v));
//
//                assertEquals(expectedVariables.keySet(), actualVariables.keySet());
//                for (Map.Entry<String, Variable> expectedEntry : expectedVariables.entrySet()) {
//                    Variable actual = actualVariables.get(expectedEntry.getKey());
//                    cleanVariable(actual);
//                    cleanVariable(expectedEntry.getValue());
//                    assertEquals(expectedEntry.getKey(), expectedEntry.getValue(), actual);
//                }
//            }
//        }
    }

    public void cleanVariable(Variable variable) {
//        variable.setDescription(null);
        if (variable.getAllowedValues() != null && variable.getAllowedValues().isEmpty()) {
            variable.setAllowedValues(null);
        }
        if (StringUtils.isEmpty(variable.getDependsOn())) {
            variable.setDependsOn(null);
        }
        variable.setAttributes(null);
        if (variable.getVariables() != null) {
            ArrayList<Variable> l = new ArrayList<>(variable.getVariables());
            l.sort(Comparator.comparing(Variable::getId));
            for (Variable subVariable : l) {
                cleanVariable(subVariable);
            }
            variable.setVariables(new LinkedHashSet<>(l));
        }
    }

    @Test
    public void testCreateStudyWithAllUsersAsMembers() throws CatalogException {
        catalogManager.getOrganizationManager().updateConfiguration(organizationId, new OrganizationConfiguration()
                .setUser(new UserOrganizationConfiguration("21000101000000", true)), QueryOptions.empty(), ownerToken);

        Study study = catalogManager.getStudyManager().create(projectFqn1, new Study().setId("newStudy"), INCLUDE_RESULT, ownerToken).first();

        List<String> userList = catalogManager.getUserManager().search(organizationId, new Query(),
                        new QueryOptions(QueryOptions.INCLUDE, UserDBAdaptor.QueryParams.ID.key()), ownerToken).getResults()
                .stream().map(User::getId).collect(Collectors.toList());
        assertEquals(8, userList.size());


        assertEquals(2, study.getGroups().size());
        for (Group group : study.getGroups()) {
            switch (group.getId()) {
                case StudyManager.ADMINS:
                    assertEquals(3, group.getUserIds().size());
                    assertTrue(group.getUserIds().containsAll(Arrays.asList(orgOwnerUserId, orgAdminUserId1, orgAdminUserId2)));
                    break;
                case StudyManager.MEMBERS:
                    assertEquals(userList.size(), group.getUserIds().size());
                    assertTrue(group.getUserIds().containsAll(userList));
                    break;
                default:
                    fail("Unexpected group: " + group.getId());
            }
        }
    }

    @Test
    public void removeFromMembersGroupWithAddToMembersConfigurationTest() throws CatalogException {
        catalogManager.getOrganizationManager().updateConfiguration(organizationId, new OrganizationConfiguration()
                .setUser(new UserOrganizationConfiguration("21000101000000", true)), QueryOptions.empty(), ownerToken);

        thrown.expect(CatalogException.class);
        thrown.expectMessage("organization configuration");
        catalogManager.getStudyManager().updateGroup(studyId, StudyManager.MEMBERS, ParamUtils.BasicUpdateAction.REMOVE,
                new GroupUpdateParams(Collections.singletonList(normalUserId1)), ownerToken);
    }

    @Test
    public void testCreateDuplicatedVariableSets() throws Exception {
        Study study = catalogManager.getStudyManager().get(studyFqn, null, ownerToken).first();

        // Create a new variable set changing the id
        study.getVariableSets().get(0).setId("newId");
        catalogManager.getStudyManager().createVariableSet(studyFqn, study.getVariableSets().get(0), ownerToken);
        Study study2 = catalogManager.getStudyManager().get(studyFqn, null, ownerToken).first();
        assertEquals(study.getVariableSets().size() + 1, study2.getVariableSets().size());

        // Replicate the first of the variable sets for creation
        thrown.expect(CatalogException.class);
        thrown.expectMessage("already exists");
        catalogManager.getStudyManager().createVariableSet(studyFqn, study.getVariableSets().get(0), ownerToken);
    }

    @Test
    public void internalVariableSetTest() throws CatalogException {
        Study study = catalogManager.getStudyManager().create(project1, "newStudy", "newStudy", "newStudy", null, null,
                null, null, null, INCLUDE_RESULT, ownerToken).first();

        Set<Variable> variables = new HashSet<>();
        variables.add(new Variable().setId("a").setType(Variable.VariableType.STRING));
        variables.add(new Variable().setId("b").setType(Variable.VariableType.MAP_INTEGER).setAllowedKeys(Arrays.asList("b1", "b2")));
        VariableSet variableSet = new VariableSet("myInternalVset", "", false, false, true, "", variables, null, 1, null);

        OpenCGAResult<VariableSet> result = catalogManager.getStudyManager().createVariableSet(study.getId(), variableSet, ownerToken);
        assertEquals(1, result.getNumUpdated());
        assertEquals(1, result.getNumResults());
        assertEquals(1, result.getResults().size());

        // An internal variable set should never be returned
        study = catalogManager.getStudyManager().get("newStudy", QueryOptions.empty(), ownerToken).first();
        for (VariableSet vset : study.getVariableSets()) {
            assertNotEquals(variableSet.getId(), vset.getId());
            assertFalse(vset.isInternal());
        }

        // But if I try to create another one with the same id, it should fail
        thrown.expect(CatalogException.class);
        thrown.expectMessage("exists");
        catalogManager.getStudyManager().createVariableSet(study.getId(), variableSet, ownerToken);
    }

    @Test
    public void updateInternalRecessiveGene() throws CatalogException {
        Study study = catalogManager.getStudyManager().create(project1, "newStudy", "newStudy", "newStudy", null, null, null, null, null,
                INCLUDE_RESULT, ownerToken).first();
        assertEquals(RecessiveGeneSummaryIndex.Status.NOT_INDEXED, study.getInternal().getIndex().getRecessiveGene().getStatus());

        String date = TimeUtils.getTime();
        catalogManager.getStudyManager().updateSummaryIndex("newStudy",
                new RecessiveGeneSummaryIndex(RecessiveGeneSummaryIndex.Status.INDEXED, date), ownerToken);
        study = catalogManager.getStudyManager().get("newStudy", QueryOptions.empty(), ownerToken).first();

        assertEquals(RecessiveGeneSummaryIndex.Status.INDEXED, study.getInternal().getIndex().getRecessiveGene().getStatus());
        assertEquals(date, study.getInternal().getIndex().getRecessiveGene().getModificationDate());

        catalogManager.getStudyManager().updateGroup("newStudy", "members", ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList(normalUserId1)), ownerToken);

        thrown.expect(CatalogAuthorizationException.class);
        thrown.expectMessage("admin");
        catalogManager.getStudyManager().updateSummaryIndex("newStudy",
                new RecessiveGeneSummaryIndex(RecessiveGeneSummaryIndex.Status.INDEXED, date), normalToken1);
    }

    @Test
    public void updateClinicalConfiguration() throws CatalogException {
        Study study = catalogManager.getStudyManager().create(project1, "newStudy", "newStudy", "newStudy", null, null, null, null, null,
                INCLUDE_RESULT, ownerToken).first();
        assertNotNull(study.getInternal().getConfiguration());
        assertNotNull(study.getInternal().getConfiguration().getClinical());
        assertFalse(study.getInternal().getConfiguration().getClinical().getPriorities().isEmpty());
        assertFalse(study.getInternal().getConfiguration().getClinical().getFlags().isEmpty());
        assertFalse(study.getInternal().getConfiguration().getClinical().getStatus().isEmpty());

        study.getInternal().getConfiguration().getClinical().setPriorities(Collections.singletonList(new ClinicalPriorityValue("bla", "bla", 1, true)));
        catalogManager.getClinicalAnalysisManager().configureStudy("newStudy", study.getInternal().getConfiguration().getClinical(), ownerToken);

        study = catalogManager.getStudyManager().get("newStudy", QueryOptions.empty(), ownerToken).first();
        assertNotNull(study.getInternal().getConfiguration());
        assertNotNull(study.getInternal().getConfiguration().getClinical());
        assertFalse(study.getInternal().getConfiguration().getClinical().getPriorities().isEmpty());
        assertEquals(1, study.getInternal().getConfiguration().getClinical().getPriorities().size());
        assertEquals("bla", study.getInternal().getConfiguration().getClinical().getPriorities().get(0).getId());
        assertFalse(study.getInternal().getConfiguration().getClinical().getFlags().isEmpty());
        assertFalse(study.getInternal().getConfiguration().getClinical().getStatus().isEmpty());

        catalogManager.getClinicalAnalysisManager().configureStudy("newStudy", ClinicalAnalysisStudyConfiguration.defaultConfiguration(), ownerToken);

        study = catalogManager.getStudyManager().get("newStudy", QueryOptions.empty(), ownerToken).first();
        assertNotNull(study.getInternal().getConfiguration());
        assertNotNull(study.getInternal().getConfiguration().getClinical());
        assertFalse(study.getInternal().getConfiguration().getClinical().getPriorities().isEmpty());
        assertTrue(study.getInternal().getConfiguration().getClinical().getPriorities().size() > 1);
        assertFalse(study.getInternal().getConfiguration().getClinical().getFlags().isEmpty());
        assertFalse(study.getInternal().getConfiguration().getClinical().getStatus().isEmpty());
    }


    @Test
    public void testSetVariantEngineConfiguration() throws CatalogException {
        Study study = catalogManager.getStudyManager().get(studyFqn, null, ownerToken).first();
        System.out.println("getVariantEngineConfiguration() = "
                + study.getInternal().getConfiguration().getVariantEngine());

        catalogManager.getStudyManager().setVariantEngineConfigurationOptions(studyFqn, new ObjectMap("k1", "v1"), ownerToken);
        study = catalogManager.getStudyManager().get(studyFqn, null, ownerToken).first();
        System.out.println("getVariantEngineConfiguration() = "
                + study.getInternal().getConfiguration().getVariantEngine());
        assertEquals(new ObjectMap("k1", "v1"), study.getInternal().getConfiguration().getVariantEngine().getOptions());

        catalogManager.getStudyManager().setVariantEngineConfigurationOptions(studyFqn, new ObjectMap("k2", "v2"), ownerToken);
        study = catalogManager.getStudyManager().get(studyFqn, null, ownerToken).first();
        System.out.println("getVariantEngineConfiguration() = "
                + study.getInternal().getConfiguration().getVariantEngine());
        assertEquals(new ObjectMap("k2", "v2"), study.getInternal().getConfiguration().getVariantEngine().getOptions());

        SampleIndexConfiguration sampleIndexConfiguration = SampleIndexConfiguration.defaultConfiguration();
        catalogManager.getStudyManager()
                .setVariantEngineConfigurationSampleIndex(studyFqn, sampleIndexConfiguration, ownerToken);
        study = catalogManager.getStudyManager().get(studyFqn, null, ownerToken).first();
        System.out.println("getVariantEngineConfiguration() = "
                + study.getInternal().getConfiguration().getVariantEngine());
        assertEquals(sampleIndexConfiguration, study.getInternal().getConfiguration().getVariantEngine().getSampleIndex());

    }

    @Test
    public void uploadTemplates() throws IOException, CatalogException {
        InputStream inputStream = getClass().getResource("/template.zip").openStream();
        OpenCGAResult<String> result = catalogManager.getStudyManager().uploadTemplate(studyFqn, "template.zip", inputStream, ownerToken);
        assertFalse(StringUtils.isEmpty(result.first()));
        System.out.println(result.first());

        inputStream = getClass().getResource("/template.zip").openStream();
        result = catalogManager.getStudyManager().uploadTemplate(studyFqn, "template.zip", inputStream, ownerToken);
        System.out.println(result.first());
    }

    @Test
    public void deleteTemplates() throws IOException, CatalogException {
        InputStream inputStream = getClass().getResource("/template.zip").openStream();
        String templateId = catalogManager.getStudyManager().uploadTemplate(studyFqn, "template.zip", inputStream, ownerToken).first();

        Boolean deleted = catalogManager.getStudyManager().deleteTemplate(studyFqn, templateId, ownerToken).first();
        assertTrue(deleted);

        thrown.expectMessage("doesn't exist");
        thrown.expect(CatalogException.class);
        catalogManager.getStudyManager().deleteTemplate(studyFqn, templateId, ownerToken);
    }

    @Test
    public void emptyGroupTest() throws CatalogException {
        // In the list of users we add it as null to test it properly
        catalogManager.getStudyManager().createGroup(studyFqn, "@test", null, ownerToken);
        Group first = catalogManager.getStudyManager().getGroup(studyFqn, "@test", ownerToken).first();
        assertNotNull(first.getUserIds());

        catalogManager.getUserManager().create("dummy", "dummy", "dummy@mail.com", TestParamConstants.PASSWORD, organizationId, 0L, opencgaToken);
        catalogManager.getStudyManager().createGroup(studyFqn, "@test2", Collections.singletonList("dummy"), ownerToken);
        catalogManager.getStudyManager().updateAcl(studyFqn, "@test2", new StudyAclParams("", "view_only"), ParamUtils.AclAction.ADD, ownerToken);

        String dummyToken = catalogManager.getUserManager().login(organizationId, "dummy", TestParamConstants.PASSWORD).first().getToken();
        OpenCGAResult<File> search = catalogManager.getFileManager().search(studyFqn, new Query(), new QueryOptions(), dummyToken);
        assertTrue(search.getNumResults() > 0);
    }

    // -----------------------------------------------------------------------
    // Samplesheet registration tests
    // -----------------------------------------------------------------------

    @Test
    public void testRegisterFromSamplesheetMinimal() throws Exception {
        String samplesheet = "#sample\n"
                + "SAMPLE_001\n"
                + "SAMPLE_002\n"
                + "SAMPLE_003\n";

        OpenCGAResult<Map<String, Integer>> result = catalogManager.getStudyManager()
                .registerFromSamplesheetContent(studyFqn, samplesheet, ownerToken);

        Map<String, Integer> counts = result.first();
        assertEquals(3, (int) counts.get("individualsCreated"));
        assertEquals(3, (int) counts.get("samplesCreated"));

        // Verify individuals and samples exist
        assertEquals(1, catalogManager.getIndividualManager().get(studyFqn, "SAMPLE_001", QueryOptions.empty(), ownerToken).getNumResults());
        assertEquals(1, catalogManager.getSampleManager().get(studyFqn, "SAMPLE_002", QueryOptions.empty(), ownerToken).getNumResults());
    }

    @Test
    public void testRegisterFromSamplesheetWithMetadata() throws Exception {
        String samplesheet = "#sample,individual,gender,disorder\n"
                + "S001,IND001,male,HP:0001250\n"
                + "S002,IND002,female,\n"
                + "S003,IND003,unknown,OMIM:614856\n";

        OpenCGAResult<Map<String, Integer>> result = catalogManager.getStudyManager()
                .registerFromSamplesheetContent(studyFqn, samplesheet, ownerToken);

        Map<String, Integer> counts = result.first();
        assertEquals(3, (int) counts.get("individualsCreated"));
        assertEquals(3, (int) counts.get("samplesCreated"));

        // Verify individual has correct sex
        org.opencb.opencga.core.models.individual.Individual ind1 = catalogManager.getIndividualManager()
                .get(studyFqn, "IND001", QueryOptions.empty(), ownerToken).first();
        assertEquals("MALE", ind1.getSex().getId());

        // Verify individual has disorder
        assertFalse(ind1.getDisorders().isEmpty());
        assertEquals("HP:0001250", ind1.getDisorders().get(0).getId());

        // Verify sample is linked to individual
        org.opencb.opencga.core.models.sample.Sample s1 = catalogManager.getSampleManager()
                .get(studyFqn, "S001", QueryOptions.empty(), ownerToken).first();
        assertEquals("IND001", s1.getIndividualId());
    }

    @Test
    public void testRegisterFromSamplesheetTsv() throws Exception {
        String samplesheet = "#sample\tindividual\tgender\n"
                + "S001\tIND001\tmale\n"
                + "S002\tIND002\tfemale\n";

        OpenCGAResult<Map<String, Integer>> result = catalogManager.getStudyManager()
                .registerFromSamplesheetContent(studyFqn, samplesheet, ownerToken);

        Map<String, Integer> counts = result.first();
        assertEquals(2, (int) counts.get("individualsCreated"));
        assertEquals(2, (int) counts.get("samplesCreated"));
    }

    @Test
    public void testRegisterFromSamplesheetIdempotent() throws Exception {
        String samplesheet = "#sample,individual\n"
                + "S001,IND001\n"
                + "S002,IND002\n";

        // First call — creates
        catalogManager.getStudyManager().registerFromSamplesheetContent(studyFqn, samplesheet, ownerToken);

        // Second call — should not fail, entities already exist
        OpenCGAResult<Map<String, Integer>> result = catalogManager.getStudyManager()
                .registerFromSamplesheetContent(studyFqn, samplesheet, ownerToken);

        Map<String, Integer> counts = result.first();
        assertEquals(0, (int) counts.get("individualsCreated"));
        assertEquals(0, (int) counts.get("samplesCreated"));
        assertEquals(2, (int) counts.get("individualsExisting"));
        assertEquals(2, (int) counts.get("samplesExisting"));
    }

    @Test(expected = CatalogException.class)
    public void testRegisterFromSamplesheetDuplicateSample() throws Exception {
        String samplesheet = "#sample\n"
                + "S001\n"
                + "S001\n";

        catalogManager.getStudyManager().registerFromSamplesheetContent(studyFqn, samplesheet, ownerToken);
    }

    @Test(expected = CatalogException.class)
    public void testRegisterFromSamplesheetInvalidSex() throws Exception {
        String samplesheet = "#sample,gender\n"
                + "S001,invalid_sex\n";

        catalogManager.getStudyManager().registerFromSamplesheetContent(studyFqn, samplesheet, ownerToken);
    }

    @Test(expected = CatalogException.class)
    public void testRegisterFromSamplesheetEmptyContent() throws Exception {
        catalogManager.getStudyManager().registerFromSamplesheetContent(studyFqn, "", ownerToken);
    }

    @Test(expected = CatalogException.class)
    public void testRegisterFromSamplesheetNoHeader() throws Exception {
        String samplesheet = "S001\n";
        catalogManager.getStudyManager().registerFromSamplesheetContent(studyFqn, samplesheet, ownerToken);
    }

    @Test
    public void testRegisterFromSamplesheetWithSomatic() throws Exception {
        String samplesheet = "#sample,individual,somatic\n"
                + "S001,IND001,false\n"
                + "S002,IND001,true\n";

        catalogManager.getStudyManager().registerFromSamplesheetContent(studyFqn, samplesheet, ownerToken);

        org.opencb.opencga.core.models.sample.Sample s1 = catalogManager.getSampleManager()
                .get(studyFqn, "S001", QueryOptions.empty(), ownerToken).first();
        assertFalse(s1.isSomatic());

        org.opencb.opencga.core.models.sample.Sample s2 = catalogManager.getSampleManager()
                .get(studyFqn, "S002", QueryOptions.empty(), ownerToken).first();
        assertTrue(s2.isSomatic());
    }

    @Test
    public void testRegisterFromSamplesheetExistingSampleWithIndividual() throws Exception {
        // Pre-create individual and sample linked together
        catalogManager.getIndividualManager().create(studyFqn,
                new org.opencb.opencga.core.models.individual.Individual().setId("IND_PRE"), QueryOptions.empty(), ownerToken);
        catalogManager.getSampleManager().create(studyFqn,
                new org.opencb.opencga.core.models.sample.Sample().setId("S_PRE").setIndividualId("IND_PRE"),
                QueryOptions.empty(), ownerToken);

        // Samplesheet with same sample — no individual column, should reuse existing
        String samplesheet = "#sample\n"
                + "S_PRE\n";

        OpenCGAResult<Map<String, Integer>> result = catalogManager.getStudyManager()
                .registerFromSamplesheetContent(studyFqn, samplesheet, ownerToken);

        Map<String, Integer> counts = result.first();
        assertEquals(0, (int) counts.get("individualsCreated"));
        assertEquals(0, (int) counts.get("samplesCreated"));
        assertEquals(1, (int) counts.get("samplesExisting"));
    }

    @Test(expected = CatalogException.class)
    public void testRegisterFromSamplesheetConflictingIndividual() throws Exception {
        // Pre-create individual and sample linked together
        catalogManager.getIndividualManager().create(studyFqn,
                new org.opencb.opencga.core.models.individual.Individual().setId("IND_ORIG"), QueryOptions.empty(), ownerToken);
        catalogManager.getSampleManager().create(studyFqn,
                new org.opencb.opencga.core.models.sample.Sample().setId("S_CONFLICT").setIndividualId("IND_ORIG"),
                QueryOptions.empty(), ownerToken);

        // Samplesheet tries to assign a different individual — should fail
        String samplesheet = "#sample,individual\n"
                + "S_CONFLICT,IND_DIFFERENT\n";

        catalogManager.getStudyManager().registerFromSamplesheetContent(studyFqn, samplesheet, ownerToken);
    }

    @Test(expected = CatalogException.class)
    public void testRegisterFromSamplesheetMissingSampleColumn() throws Exception {
        // Only 'individual' column, no 'sample' — should fail
        String samplesheet = "#individual\n"
                + "IND001\n";

        catalogManager.getStudyManager().registerFromSamplesheetContent(studyFqn, samplesheet, ownerToken);
    }
}
