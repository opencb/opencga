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

import org.apache.avro.Schema;
import org.apache.solr.common.StringUtils;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.AvroToAnnotationConverter;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.config.storage.SampleIndexConfiguration;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.study.*;
import org.opencb.opencga.core.models.study.configuration.ClinicalAnalysisStudyConfiguration;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class StudyManagerTest extends AbstractManagerTest {

    @Test
    public void testDefaultVariableSets() throws Exception {
        String fqn = catalogManager.getStudyManager().create(project1, "newStudy", "newStudy", "newStudy", null, null,
                null, null, null, new QueryOptions(), token).first().getFqn();

        Study study = catalogManager.getStudyManager().get(fqn, null, token).first();

        Set<String> s = new Reflections(new ResourcesScanner(), "variablesets/").getResources(Pattern.compile(".*\\.json"));

        // This variable set is internal so it will not be returned from study
        s.remove("variablesets/sample-variant-stats-variableset.json");
        assertEquals(s.size(), study.getVariableSets().size());
        assertEquals(s, study.getVariableSets().stream().map(v->v.getAttributes().get("resource")).collect(Collectors.toSet()));

        for (VariableSet variableSet : study.getVariableSets()) {
            Object avroClassStr = variableSet.getAttributes().get("avroClass");
            System.out.println("variableSet.getAttributes().get(\"avroClass\") = " + avroClassStr);
            if (avroClassStr != null) {
                Class<?> avroClass = Class.forName(avroClassStr.toString().split(" ")[1]);
                Schema schema = (Schema) avroClass.getMethod("getClassSchema").invoke(null);
                Map<String, Variable> expectedVariables = AvroToAnnotationConverter.convertToVariableSet(schema).stream().collect(Collectors.toMap(Variable::getId, v -> v));
                Map<String, Variable> actualVariables = variableSet.getVariables().stream().collect(Collectors.toMap(Variable::getId, v->v));

                assertEquals(expectedVariables.keySet(), actualVariables.keySet());
                for (Map.Entry<String, Variable> expectedEntry : expectedVariables.entrySet()) {
                    Variable actual = actualVariables.get(expectedEntry.getKey());
                    cleanVariable(actual);
                    cleanVariable(expectedEntry.getValue());
                    assertEquals(expectedEntry.getKey(), expectedEntry.getValue(), actual);
                }
            }
        }
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
        if (variable.getVariableSet() != null) {
            ArrayList<Variable> l = new ArrayList<>(variable.getVariableSet());
            l.sort(Comparator.comparing(Variable::getId));
            for (Variable subVariable : l) {
                cleanVariable(subVariable);
            }
            variable.setVariableSet(new LinkedHashSet<>(l));
        }
    }

    @Test
    public void testCreateDuplicatedVariableSets() throws Exception {
        Study study = catalogManager.getStudyManager().get(studyFqn, null, token).first();

        // Create a new variable set changing the id
        study.getVariableSets().get(0).setId("newId");
        catalogManager.getStudyManager().createVariableSet(studyFqn, study.getVariableSets().get(0), token);
        Study study2 = catalogManager.getStudyManager().get(studyFqn, null, token).first();
        assertEquals(study.getVariableSets().size() + 1, study2.getVariableSets().size());

        // Replicate the first of the variable sets for creation
        thrown.expect(CatalogException.class);
        thrown.expectMessage("already exists");
        catalogManager.getStudyManager().createVariableSet(studyFqn, study.getVariableSets().get(0), token);
    }

    @Test
    public void internalVariableSetTest() throws CatalogException {
        Study study = catalogManager.getStudyManager().create(project1, "newStudy", "newStudy", "newStudy", null, null,
                null, null, null, new QueryOptions(), token).first();

        Set<Variable> variables = new HashSet<>();
        variables.add(new Variable().setId("a").setType(Variable.VariableType.STRING));
        variables.add(new Variable().setId("b").setType(Variable.VariableType.MAP_INTEGER).setAllowedKeys(Arrays.asList("b1", "b2")));
        VariableSet variableSet = new VariableSet("myInternalVset", "", false, false, true, "", variables, null, 1, null);

        OpenCGAResult<VariableSet> result = catalogManager.getStudyManager().createVariableSet(study.getId(), variableSet, token);
        assertEquals(1, result.getNumUpdated());
        assertEquals(1, result.getNumResults());
        assertEquals(1, result.getResults().size());

        // An internal variable set should never be returned
        study = catalogManager.getStudyManager().get("newStudy", QueryOptions.empty(), token).first();
        for (VariableSet vset : study.getVariableSets()) {
            assertNotEquals(variableSet.getId(), vset.getId());
            assertFalse(vset.isInternal());
        }

        // But if I try to create another one with the same id, it should fail
        thrown.expect(CatalogException.class);
        thrown.expectMessage("exists");
        catalogManager.getStudyManager().createVariableSet(study.getId(), variableSet, token);
    }

    @Test
    public void updateInternalRecessiveGene() throws CatalogException {
        Study study = catalogManager.getStudyManager().create(project1, "newStudy", "newStudy", "newStudy", null, null,
                null, null, null, new QueryOptions(), token).first();
        assertEquals(RecessiveGeneSummaryIndex.Status.NOT_INDEXED, study.getInternal().getIndex().getRecessiveGene().getStatus());

        String date = TimeUtils.getTime();
        catalogManager.getStudyManager().updateSummaryIndex("newStudy",
                new RecessiveGeneSummaryIndex(RecessiveGeneSummaryIndex.Status.INDEXED, date), token);
        study = catalogManager.getStudyManager().get("newStudy", QueryOptions.empty(), token).first();

        assertEquals(RecessiveGeneSummaryIndex.Status.INDEXED, study.getInternal().getIndex().getRecessiveGene().getStatus());
        assertEquals(date, study.getInternal().getIndex().getRecessiveGene().getModificationDate());

        catalogManager.getStudyManager().updateGroup("newStudy", "members", ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList("user2")), token);

        thrown.expect(CatalogAuthorizationException.class);
        thrown.expectMessage("admin");
        catalogManager.getStudyManager().updateSummaryIndex("newStudy",
                new RecessiveGeneSummaryIndex(RecessiveGeneSummaryIndex.Status.INDEXED, date), sessionIdUser2);
    }

    @Test
    public void updateClinicalConfiguration() throws CatalogException {
        Study study = catalogManager.getStudyManager().create(project1, "newStudy", "newStudy", "newStudy", null, null,
                null, null, null, new QueryOptions(), token).first();
        assertNotNull(study.getInternal().getConfiguration());
        assertNotNull(study.getInternal().getConfiguration().getClinical());
        assertFalse(study.getInternal().getConfiguration().getClinical().getPriorities().isEmpty());
        assertFalse(study.getInternal().getConfiguration().getClinical().getFlags().isEmpty());
        assertFalse(study.getInternal().getConfiguration().getClinical().getStatus().isEmpty());

        ClinicalAnalysisStudyConfiguration clinicalConfiguration = new ClinicalAnalysisStudyConfiguration(Collections.emptyMap(), null,
                Collections.emptyList(), Collections.emptyMap(), null);
        catalogManager.getClinicalAnalysisManager().configureStudy("newStudy", clinicalConfiguration, token);

        study = catalogManager.getStudyManager().get("newStudy", QueryOptions.empty(), token).first();
        assertNotNull(study.getInternal().getConfiguration());
        assertNotNull(study.getInternal().getConfiguration().getClinical());
        assertTrue(study.getInternal().getConfiguration().getClinical().getPriorities().isEmpty());
        assertTrue(study.getInternal().getConfiguration().getClinical().getFlags().isEmpty());
        assertTrue(study.getInternal().getConfiguration().getClinical().getStatus().isEmpty());

        clinicalConfiguration = ClinicalAnalysisStudyConfiguration.defaultConfiguration();
        catalogManager.getClinicalAnalysisManager().configureStudy("newStudy", clinicalConfiguration, token);

        study = catalogManager.getStudyManager().get("newStudy", QueryOptions.empty(), token).first();
        assertNotNull(study.getInternal().getConfiguration());
        assertNotNull(study.getInternal().getConfiguration().getClinical());
        assertFalse(study.getInternal().getConfiguration().getClinical().getPriorities().isEmpty());
        assertFalse(study.getInternal().getConfiguration().getClinical().getFlags().isEmpty());
        assertFalse(study.getInternal().getConfiguration().getClinical().getStatus().isEmpty());

    }


    @Test
    public void testSetVariantEngineConfiguration() throws CatalogException {
        Study study = catalogManager.getStudyManager().get(studyFqn, null, token).first();
        System.out.println("getVariantEngineConfiguration() = "
                + study.getInternal().getConfiguration().getVariantEngine());

        catalogManager.getStudyManager().setVariantEngineConfigurationOptions(studyFqn, new ObjectMap("k1", "v1"), token);
        study = catalogManager.getStudyManager().get(studyFqn, null, token).first();
        System.out.println("getVariantEngineConfiguration() = "
                + study.getInternal().getConfiguration().getVariantEngine());
        assertEquals(new ObjectMap("k1", "v1"), study.getInternal().getConfiguration().getVariantEngine().getOptions());

        catalogManager.getStudyManager().setVariantEngineConfigurationOptions(studyFqn, new ObjectMap("k2", "v2"), token);
        study = catalogManager.getStudyManager().get(studyFqn, null, token).first();
        System.out.println("getVariantEngineConfiguration() = "
                + study.getInternal().getConfiguration().getVariantEngine());
        assertEquals(new ObjectMap("k2", "v2"), study.getInternal().getConfiguration().getVariantEngine().getOptions());

        SampleIndexConfiguration sampleIndexConfiguration = SampleIndexConfiguration.defaultConfiguration();
        catalogManager.getStudyManager()
                .setVariantEngineConfigurationSampleIndex(studyFqn, sampleIndexConfiguration, token);
        study = catalogManager.getStudyManager().get(studyFqn, null, token).first();
        System.out.println("getVariantEngineConfiguration() = "
                + study.getInternal().getConfiguration().getVariantEngine());
        assertEquals(sampleIndexConfiguration, study.getInternal().getConfiguration().getVariantEngine().getSampleIndex());

    }

    @Test
    public void uploadTemplates() throws IOException, CatalogException {
        InputStream inputStream = getClass().getResource("/template.zip").openStream();
        OpenCGAResult<String> result = catalogManager.getStudyManager().uploadTemplate(studyFqn, "template.zip", inputStream, token);
        assertFalse(StringUtils.isEmpty(result.first()));
        System.out.println(result.first());

        inputStream = getClass().getResource("/template.zip").openStream();
        result = catalogManager.getStudyManager().uploadTemplate(studyFqn, "template.zip", inputStream, token);
        System.out.println(result.first());
    }

    @Test
    public void deleteTemplates() throws IOException, CatalogException {
        InputStream inputStream = getClass().getResource("/template.zip").openStream();
        String templateId = catalogManager.getStudyManager().uploadTemplate(studyFqn, "template.zip", inputStream, token).first();

        Boolean deleted = catalogManager.getStudyManager().deleteTemplate(studyFqn, templateId, token).first();
        assertTrue(deleted);

        thrown.expectMessage("doesn't exist");
        thrown.expect(CatalogException.class);
        catalogManager.getStudyManager().deleteTemplate(studyFqn, templateId, token);
    }

    @Test
    public void emptyGroupTest() throws CatalogException {
        // In the list of users we add it as null to test it properly
        catalogManager.getStudyManager().createGroup(studyFqn, "@test", null, token);
        Group first = catalogManager.getStudyManager().getGroup(studyFqn, "@test", token).first();
        assertNotNull(first.getUserIds());

        catalogManager.getUserManager().create("dummy", "dummy", "dummy@mail.com", "dummy", "", 0L, Account.AccountType.GUEST, token);
        catalogManager.getStudyManager().createGroup(studyFqn, "@test2", Collections.singletonList("dummy"), token);
        catalogManager.getStudyManager().updateAcl(studyFqn, "@test2", new StudyAclParams("", "view_only"), ParamUtils.AclAction.ADD, token);

        String dummyToken = catalogManager.getUserManager().login("dummy", "dummy").getToken();
        OpenCGAResult<File> search = catalogManager.getFileManager().search(studyFqn, new Query(), new QueryOptions(), dummyToken);
        assertTrue(search.getNumResults() > 0);
    }
}
