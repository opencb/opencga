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
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.AvroToAnnotationConverter;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyUpdateParams;
import org.opencb.opencga.core.models.study.Variable;
import org.opencb.opencga.core.models.study.VariableSet;
import org.opencb.opencga.core.models.study.configuration.ClinicalAnalysisStudyConfiguration;
import org.opencb.opencga.core.models.study.configuration.StudyConfiguration;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;

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

        assertEquals(s.size(), study.getVariableSets().size());
        assertEquals(s, study.getVariableSets().stream().map(v->v.getAttributes().get("resource")).collect(Collectors.toSet()));

        for (VariableSet variableSet : study.getVariableSets()) {
            Object avroClassStr = variableSet.getAttributes().get("avroClass");
            System.out.println("variableSet.getAttributes().get(\"avroClass\") = " + avroClassStr);
            if (avroClassStr != null) {
                Class<?> avroClass = Class.forName(avroClassStr.toString());
                Schema schema = (Schema) avroClass.getMethod("getClassSchema").invoke(null);
                Map<String, Variable> expectedVariables = AvroToAnnotationConverter.convertToVariableSet(schema).stream().collect(Collectors.toMap(Variable::getId, v -> v));
                Map<String, Variable> actualVariables = variableSet.getVariables().stream().collect(Collectors.toMap(Variable::getId, v->v));

                assertEquals(expectedVariables.keySet(), actualVariables.keySet());
                for (Map.Entry<String, Variable> expectedEntry : expectedVariables.entrySet()) {
                    Variable actual = actualVariables.get(expectedEntry.getKey());
                    cleanVariable(actual);
                    cleanVariable(expectedEntry.getValue());
                    assertEquals(expectedEntry.getKey(), expectedEntry.getValue().toString(), actual.toString());
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
    public void updateClinicalConfiguration() throws CatalogException {
        Study study = catalogManager.getStudyManager().create(project1, "newStudy", "newStudy", "newStudy", null, null,
                null, null, null, new QueryOptions(), token).first();
        assertNotNull(study.getConfiguration());
        assertNotNull(study.getConfiguration().getClinical());
        assertFalse(study.getConfiguration().getClinical().getPriorities().isEmpty());
        assertFalse(study.getConfiguration().getClinical().getFlags().isEmpty());
        assertFalse(study.getConfiguration().getClinical().getStatus().isEmpty());

        ClinicalAnalysisStudyConfiguration configuration = new ClinicalAnalysisStudyConfiguration(Collections.emptyMap(), null,
                Collections.emptyList(), Collections.emptyMap(), null);
        StudyUpdateParams updateParams = new StudyUpdateParams()
                .setConfiguration(new StudyConfiguration(configuration));
        catalogManager.getStudyManager().update("newStudy", updateParams, QueryOptions.empty(), token);

        study = catalogManager.getStudyManager().get("newStudy", QueryOptions.empty(), token).first();
        assertNotNull(study.getConfiguration());
        assertNotNull(study.getConfiguration().getClinical());
        assertTrue(study.getConfiguration().getClinical().getPriorities().isEmpty());
        assertTrue(study.getConfiguration().getClinical().getFlags().isEmpty());
        assertTrue(study.getConfiguration().getClinical().getStatus().isEmpty());
    }
}
