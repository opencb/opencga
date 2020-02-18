package org.opencb.opencga.catalog.managers;

import org.apache.avro.Schema;
import org.apache.solr.common.StringUtils;
import org.junit.Test;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.utils.AvroToAnnotationConverter;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.Variable;
import org.opencb.opencga.core.models.study.VariableSet;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class StudyManagerTest extends AbstractManagerTest {

    @Test
    public void testDefaultVariableSets() throws Exception {
        String fqn = catalogManager.getStudyManager().create(project1, "newStudy", "newStudy", "newStudy", Study.Type.COLLECTION,
                null, null, null, null, null, null, null, null, null, new QueryOptions(), token).first().getFqn();

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
}
