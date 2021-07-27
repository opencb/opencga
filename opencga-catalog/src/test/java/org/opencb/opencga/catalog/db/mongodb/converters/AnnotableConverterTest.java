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

package org.opencb.opencga.catalog.db.mongodb.converters;

import org.bson.Document;
import org.junit.Test;
import org.opencb.biodata.models.clinical.qc.SampleQcVariantStats;
import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleQualityControl;
import org.opencb.opencga.core.models.sample.SampleVariantQualityControlMetrics;
import org.opencb.opencga.core.models.study.Variable;
import org.opencb.opencga.core.models.study.VariableSet;

import java.util.*;

import static org.junit.Assert.*;

public class AnnotableConverterTest {

    @Test
    public void annotationToDB1() throws Exception {
        Variable v = new Variable().setId("a").setType(Variable.VariableType.BOOLEAN);
        Set<Variable> setOfVariables = new HashSet<>();
        setOfVariables.add(v);
        VariableSet variableSet = new VariableSet().setVariables(setOfVariables).setUid(1);

        Map<String, Object> myMap = new HashMap<>();
        myMap.put("b", "nothing");

        AnnotationConverter annotableConverter = new AnnotationConverter();
        List<Document> document1 = annotableConverter.annotationToDB(variableSet, "annotName", myMap);
        assertTrue(document1.isEmpty());

        myMap.put("a", "hello");
        List<Document> document2 = annotableConverter.annotationToDB(variableSet, "annotName", myMap);
        assertEquals(1, document2.size());
        assertEquals(6, document2.get(0).size());
        assertEquals("a", document2.get(0).get(AnnotationConverter.ID));
        assertEquals("annotName", document2.get(0).get(AnnotationConverter.ANNOTATION_SET_NAME));
        assertEquals(1L, document2.get(0).get(AnnotationConverter.VARIABLE_SET));
        assertEquals("hello", document2.get(0).get(AnnotationConverter.VALUE));
    }

    @Test
    public void annotationToDB2() throws Exception {
        // Variable set:   a: [{ b: [{c: xx} ] }]
        Set<Variable> bcSet = new HashSet<>();
        bcSet.add(new Variable().setId("c").setType(Variable.VariableType.BOOLEAN));
        Set<Variable> bSet = new HashSet<>();
        bSet.add(new Variable().setId("b").setMultiValue(true).setType(Variable.VariableType.OBJECT).setVariableSet(bcSet));
        Set<Variable> rootSet = new HashSet<>();
        rootSet.add(new Variable().setId("a").setMultiValue(true).setType(Variable.VariableType.OBJECT).setVariableSet(bSet));

        VariableSet vs = new VariableSet().setVariables(rootSet).setUid(1);

        Map<String, Object> myMap = new HashMap<>();
        myMap.put("nothing", "nothing");
        myMap.put("a", Arrays.asList(
                new ObjectMap("b", Arrays.asList(
                        new ObjectMap("c", true),
                        new ObjectMap("c", false)
                )),
                new ObjectMap("b", Arrays.asList(
                        new ObjectMap("c", false),
                        new ObjectMap("c", true)
                )),
                new ObjectMap("no_b_key", 0),
                new ObjectMap("b", Arrays.asList(
                        new ObjectMap("no_c_key", 0),
                        new ObjectMap("c", true)
                ))
        ));

        AnnotationConverter annotableConverter = new AnnotationConverter();
        List<Document> document = annotableConverter.annotationToDB(vs, "annotName", myMap);

        assertEquals(1, document.size());
        assertEquals(8, document.get(0).size());
        assertEquals(5, ((List) document.get(0).get(AnnotationConverter.VALUE)).size());
        assertEquals(Arrays.asList(true, false, false, true, true), document.get(0).get(AnnotationConverter.VALUE));
        assertEquals(Arrays.asList(Arrays.asList(1, 1), Arrays.asList(1, 1), 0, Arrays.asList(0, 1)),
                document.get(0).get(AnnotationConverter.COUNT_ELEMENTS));
        // Check data type
        assertTrue(((List) (((List) document.get(0).get(AnnotationConverter.COUNT_ELEMENTS)).get(0))).get(0) instanceof Integer);
        assertEquals(Arrays.asList(0, 1), document.get(0).get(AnnotationConverter.ARRAY_LEVEL));
        assertTrue(((List) document.get(0).get(AnnotationConverter.ARRAY_LEVEL)).get(0) instanceof Integer);
        assertEquals("annotName", document.get(0).get(AnnotationConverter.ANNOTATION_SET_NAME));
        assertEquals("a.b.c", document.get(0).get(AnnotationConverter.ID));
        assertEquals(1L, document.get(0).get(AnnotationConverter.VARIABLE_SET));
    }

    @Test
    public void annotationToDB3() throws Exception {
        // Variable set:   a: [{ b: {c: [xx] } }]
        Set<Variable> bcSet = new HashSet<>();
        bcSet.add(new Variable().setId("c").setMultiValue(true).setType(Variable.VariableType.BOOLEAN));
        Set<Variable> bSet = new HashSet<>();
        bSet.add(new Variable().setId("b").setType(Variable.VariableType.OBJECT).setVariableSet(bcSet));
        Set<Variable> rootSet = new HashSet<>();
        rootSet.add(new Variable().setId("a").setMultiValue(true).setType(Variable.VariableType.OBJECT).setVariableSet(bSet));

        VariableSet vs = new VariableSet().setVariables(rootSet).setUid(1);

        Map<String, Object> myMap = new HashMap<>();
        myMap.put("nothing", "nothing");
        myMap.put("a", Arrays.asList(
                new ObjectMap("b",
                        new ObjectMap("c", Arrays.asList(true, false))
                ),
                new ObjectMap("b", new ObjectMap("d", "something else")),
                new ObjectMap("no_b_key", 0),
                new ObjectMap("b", new ObjectMap()
                        .append("c", Arrays.asList(false, false, true))
                        .append("no_c_key", 0)
                )
        ));

        AnnotationConverter annotableConverter = new AnnotationConverter();
        List<Document> document = annotableConverter.annotationToDB(vs, "annotName", myMap);

        assertEquals(1, document.size());
        assertEquals(8, document.get(0).size());
        assertEquals(5, ((List) document.get(0).get(AnnotationConverter.VALUE)).size());
        assertEquals(Arrays.asList(true, false, false, false, true), document.get(0).get(AnnotationConverter.VALUE));
        assertEquals(Arrays.asList(Arrays.asList(2), 0, 0, Arrays.asList(3)), document.get(0).get(AnnotationConverter.COUNT_ELEMENTS));
        assertEquals(Arrays.asList(0, 2), document.get(0).get(AnnotationConverter.ARRAY_LEVEL));
        assertEquals("annotName", document.get(0).get(AnnotationConverter.ANNOTATION_SET_NAME));
        assertEquals("a.b.c", document.get(0).get(AnnotationConverter.ID));
        assertEquals(1L, document.get(0).get(AnnotationConverter.VARIABLE_SET));
    }

    @Test
    public void annotationToDB4() {
        // Variable set:   a: { ab1: {ab1c1: [boolean], ab1c2: string }, ab2: [{ ab2c1: { ab2c1d1: [numbers], ab2c1d2: string  } }],
        //                      ab3: [{ ab3c1: [{ ab3c1d1: [string], ab3c1d2: number }] }] }
        Set<Variable> ab1Set = new HashSet<>();
        ab1Set.add(new Variable().setId("ab1c1").setMultiValue(true).setType(Variable.VariableType.BOOLEAN));
        ab1Set.add(new Variable().setId("ab1c2").setType(Variable.VariableType.STRING));

        Set<Variable> ab2c1Set = new HashSet<>();
        ab2c1Set.add(new Variable().setId("ab2c1d1").setMultiValue(true).setType(Variable.VariableType.INTEGER));
        ab2c1Set.add(new Variable().setId("ab2c1d2").setType(Variable.VariableType.STRING));
        Set<Variable> ab2Set = new HashSet<>();
        ab2Set.add(new Variable().setId("ab2c1").setType(Variable.VariableType.OBJECT).setVariableSet(ab2c1Set));

        Set<Variable> ab3c1Set = new HashSet<>();
        ab3c1Set.add(new Variable().setId("ab3c1d1").setMultiValue(true).setType(Variable.VariableType.STRING));
        ab3c1Set.add(new Variable().setId("ab3c1d2").setType(Variable.VariableType.DOUBLE));
        Set<Variable> ab3Set = new HashSet<>();
        ab3Set.add(new Variable().setId("ab3c1").setType(Variable.VariableType.OBJECT).setMultiValue(true).setVariableSet(ab3c1Set));

        Set<Variable> aSet = new HashSet<>();
        aSet.add(new Variable().setId("ab1").setType(Variable.VariableType.OBJECT).setVariableSet(ab1Set));
        aSet.add(new Variable().setId("ab2").setMultiValue(true).setType(Variable.VariableType.OBJECT).setVariableSet(ab2Set));
        aSet.add(new Variable().setId("ab3").setMultiValue(true).setType(Variable.VariableType.OBJECT).setVariableSet(ab3Set));

        Set<Variable> rootSet = new HashSet<>();
        rootSet.add(new Variable().setId("a").setType(Variable.VariableType.OBJECT).setVariableSet(aSet));

        VariableSet vs = new VariableSet().setVariables(rootSet).setUid(1).setId("vsId");

        // We create a dummy full annotation for the variable set we just created
        Map<String, Object> myMap = new HashMap<>();
        myMap.put("nothing", "nothing");
        myMap.put("a", new ObjectMap()
                .append("ab1", new ObjectMap()
                        .append("ab1c1", Arrays.asList(true, false, false))
                        .append("ab1c2", "hello world")
                        .append("ab1c3", "this should not be taken into account")
                )
                .append("ab2", Arrays.asList(
                        new ObjectMap("ab2c1", new ObjectMap()
                                .append("ab2c1d1", Arrays.asList(1, 2, 3, 4))
                                .append("ab2c1d2", "hello ab2c1d2 1")
                        ),
                        new ObjectMap("ab2c1", new ObjectMap()
                                .append("ab2c1d1", Arrays.asList(11, 12, 13, 14))
                                .append("ab2c1d2", "hello ab2c1d2 2")
                        ),
                        new ObjectMap("ab2c1", new ObjectMap()
                                .append("ab2c1d1", Arrays.asList(21))
                        )
                ))
                .append("ab3", Arrays.asList(
                        new ObjectMap("ab3c1", Arrays.asList(
                                new ObjectMap()
                                        .append("ab3c1d1", Arrays.asList("hello"))
                                        .append("ab3c1d2", 2.0),
                                new ObjectMap()
                                        .append("ab3c1d2", 4.0)
                        )),
                        new ObjectMap("ab3c1", Arrays.asList(
                                new ObjectMap()
                                        .append("ab3c1d1", Arrays.asList("hello2", "bye2")),
                                new ObjectMap()
                                        .append("ab3c1d1", Arrays.asList("byeee2", "hellooo2"))
                                        .append("ab3c1d2", 24.0)
                        ))
                ))
        );

        AnnotationConverter annotableConverter = new AnnotationConverter();
        List<Document> documentList = annotableConverter.annotationToDB(vs, "annotName", myMap);

        // We convert it back
        List<AnnotationSet> annotationSets = annotableConverter.fromDBToAnnotation(documentList, new Document()
                .append(String.valueOf(vs.getUid()), vs.getId()), new QueryOptions());
        assertEquals(1, annotationSets.size());
        assertEquals("annotName", annotationSets.get(0).getId());
        assertEquals(vs.getId(), annotationSets.get(0).getVariableSetId());

        // And back again to the mongo properties
        List<Document> documentList2 = annotableConverter.annotationToDB(vs, "annotName", annotationSets.get(0).getAnnotations());

        // And now we check if the conversion to mongo and the conversion map -> mongo -> map -> mongo gives the expected result
        for (List<Document> myDocList : Arrays.asList(documentList, documentList2)) {

            assertEquals(6, myDocList.size());
            for (Document document : myDocList) {
                switch ((String) document.get(AnnotationConverter.ID)) {
                    case "a.ab1.ab1c1":
                        assertEquals(8, document.size());
                        assertEquals(Arrays.asList(true, false, false), document.get(AnnotationConverter.VALUE));
                        assertEquals(Arrays.asList(3), document.get(AnnotationConverter.COUNT_ELEMENTS));
                        assertEquals(Arrays.asList(2), document.get(AnnotationConverter.ARRAY_LEVEL));
                        break;
                    case "a.ab1.ab1c2":
                        assertEquals(6, document.size());
                        assertEquals("hello world", document.get(AnnotationConverter.VALUE));
                        break;
                    case "a.ab2.ab2c1.ab2c1d1":
                        assertEquals(8, document.size());
                        assertEquals(Arrays.asList(1, 2, 3, 4, 11, 12, 13, 14, 21), document.get(AnnotationConverter.VALUE));
                        assertEquals(Arrays.asList(Arrays.asList(4), Arrays.asList(4), Arrays.asList(1)),
                                document.get(AnnotationConverter.COUNT_ELEMENTS));

                        assertEquals(Arrays.asList(1, 3), document.get(AnnotationConverter.ARRAY_LEVEL));
                        break;
                    case "a.ab2.ab2c1.ab2c1d2":
                        assertEquals(8, document.size());
                        assertEquals(Arrays.asList("hello ab2c1d2 1", "hello ab2c1d2 2"), document.get(AnnotationConverter.VALUE));
                        assertEquals(Arrays.asList(1, 1, 0), document.get(AnnotationConverter.COUNT_ELEMENTS));
                        assertEquals(Arrays.asList(1), document.get(AnnotationConverter.ARRAY_LEVEL));
                        break;
                    case "a.ab3.ab3c1.ab3c1d1":
                        assertEquals(8, document.size());
                        assertEquals(Arrays.asList(Arrays.asList("hello"), Arrays.asList("hello2", "bye2"),
                                Arrays.asList("byeee2", "hellooo2")), document.get(AnnotationConverter.VALUE));
                        assertEquals(Arrays.asList(Arrays.asList(1, 0), Arrays.asList(1, 1)), document.get(AnnotationConverter
                                .COUNT_ELEMENTS));
                        assertEquals(Arrays.asList(1, 2), document.get(AnnotationConverter.ARRAY_LEVEL));
                        break;
                    case "a.ab3.ab3c1.ab3c1d2":
                        assertEquals(8, document.size());
                        assertEquals(Arrays.asList(2.0, 4.0, 24.0), document.get(AnnotationConverter.VALUE));
                        assertEquals(Arrays.asList(Arrays.asList(1, 1), Arrays.asList(0, 1)),
                                document.get(AnnotationConverter.COUNT_ELEMENTS));
                        assertEquals(Arrays.asList(1, 2), document.get(AnnotationConverter.ARRAY_LEVEL));
                        break;
                    default:
                        fail("Unexpected ID found: " + document.get(AnnotationConverter.ID));
                }
            }
        }

    }


    @Test
    public void convertAvroTest() throws Exception {
        AnnotableConverter<Sample> c = new AnnotableConverter<>(Sample.class);
        c.convertToStorageType(
                new Sample().setQualityControl(
                        new SampleQualityControl().setVariant(
                                new SampleVariantQualityControlMetrics().setVariantStats(Collections.singletonList(
                                        new SampleQcVariantStats().setStats(
                                                new SampleVariantStats()))))));
    }
}