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
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.study.Variable;
import org.opencb.opencga.core.models.study.VariableSet;

import java.util.*;

/**
 * Created by wasim on 13/08/18.
 */
public class AnnotationHelper {

    public static VariableSet createVariableSet() {
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

        return new VariableSet().setVariables(rootSet).setUid(1).setId("vsId");
    }

    public static List<AnnotationSet> createAnnotation() {
        VariableSet vs = createVariableSet();

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
                .append(String.valueOf(vs.getUid()), vs.getId()), new QueryOptions(ParamConstants.FLATTEN_ANNOTATIONS, true));

        return annotationSets;
    }
}
