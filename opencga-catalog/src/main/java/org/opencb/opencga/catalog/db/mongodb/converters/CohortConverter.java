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
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.study.VariableSet;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 3/22/16.
 */
public class CohortConverter extends AnnotableConverter<Cohort> {

    public CohortConverter() {
        super(Cohort.class);
    }

    @Override
    public Document convertToStorageType(Cohort object, List<VariableSet> variableSetList) {
        Document document = super.convertToStorageType(object, variableSetList);
        document.remove(CohortDBAdaptor.QueryParams.ANNOTATION_SETS.key());

        document.put("uid", object.getUid());
        document.put("studyUid", object.getStudyUid());

        validateSamplesToUpdate(document);
        return document;
    }

    public void validateSamplesToUpdate(Document document) {
        List<Document> samples = (List) document.get("samples");
        if (samples != null) {
            // We make sure we don't store duplicates
            Set<Long> sampleSet = new HashSet<>();
            for (Document sample : samples) {
                long id = sample.getInteger("uid").longValue();
                if (id > 0) {
                    sampleSet.add(id);
                }
            }

            document.put("samples",
                    sampleSet.stream()
                            .map(sample -> new Document("uid", sample))
                            .collect(Collectors.toList()));
        }
    }

}
