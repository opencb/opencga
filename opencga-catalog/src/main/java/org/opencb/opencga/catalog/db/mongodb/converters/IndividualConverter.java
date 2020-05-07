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
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.VariableSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 19/01/16.
 */
public class IndividualConverter extends AnnotableConverter<Individual> {

    public IndividualConverter() {
        super(Individual.class);
    }

    @Override
    public Document convertToStorageType(Individual object, List<VariableSet> variableSetList) {
        Document document = super.convertToStorageType(object, variableSetList);
        document.remove(IndividualDBAdaptor.QueryParams.ANNOTATION_SETS.key());

        document.put("uid", object.getUid());
        document.put("studyUid", object.getStudyUid());

        Document father = (Document) document.get("father");
        long fatherId = father != null ? (father.getInteger("uid") == 0 ? -1L : father.getInteger("uid").longValue()) : -1L;
        document.put("father", fatherId > 0 ? new Document("uid", fatherId) : new Document());

        Document mother = (Document) document.get("mother");
        long motherId = mother != null ? (mother.getInteger("uid") == 0 ? -1L : mother.getInteger("uid").longValue()) : -1L;
        document.put("mother", motherId > 0 ? new Document("uid", motherId) : new Document());

        validateSamplesToUpdate(document);

        return document;
    }

    public void validateSamplesToUpdate(Document document) {
        List<Document> samples = (List) document.get("samples");
        if (samples != null) {
            // We make sure we don't store duplicates
            Map<Long, Sample> sampleMap = new HashMap<>();
            for (Document sample : samples) {
                long id = sample.getInteger("uid").longValue();
                int version = sample.getInteger("version");
                if (id > 0) {
                    Sample tmpSample = new Sample()
                            .setVersion(version);
                    tmpSample.setUid(id);
                    sampleMap.put(id, tmpSample);
                }
            }

            document.put("samples",
                    sampleMap.entrySet().stream()
                            .map(entry -> new Document()
                                    .append("uid", entry.getValue().getUid())
                                    .append("version", entry.getValue().getVersion()))
                            .collect(Collectors.toList()));
        }
    }

}
