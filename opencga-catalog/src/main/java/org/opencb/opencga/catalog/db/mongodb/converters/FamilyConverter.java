/*
 * Copyright 2015-2017 OpenCB
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
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.core.models.Family;
import org.opencb.opencga.core.models.Individual;
import org.opencb.opencga.core.models.VariableSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 03/05/17.
 */
public class FamilyConverter extends AnnotableConverter<Family> {

    public FamilyConverter() {
        super(Family.class);
    }

    @Override
    public Family convertToDataModelType(Document document) {
        return super.convertToDataModelType(document);
    }

    @Override
    public Document convertToStorageType(Family object, List<VariableSet> variableSetList) {
        Document document = super.convertToStorageType(object, variableSetList);
        document.remove(FamilyDBAdaptor.QueryParams.ANNOTATION_SETS.key());

        document.put("id", document.getInteger("id").longValue());
        validateDocumentToUpdate(document);
        return document;
    }

    public void validateDocumentToUpdate(Document document) {
        List<Document> memberList = (List) document.get("members");
        if (memberList != null) {
            // We make sure we don't store duplicates
            Map<Long, Individual> individualMap = new HashMap<>();
            for (Document individual : memberList) {
                long id = individual.getInteger("id").longValue();
                int version = individual.getInteger("version");
                if (id > 0) {
                    individualMap.put(id, new Individual().setId(id).setVersion(version));
                }
            }

            document.put("members",
                    individualMap.entrySet().stream()
                            .map(entry -> new Document()
                                    .append("id", entry.getValue().getId())
                                    .append("version", entry.getValue().getVersion()))
                            .collect(Collectors.toList()));
        }
    }
}
