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

import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.study.VariableSet;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
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

        document.put("uid", object.getUid());
        document.put("studyUid", object.getStudyUid());
        validateDocumentToUpdate(document);
        return document;
    }

    public void validateDocumentToUpdate(Document document) {
        List<Document> memberList = document.getList("members", Document.class);
        if (memberList != null) {
            // We make sure we don't store duplicates
            Set<Long> individualSet = new HashSet<>();
            for (Document individual : memberList) {
                long uid = individual.get("uid", Number.class).longValue();
                if (uid <= 0) {
                    throw new IllegalArgumentException("Missing uid value for member '" + individual.getString("id") + "'.");
                }
                if (individualSet.contains(uid)) {
                    throw new IllegalArgumentException("Duplicated member '" + individual.getString("id") + " (" + uid + ")' found.");
                }
                individualSet.add(uid);
            }

            document.put("members",
                    memberList.stream()
                            .map(entry -> new Document()
                                    .append("uid", entry.get("uid", Number.class).longValue())
                                    .append("version", entry.get("version", Number.class).intValue())
                            )
                            .collect(Collectors.toList()));
        }

        List<Document> disorderList = document.getList("disorders", Document.class);
        if (disorderList != null) {
            for (Document disorder : disorderList) {
                fixPhenotypeFields(disorder.getList("evidences", Document.class));
            }
        }
        fixPhenotypeFields(document.getList("phenotypes", Document.class));
    }

    private void fixPhenotypeFields(List<Document> phenotypeList) {
        if (CollectionUtils.isNotEmpty(phenotypeList)) {
            for (Document phenotype : phenotypeList) {
                phenotype.put("status", Phenotype.Status.UNKNOWN.name());
                phenotype.put("ageOfOnset", "-1");
            }
        }
    }
}
