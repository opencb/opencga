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
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.opencga.core.models.Interpretation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by pfurio on 05/06/17.
 */
public class ClinicalAnalysisConverter extends GenericDocumentComplexConverter<ClinicalAnalysis> {

    public ClinicalAnalysisConverter() {
        super(ClinicalAnalysis.class);
    }

    @Override
    public Document convertToStorageType(ClinicalAnalysis object) {
        Document document = super.convertToStorageType(object);
        document.put("uid", object.getUid());
        document.put("studyUid", object.getStudyUid());

        document.put("interpretations", convertInterpretations(object.getInterpretations()));

        validateDocumentToUpdate(document);

        return document;
    }

    public void validateDocumentToUpdate(Document document) {
        validateInterpretationToUpdate(document);
        validateFamilyToUpdate(document);
        validateSubjectsToUpdate(document);
    }

    public void validateFamilyToUpdate(Document document) {
    }

    public void validateSubjectsToUpdate(Document document) {
    }

    public void validateInterpretationToUpdate(Document document) {
        List<Document> interpretationList = (List) document.get("interpretations");
        if (interpretationList != null) {
            List<Document> newInterpretationList = new ArrayList<>();
            for (int i = 0; i < interpretationList.size(); i++) {
                newInterpretationList.add(new Document("uid", interpretationList.get(i).get("uid")));
            }

            document.put("interpretations", newInterpretationList);
        }
    }

    public List<Document> convertInterpretations(List<Interpretation> interpretationList) {
        if (interpretationList == null || interpretationList.isEmpty()) {
            return Collections.emptyList();
        }
        List<Document> interpretations = new ArrayList(interpretationList.size());
        for (Interpretation interpretation : interpretationList) {
            long interpretationId = interpretation != null ? (interpretation.getUid() == 0 ? -1L : interpretation.getUid()) : -1L;
            if (interpretationId > 0) {
                interpretations.add(new Document("uid", interpretationId));
            }
        }
        return interpretations;
    }
}
