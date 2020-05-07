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
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.Interpretation;

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
    public Document convertToStorageType(ClinicalAnalysis clinicalAnalysis) {
        Document document = super.convertToStorageType(clinicalAnalysis);
        document.put("uid", clinicalAnalysis.getUid());
        document.put("studyUid", clinicalAnalysis.getStudyUid());

        document.put("interpretation", convertInterpretation(clinicalAnalysis.getInterpretation()));
        document.put("secondaryInterpretations", convertInterpretations(clinicalAnalysis.getSecondaryInterpretations()));

        validateDocumentToUpdate(document);

        return document;
    }

    public void validateDocumentToUpdate(Document document) {
        validateInterpretationToUpdate(document);
        validateSecondaryInterpretationsToUpdate(document);
        validateFamilyToUpdate(document);
        validateProbandToUpdate(document);
    }

    public void validateFamilyToUpdate(Document document) {
        Document family = (Document) document.get(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key());
        if (family != null) {
            // Store the uid as a long value
            family.put("uid", getLongValue(family, "uid"));
            family.remove("studyUid");

            // Check if family contains members
            List<Document> members = (List<Document>) family.get(FamilyDBAdaptor.QueryParams.MEMBERS.key());
            if (ListUtils.isNotEmpty(members)) {
                for (Document member : members) {
                   validateProbandToUpdate(member);
                }
            }
        }
    }

    public void validateProbandToUpdate(Document document) {
        Document member = (Document) document.get(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND.key());
        if (member != null) {
            member.put("uid", getLongValue(member, "uid"));
            member.remove("studyUid");

            Document father = (Document) member.get(IndividualDBAdaptor.QueryParams.FATHER.key());
            if (father != null) {
                father.put("uid", getLongValue(father, "uid"));
                father.remove("studyUid");
            }

            Document mother = (Document) member.get(IndividualDBAdaptor.QueryParams.MOTHER.key());
            if (mother != null) {
                mother.put("uid", getLongValue(mother, "uid"));
                mother.remove("studyUid");
            }

            List<Document> samples = (List<Document>) member.get(IndividualDBAdaptor.QueryParams.SAMPLES.key());
            if (ListUtils.isNotEmpty(samples)) {
                for (Document sample : samples) {
                    sample.put("uid", getLongValue(sample, "uid"));
                    sample.remove("studyUid");
                }
            }
        }
    }

    public void validateInterpretationToUpdate(Document document) {
        Document interpretation = (Document) document.get("interpretation");
        if (interpretation != null) {
            document.put("interpretation", new Document("uid", interpretation.get("uid")));
        }
    }

    public void validateSecondaryInterpretationsToUpdate(Document document) {
        List<Document> interpretationList = (List) document.get("secondaryInterpretations");
        if (interpretationList != null) {
            List<Document> newInterpretationList = new ArrayList<>();
            for (int i = 0; i < interpretationList.size(); i++) {
                newInterpretationList.add(new Document("uid", interpretationList.get(i).get("uid")));
            }

            document.put("secondaryInterpretations", newInterpretationList);
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

    public Document convertInterpretation(Interpretation interpretation) {
        if (interpretation == null) {
            return null;
        }

        long interpretationId = interpretation != null ? (interpretation.getUid() == 0 ? -1L : interpretation.getUid()) : -1L;
        if (interpretationId > 0) {
            return new Document("uid", interpretationId);
        }
        return null;
    }
}
