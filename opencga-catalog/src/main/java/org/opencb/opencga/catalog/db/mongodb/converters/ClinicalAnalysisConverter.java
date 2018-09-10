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
import org.opencb.opencga.core.models.Sample;

import java.util.ArrayList;
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

        long familyId = object.getFamily() != null ? (object.getFamily().getUid() == 0 ? -1L : object.getFamily().getUid()) : -1L;
        document.put("family", new Document("uid", familyId));

        long somaticId = object.getSomatic() != null ? (object.getSomatic().getUid() == 0 ? -1L : object.getSomatic().getUid()) : -1L;
        document.put("somatic", new Document("uid", somaticId));

        long germlineId = object.getGermline() != null ? (object.getGermline().getUid() == 0 ? -1L : object.getGermline().getUid()) : -1L;
        document.put("germline", new Document("uid", germlineId));

        if (object.getProband() != null) {
            long probandId = object.getProband().getUid() <= 0 ? -1L : object.getProband().getUid();
            List<Document> sampleList = new ArrayList<>();
            if (object.getProband().getSamples() != null) {
                for (Sample sample : object.getProband().getSamples()) {
                    sampleList.add(new Document("uid", sample.getUid()));
                }
            }

            document.put("proband", new Document()
                    .append("uid", probandId)
                    .append("samples", sampleList)
            );
        }

        validateDocumentToUpdate(document);

        return document;
    }

    public void validateDocumentToUpdate(Document document) {
        validateInterpretationToUpdate(document);
        validateFamilyToUpdate(document);
        validateSubjectsToUpdate(document);
    }

    public void validateFamilyToUpdate(Document document) {
        Document family = (Document) document.get("family");
        if (family != null) {
            long familyId = getLongValue(family, "uid");
            familyId = familyId <= 0 ? -1L : familyId;
            document.put("family", new Document("uid", familyId));
        }
    }

    public void validateSubjectsToUpdate(Document document) {
        Document proband = (Document) document.get("proband");
        if (proband != null && !proband.isEmpty()) {

            long probandId = getLongValue(proband, "uid");
            probandId = probandId <= 0 ? -1L : probandId;

            List<Document> sampleDocList = (List) proband.get("samples");
            List<Document> sampleList = new ArrayList<>(sampleDocList.size());
            if (sampleDocList != null) {
                for (Document sampleDocument : sampleDocList) {
                    long sampleId = getLongValue(sampleDocument, "uid");
                    sampleId = sampleId <= 0 ? -1L : sampleId;
                    sampleList.add(new Document("uid", sampleId));
                }
            }

            document.put("proband", new Document()
                    .append("uid", probandId)
                    .append("samples", sampleList)
            );
        }
    }

    public void validateInterpretationToUpdate(Document document) {
        List<Document> interpretationList = (List) document.get("interpretations");
        if (interpretationList != null) {
            for (int i = 0; i < interpretationList.size(); i++) {
                validateInterpretation(interpretationList.get(i));
            }
        }
    }

    public void validateInterpretation(Document interpretation) {
        if (interpretation != null) {
            Document file = (Document) interpretation.get("file");
            long fileId = file != null ? getLongValue(file, "uid") : -1L;
            fileId = fileId <= 0 ? -1L : fileId;
            interpretation.put("file", fileId > 0 ? new Document("uid", fileId) : new Document());
        }
    }
}
