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
        document.put("id", document.getInteger("id").longValue());

        long familyId = object.getFamily() != null ? (object.getFamily().getId() == 0 ? -1L : object.getFamily().getId()) : -1L;
        document.put("family", new Document("id", familyId));

        if (object.getSubject() != null) {
            long probandId = object.getSubject().getId() <= 0 ? -1L : object.getSubject().getId();
            List<Document> sampleList = new ArrayList<>();
            if (object.getSubject().getSamples() != null) {
                for (Sample sample : object.getSubject().getSamples()) {
                    sampleList.add(new Document("id", sample.getId()));
                }
            }
            document.put("subject", new Document()
                    .append("id", probandId)
                    .append("samples", sampleList));
        }

        validateDocumentToUpdate(document);

        return document;
    }

    public void validateDocumentToUpdate(Document document) {
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
            long fileId = file != null ? (file.getInteger("id") == 0 ? -1L : file.getInteger("id").longValue()) : -1L;
            interpretation.put("file", fileId > 0 ? new Document("id", fileId) : new Document());
        }
    }
}
