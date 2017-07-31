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
import org.opencb.opencga.catalog.models.ClinicalAnalysis;

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

        long probandId = object.getProband() != null ? (object.getProband().getId() == 0 ? -1L : object.getProband().getId()) : -1L;
        document.put("proband", new Document("id", probandId));

        long sampleId = object.getSample() != null ? (object.getSample().getId() == 0 ? -1L : object.getSample().getId()) : -1L;
        document.put("sample", new Document("id", sampleId));

        return document;
    }
}
