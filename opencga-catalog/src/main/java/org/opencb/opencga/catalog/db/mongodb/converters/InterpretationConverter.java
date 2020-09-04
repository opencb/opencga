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
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.core.models.clinical.Interpretation;

public class InterpretationConverter extends OpenCgaMongoConverter<Interpretation> {

    public InterpretationConverter() {
        super(Interpretation.class);
    }

    @Override
    public Document convertToStorageType(Interpretation interpretation) {
        Document document = super.convertToStorageType(interpretation);
        document.put(ClinicalAnalysisDBAdaptor.QueryParams.UID.key(), interpretation.getUid());
        document.put(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), interpretation.getStudyUid());
        return document;
    }

}
