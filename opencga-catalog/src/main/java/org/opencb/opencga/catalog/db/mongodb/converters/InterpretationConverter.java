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
import org.opencb.opencga.catalog.db.api.PanelDBAdaptor;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.panel.Panel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class InterpretationConverter extends OpenCgaMongoConverter<Interpretation> {

    public InterpretationConverter() {
        super(Interpretation.class);
    }

    @Override
    public Document convertToStorageType(Interpretation interpretation) {
        Document document = super.convertToStorageType(interpretation);
        document.put(ClinicalAnalysisDBAdaptor.QueryParams.UID.key(), interpretation.getUid());
        document.put(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), interpretation.getStudyUid());

        validateDocumentToUpdate(document);

        return document;
    }

    public void validateDocumentToUpdate(Document document) {
        validatePanelsToUpdate(document);
    }

    public void validatePanelsToUpdate(Document document) {
        List<Document> panels = (List) document.get(ClinicalAnalysisDBAdaptor.QueryParams.PANELS.key());
        if (panels != null) {
            // We make sure we don't store duplicates
            Map<Long, Panel> panelMap = new HashMap<>();
            for (Document panel : panels) {
                long uid = panel.getInteger(PanelDBAdaptor.QueryParams.UID.key()).longValue();
                int version = panel.getInteger(PanelDBAdaptor.QueryParams.VERSION.key());
                if (uid > 0) {
                    Panel tmpPanel = new Panel()
                            .setId(panel.getString(PanelDBAdaptor.QueryParams.ID.key()))
                            .setVersion(version);
                    tmpPanel.setUid(uid);
                    panelMap.put(uid, tmpPanel);
                }
            }

            document.put(ClinicalAnalysisDBAdaptor.QueryParams.PANELS.key(),
                    panelMap.entrySet().stream()
                            .map(entry -> new Document()
                                    .append(PanelDBAdaptor.QueryParams.ID.key(), entry.getValue().getId())
                                    .append(PanelDBAdaptor.QueryParams.UID.key(), entry.getValue().getUid())
                                    .append(PanelDBAdaptor.QueryParams.VERSION.key(), entry.getValue().getVersion()))
                            .collect(Collectors.toList()));
        }
    }

}
