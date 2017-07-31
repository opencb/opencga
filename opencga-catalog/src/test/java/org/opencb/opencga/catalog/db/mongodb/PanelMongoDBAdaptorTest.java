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

package org.opencb.opencga.catalog.db.mongodb;

import org.junit.Test;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.DiseasePanel;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

/**
 * Created by pfurio on 01/06/16.
 */
public class PanelMongoDBAdaptorTest extends MongoDBAdaptorTest {

    @Test
    public void createPanel() throws CatalogDBException {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();

        DiseasePanel diseasePanel = new DiseasePanel(-1, "panel1", "cancer", ".....", Arrays.asList("BRCA2","PEPE","JUAN","RET"),
                Collections.emptyList(), Collections.emptyList(), new DiseasePanel.PanelStatus());

        QueryResult<DiseasePanel> panel = catalogPanelDBAdaptor.insert(diseasePanel, studyId, new QueryOptions());
        assertEquals(1, panel.getNumResults());
    }

    @Test
    public void getPanel() throws CatalogDBException {
        long studyId = user3.getProjects().get(0).getStudies().get(0).getId();
        DiseasePanel diseasePanel = new DiseasePanel(-1, "panel1", "cancer", ".....", Arrays.asList("BRCA2,PEPE,JUAN,RET"),
                Collections.emptyList(), Collections.emptyList(), new DiseasePanel.PanelStatus());
        QueryResult<DiseasePanel> panel = catalogPanelDBAdaptor.insert(diseasePanel, studyId, new QueryOptions());

        QueryResult<DiseasePanel> panel1 = catalogPanelDBAdaptor.get(panel.first().getId(), new QueryOptions());
        assertEquals(1, panel1.getNumResults());
    }
}
