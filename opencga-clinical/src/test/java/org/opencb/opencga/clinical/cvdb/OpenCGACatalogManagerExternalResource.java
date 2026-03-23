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

package org.opencb.opencga.clinical.cvdb;

import org.apache.commons.collections4.CollectionUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.panel.PanelImportParams;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

/**
 * Created on 05/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class OpenCGACatalogManagerExternalResource extends CatalogManagerExternalResource {

    public static final String ADMIN_PASSWORD = "4dMiNiStR4t0R.";
    public static final String PASSWORD = "p4sSw0rD.";

    public OpenCGACatalogManagerExternalResource() {
        super(Paths.get("../opencga-home/opencga-app/app/analysis/"));
    }

    @Deprecated
    public void loadClinicalAnalsysesInCatalog(List<String> caFilenames, String studyId, String sessionIdUser)
            throws IOException, CatalogException {
        for (String caFilename : caFilenames) {
            InputStream is = OpenCGACatalogManagerExternalResource.class.getClassLoader().getResourceAsStream(caFilename);
            GZIPInputStream gzipInputStream = new GZIPInputStream(is);
            ClinicalAnalysis clinicalAnalysis = JacksonUtils.getDefaultObjectMapper().readerFor(ClinicalAnalysis.class)
                    .readValue(gzipInputStream);

            // Import panels
            try {
                List<String> panelIds = new ArrayList<>();
                if (CollectionUtils.isNotEmpty(clinicalAnalysis.getPanels())) {
                    panelIds = clinicalAnalysis.getPanels().stream().map(p -> p.getId()).collect(Collectors.toList());
                }
                PanelImportParams params = new PanelImportParams()
                        .setSource(PanelImportParams.Source.PANEL_APP)
                        .setPanelIds(panelIds);

                catalogManager.getPanelManager().importFromSource(studyId, params, QueryOptions.empty(), sessionIdUser);
            } catch (CatalogException e) {
                System.out.println("---------------------------------------------------------------------------------");
                System.out.println("Impossible to load clinical analysis file " + caFilename + ": " + e.getMessage());
                System.out.println("---------------------------------------------------------------------------------");
                continue;
            }

            // Create family
            if (clinicalAnalysis.getFamily() != null) {
                catalogManager.getFamilyManager().create(studyId, clinicalAnalysis.getFamily(), CvdbSolrEngineIndexTest.INCLUDE_RESULT,
                        sessionIdUser);
            }

            // Create clinical analysis
            clinicalAnalysis.getInterpretation().setId(null);
            if (CollectionUtils.isNotEmpty(clinicalAnalysis.getSecondaryInterpretations())) {
                for (Interpretation secondaryInterpretation : clinicalAnalysis.getSecondaryInterpretations()) {
                    secondaryInterpretation.setId(null);
                }
            }
            catalogManager.getClinicalAnalysisManager().create(studyId, clinicalAnalysis, true, CvdbSolrEngineIndexTest.INCLUDE_RESULT,
                    sessionIdUser);
        }
    }
}
