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

package org.opencb.opencga.analysis.clinical;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.annotations.TsvAnnotationLoader;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AnnotationSetManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisUpdateParams;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.tools.annotations.Tool;

import java.util.Collections;

@Tool(id = ClinicalTsvAnnotationLoader.ID, resource = Enums.Resource.CLINICAL_ANALYSIS, type = Tool.Type.OPERATION,
        description = "Load annotations from TSV file.", priority = Enums.Priority.HIGH)
public class ClinicalTsvAnnotationLoader extends TsvAnnotationLoader {
    public final static String ID = "clinical-tsv-load";

    @Override
    public int count(Query query) throws CatalogException {
        return catalogManager.getClinicalAnalysisManager().count(study, query, token).getNumResults();
    }

    @Override
    public void addAnnotationSet(String entryId, AnnotationSet annotationSet, QueryOptions options) throws CatalogException {
        ClinicalAnalysisUpdateParams updateParams = new ClinicalAnalysisUpdateParams()
                .setAnnotationSets(Collections.singletonList(annotationSet));
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(Constants.ACTIONS, new ObjectMap(AnnotationSetManager.ANNOTATION_SETS, ParamUtils.BasicUpdateAction.ADD));

        catalogManager.getClinicalAnalysisManager().update(study, entryId, updateParams, queryOptions, token);
    }
}
