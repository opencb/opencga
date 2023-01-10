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

package org.opencb.opencga.storage.core.variant.annotation.annotators;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.cellbase.client.config.ClientConfiguration;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.cellbase.core.models.DataRelease;
import org.opencb.cellbase.core.result.CellBaseDataResponse;
import org.opencb.cellbase.core.result.CellBaseDataResult;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.utils.CellBaseUtils;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created on 23/11/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CellBaseRestVariantAnnotator extends AbstractCellBaseVariantAnnotator {

    private final CellBaseClient cellBaseClient;
    private final CellBaseUtils cellBaseUtils;

    public CellBaseRestVariantAnnotator(StorageConfiguration storageConfiguration, ProjectMetadata projectMetadata, ObjectMap options)
            throws VariantAnnotatorException {
        super(storageConfiguration, projectMetadata, options);

        String cellbaseRest = storageConfiguration.getCellbase().getUrl();
        if (StringUtils.isEmpty(cellbaseRest)) {
            throw new VariantAnnotatorException("Missing defaultValue \"CellBase Hosts\"");
        }

        checkNotNull(cellbaseRest, "cellbase hosts");
        ClientConfiguration clientConfiguration = storageConfiguration.getCellbase().toClientConfiguration();

        int timeoutMillis = options.getInt(
                VariantStorageOptions.ANNOTATION_TIMEOUT.key(),
                VariantStorageOptions.ANNOTATION_TIMEOUT.defaultValue());

        clientConfiguration.getRest().setTimeout(timeoutMillis);
        cellBaseClient = new CellBaseClient(species, assembly, cellbaseDataRelease, clientConfiguration);
        cellBaseUtils = new CellBaseUtils(cellBaseClient);
        logger.info("Annotating with Cellbase REST. {}", cellBaseUtils);

        try {
            cellBaseUtils.validateCellBaseConnection();
        } catch (IOException e) {
            throw new VariantAnnotatorException(e.getMessage(), e);
        }

    }

    @Override
    protected List<CellBaseDataResult<VariantAnnotation>> annotateFiltered(List<Variant> variants) throws VariantAnnotatorException {
        if (variants.isEmpty()) {
            return Collections.emptyList();
        }
        try {
            List<String> variantIds = variants.stream().map(variantSerializer).collect(Collectors.toList());
            // Make a copy of QueryOptions, as it might be modified from the client.
            CellBaseDataResponse<VariantAnnotation> response = cellBaseClient.getVariantClient()
                    .getAnnotationByVariantIds(variantIds, new QueryOptions(queryOptions), true);
            return response.getResponses();
        } catch (IOException e) {
            throw new VariantAnnotatorException("Error fetching variants from " + getDebugInfo("/genomic/variant/annotation"));
        }
    }

    @Override
    public ProjectMetadata.VariantAnnotationMetadata getVariantAnnotationMetadata() throws VariantAnnotatorException {
        DataRelease dataRelease = null;
        try {
            if (cellBaseUtils.supportsDataRelease()) {
                dataRelease = cellBaseClient.getMetaClient()
                        .dataReleases()
                        .allResults()
                        .stream()
                        .filter(dr -> String.valueOf(dr.getRelease()).equals(cellBaseClient.getDataRelease()))
                        .findFirst()
                        .orElse(null);
            }
        } catch (IOException e) {
            throw new VariantAnnotatorException("Error fetching CellBase information from "
                    + getDebugInfo("/meta/" + species + "/dataReleases") + ". ");
        }
        return new ProjectMetadata.VariantAnnotationMetadata(-1, null, null,
                getVariantAnnotatorProgram(),
                getVariantAnnotatorSourceVersion(),
                dataRelease);
    }

    private ProjectMetadata.VariantAnnotatorProgram getVariantAnnotatorProgram() throws VariantAnnotatorException {
        CellBaseDataResponse<ObjectMap> response;
        try {
            response = cellBaseClient.getMetaClient().about();
        } catch (IOException e) {
            throw new VariantAnnotatorException("Error fetching CellBase information from " + getDebugInfo("/meta/about"), e);
        }
        Event event = errorEvent(response);
        if (event != null) {
            throw new VariantAnnotatorException("Error fetching CellBase information from " + getDebugInfo("/meta/about") + ". "
                    + event.getName() + " : " + event.getMessage());
        }
        ObjectMap about = response.firstResult();
        if (about == null) {
            throw new VariantAnnotatorException("Error fetching CellBase information from " + getDebugInfo("/meta/about"));
        }
        ProjectMetadata.VariantAnnotatorProgram program = new ProjectMetadata.VariantAnnotatorProgram();

        for (Map.Entry<String, Object> entry : about.entrySet()) {
            String key = entry.getKey().toLowerCase();
            String value = entry.getValue().toString();

            if (key.contains("program")) {
                program.setName(value);
            } else if (key.contains("commit")) {
                program.setCommit(value);
            } else if (key.contains("version")) {
                program.setVersion(value);
            }
        }
        return program;
    }

    private List<ObjectMap> getVariantAnnotatorSourceVersion() throws VariantAnnotatorException {
        CellBaseDataResponse<ObjectMap> response;
        try {
            response = cellBaseClient.getMetaClient().versions();
        } catch (IOException e) {
            throw new VariantAnnotatorException("Error fetching CellBase source information from " + getDebugInfo("/meta/versions"), e);
        }
        Event event = errorEvent(response);
        if (event != null) {
            throw new VariantAnnotatorException(
                    "Error fetching CellBase source information from " + getDebugInfo("/meta/versions") + ". "
                    + event.getName() + " : " + event.getMessage());
        }
        List<ObjectMap> objectMaps = new ArrayList<>();
        if (response.getResponses() != null) {
            for (CellBaseDataResult<ObjectMap> r : response.getResponses()) {
                if (r.getResults() != null) {
                    objectMaps.addAll(r.getResults());
                }
            }
        }
        if (objectMaps.isEmpty()) {
            throw new VariantAnnotatorException("Error fetching CellBase source information from " + getDebugInfo("/meta/versions"));
        }
        return objectMaps;
    }

    private String getDebugInfo(String path) {
        return cellBaseUtils.toString()
                + " path: '" + path + "'";
    }

    private Event errorEvent(CellBaseDataResponse<ObjectMap> response) {
        if (response.getEvents() == null) {
            return null;
        } else {
            return response.getEvents()
                    .stream()
                    .filter(e -> e.getType() == Event.Type.ERROR)
                    .findAny()
                    .orElse(null);
        }
    }
}
