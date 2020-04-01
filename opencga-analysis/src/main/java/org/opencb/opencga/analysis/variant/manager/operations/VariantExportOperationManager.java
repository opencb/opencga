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

package org.opencb.opencga.analysis.variant.manager.operations;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.variant.metadata.CatalogVariantMetadataFactory;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.metadata.VariantMetadataFactory;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.List;

/**
 * Created on 06/12/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantExportOperationManager extends OperationManager {

    public VariantExportOperationManager(VariantStorageManager variantStorageManager, VariantStorageEngine engine) {
        super(variantStorageManager, engine);
    }

    public void export(String outputFileStr, VariantWriterFactory.VariantOutputFormat outputFormat, String variantsFile,
                       Query query, QueryOptions queryOptions, String token) throws Exception {
        URI outputFile;
        if (!VariantWriterFactory.isStandardOutput(outputFileStr)) {
            URI outdirUri;
            try {
                outdirUri = UriUtils.createUri(outputFileStr);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(e);
            }
            if (StringUtils.isEmpty(outdirUri.getScheme()) || outdirUri.getScheme().equals("file")) {
                String outputFileName;
                java.io.File file = Paths.get(outdirUri).toFile();
                if (!file.exists() || !file.isDirectory()) {
                    outputFileName = outdirUri.resolve(".").relativize(outdirUri).toString();
                    outdirUri = outdirUri.resolve(".");
                } else {
                    try {
                        outdirUri = UriUtils.createDirectoryUri(outputFileStr);
                    } catch (URISyntaxException e) {
                        throw new IllegalArgumentException(e);
                    }
                    outputFileName = buildOutputFileName(query, token);
                }
                outputFile = outdirUri.resolve(outputFileName);
                outputFile = VariantWriterFactory.checkOutput(outputFile, outputFormat);
            } else {
                outputFile = outdirUri;
            }
        } else {
            outputFile = null;
        }

        VariantMetadataFactory metadataExporter =
                new CatalogVariantMetadataFactory(catalogManager, variantStorageEngine.getDBAdaptor(), token);

        URI variantsFileUri = StringUtils.isEmpty(variantsFile) ? null : UriUtils.createUri(variantsFile);
        variantStorageEngine.exportData(outputFile, outputFormat, variantsFileUri, query, queryOptions, metadataExporter);
    }

    private String buildOutputFileName(Query query, String token) throws CatalogException {
        VariantCatalogQueryUtils catalogUtils = new VariantCatalogQueryUtils(catalogManager);
        List<String> studyNames = catalogUtils.getStudies(query, token);
        List<Region> regions = Region.parseRegions(query.getString(VariantQueryParam.REGION.key()));

        String fileName;
        if (studyNames != null && studyNames.size() == 1) {
            String study = studyNames.get(0);
            String[] split = study.split(":");
            fileName = split[split.length - 1] + ".";
        } else {
            fileName = "";
        }

        if (regions == null || regions.size() != 1) {
            return fileName + "export";
        } else {
            return fileName + regions.get(0).toString() + ".export";
        }
    }

}
