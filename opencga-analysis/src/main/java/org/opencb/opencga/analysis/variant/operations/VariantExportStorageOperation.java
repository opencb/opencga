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

package org.opencb.opencga.analysis.variant.operations;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.OpenCgaAnalysis;
import org.opencb.opencga.analysis.variant.VariantCatalogQueryUtils;
import org.opencb.opencga.analysis.variant.metadata.CatalogVariantMetadataFactory;
import org.opencb.opencga.core.annotations.Analysis;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.models.DataStore;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
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
@Analysis(id = VariantExportStorageOperation.ID, type = Analysis.AnalysisType.VARIANT)
public class VariantExportStorageOperation extends OpenCgaAnalysis {

    public static final String ID = "variant-export";
    private Query query;
    private VariantWriterFactory.VariantOutputFormat outputFormat;
    private String variantsFile;
    private String outputFileStr;

    private DataStore dataStore;
    private URI outputFile;

    public VariantExportStorageOperation setQuery(Query query) {
        this.query = query;
        return this;
    }

    public VariantExportStorageOperation setOutputFormat(VariantWriterFactory.VariantOutputFormat outputFormat) {
        this.outputFormat = outputFormat;
        return this;
    }

    public VariantExportStorageOperation setVariantsFile(String variantsFile) {
        this.variantsFile = variantsFile;
        return this;
    }

    public VariantExportStorageOperation setOutputFile(String outputFile) {
        this.outputFileStr = outputFile;
        return this;
    }

    @Override
    protected void check() throws Exception {
        super.check();
        VariantCatalogQueryUtils catalogUtils = new VariantCatalogQueryUtils(catalogManager);
        String study = catalogUtils.getAnyStudy(query, token);
        List<String> studies = catalogUtils.getStudies(query, token);
        dataStore = variantStorageManager.getDataStore(study, token);

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
                    List<Region> regions = Region.parseRegions(query.getString(VariantQueryParam.REGION.key()));
                    outputFileName = buildOutputFileName(studies, regions);
                }
                outputFile = outdirUri.resolve(outputFileName);
                outputFile = VariantWriterFactory.checkOutput(outputFile, outputFormat);
            } else {
                outputFile = outdirUri;
            }
        } else {
            outputFile = null;
        }

        params.putIfNotNull("query", query);
        params.putIfNotNull("outputFormat", outputFormat);
        params.putIfNotNull("variantsFile", variantsFile);
        params.putIfNotNull("outputFile", outputFile);
        params.putIfNotNull("standardOutput", VariantWriterFactory.isStandardOutput(outputFileStr));
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            try (VariantStorageEngine variantStorageEngine = getVariantStorageEngine(dataStore)) {
                VariantMetadataFactory metadataExporter =
                        new CatalogVariantMetadataFactory(catalogManager, variantStorageEngine.getDBAdaptor(), token);

                URI variantsFileUri = StringUtils.isEmpty(variantsFile) ? null : UriUtils.createUri(variantsFile);
                variantStorageEngine.exportData(outputFile, outputFormat, variantsFileUri, query, new QueryOptions(params), metadataExporter);
            }
        });
    }

    private VariantStorageEngine getVariantStorageEngine(DataStore dataStore) throws StorageEngineException {
        return StorageEngineFactory.get(variantStorageManager.getStorageConfiguration())
                .getVariantStorageEngine(dataStore.getStorageEngine(), dataStore.getDbName());
    }

    private String buildOutputFileName(List<String> studyNames, List<Region> regions) {
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
