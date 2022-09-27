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

package org.opencb.opencga.analysis.variant;

import org.apache.solr.common.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.io.IOManager;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.models.variant.VariantExportParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;

import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Tool(id = VariantExportTool.ID, description = VariantExportTool.DESCRIPTION,
        scope = Tool.Scope.PROJECT, resource = Enums.Resource.VARIANT)
public class VariantExportTool extends OpenCgaTool {
    public static final String ID = "variant-export";
    public static final String DESCRIPTION = "Filter and export variants from the variant storage to a file";

    @ToolParams
    protected VariantExportParams toolParams = new VariantExportParams();

    private VariantWriterFactory.VariantOutputFormat outputFormat;

    @Override
    protected void check() throws Exception {
        super.check();

        if (StringUtils.isEmpty(toolParams.getOutputFileFormat())) {
            toolParams.setOutputFileFormat(VariantWriterFactory.VariantOutputFormat.VCF.toString());
        }
        if (toolParams.getLimit() != null && toolParams.getLimit() == 0) {
            toolParams.setLimit(null);
        }

        outputFormat = VariantWriterFactory.toOutputFormat(toolParams.getOutputFileFormat(), toolParams.getOutputFileName());
        if (outputFormat.isPlain()) {
            outputFormat = outputFormat.withGzip();
        }
    }

    @Override
    protected List<String> getSteps() {
        return Arrays.asList(ID, "move-files");
    }

    @Override
    protected void run() throws Exception {
        List<URI> uris = new ArrayList<>(2);
        step(ID, () -> {
            Path outDir = getScratchDir();
            String outputFile = StringUtils.isEmpty(toolParams.getOutputFileName())
                    ? outDir.toString()
                    : outDir.resolve(toolParams.getOutputFileName()).toString();
            Query query = toolParams.toQuery();
            QueryOptions queryOptions = new QueryOptions(params);
            for (VariantQueryParam param : VariantQueryParam.values()) {
                queryOptions.remove(param.key());
            }
            uris.addAll(variantStorageManager.exportData(outputFile,
                    outputFormat,
                    toolParams.getVariantsFile(), query, queryOptions, token));
        });
        step("move-files", () -> {
            IOManager ioManager = catalogManager.getIoManagerFactory().get(uris.get(0));
            for (URI uri : uris) {
                String fileName = UriUtils.fileName(uri);
                ioManager.move(uri, getOutDir().resolve(fileName).toUri());
            }
        });
    }
}
