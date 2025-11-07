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
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.variant.DeprecatedVariantWalkerParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

@Tool(id = VariantWalkerTool.ID, description = VariantWalkerTool.DESCRIPTION,
        scope = Tool.Scope.PROJECT, resource = Enums.Resource.VARIANT)
public class VariantWalkerTool extends OpenCgaTool {
    public static final String ID = "deprecated-variant-walk";
    public static final String DESCRIPTION = "Filter and walk variants from the variant storage to produce a file";

    @ToolParams
    protected DeprecatedVariantWalkerParams toolParams = new DeprecatedVariantWalkerParams();

    private VariantWriterFactory.VariantOutputFormat format;

    @Override
    protected void check() throws Exception {
        super.check();

        if (StringUtils.isEmpty(toolParams.getInputFormat())) {
            toolParams.setInputFormat(VariantWriterFactory.VariantOutputFormat.VCF.toString());
        }

        format = VariantWriterFactory.toOutputFormat(toolParams.getInputFormat(), toolParams.getOutputFileName());
        if (format.isBinary()) {
            throw new IllegalArgumentException("Binary format not supported for VariantWalkerTool");
        }
        if (!format.isPlain()) {
            format = format.inPlain();
        }

        if (StringUtils.isEmpty(toolParams.getOutputFileName())) {
            toolParams.setOutputFileName("output.txt.gz");
        } else if (!toolParams.getOutputFileName().endsWith(".gz")) {
            toolParams.setOutputFileName(toolParams.getOutputFileName() + ".gz");
        }
    }

    @Override
    protected List<String> getSteps() {
        return Arrays.asList(ID, "move-files");
    }

    @Override
    protected void run() throws Exception {
        step(ID, () -> {
            Path outDir = getOutDir();
            String outputFile = outDir.resolve(toolParams.getOutputFileName()).toString();
            Query query = toolParams.toQuery();
            QueryOptions queryOptions = new QueryOptions().append(QueryOptions.INCLUDE, toolParams.getInclude())
                    .append(QueryOptions.EXCLUDE, toolParams.getExclude());
            variantStorageManager.walkData(outputFile,
                    format, query, queryOptions, toolParams.getDockerImage(), toolParams.getCommandLine(), token);
        });
    }
}
