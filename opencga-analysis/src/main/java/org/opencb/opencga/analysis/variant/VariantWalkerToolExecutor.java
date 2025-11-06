package org.opencb.opencga.analysis.variant;

import org.apache.solr.common.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.db.api.ExternalToolDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.externalTool.Container;
import org.opencb.opencga.core.models.externalTool.ExternalTool;
import org.opencb.opencga.core.models.externalTool.ExternalToolParams;
import org.opencb.opencga.core.models.externalTool.ExternalToolType;
import org.opencb.opencga.core.models.variant.VariantWalkerParams;
import org.opencb.opencga.core.models.variant.VariantWalkerToolParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;

import java.nio.file.Path;
import java.util.Collections;

@Tool(id = VariantWalkerToolExecutor.ID, description = VariantWalkerToolExecutor.DESCRIPTION,
        scope = Tool.Scope.PROJECT, resource = Enums.Resource.VARIANT)
public class VariantWalkerToolExecutor extends OpenCgaTool {
    public static final String ID = "variant-walk";
    public static final String DESCRIPTION = "Filter and walk variants from the variant storage to produce a file";

    @ToolParams
    protected VariantWalkerToolParams externalToolParams = new VariantWalkerToolParams();
    private VariantWalkerParams toolParams;

    private VariantWriterFactory.VariantOutputFormat format;
    private String dockerImage;

    @Override
    protected void check() throws Exception {
        super.check();
        toolParams = externalToolParams.getParams();

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

        Container container = generateDockerObject(externalToolParams);
        checkDockerObject(container);
    }

    @Override
    public void run() throws ToolException {
        step(VariantWalkerToolExecutor.ID, () -> {
            Path outDir = getOutDir();
            String outputFile = outDir.resolve(toolParams.getOutputFileName()).toString();
            Query query = toolParams.toQuery();
            QueryOptions queryOptions = new QueryOptions()
                    .append(QueryOptions.INCLUDE, toolParams.getInclude())
                    .append(QueryOptions.EXCLUDE, toolParams.getExclude());
            variantStorageManager.walkData(outputFile,
                    format, query, queryOptions, dockerImage, toolParams.getCommandLine(), token);
        });

    }

    private Container generateDockerObject(ExternalToolParams<VariantWalkerParams> runParams) throws CatalogException, ToolException {
        OpenCGAResult<ExternalTool> result;
        if (runParams.getVersion() != null) {
            Query query = new Query(ExternalToolDBAdaptor.QueryParams.VERSION.key(), runParams.getVersion());
            result = catalogManager.getExternalToolManager().get(externalToolParams.getStudy(),
                    Collections.singletonList(runParams.getId()), query, QueryOptions.empty(), false, token);
        } else {
            result = catalogManager.getExternalToolManager().get(externalToolParams.getStudy(), runParams.getId(), QueryOptions.empty(),
                    token);
        }
        if (result.getNumResults() == 0) {
            throw new ToolException("Variant walker tool '" + runParams.getId() + "' not found");
        }
        ExternalTool externalTool = result.first();

        if (externalTool == null) {
            throw new ToolException("Variant walker tool '" + runParams.getId() + "' is null");
        }
        if (externalTool.getType() != ExternalToolType.WALKER) {
            throw new ToolException("User tool '" + runParams.getId() + "' is not of type " + ExternalToolType.WALKER);
        }
        if (externalTool.getContainer() == null) {
            throw new ToolException("User tool '" + runParams.getId() + "' does not have a docker object");
        }

        return externalTool.getContainer();
    }

    private void checkDockerObject(Container container) throws ToolException, CatalogException {
        if (org.apache.commons.lang3.StringUtils.isEmpty(container.getName())) {
            throw new ToolException("Missing docker image name");
        }
        this.dockerImage = container.getName();
        String tag = "";
        if (org.apache.commons.lang3.StringUtils.isNotEmpty(container.getTag())) {
            tag = container.getTag();
            if (org.apache.commons.lang3.StringUtils.isNotEmpty(container.getDigest())) {
                tag += "@" + container.getDigest();
            }
        }
        if (org.apache.commons.lang3.StringUtils.isNotEmpty(tag)) {
            this.dockerImage += ":" + tag;
        }
    }

}
