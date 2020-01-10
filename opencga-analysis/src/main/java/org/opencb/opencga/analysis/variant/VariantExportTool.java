package org.opencb.opencga.analysis.variant;

import org.apache.solr.common.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.core.annotations.Tool;
import org.opencb.opencga.core.api.variant.VariantExportParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;

@Tool(id = VariantExportTool.ID, description = VariantExportTool.DESCRIPTION,
        scope = Tool.Scope.PROJECT, resource = Enums.Resource.VARIANT)
public class VariantExportTool extends OpenCgaTool {
    public static final String ID = "variant-export";
    public static final String DESCRIPTION = "Filter and export variants from the variant storage to a file";

    private VariantExportParams toolParams = new VariantExportParams();
    private VariantWriterFactory.VariantOutputFormat outputFormat;

    public VariantExportTool setQuery(Query query) {
        toolParams.appendQuery(query);
        return this;
    }

    public VariantExportTool setOutputFormat(VariantWriterFactory.VariantOutputFormat outputFormat) {
        toolParams.setOutputFormat(outputFormat.toString());
        return this;
    }

    public VariantExportTool setVariantsFile(String variantsFile) {
        toolParams.setVariantsFile(variantsFile);
        return this;
    }

    public VariantExportTool setOutputFileName(String outputFile) {
        toolParams.setOutputFileName(outputFile);
        return this;
    }

    @Override
    protected void check() throws Exception {
        super.check();
        toolParams.updateParams(params);

        if (StringUtils.isEmpty(toolParams.getOutputFormat())) {
            toolParams.setOutputFormat(VariantWriterFactory.VariantOutputFormat.VCF.toString());
        }
        if (toolParams.isCompress()) {
            if (!toolParams.getOutputFormat().toLowerCase().endsWith(".gz")) {
                toolParams.setOutputFormat(toolParams.getOutputFormat() + ".GZ");
            }
        }

        outputFormat = VariantWriterFactory.toOutputFormat(toolParams.getOutputFormat(), toolParams.getOutputFileName());
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            String outputFile = StringUtils.isEmpty(toolParams.getOutputFileName())
                    ? getOutDir().toString()
                    : getOutDir().resolve(toolParams.getOutputFileName()).toString();
            Query query = toolParams.toQuery();
            QueryOptions queryOptions = new QueryOptions(params);
            for (VariantQueryParam param : VariantQueryParam.values()) {
                queryOptions.remove(param.key());
            }
            variantStorageManager.exportData(outputFile,
                    outputFormat,
                    toolParams.getVariantsFile(), query, queryOptions, token);
        });
    }
}
