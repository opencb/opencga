package org.opencb.opencga.analysis.variant.operations;

import org.apache.solr.common.StringUtils;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.models.variant.VariantFileDeleteParams;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;

/**
 * Created on 07/07/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Tool(id = VariantFileDeleteOperationTool.ID, description = VariantFileDeleteOperationTool.DESCRIPTION,
        type = Tool.Type.OPERATION, resource = Enums.Resource.VARIANT)
public class VariantFileDeleteOperationTool extends OperationTool {

    public static final String ID = "variant-file-delete";
    public static final String DESCRIPTION = "Remove variant files from the variant storage";

    private String study;
    private VariantFileDeleteParams variantFileDeleteParams;
    private boolean removeStudy;

    @Override
    protected void check() throws Exception {
        super.check();
        study = getStudyFqn();
        variantFileDeleteParams = VariantFileDeleteParams.fromParams(VariantFileDeleteParams.class, params);

        if (StringUtils.isEmpty(study)) {
            throw new ToolException("Missing study");
        }
        if (CollectionUtils.isEmpty(variantFileDeleteParams.getFile())) {
            throw new ToolException("Missing file/s");
        }
        removeStudy = variantFileDeleteParams.getFile().size() == 1
                && variantFileDeleteParams.getFile().get(0).equalsIgnoreCase(VariantQueryUtils.ALL);

        params.put(VariantStorageOptions.RESUME.key(), variantFileDeleteParams.isResume());
    }

    @Override
    protected void run() throws Exception {

        step(() -> {
            if (removeStudy) {
                variantStorageManager.removeStudy(study, params, token);
            } else {
                variantStorageManager.removeFile(study, variantFileDeleteParams.getFile(), params, token);
            }
        });
    }

}
