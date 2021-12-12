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

package org.opencb.opencga.analysis.variant.operations;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.solr.common.StringUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.variant.VariantSampleDeleteParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;

import java.net.URI;

/**
 * Created on 07/07/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Tool(id = VariantSampleDeleteOperationTool.ID, description = VariantSampleDeleteOperationTool.DESCRIPTION,
        type = Tool.Type.OPERATION, resource = Enums.Resource.VARIANT)
public class VariantSampleDeleteOperationTool extends OperationTool {

    public static final String ID = "variant-sample-delete";
    public static final String DESCRIPTION = "Remove variant samples from the variant storage";

    private String study;

    @ToolParams
    protected VariantSampleDeleteParams variantSampleDeleteParams;
    private boolean removeStudy;

    @Override
    protected void check() throws Exception {
        super.check();
        study = getStudyFqn();

        if (StringUtils.isEmpty(study)) {
            throw new ToolException("Missing study");
        }
        if (CollectionUtils.isEmpty(variantSampleDeleteParams.getSample())) {
            throw new ToolException("Missing sample/s");
        }
        removeStudy = variantSampleDeleteParams.getSample().size() == 1
                && variantSampleDeleteParams.getSample().get(0).equalsIgnoreCase(VariantQueryUtils.ALL);

        params.put(VariantStorageOptions.RESUME.key(), variantSampleDeleteParams.isResume());
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            URI outdir = getOutDir(keepIntermediateFiles).toUri();
            if (removeStudy) {
                variantStorageManager.removeStudy(study, params, outdir, token);
            } else {
                variantStorageManager.removeSample(study, variantSampleDeleteParams.getSample(), params, outdir, token);
            }
        });
    }

}
