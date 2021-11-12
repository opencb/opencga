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

import org.apache.solr.common.StringUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.variant.VariantStudyDeleteParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;

import java.net.URI;

/**
 * Created on 07/07/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Tool(id = VariantStudyDeleteOperationTool.ID, description = VariantStudyDeleteOperationTool.DESCRIPTION,
        type = Tool.Type.OPERATION, resource = Enums.Resource.VARIANT)
public class VariantStudyDeleteOperationTool extends OperationTool {

    public static final String ID = "variant-study-delete";
    public static final String DESCRIPTION = "Remove whole study from the variant storage";

    private String study;
    @ToolParams
    protected VariantStudyDeleteParams toolParams;

    @Override
    protected void check() throws Exception {
        super.check();
        study = getStudyFqn();

        if (StringUtils.isEmpty(study)) {
            throw new ToolException("Missing study");
        }
        params.put(VariantStorageOptions.RESUME.key(), toolParams.isResume());
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            URI outdir = getOutDir(keepIntermediateFiles).toUri();
            variantStorageManager.removeStudy(study, params, outdir, token);
        });
    }

}
