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

package org.opencb.opencga.core.models.file;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class FileLibraryExperiment {

    @DataField(description = ParamConstants.FILE_LIBRARY_EXPERIMENT_PREPARATION_KIT_DESCRIPTION)
    private String preparationKit;
    @DataField(description = ParamConstants.FILE_LIBRARY_EXPERIMENT_PREPARATION_KIT_MANUFACTURER_DESCRIPTION)
    private String preparationKitManufacturer;
    @DataField(description = ParamConstants.FILE_LIBRARY_EXPERIMENT_CAPTURE_MANUFACTURER_DESCRIPTION)
    private String captureManufacturer;
    @DataField(description = ParamConstants.FILE_LIBRARY_EXPERIMENT_CAPTURE_KIT_DESCRIPTION)
    private String captureKit;
    @DataField(description = ParamConstants.FILE_LIBRARY_EXPERIMENT_CAPTURE_VERSION_DESCRIPTION)
    private String captureVersion;
    @DataField(description = ParamConstants.FILE_LIBRARY_EXPERIMENT_TARGETED_REGION_DESCRIPTION)
    private File targetedRegion;

}
