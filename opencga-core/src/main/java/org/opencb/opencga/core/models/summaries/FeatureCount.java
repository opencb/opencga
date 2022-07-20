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

package org.opencb.opencga.core.models.summaries;

/**
 * Created by pfurio on 12/08/16.
 */
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class FeatureCount {
    @DataField(description = ParamConstants.FEATURE_COUNT_NAME_DESCRIPTION)
    private Object name;
    @DataField(description = ParamConstants.FEATURE_COUNT_COUNT_DESCRIPTION)
    private long count;

    public FeatureCount(Object name, long count) {
        this.name = name;
        this.count = count;
    }

    public Object getName() {
        return name;
    }

    public FeatureCount setName(Object name) {
        this.name = name;
        return this;
    }

    public long getCount() {
        return count;
    }

    public FeatureCount setCount(long count) {
        this.count = count;
        return this;
    }
}
