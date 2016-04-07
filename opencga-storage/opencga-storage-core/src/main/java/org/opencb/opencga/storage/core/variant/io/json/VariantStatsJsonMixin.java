/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.storage.core.variant.io.json;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.opencb.biodata.models.variant.stats.VariantHardyWeinbergStats;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public abstract class VariantStatsJsonMixin {

    @JsonIgnore
    public abstract Object getImpl();

    @JsonIgnore
    public abstract String[] getAltAlleles();

    @JsonIgnore
    public abstract boolean isIndel();

    @JsonIgnore
    public abstract boolean isSNP();

    @JsonIgnore
    public abstract String getId();

    @JsonIgnore
    public abstract VariantHardyWeinbergStats getHw();

    @JsonIgnore
    public abstract boolean isTransition();

    @JsonIgnore
    public abstract boolean isTransversion();
}
