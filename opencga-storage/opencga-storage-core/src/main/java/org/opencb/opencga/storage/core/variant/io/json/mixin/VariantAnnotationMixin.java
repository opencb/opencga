/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.storage.core.variant.io.json.mixin;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.opencb.biodata.models.variant.avro.GeneCancerAssociation;

import java.util.List;

/**
 * Created by jacobo on 2/02/15.
 */

@JsonIgnoreProperties({"proteinSubstitutionScores", "variantTraitAssociation"})
public abstract class VariantAnnotationMixin {

    @JsonAlias("cancerGeneAssociations")
    public abstract List<GeneCancerAssociation> getGeneCancerAssociations();

}
